#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""P1#11 采集侧回归用例：盘面三表 + 快照标记（2026-10-27 第二轮）。

背景
----
P1#11 上半场只覆盖 `portfolio_snapshots / portfolio_summary / index_quotes /
etf_technical` 四张主表（`PortfolioAnalyzer._compute_persist_enabled`，运行级判定）。
盘面类三张表由**四个互不相干的写入点**写，且日期来源各不相同，全部漏网。

实测污染（2026-10-01~10-07 国庆，6 个非交易日）：

  - `fund_flows`：每天 90 行，与 09-30 的 90 行**逐字相同**（含 net_inflow 数值）
  - `sector_daily_change`：10-01 / 10-07 的 13 行 MIN/MAX 与 09-30 完全一致（−2.99 / 4.18）
  - `market_breadth`：同期同样有行

这不是「多写了几行空行」，而是把前一交易日的资金流向**伪装成新交易日数据**。

本文件钉住五件事
----------------
  (A) 守卫三态语义：交易日 allow / 非交易日 deny / 缺表年 degraded-allow（判不了 ≠ 判过了）
  (B) 🔴 判据按「被写入的日期」而非「运行日」——历史回填必须不被误杀
  (C) `market_breadth` 休市日**不发起网络请求**（不是取回来再丢）
  (D) `save_portfolio_snapshot` 同事务写完成标记，三态 complete/partial/empty
  (E) 哨兵标记交叉核对：orphan（有行无标）与 marker_mismatch（行数≠标记），
      且历史日期（标记启用日之前）**不得误报**
"""
import os
import re
import sqlite3
import sys

import pandas as pd
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from src.utils.holiday_write_guard import (  # noqa: E402
    ALLOW, ALLOW_DEGRADED, DENY, check_write_date, filter_rows_by_trading_date,
)


# ---------------------------------------------------------------------------
# (A)(B) 守卫三态 + 按写入日期判定
# ---------------------------------------------------------------------------
class TestGuardSemantics:
    @pytest.mark.parametrize("d,expect_allow,name", [
        ("2026-10-01", False, "国庆首日"),
        ("2026-10-07", False, "国庆末日"),
        ("2026-10-08", True, "国庆后开市"),
        ("2026-02-18", False, "春节"),
        ("2026-02-24", True, "春节后开市"),
        ("2026-10-25", False, "周日"),
        ("20261001", False, "YYYYMMDD 格式（market_breadth 实际格式）"),
        ("20261027", True, "YYYYMMDD 格式-交易日"),
    ])
    def test_holiday_and_trading_day(self, d, expect_allow, name):
        allow, status, reason = check_write_date(d, table="test")
        assert allow is expect_allow, f"{name}: {reason}"
        assert status in (ALLOW, DENY)

    def test_date_with_time_suffix(self):
        """带时间尾巴的日期必须能解析为当天，不能退化成 unparsed 放行。"""
        allow, status, _ = check_write_date("2026-10-05 15:30:00", table="test")
        assert allow is False
        assert status == DENY

    def test_uncovered_year_is_degraded_not_deny(self):
        """2023 无官方休市表 ⇒ 判不了，按旧行为放行，但状态必须与「判过了」区分。

        🔴 反证：若这里返回 DENY，等于用「没有能力判断」冒充「判断结果是非交易日」，
        会让历史回填在 2023 年整段静默断流。
        """
        allow, status, reason = check_write_date("2023-10-02", table="test")
        assert allow is True
        assert status == ALLOW_DEGRADED
        assert "缺" in reason or "无官方休市表" in reason

    def test_force_env_escape_hatch(self, monkeypatch):
        monkeypatch.setenv("PT_FORCE_PERSIST", "1")
        allow, status, _ = check_write_date("2026-10-01", table="test")
        assert allow is True
        assert status == "allow_forced"

    def test_filter_keeps_trading_drops_holiday(self):
        rows = [("2026-09-30", "a"), ("2026-10-01", "a"),
                ("2026-10-07", "a"), ("2026-10-08", "a")]
        kept, dropped, summary = filter_rows_by_trading_date(
            rows, date_getter=lambda r: r[0], table="test")
        assert kept == [("2026-09-30", "a"), ("2026-10-08", "a")]
        assert dropped == {"2026-10-01": 1, "2026-10-07": 1}
        assert "2026-10-01" in summary and "2026-10-07" in summary

    def test_filter_no_drop_returns_empty_summary(self):
        kept, dropped, summary = filter_rows_by_trading_date(
            [("2026-10-08", "a")], date_getter=lambda r: r[0], table="test")
        assert len(kept) == 1 and dropped == {} and summary == ""


# ---------------------------------------------------------------------------
# (B) fund_flows：按行内日期过滤，历史回填不被误杀
# ---------------------------------------------------------------------------
def _mk_fund_df(date_str, code="BK0478", net=1.0e8):
    return pd.DataFrame([{
        "date": date_str, "code": code, "name": "军工", "category": "sector",
        "net_inflow": net, "buy_amount": net * 2, "sell_amount": net,
    }])


@pytest.fixture()
def tmp_db(tmp_path):
    from src.utils.database import DatabaseManager
    p = str(tmp_path / "t.db")
    DatabaseManager(p)
    return p


def _count(db, sql, args=()):
    con = sqlite3.connect(db)
    try:
        return con.execute(sql, args).fetchone()[0]
    finally:
        con.close()


class TestFundFlowsGuard:
    def test_mixed_batch_writes_only_trading_day(self, tmp_db):
        from src.data_sources.fund_flow import save_fund_flows
        con = sqlite3.connect(tmp_db)
        n = save_fund_flows(con, _mk_fund_df("2026-10-01"))
        con.close()
        assert n == 0, "休市日行必须被拒绝"
        assert _count(tmp_db, "SELECT COUNT(*) FROM fund_flows WHERE date='2026-10-01'") == 0

        con = sqlite3.connect(tmp_db)
        n = save_fund_flows(con, _mk_fund_df("2026-09-30"))
        con.close()
        assert n == 1
        assert _count(tmp_db, "SELECT COUNT(*) FROM fund_flows WHERE date='2026-09-30'") == 1

    def test_historical_backfill_not_killed(self, tmp_db):
        """🔴 核心反证：判据若写成「运行日非交易日就全拦」，历史回填会被整批误杀。

        本函数按**行内 date** 判定，08 月的历史交易日必须照常写入。
        """
        from src.data_sources.fund_flow import save_fund_flows
        con = sqlite3.connect(tmp_db)
        n = save_fund_flows(con, _mk_fund_df("2026-08-14"))
        con.close()
        assert n == 1
        assert _count(tmp_db, "SELECT COUNT(*) FROM fund_flows WHERE date='2026-08-14'") == 1


# ---------------------------------------------------------------------------
# (C) market_breadth：休市日不发起网络请求
# ---------------------------------------------------------------------------
class TestMarketBreadthGuard:
    def test_holiday_early_return_without_network(self, tmp_db, monkeypatch):
        import src.data_sources.market_breadth as mb
        calls = {"zt": 0, "dt": 0}
        monkeypatch.setattr(mb, "fetch_zt_pool", lambda d: calls.__setitem__("zt", calls["zt"] + 1) or pd.DataFrame())
        monkeypatch.setattr(mb, "fetch_dtgc_pool", lambda d: calls.__setitem__("dt", calls["dt"] + 1) or pd.DataFrame())

        con = sqlite3.connect(tmp_db)
        r = mb.collect_market_breadth(con, "20261001")
        con.close()

        assert r["skipped"] is True
        assert r["skip_reason"]
        assert calls == {"zt": 0, "dt": 0}, "休市日不应发起涨停/跌停池请求"
        assert _count(tmp_db, "SELECT COUNT(*) FROM market_breadth WHERE date='20261001'") == 0

    def test_trading_day_not_skipped(self, tmp_db, monkeypatch):
        import src.data_sources.market_breadth as mb
        monkeypatch.setattr(mb, "fetch_zt_pool", lambda d: pd.DataFrame())
        monkeypatch.setattr(mb, "fetch_dtgc_pool", lambda d: pd.DataFrame())
        con = sqlite3.connect(tmp_db)
        r = mb.collect_market_breadth(con, "20261027")
        con.close()
        assert r["skipped"] is False
        assert _count(tmp_db, "SELECT COUNT(*) FROM market_breadth WHERE date='20261027'") == 1


# ---------------------------------------------------------------------------
# (D) 快照完成标记
# ---------------------------------------------------------------------------
def _holdings(n=3, drop_code_at=None):
    out = []
    for i in range(n):
        h = {"code": "C%d" % i, "name": "标的%d" % i, "quantity": 100,
             "cost_price": 1.0, "current_price": 1.1,
             "market_value": 110.0, "pnl": 10.0}
        if drop_code_at == i:
            h = {"name": "无码货"}
        out.append(h)
    return out


class TestSnapshotMarker:
    def test_complete(self, tmp_db):
        from src.utils.database import DatabaseManager
        DatabaseManager(tmp_db).save_portfolio_snapshot("2026-10-28", _holdings(3))
        row = sqlite3.connect(tmp_db).execute(
            "SELECT rows_written, rows_expected, skipped, status FROM ingest_markers "
            "WHERE table_name='portfolio_snapshots' AND date='2026-10-28'").fetchone()
        assert row == (3, 3, 0, "complete")

    def test_partial_on_missing_code(self, tmp_db):
        from src.utils.database import DatabaseManager
        DatabaseManager(tmp_db).save_portfolio_snapshot("2026-10-28", _holdings(3, drop_code_at=2))
        row = sqlite3.connect(tmp_db).execute(
            "SELECT rows_written, rows_expected, skipped, status, detail FROM ingest_markers "
            "WHERE table_name='portfolio_snapshots' AND date='2026-10-28'").fetchone()
        assert row[:4] == (2, 3, 1, "partial")
        assert "无码货" in (row[4] or "")

    def test_empty_still_marks(self, tmp_db):
        """空写入也必须留标记：否则「写入方跑过但产出 0 行」与「写入方没跑」同形。"""
        from src.utils.database import DatabaseManager
        DatabaseManager(tmp_db).save_portfolio_snapshot("2026-10-28", [])
        row = sqlite3.connect(tmp_db).execute(
            "SELECT rows_written, rows_expected, status FROM ingest_markers "
            "WHERE table_name='portfolio_snapshots' AND date='2026-10-28'").fetchone()
        assert row == (0, 0, "empty")


# ---------------------------------------------------------------------------
# (E) 哨兵标记交叉核对
# ---------------------------------------------------------------------------
@pytest.fixture()
def sentinel():
    sys.path.insert(0, os.path.join(ROOT, "scripts"))
    import run_sentinel
    return run_sentinel


class TestSentinelMarkerCrossCheck:
    def _seed(self, db, date, n=3):
        con = sqlite3.connect(db)
        for i in range(n):
            con.execute(
                "INSERT OR REPLACE INTO portfolio_snapshots "
                "(date, code, name, quantity, cost_price, current_price, market_value, pnl) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (date, "C%d" % i, "标的%d" % i, 100, 1.0, 1.1, 110.0, 10.0))
        con.commit()
        con.close()

    def _mark(self, db, date, written, status="complete"):
        con = sqlite3.connect(db)
        con.execute(
            "INSERT OR REPLACE INTO ingest_markers "
            "(table_name, date, rows_written, rows_expected, skipped, status, detail) "
            "VALUES ('portfolio_snapshots', ?, ?, ?, 0, ?, '')",
            (date, written, written, status))
        con.commit()
        con.close()

    def test_rows_with_marker_pass(self, tmp_db, sentinel):
        self._seed(tmp_db, "2026-10-28")
        self._mark(tmp_db, "2026-10-28", 3)
        p = sentinel._snapshot_probe("2026-10-28", tmp_db)
        assert p["marker_flag"] is None
        assert p["marker"]["status"] == "complete"

    def test_rows_without_marker_is_orphan(self, tmp_db, sentinel):
        self._seed(tmp_db, "2026-10-28")
        p = sentinel._snapshot_probe("2026-10-28", tmp_db)
        assert p["marker_flag"] == "orphan"

    def test_historical_date_without_marker_not_flagged(self, tmp_db, sentinel):
        """🔴 反面守卫：标记启用日（2026-10-27）之前的历史日期永远无标记，
        若也判 orphan，每次回溯历史都刷假告警 ⇒ 告警疲劳 ⇒ 信号失效。"""
        self._seed(tmp_db, "2026-09-30")
        p = sentinel._snapshot_probe("2026-09-30", tmp_db)
        assert p["marker_flag"] is None

    def test_row_count_mismatch(self, tmp_db, sentinel):
        self._seed(tmp_db, "2026-10-28", n=3)
        self._mark(tmp_db, "2026-10-28", 99)
        p = sentinel._snapshot_probe("2026-10-28", tmp_db)
        assert p["marker_flag"] == "marker_mismatch"
        assert p["rows"] == 3


# ---------------------------------------------------------------------------
# 源码守卫：三张盘面表的写入点必须接守卫
# ---------------------------------------------------------------------------
class TestSourceGuards:
    """钉住「写入点不得绕过守卫」。

    与 test_persist_gate_holiday 的 AST 守卫同思路：修一处漏一处是本类缺陷的
    标准形态（三张表四个写入点分散在 4 个文件），靠结构断言而非人工记忆。
    """

    CASES = [
        ("src/data_sources/fund_flow.py", "check_write_date"),
        ("src/data_sources/market_breadth.py", "check_write_date"),
        ("scripts/gen_combo_report.py", "check_write_date"),
        ("scripts/backfill_sector_change.py", "filter_rows_by_trading_date"),
    ]

    @pytest.mark.parametrize("rel,needle", CASES)
    def test_writer_references_guard(self, rel, needle):
        path = os.path.join(ROOT, rel)
        src = open(path, encoding="utf-8").read()
        assert needle in src, f"{rel} 未引用 {needle}（写入点绕过了休市守卫）"

    def test_backfill_calls_filter_before_insert(self):
        path = os.path.join(ROOT, "scripts/backfill_sector_change.py")
        src = open(path, encoding="utf-8").read()
        i_filter = src.find("filter_rows_by_trading_date(")
        # 定位真正的写入语句（排除 import 行与函数定义行）
        i_insert = src.find("INSERT OR REPLACE INTO sector_daily_change")
        assert i_filter != -1 and i_insert != -1
        assert i_filter < i_insert, "过滤必须发生在写入之前"

    def test_market_breadth_guard_precedes_fetch(self):
        path = os.path.join(ROOT, "src/data_sources/market_breadth.py")
        src = open(path, encoding="utf-8").read()
        m = re.search(r"def collect_market_breadth\(.*?\n(.*?)\n    zt = fetch_zt_pool",
                      src, re.S)
        assert m, "collect_market_breadth 结构变化，请同步本守卫"
        assert "check_write_date" in m.group(1), "守卫必须在 fetch 之前（否则白取一遍再丢）"
