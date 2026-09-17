#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""(b) 口径标注回归：报告必须**无条件**声明 `daily_return` 的收窄口径。

背景（2026-09-17 裁定 B）：
  `daily_return` 已从「全持仓 total_value/prev_value」改成
  「当日价新鲜的共同篮子」，而 `total_value` 仍是**全持仓**。
  ⇒ 两者口径不同。读者拿 `1,527,929 × daily_return` 会算出错金额。
  ⇒ 报告若不加标注，就是**又一次静默换口径**（与 09-15 跨日期拼接同类）。

本文件锁死三条硬约束：
  (1) 声明是**无条件**的 —— 不由任何"有没有运行期数据"决定；缺数据也要印
      「覆盖度未记录」，**不许静默省略**（离线"只读库 + 独立重渲染"路径即如此）；
  (2) 有覆盖度时数字必须**真的印出来**（判别力：删掉插入点本条必红）；
  (3) `_fmt_dr_coverage` 只许拼**可核字段**，不许写推断句（因果未知 ↓）。

已知限制（非本测试可解，已登记 backlog）：
  `daily_return_coverage` 是运行期键，`save_portfolio_summary` 用显式
  `summary.get(...)` 取 15 个字段 ⇒ 不落库 ⇒ 邮件路径（另起进程 +
  `_load_summary()` 只读 portfolio_summary 表）**永远**印「覆盖度未记录」。
  数字只在同进程传 summary 时可见。要让邮件也带上数字，须改持久化通道 ——
  属于写库侧改动，本轮不做。

本测试全程 tmp_path 现造 SQLite：不触碰生产库、不发邮件。
"""
from __future__ import annotations

import sqlite3
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.enhanced_report import (  # noqa: E402
    EnhancedReportBuilder,
    _fmt_dr_coverage,
)

TODAY = date.today().isoformat()
TODAY_CN = datetime.strptime(TODAY, "%Y-%m-%d").strftime("%Y年%m月%d日")


# ============================================================================
# 造库：结构与生产库一致（只保留本报告用到的列）
# ============================================================================

_SCHEMA = (
    """CREATE TABLE portfolio_summary (
           date TEXT PRIMARY KEY, total_value REAL, total_cost REAL, total_pnl REAL,
           daily_pnl REAL, daily_return REAL, vs_hs300 REAL, profit_count INTEGER,
           loss_count INTEGER, sharpe_ratio REAL, max_drawdown REAL, volatility REAL,
           snapshot_type TEXT DEFAULT 'daily')""",
    """CREATE TABLE portfolio_snapshots (
           id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT NOT NULL, code TEXT NOT NULL,
           name TEXT, quantity REAL, cost_price REAL, current_price REAL,
           market_value REAL, pnl REAL, pnl_rate REAL, ytd_return REAL, beta REAL,
           UNIQUE(date, code))""",
    """CREATE TABLE alerts (
           id INTEGER PRIMARY KEY AUTOINCREMENT, rule_name TEXT, level TEXT, message TEXT,
           created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, acknowledged BOOLEAN DEFAULT 0)""",
    """CREATE TABLE index_quotes (
           date TEXT, code TEXT, name TEXT, close REAL, change_pct REAL)""",
    """CREATE TABLE etf_technical (
           date TEXT, code TEXT, ma_signal TEXT, macd_signal TEXT, rsi_value REAL,
           rsi_status TEXT, kdj_signal TEXT, bollinger_position REAL, atr_pct REAL,
           trend TEXT)""",
    """CREATE TABLE etf_features (
           date TEXT, code TEXT, vol_20d REAL, vol_60d REAL)""",
    """CREATE TABLE etf_price_history (
           date TEXT, code TEXT, close REAL)""",
)


def _build_db(root: Path) -> Path:
    db_path = root / "data" / "database" / "portfolio.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    (root / "data" / "reports").mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    for ddl in _SCHEMA:
        conn.execute(ddl)
    conn.commit()
    conn.close()
    return db_path


def _add_summary(db_path: Path, d: str, total_value=1000000.0,
                 daily_return=-0.5, daily_pnl=-1000.0) -> None:
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "INSERT OR REPLACE INTO portfolio_summary "
        "(date, total_value, total_cost, total_pnl, daily_pnl, daily_return, "
        " profit_count, loss_count, sharpe_ratio, max_drawdown, volatility) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (d, total_value, 900000.0, 100000.0, daily_pnl, daily_return,
         3, 2, 0.8, 6.7, 22.0),
    )
    conn.commit()
    conn.close()


def _add_snapshot(db_path: Path, d: str, code: str, name: str, mv=100000.0) -> None:
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "INSERT OR REPLACE INTO portfolio_snapshots "
        "(date, code, name, quantity, cost_price, current_price, market_value, pnl, pnl_rate) "
        "VALUES (?,?,?,?,?,?,?,?,?)",
        (d, code, name, 10000.0, 1.0, 10.0, mv, 500.0, 1.5),
    )
    conn.commit()
    conn.close()


@pytest.fixture
def report_env(tmp_path):
    """summary 与 snapshots 同为今日 —— 一个能正常渲染的报告环境。"""
    db = _build_db(tmp_path)
    _add_summary(db, TODAY)
    _add_snapshot(db, TODAY, "CCCCCC", "今日持仓", mv=1000000.0)
    return tmp_path, db


def _render(db_path, coverage=None):
    """渲染报告；coverage 非 None 时经 `_load_summary` 注入运行期键。

    **不重算覆盖度**：只把调用方给的 dict 塞进去。重算会得到与运行时
    不同定义的数，那是把「看起来像同一个量」的假数字印进报告。
    """
    if coverage is not None:
        orig = EnhancedReportBuilder._load_summary

        def _patched(self):
            row = orig(self)
            if row:
                row["daily_return_coverage"] = coverage
            return row

        EnhancedReportBuilder._load_summary = _patched
        try:
            return EnhancedReportBuilder(str(db_path), theme="light").build_full_report()
        finally:
            EnhancedReportBuilder._load_summary = orig
    return EnhancedReportBuilder(str(db_path), theme="light").build_full_report()


_REAL_COV = {
    "computed": True,
    "freshness_source": "realtime_quote",
    "included_n": 22,
    "total_n": 34,
    "comparable_n": 34,
    "prev_value_included": 933195.10,
    "prev_value_comparable": 1527928.85,
    "value_share": 0.6108,
    "excluded_stale": ["161725", "160222"],
    "caliber": "fresh_price_basket",
}


# ============================================================================
# 1. 单元：_fmt_dr_coverage 只拼可核字段
# ============================================================================

class TestFmtDrCoverage:
    def test_missing_coverage_says_unrecorded(self):
        """读不到覆盖度必须**显式**说「覆盖度未记录」，不许返回空串/None。"""
        for bad in (None, {}, "x", 0, [], {"unrelated": 1}):
            out = _fmt_dr_coverage(bad)
            assert out == "覆盖度未记录", f"{bad!r} -> {out!r}"
            assert out, "不得返回空串（空串在卡片里等于静默省略）"

    def test_renders_included_and_share(self):
        out = _fmt_dr_coverage(_REAL_COV)
        assert "22/34" in out, out
        assert "61.08%" in out, out

    def test_share_is_recomputed_not_hardcoded(self):
        """换一个 share，印出的百分比必须跟着变（防硬编码）。"""
        cov = dict(_REAL_COV, value_share=0.1234)
        assert "12.34%" in _fmt_dr_coverage(cov)
        assert "61.08%" not in _fmt_dr_coverage(cov)

    def test_partial_coverage_does_not_crash(self):
        """字段缺失时只印能印的那部分，不抛异常、不印 'None'。"""
        out = _fmt_dr_coverage({"included_n": 22, "comparable_n": 34})
        assert "22/34" in out
        assert "None" not in out
        out2 = _fmt_dr_coverage({"value_share": 0.5})
        assert "50.00%" in out2
        assert "None" not in out2

    def test_fallback_caliber_is_declared(self):
        """兜底口径（全持仓）必须在**同一句话里**标出来。"""
        cov = dict(_REAL_COV, caliber="total_value_fallback")
        out = _fmt_dr_coverage(cov)
        assert "口径回退" in out, out
        assert "total_value" in out, out

    def test_no_causal_inference(self):
        """只许放可逐项核对的代码事实；涉及未知定义的因果一律不写。

        归因错比行号错更危险 —— 报告里一句错的因果，读者会当成结论。
        """
        out = _fmt_dr_coverage(_REAL_COV)
        for banned in ("因此", "所以", "不可比", "说明", "意味着", "导致"):
            assert banned not in out, f"出现推断用词 {banned!r}: {out}"


# ============================================================================
# 2. 集成：报告正文里的口径声明
# ============================================================================

class TestCaliberNoteInReport:
    def test_declaration_is_unconditional(self, report_env):
        """即使拿不到覆盖度，收窄口径 + 「不可相乘」也必须照印。"""
        _, db = report_env
        html = _render(db)

        assert html != "暂无足够数据生成报告", "夹具没渲染出报告，测试无效"
        assert "不可相乘" in html, "缺「不可相乘」⇒ 读者会拿 total_value × daily_return"
        assert "当日有行情价的标的" in html, "缺收窄口径声明 ⇒ 又是一次静默换口径"
        assert "两者口径不同" in html
        # ① 不许静默省略：拿不到数字就要说拿不到
        assert "覆盖度未记录" in html

    def test_unconditional_means_no_hardcoded_number(self, report_env):
        """反证：没有覆盖度时，报告里**不得**凭空出现任何覆盖率数字。"""
        _, db = report_env
        html = _render(db)
        assert "61.08%" not in html, "无运行期数据却印出覆盖率 ⇒ 硬编码/伪造"
        assert "22/34" not in html

    def test_numbers_appear_when_coverage_available(self, report_env):
        """有覆盖度时数字必须真的落进 HTML（判别力：删插入点本条必红）。"""
        _, db = report_env
        html = _render(db, coverage=_REAL_COV)

        assert "22/34" in html, "有覆盖度却没印纳入只数"
        assert "61.08%" in html, "有覆盖度却没印市值占比"
        assert "不可相乘" in html
        # 总市值口径写在旁边（34 只来自 total_n，不是写死的）
        assert "全部持仓 34 只" in html

    def test_total_scope_follows_total_n(self, report_env):
        """`total_n` 变化时「全部持仓 N 只」跟着变（防写死 34）。"""
        _, db = report_env
        html = _render(db, coverage=dict(_REAL_COV, total_n=35))
        assert "全部持仓 35 只" in html
        assert "全部持仓 34 只" not in html

    def test_fallback_caliber_visible_in_report(self, report_env):
        """走了兜底分支时，读者必须能从报告里看出这次口径不同。"""
        _, db = report_env
        html = _render(db, coverage=dict(_REAL_COV, caliber="total_value_fallback"))
        assert "口径回退" in html
        assert "total_value" in html

    def test_note_sits_next_to_daily_pnl_card(self, report_env):
        """标注必须与「当日盈亏」卡片同区，不能落到页面别处。"""
        _, db = report_env
        html = _render(db, coverage=_REAL_COV)
        i_card = html.find("当日盈亏")
        i_note = html.find("不可相乘")
        assert i_card != -1 and i_note != -1
        assert 0 < i_note - i_card < 4000, (
            f"标注距卡片 {i_note - i_card} 字符，疑似跑到别的区块"
        )
