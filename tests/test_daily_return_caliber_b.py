#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""口径 B 回归用例：`daily_return` = 「当日价新鲜标的」口径（2026-09-17 裁定）。

背景
----
`daily_return` 的分子/分母此前是「当日持仓 ∩ 前日快照」（`common_codes`）。
但场外基金 T+1 披露，15:30 时当日净值根本不存在，其「当日价」实际取自更早一天
（`_merge_positions` 的前向填充）。把它算进来 = 把一段**多日**收益压成
**一个交易日**的收益 —— 09-16 那根 +15~18% 就是这么来的。

裁定 B：**D 日价格非当日的标的，不进分子也不进分母**；覆盖度必须随值一起报出，
且 `daily_return`（22 只口径）与 `total_value`（全持仓口径）**不可相乘**。

本文件钉住的五件事
------------------
  (A) `_merge_positions` 必须把「价格当日可得」的 code 与前向填充的 code **分开报出**；
  (B) `daily_return` 只用新鲜篮子算；覆盖率/被排除清单必须一并返回；
  (C) 🔴 **反证**：真实平价日（合法值 `0.0`）**不得**触发全持仓兜底分支 ——
      旧实现用 `daily_return == 0` 当「未计算」哨兵，会把平价日静默换回全持仓口径；
  (D) 真的算不出新鲜篮子时才兜底，且**必须落 error 级告警**（不许静默换口径）；
  (E) `merge_info` 缺失时必须按「实时行情集合 → 持仓行日期」顺序退化，并**留痕来源**。

全程只在 tmp_path 造库；不触碰任何真实库。
"""
from __future__ import annotations

import datetime as dt
import sqlite3
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# 2026-09-16 实况标的域：场内 22 + 场外 12 = 34
ETF_CODES = ["159220", "159267", "159300", "159650", "159770", "159796",
             "159819", "159949", "159992", "510300", "510500", "511380",
             "511520", "512010", "512100", "512810", "515010", "515120",
             "516160", "561910", "563020", "588000"]
OTC_CODES = ["001194", "001323", "001407", "001437", "001765", "002152",
             "007994", "008269", "100032", "166301", "519770", "880013"]
BASE_CODES = ETF_CODES + OTC_CODES

TARGET_THU = "2026-09-17"      # 目标日（周四）
PREV_WED = "2026-09-16"        # 前一交易日（周三）


class _DbStub:
    """只提供 `.db_path`（`_merge_positions` / `_calculate_summary` 只用它连库）。"""

    def __init__(self, path: Path):
        self.db_path = str(path)


def _analyzer(db: Path, today: str):
    from src.analysis.portfolio import PortfolioAnalyzer

    a = PortfolioAnalyzer.__new__(PortfolioAnalyzer)   # 不跑 __init__（会连数据源）
    a.db = _DbStub(db)
    a.today = today
    return a


def _make_db(tmp_path: Path, with_summary: bool = True,
             prev_total_value: float = 3400.0,
             prev_mv_overrides=None) -> Path:
    """造库：09-16 前若干工作日各 34 只；09-16 一行 34 只（qty 100 / mv 100）。

    `_calculate_summary` 读 `portfolio_summary` 取「前日 total_value」，故该表必须存在，
    否则异常被 `except (sqlite3.OperationalError, KeyError)` 吃掉 ⇒ 静默走兜底分支。
    """
    db = tmp_path / "data" / "database" / "portfolio.db"
    db.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db))
    conn.execute("""CREATE TABLE portfolio_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT NOT NULL, code TEXT NOT NULL,
        name TEXT, quantity REAL, cost_price REAL, current_price REAL,
        market_value REAL, pnl REAL, pnl_rate REAL, ytd_return REAL, beta REAL,
        UNIQUE(date, code))""")
    conn.execute("""CREATE TABLE alerts (
        id INTEGER PRIMARY KEY AUTOINCREMENT, rule_name TEXT NOT NULL, level TEXT NOT NULL,
        message TEXT, created_at TEXT, acknowledged INTEGER DEFAULT 0)""")
    if with_summary:
        conn.execute("""CREATE TABLE portfolio_summary (
            date TEXT PRIMARY KEY, total_value REAL, total_cost REAL, total_pnl REAL,
            daily_pnl REAL, daily_return REAL, vs_hs300 REAL)""")

    overrides = prev_mv_overrides or {}
    for d in ("2026-09-10", "2026-09-11", "2026-09-14", PREV_WED):
        for c in BASE_CODES:
            mv = 100.0
            if d == PREV_WED:
                mv = overrides.get(c, 100.0)
            conn.execute(
                "INSERT INTO portfolio_snapshots "
                "(date, code, name, quantity, cost_price, current_price, market_value) "
                "VALUES (?,?,?,?,?,?,?)",
                (d, c, f"标的{c}", 100.0, 1.0, mv / 100.0, mv))
    if with_summary:
        conn.execute("INSERT INTO portfolio_summary (date, total_value) VALUES (?,?)",
                     (PREV_WED, prev_total_value))
    conn.commit()
    conn.close()
    return db


def _file_positions(codes, price: float = 1.0, quantity: float = 100.0):
    """模拟 `position_reader.read_positions()`：场内台账 + 当日实时价。"""
    return [{"code": c, "name": f"标的{c}", "quantity": quantity,
             "cost_price": 1.0, "current_price": price,
             "realtime_price": price, "market_value": price * quantity,
             "realtime_market_value": price * quantity,
             "pnl": 0.0, "pnl_rate": 0.0, "ytd_return": 0.0, "beta": 1.0}
            for c in codes]


def _alerts(db: Path):
    conn = sqlite3.connect(str(db))
    try:
        return conn.execute("SELECT rule_name, level FROM alerts ORDER BY id").fetchall()
    finally:
        conn.close()


def _run(a, file_rows):
    """合并 + 汇总（与 run_daily_analysis 的真实调用顺序一致）。"""
    merged, info = a._merge_positions(file_rows)
    return merged, info, a._calculate_summary(merged, {}, {}, merge_info=info)


# ------------------------------------------- (A) 新鲜/陈旧必须分开报出 ---

def test_merge_reports_fresh_and_stale_price_codes_separately(tmp_path):
    """(A) 文件 22 只 = 价格当日；库内前向填充的 12 只场外 = 价格非当日。"""
    db = _make_db(tmp_path)
    a = _analyzer(db, TARGET_THU)
    merged, info = a._merge_positions(_file_positions(ETF_CODES))

    assert len(merged) == 34
    assert info["carried_n"] == 12
    assert info["fresh_codes"] == sorted(ETF_CODES), "文件侧必须全部算「当日价」"
    assert info["stale_price_codes"] == sorted(OTC_CODES), \
        "场外 12 只价格取自 09-16（age=1）⇒ 必须被标为「价格非当日」"
    # 判据复用 merge 的 age 语义，不另立第二套
    for c in info["carried"]:
        assert c["code"] in info["stale_price_codes"]
        assert c["age_days"] == 1 and c["date"] == PREV_WED


# ------------------------------------- (B) daily_return 只用新鲜篮子 ---

def test_daily_return_uses_fresh_basket_only_with_coverage(tmp_path):
    """(B) 22 只场内 +2% ⇒ daily_return=+2.00%；场外 12 只不进分子分母。

    覆盖度：纳入 22 只、可比 34 只、占前日可比市值 2200/3400 = 64.71%。
    """
    db = _make_db(tmp_path, prev_total_value=3400.0)
    a = _analyzer(db, TARGET_THU)
    merged, info, s = _run(a, _file_positions(ETF_CODES, price=1.02))

    assert s["daily_return"] == pytest.approx(2.00, abs=1e-9)
    cov = s["daily_return_coverage"]
    assert cov["computed"] is True
    assert cov["caliber"] == "fresh_price_basket"
    assert cov["freshness_source"] == "merge"
    assert cov["included_n"] == 22
    assert cov["comparable_n"] == 34
    assert cov["total_n"] == 34
    assert cov["excluded_stale"] == sorted(OTC_CODES)
    assert cov["prev_value_included"] == pytest.approx(2200.0)
    assert cov["prev_value_comparable"] == pytest.approx(3400.0)
    assert cov["value_share"] == pytest.approx(0.6471, abs=5e-5)
    assert _alerts(db) == [], "正常路径不产生任何口径切换告警"


def test_stale_only_day_yields_fallback_not_a_silent_zero(tmp_path):
    """(B') 若当日**只有**价格非当日的标的（无新鲜篮子）⇒ 必须兜底并落告警。"""
    db = _make_db(tmp_path, prev_total_value=3400.0)
    a = _analyzer(db, TARGET_THU)
    # 文件为空 ⇒ 合并后只剩 12 只场外（全部 age=1），无新鲜可比篮子
    merged, info, s = _run(a, [])

    assert info["fresh_codes"] == []
    cov = s["daily_return_coverage"]
    assert cov["computed"] is False
    assert cov["caliber"] == "total_value_fallback"
    assert cov["included_n"] == 0
    names = [r[0] for r in _alerts(db)]
    assert "daily_return_caliber_fallback" in names, "换口径必须落告警，不许静默"


# ------------------- (C) 🔴 反证：真实平价日不得触发全持仓兜底 ---

def test_flat_day_keeps_legal_zero_and_does_not_fall_back(tmp_path):
    """(C) 反证：共同篮子当日净零 ⇒ `daily_return` 必须是合法值 `0.0`，不走兜底。

    反证能力（本用例的判别力）：把 `portfolio_summary` 的前日 total_value 故意设成
    3000，而当日 `total_value` = 3600。若旧哨兵 `daily_return == 0` 仍生效，兜底
    分支会算出 `(3600-3000)/3000*100 = +20.0` —— 与 `0.0` 相差 20 个百分点，
    所以这个用例能真正分辨「哨兵修好了」还是「还是旧的」。
    """
    db = _make_db(tmp_path, prev_total_value=3000.0,
                  prev_mv_overrides={"880013": 300.0})
    a = _analyzer(db, TARGET_THU)
    merged, info, s = _run(a, _file_positions(ETF_CODES, price=1.0))

    cov = s["daily_return_coverage"]
    # 1) 篮子当日净零 ⇒ 合法值 0.0，且**标记为已计算**
    assert s["daily_return"] == pytest.approx(0.0, abs=1e-9)
    assert cov["computed"] is True, "平价日必须被判定为『已计算』"
    assert cov["caliber"] == "fresh_price_basket"
    # 2) 反证的判别力：兜底分支若被触发，值会是 +20.0 而不是 0.0
    total_value = s["total_value"]
    prev_value = 3000.0
    old_would_be = (total_value - prev_value) / prev_value * 100
    assert abs(old_would_be) > 1.0, (
        "本用例的库必须让『旧哨兵兜底』产出显著非零值，否则无法证伪旧实现；"
        f"实测 old_would_be={old_would_be}")
    assert s["daily_return"] != pytest.approx(old_would_be, abs=1e-6)
    # 3) 不得留下任何口径切换告警
    assert [r[0] for r in _alerts(db)] == []


def test_old_sentinel_would_have_misfired(tmp_path):
    """(C') 反证的显式记录：同一条库 + 旧判定式 ⇒ 会误判为「未计算」而换口径。"""
    db = _make_db(tmp_path, prev_total_value=3000.0,
                  prev_mv_overrides={"880013": 300.0})
    a = _analyzer(db, TARGET_THU)
    merged, info, s = _run(a, _file_positions(ETF_CODES, price=1.0))

    daily_return = s["daily_return"]
    # 旧实现的判定式
    assert (daily_return == 0) is True, "旧式会把平价日判成『未计算』"
    # 新实现的判定式（独立布尔哨兵）
    assert s["daily_return_coverage"]["computed"] is True, "新式正确识别为『已计算』"
    # 两者分歧 ⇒ 旧实现确实会走进兜底（本用例是"旧实现确有 bug"的机器可复现证据）
    assert (daily_return == 0) != (not s["daily_return_coverage"]["computed"])


# ------------------- (D) 真无前日快照 ⇒ 兜底 + 告警 ---

def test_no_prev_snapshot_falls_back_and_alerts(tmp_path):
    """(D) 无前日快照行（`prev_snapshots` 为空）⇒ 兜底 + error 级告警落库。"""
    db = _make_db(tmp_path, prev_total_value=3000.0)
    # 删掉 09-16 的快照行，只留更早的日期 ⇒ prev_dt 变成 09-14，其行仍在。
    conn = sqlite3.connect(str(db))
    conn.execute("DELETE FROM portfolio_snapshots WHERE date = ?", (PREV_WED,))
    conn.commit()
    conn.close()

    a = _analyzer(db, TARGET_THU)
    merged, info, s = _run(a, _file_positions(ETF_CODES, price=1.0))

    cov = s["daily_return_coverage"]
    assert cov["computed"] is False
    assert cov["caliber"] == "total_value_fallback"
    rows = _alerts(db)
    assert [r[0] for r in rows] == ["daily_return_caliber_fallback"]
    assert rows[0][1] == "error", "换口径告警必须是 error 级（能进提醒通道）"


# ------------------- (E) 新鲜度来源必须留痕（退化链）---

def test_freshness_source_falls_back_and_is_recorded(tmp_path):
    """(E) `merge_info=None` 时按「实时行情集合 → 持仓行日期」退化，且来源留痕。"""
    db = _make_db(tmp_path, prev_total_value=3400.0)
    a = _analyzer(db, TARGET_THU)

    positions = _file_positions(BASE_CODES, price=1.0)   # 34 只，价格全部"当日"
    # ① 有实时行情集合 ⇒ realtime_quote
    a._quoted_codes = set(ETF_CODES)
    s1 = a._calculate_summary(positions, {}, {}, merge_info=None)
    assert s1["daily_return_coverage"]["freshness_source"] == "realtime_quote"
    assert s1["daily_return_coverage"]["included_n"] == 22

    # ② 连实时行情集合都没有 ⇒ 退回「持仓行自身日期」
    del a._quoted_codes
    # ②a 持仓行**没有** `date` 键（例如直接构造的持仓）⇒ 无 as-of 证据，
    #     必须**保守视为当日**、不做收窄。反证：若这里判「非当日」，共同篮子变空
    #     ⇒ daily_return 会翻到「全持仓」口径，是比"不收窄"更大的静默口径变更。
    positions_no_date = [dict(p) for p in positions]
    for p in positions_no_date:
        p.pop("date", None)
    s2a = a._calculate_summary(positions_no_date, {}, {}, merge_info=None)
    assert s2a["daily_return_coverage"]["freshness_source"] == "row_date"
    assert s2a["daily_return_coverage"]["included_n"] == 34, \
        "缺 as-of 证据时必须保守视为当日，不得把篮子清空"
    assert s2a["daily_return_coverage"]["caliber"] == "fresh_price_basket"

    # ②b 行日期 == 目标日 ⇒ 当日
    positions2 = [dict(p, date=TARGET_THU) for p in positions]
    s2 = a._calculate_summary(positions2, {}, {}, merge_info=None)
    assert s2["daily_return_coverage"]["freshness_source"] == "row_date"
    assert s2["daily_return_coverage"]["included_n"] == 34

    # ③ 行日期早于目标日 ⇒ 全部不算新鲜（退化链每一档都必须真的收窄）
    for p in positions2:
        p["date"] = PREV_WED
    s3 = a._calculate_summary(positions2, {}, {}, merge_info=None)
    assert s3["daily_return_coverage"]["included_n"] == 0
    assert s3["daily_return_coverage"]["caliber"] == "total_value_fallback"


def test_summary_extra_key_does_not_break_persistence_contract(tmp_path):
    """(F) 新增 `daily_return_coverage` 键不得影响 `save_portfolio_summary` 的取键方式。

    `save_portfolio_summary` 用显式 `summary.get(...)` 取 15 个字段；多出来的键会被忽略。
    这里用与生产同形的 INSERT 验证「多一个键」不会让落库失败。
    """
    db = _make_db(tmp_path, prev_total_value=3400.0)
    a = _analyzer(db, TARGET_THU)
    _, _, s = _run(a, _file_positions(ETF_CODES, price=1.02))
    assert "daily_return_coverage" in s

    conn = sqlite3.connect(str(db))
    conn.execute("""CREATE TABLE IF NOT EXISTS portfolio_summary_15 (
        date TEXT PRIMARY KEY, total_value REAL, total_cost REAL, total_pnl REAL,
        daily_pnl REAL, daily_return REAL, vs_hs300 REAL, profit_count INTEGER,
        loss_count INTEGER, sharpe_ratio REAL, max_drawdown REAL,
        max_drawdown_60d REAL, max_drawdown_1y REAL, max_drawdown_all REAL,
        volatility REAL)""")
    conn.execute("""INSERT OR REPLACE INTO portfolio_summary_15
        (date, total_value, total_cost, total_pnl, daily_pnl, daily_return, vs_hs300,
         profit_count, loss_count, sharpe_ratio, max_drawdown, max_drawdown_60d,
         max_drawdown_1y, max_drawdown_all, volatility)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (TARGET_THU, s.get("total_value"), s.get("total_cost"), s.get("total_pnl"),
         s.get("daily_pnl"), s.get("daily_return"), s.get("vs_hs300"),
         s.get("profit_count"), s.get("loss_count"), s.get("sharpe_ratio"),
         s.get("max_drawdown"), s.get("max_drawdown_60d"), s.get("max_drawdown_1y"),
         s.get("max_drawdown_all"), s.get("volatility")))
    conn.commit()
    got = conn.execute("SELECT daily_return FROM portfolio_summary_15 WHERE date = ?",
                       (TARGET_THU,)).fetchone()[0]
    conn.close()
    assert got == pytest.approx(2.00, abs=1e-9)
