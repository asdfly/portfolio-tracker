"""数据问题十二 · 消费侧闸门 单元测试

覆盖：
  1) 纯函数 classify_split_merge 与 is_split_merge_pseudo_return 的判据边界
     （A族 / B族 真拆分 → True；C族 月末估值切换 → False；真实申赎 → False）。
  2) split_merge_pseudo_return_dates 组合级拆分日集合（合成库）。
  3) 只读集成测试：对生产库 portfolio.db 实库 13 例逐一判定
     （A族 5 + B族 2 → True；C族 6 → False）——满足"用真实行验证"要求。
  4) nav_engine 消费侧接线：合成库中 +250% 伪收益日被中性化，TWR 不被累乘吸收，
     is_split_merge 标志落库；非拆分日不受影响（回归安全）。
不写生产库、不回填快照行。
"""
import os
import sqlite3

import numpy as np
import pytest

from src.analysis.split_merge_guard import (
    classify_split_merge,
    is_split_merge_pseudo_return,
    split_merge_pseudo_return_dates,
    VAL_SWITCH_QTY_MIN,
    VAL_SWITCH_QTY_MAX,
    SPLIT_QTY_DEV,
    MV_CONTINUITY,
)

try:
    from config.settings import DATABASE_PATH
    _LIVE_DB = str(DATABASE_PATH)
except Exception:  # pragma: no cover
    _LIVE_DB = None

# A族 5 + B族 2：实库复核期望 True
LIVE_A_B = [
    ("510500", "2015-04-15"), ("512010", "2021-06-28"), ("512100", "2022-09-05"),
    ("159300", "2024-06-25"), ("516160", "2024-09-18"),
    ("512810", "2025-06-23"), ("159220", "2025-11-10"),
]
# C族 6：月末估值切换，期望 False（必须跳过）
LIVE_C = [
    ("001323", "2026-06-30"), ("001407", "2026-06-30"), ("001437", "2026-06-30"),
    ("001765", "2026-02-28"), ("166301", "2026-07-31"), ("519770", "2026-06-30"),
]


# ---------------------------------------------------------------------------
# 1) 纯函数判据边界
# ---------------------------------------------------------------------------
def test_classify_split_merge_true_cases():
    # B族：qty 大幅变化 + mv 连续
    assert classify_split_merge(0.5, 1.0) is True
    assert classify_split_merge(2.0, 1.016) is True
    # A族（实库）：qty 大幅变化 + mv 连续
    assert classify_split_merge(0.2869, 1.0) is True
    assert classify_split_merge(3.8349, 1.0) is True


def test_classify_split_merge_c_family_skipped():
    # C族跳过带内的 qty_ratio → False（月末估值切换）
    assert classify_split_merge(1.0582, 0.72) is False
    assert classify_split_merge(1.2507, 1.3673) is False
    assert classify_split_merge(VAL_SWITCH_QTY_MIN, 1.0) is False
    assert classify_split_merge(VAL_SWITCH_QTY_MAX, 1.0) is False


def test_classify_split_merge_real_trade_not_flagged():
    # 真实申赎：qty 翻倍（ratio 2.0）但 mv 也翻倍（不连续）→ 不是拆分
    assert classify_split_merge(2.0, 2.0) is False
    assert classify_split_merge(0.5, 0.5) is False


def test_classify_split_merge_a_family_variant():
    # §三 A 族变体：qty 几乎不变（<3%）但 mv 跳变（>25%）→ 价格未复权伪收益
    assert classify_split_merge(1.0, 3.0) is True
    assert classify_split_merge(0.99, 1.5) is True
    # 边界：qty 变化超过 SPLIT_QTY_DEV 但 mv 不连续 → 真实申赎，False
    assert classify_split_merge(1.0 + SPLIT_QTY_DEV + 0.01, 1.5) is False


def test_classify_split_merge_none_safe():
    assert classify_split_merge(None, None) is False
    assert classify_split_merge(2.0, None) is False  # mv 未知，保守不判


# ---------------------------------------------------------------------------
# 2) 合成库：is_split_merge_pseudo_return 与 split_merge_pseudo_return_dates
# ---------------------------------------------------------------------------
def _make_synthetic_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    cur = conn.cursor()
    cur.execute(
        "CREATE TABLE portfolio_snapshots ("
        "id INTEGER PRIMARY KEY, date TEXT, code TEXT, name TEXT, "
        "quantity REAL, cost_price REAL, current_price REAL, market_value REAL, "
        "pnl REAL, pnl_rate REAL, ytd_return REAL, beta REAL)")
    # 各 code：d0 基准行 + d1 事件行
    rows = [
        # A 族（B 族式干净拆分）：qty 0.5，mv 连续
        ("A", "2024-01-01", 100, 10.0, 1000.0),
        ("A", "2024-01-02", 50, 20.0, 1000.0),
        # 真实申赎：qty 2.0，mv 2.0（不连续）
        ("T", "2024-01-01", 100, 10.0, 1000.0),
        ("T", "2024-01-02", 200, 10.0, 2000.0),
        # C 族月末估值切换：qty 1.2，mv 1.2
        ("C", "2024-01-01", 100, 10.0, 1000.0),
        ("C", "2024-01-02", 120, 10.0, 1200.0),
        # §三 A 族变体：qty 不变，mv 3.0（价格未复权）
        ("V", "2024-01-01", 100, 10.0, 1000.0),
        ("V", "2024-01-02", 100, 30.0, 3000.0),
    ]
    for i, (code, d, qty, price, mv) in enumerate(rows, start=1):
        cur.execute(
            "INSERT INTO portfolio_snapshots VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (i, d, code, code, qty, 10.0, price, mv, 0.0, 0.0, 0.0, 0.0))
    conn.commit()
    return conn


def test_is_split_merge_pseudo_return_synthetic():
    conn = _make_synthetic_db()
    # A 族干净拆分 + V 族 A 变体 → True
    assert is_split_merge_pseudo_return(conn, "A", "2024-01-02") is True
    assert is_split_merge_pseudo_return(conn, "V", "2024-01-02") is True
    # 真实申赎 T、C 族估值切换 → False
    assert is_split_merge_pseudo_return(conn, "T", "2024-01-02") is False
    assert is_split_merge_pseudo_return(conn, "C", "2024-01-02") is False
    # 非事件日（d0 之前无 prev）→ False
    assert is_split_merge_pseudo_return(conn, "A", "2024-01-01") is False
    conn.close()


def test_split_merge_pseudo_return_dates_synthetic():
    conn = _make_synthetic_db()
    dates = split_merge_pseudo_return_dates(conn)
    # 仅 A 与 V 的 2024-01-02 命中（T/C 被排除）
    assert dates == {"2024-01-02"}
    conn.close()


# ---------------------------------------------------------------------------
# 3) 只读集成测试：生产库实库 13 例
# ---------------------------------------------------------------------------
@pytest.mark.skipif(
    not (_LIVE_DB and os.path.exists(_LIVE_DB)),
    reason="生产库 portfolio.db 不存在，跳过只读集成验证",
)
def test_live_db_a_b_family_flagged():
    conn = sqlite3.connect(f"file:{_LIVE_DB}?mode=ro", uri=True)
    try:
        for code, d in LIVE_A_B:
            assert is_split_merge_pseudo_return(conn, code, d) is True, (
                f"{code} {d} 应为拆分/合并伪收益日（A/B 族）")
    finally:
        conn.close()


@pytest.mark.skipif(
    not (_LIVE_DB and os.path.exists(_LIVE_DB)),
    reason="生产库 portfolio.db 不存在，跳过只读集成验证",
)
def test_live_db_c_family_skipped():
    conn = sqlite3.connect(f"file:{_LIVE_DB}?mode=ro", uri=True)
    try:
        for code, d in LIVE_C:
            assert is_split_merge_pseudo_return(conn, code, d) is False, (
                f"{code} {d} 为月末估值切换（C 族），必须跳过")
    finally:
        conn.close()


@pytest.mark.skipif(
    not (_LIVE_DB and os.path.exists(_LIVE_DB)),
    reason="生产库 portfolio.db 不存在，跳过只读集成验证",
)
def test_live_db_split_date_set_isolates_seven():
    conn = sqlite3.connect(f"file:{_LIVE_DB}?mode=ro", uri=True)
    try:
        dates = split_merge_pseudo_return_dates(conn)
        for code, d in LIVE_A_B:
            assert d in dates, f"{code} {d} 应进入组合拆分日集合"
        for code, d in LIVE_C:
            assert d not in dates, f"{code} {d}（C 族）不应进入拆分日集合"
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 4) nav_engine 消费侧接线：+250% 伪收益日中性化，TWR 不被累乘吸收
# ---------------------------------------------------------------------------
def _make_nav_db() -> sqlite3.Connection:
    """合成 portfolio_summary（含 d1 的 +250% 伪收益）与 portfolio_snapshots（d1 拆分）。"""
    conn = sqlite3.connect(":memory:")
    cur = conn.cursor()
    cur.execute("CREATE TABLE portfolio_summary ("
                "date TEXT PRIMARY KEY, total_value REAL, daily_return REAL)")
    cur.execute("CREATE TABLE trade_records ("
                "date TEXT, action TEXT, quantity REAL, price REAL, "
                "commission REAL, stamp_tax REAL, change_amount REAL)")
    cur.execute("CREATE TABLE portfolio_snapshots ("
                "id INTEGER PRIMARY KEY, date TEXT, code TEXT, name TEXT, "
                "quantity REAL, cost_price REAL, current_price REAL, market_value REAL, "
                "pnl REAL, pnl_rate REAL, ytd_return REAL, beta REAL)")
    # d0 基准；d1 +250% 伪收益（按 3500 市值）；d2 +1% 正常
    cur.executemany(
        "INSERT INTO portfolio_summary VALUES (?,?,?)",
        [("2024-01-01", 1000.0, 0.0),
         ("2024-01-02", 3500.0, 250.0),
         ("2024-01-03", 3535.0, 1.0)])
    # 拆分日 d1：qty 0.5、mv 连续 → 触发 is_split_merge
    cur.executemany(
        "INSERT INTO portfolio_snapshots VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        [(1, "2024-01-01", "X", "X", 100.0, 10.0, 10.0, 1000.0, 0, 0, 0, 0),
         (2, "2024-01-02", "X", "X", 50.0, 10.0, 20.0, 1000.0, 0, 0, 0, 0)])
    conn.commit()
    return conn


def test_nav_engine_neutralizes_split_phantom():
    from src.analysis.nav_engine import rebuild_portfolio_nav
    conn = _make_nav_db()
    n = rebuild_portfolio_nav(conn)
    assert n == 3
    cur = conn.cursor()
    rows = {r[0]: r for r in cur.execute(
        "SELECT date, twr_cumulative, is_split_merge FROM portfolio_nav ORDER BY date")}
    # d1 伪收益日：is_split_merge 落库 = True，TWR 不被 +250% 累乘
    assert rows["2024-01-02"][2] == 1, "d1 应标记 is_split_merge"
    # d2 TWR 只涨 ~1%（0% 来自 d1 中性化 + 1% 来自 d2），而非 251%
    twr_d2 = rows["2024-01-03"][1]
    assert abs(twr_d2 - 0.01) < 1e-6, f"TWR(d2)={twr_d2}，应≈0.01 而非被伪收益污染"
    # d0 无拆分标记
    assert rows["2024-01-01"][2] == 0
    conn.close()


def test_nav_engine_no_split_passthrough():
    """无拆分行时，正常日收益应原样进入 TWR（回归安全，不误伤）。"""
    from src.analysis.nav_engine import rebuild_portfolio_nav
    conn = sqlite3.connect(":memory:")
    cur = conn.cursor()
    cur.execute("CREATE TABLE portfolio_summary ("
                "date TEXT PRIMARY KEY, total_value REAL, daily_return REAL)")
    cur.execute("CREATE TABLE trade_records ("
                "date TEXT, action TEXT, quantity REAL, price REAL, "
                "commission REAL, stamp_tax REAL, change_amount REAL)")
    cur.executemany(
        "INSERT INTO portfolio_summary VALUES (?,?,?)",
        [("2024-01-01", 1000.0, 0.0),
         ("2024-01-02", 1010.0, 1.0),
         ("2024-01-03", 1020.1, 1.0)])
    conn.commit()
    rebuild_portfolio_nav(conn)
    cur = conn.cursor()
    rows = {r[0]: r for r in cur.execute(
        "SELECT date, twr_cumulative, is_split_merge FROM portfolio_nav ORDER BY date")}
    # 两天各 +1% → 累计 ≈ 2.01%
    assert abs(rows["2024-01-03"][1] - 0.0201) < 1e-4
    assert rows["2024-01-02"][2] == 0 and rows["2024-01-03"][2] == 0
    conn.close()


# ---------------------------------------------------------------------------
# 5) 消费侧接线证明：portfolio_risk / factor_attribution 真正"剔除"拆分日
#    （不仅是跑通，而是伪收益尖峰不再进入波动率 / 因子回归）
# ---------------------------------------------------------------------------
def _build_temp_risk_db(path: str):
    """构造含"拆分日 +250% 伪收益"的临时组合库（仅测试用，非生产库）。"""
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    cur.execute("CREATE TABLE portfolio_summary ("
                "date TEXT PRIMARY KEY, total_value REAL, daily_return REAL)")
    cur.execute("CREATE TABLE trade_records ("
                "date TEXT, action TEXT, quantity REAL, price REAL, "
                "commission REAL, stamp_tax REAL, change_amount REAL)")
    cur.execute("CREATE TABLE portfolio_snapshots ("
                "id INTEGER PRIMARY KEY, date TEXT, code TEXT, name TEXT, "
                "quantity REAL, cost_price REAL, current_price REAL, market_value REAL, "
                "pnl REAL, pnl_rate REAL, ytd_return REAL, beta REAL)")
    dates = [d.strftime("%Y-%m-%d") for d in
             __import__("pandas").date_range("2024-01-01", periods=60, freq="D")]
    split_idx = 30
    rows = []
    for i, d in enumerate(dates):
        dr = 250.0 if i == split_idx else 0.5  # 仅拆分日尖峰
        tv = 1000.0 * (1.005 ** i)
        rows.append((d, round(tv, 2), dr))
    cur.executemany("INSERT INTO portfolio_summary VALUES (?,?,?)", rows)
    # 拆分日标记：qty 0.5、mv 连续（B 族式干净拆分）
    sd = dates[split_idx]
    pd_ = dates[split_idx - 1]
    cur.executemany(
        "INSERT INTO portfolio_snapshots VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        [(1, pd_, "X", "X", 100.0, 10.0, 10.0, 1000.0, 0, 0, 0, 0),
         (2, sd, "X", "X", 50.0, 10.0, 20.0, 1000.0, 0, 0, 0, 0)])
    conn.commit()
    return conn


def test_portfolio_risk_excludes_split_spike(tmp_path):
    """拆分日 +250% 伪收益必须从波动率窗口剔除：年化波动率保持低位。"""
    from src.analysis.portfolio_risk import PortfolioRiskAnalyzer
    from src.utils.database import DatabaseManager

    db = tmp_path / "portfolio.db"
    _build_temp_risk_db(str(db))
    analyzer = PortfolioRiskAnalyzer()
    analyzer.db = DatabaseManager(db_path=str(db))
    # 直接调用被改动的函数，隔离"消费侧闸门"逻辑（避免 _generate_warnings 对空持仓的无关报错）
    metrics = analyzer._calculate_portfolio_metrics(positions=[], days=60)
    assert "error" not in metrics, f"风险指标计算失败: {metrics}"
    ann_vol = metrics["volatility_metrics"]["annual_volatility"]
    # 若剔除失败，单个 250% 尖峰会把年化波动率推到数千%；剔除后仅剩 ~0.5% 正常日 → 低位。
    assert ann_vol < 50.0, f"年化波动率={ann_vol}，疑似拆分日伪收益未被剔除"
    assert ann_vol == ann_vol  # 有限值
    db.unlink(missing_ok=True)


def test_factor_attribution_excludes_split_day(tmp_path, monkeypatch):
    """因子回归的输入 port_returns 必须不含拆分日（伪收益不被当成真实组合收益）。"""
    import pandas as pd
    from src.analysis import factor_attribution as fa
    from src.utils.database import DatabaseManager

    db = tmp_path / "portfolio.db"
    _build_temp_risk_db(str(db))
    # 因子构建需要 index_quotes（close），补足量纲数据使其非空
    conn = sqlite3.connect(str(db))
    cur = conn.cursor()
    cur.execute("CREATE TABLE index_quotes (date TEXT, code TEXT, name TEXT, close REAL, change_pct REAL)")
    dates = [d.strftime("%Y-%m-%d") for d in pd.date_range("2024-01-01", periods=60, freq="D")]
    codes = ["sh000300", "sh000852", "sh000015", "sh000688", "sz399006"]
    iq = []
    for i, d in enumerate(dates):
        for c in codes:
            iq.append((d, c, c, 1000.0 + i, 0.1))
    cur.executemany("INSERT INTO index_quotes VALUES (?,?,?,?,?)", iq)
    conn.commit()
    conn.close()

    captured = {}
    orig = fa.compute_factor_attribution

    def _spy(port_returns, factor_returns):
        captured["port_returns"] = port_returns
        return orig(port_returns, factor_returns)

    monkeypatch.setattr(fa, "compute_factor_attribution", _spy)
    conn2 = sqlite3.connect(str(db))
    try:
        fa.run_full_attribution(conn2, pd.DataFrame(), {}, lookback_days=60)
    finally:
        conn2.close()
    assert "port_returns" in captured, "因子归因未进入 compute_factor_attribution"
    # 拆分日（index 30 的日期）必须已从 port_returns 剔除
    split_date = dates[30]
    assert split_date not in captured["port_returns"].index, (
        f"拆分日 {split_date} 仍残留于因子回归输入，未被剔除")
    assert len(captured["port_returns"]) == 59, "应剔除 1 个拆分日观测（60→59）"
    db.unlink(missing_ok=True)
