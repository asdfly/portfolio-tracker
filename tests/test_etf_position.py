# -*- coding: utf-8 -*-
"""ETF 高低位定位引擎测试 (src/analysis/etf_position.py)。

覆盖：F1 价格分布方向性/数据不足守卫、F3 资金流反向解读、F2 估值就绪闸门、
标签分档边界、组合加权聚合、evaluate() 传入连接的端到端路径（含缺失因子归一化）。
"""
import sqlite3

import numpy as np
import pandas as pd
import pytest

from src.analysis.etf_position import (
    VAL_FULL_DAYS,
    VAL_MIN_DAYS,
    _label,
    evaluate,
    flow_position,
    portfolio_position,
    price_position,
    valuation_position,
)


def _series(values):
    idx = pd.date_range("2015-01-01", periods=len(values), freq="B")
    return pd.Series(np.asarray(values, dtype=float), index=idx)


# --------------------------------------------------------------------------- #
# F1 价格分布
# --------------------------------------------------------------------------- #
def test_price_position_high_when_at_top():
    """单调上行 → 当前价在历史最高 → P 应显著为正。"""
    p, c, d = price_position(_series(np.linspace(1.0, 3.0, 1400)))
    assert p > 50, f"处于历史最高位应给高分, 实际 {p}"
    assert 0 < c <= 1
    assert d["available"] is True
    assert d["n_points"] == 1400


def test_price_position_low_when_at_bottom():
    """单调下行 → 当前价在历史最低 → P 应显著为负。"""
    p, _, _ = price_position(_series(np.linspace(3.0, 1.0, 1400)))
    assert p < -50, f"处于历史最低位应给低分, 实际 {p}"


def test_price_position_neutral_in_middle():
    """震荡后回到中枢 → P 应接近 0（|P|<35）。"""
    x = np.linspace(0, 20 * np.pi, 1400)
    p, _, _ = price_position(_series(2.0 + 0.3 * np.sin(x)))
    assert abs(p) < 35, f"中枢附近应接近中性, 实际 {p}"


def test_price_position_guard_insufficient():
    """样本 <60 → 不可用, 置信度 0（不得给出伪结论）。"""
    p, c, d = price_position(_series(np.linspace(1, 2, 30)))
    assert (p, c) == (0.0, 0.0)
    assert d["available"] is False


# --------------------------------------------------------------------------- #
# F3 资金流（反向解读）
# --------------------------------------------------------------------------- #
def test_flow_position_inflow_pushes_high():
    """末端大额净流入（情绪高涨）→ P 为正（偏高位）。"""
    vals = list(np.random.default_rng(0).normal(0, 1, 300)) + [50.0] * 20
    p, c, d = flow_position(_series(vals))
    assert p is not None and p > 20, f"极端净流入应判偏高, 实际 {p}"
    assert c <= 0.65, "资金流置信度必须封顶 0.65"
    assert d["z_inflow"] > 0


def test_flow_position_outflow_pushes_low():
    """末端大额净流出（恐慌）→ P 为负（偏低位）。"""
    vals = list(np.random.default_rng(1).normal(0, 1, 300)) + [-50.0] * 20
    p, _, _ = flow_position(_series(vals))
    assert p is not None and p < -20, f"极端净流出应判偏低, 实际 {p}"


def test_flow_position_guard_insufficient():
    p, c, d = flow_position(_series(np.ones(30)))
    assert p is None and c == 0.0 and d["available"] is False


# --------------------------------------------------------------------------- #
# F2 估值就绪闸门
# --------------------------------------------------------------------------- #
def test_valuation_gate_blocks_short_history():
    """PE 历史 < 250 交易日 → 必须返回 None（禁止伪精度分位）。"""
    p, c, d = valuation_position([12.0] * (VAL_MIN_DAYS - 1))
    assert p is None and c == 0.0
    assert d["available"] is False and "闸门" in d["reason"]


def test_valuation_confidence_ramps_with_history():
    """250 日给 0.6 置信, 1250 日给满置信, 且单调递增。"""
    hist_min = list(np.linspace(10, 20, VAL_MIN_DAYS))
    hist_full = list(np.linspace(10, 20, VAL_FULL_DAYS))
    p1, c1, _ = valuation_position(hist_min)
    p2, c2, _ = valuation_position(hist_full)
    assert p1 is not None and p2 is not None
    assert c1 == pytest.approx(0.6, abs=1e-6)
    assert c2 == pytest.approx(1.0, abs=1e-6)
    assert p1 > 50 and p2 > 50, "当前 PE 位于历史最高应判高估"


def test_valuation_low_percentile_is_cheap():
    hist = list(np.linspace(30, 10, VAL_FULL_DAYS))  # PE 一路下行, 当前最低
    p, _, d = valuation_position(hist)
    assert p < -50 and d["pe_percentile"] < 10


# --------------------------------------------------------------------------- #
# 标签与组合聚合
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("p,expected", [
    (-100, "极低(黄金区)"), (-60, "极低(黄金区)"), (-59.9, "偏低"), (-25, "偏低"),
    (-24.9, "中性"), (0, "中性"), (24.9, "中性"), (25, "偏高"), (59.9, "偏高"),
    (60, "极高(警惕区)"), (100, "极高(警惕区)"),
])
def test_label_boundaries(p, expected):
    assert _label(p) == expected


def test_portfolio_position_weighted_math():
    results = [
        {"code": "A", "P": 80.0, "C": 0.8},
        {"code": "B", "P": -20.0, "C": 0.4},
        {"code": "C", "P": 50.0, "C": 0.9},  # 无权重, 应被剔除
    ]
    pf = portfolio_position(results, {"A": 0.75, "B": 0.25})
    assert pf["P"] == pytest.approx(55.0, abs=0.05)   # 0.75*80 + 0.25*(-20)
    assert pf["C"] == pytest.approx(0.7, abs=0.005)
    assert pf["n"] == 2 and pf["coverage"] == pytest.approx(1.0)
    assert pf["label"] == "偏低" or pf["label"] == "偏高"


def test_portfolio_position_none_without_weights():
    assert portfolio_position([{"code": "A", "P": 10.0, "C": 0.5}], {}) is None


# --------------------------------------------------------------------------- #
# evaluate() 端到端（传入连接，验证 conn 复用路径 + 缺失因子归一化）
# --------------------------------------------------------------------------- #
def _mem_db(code="999999", n=1400, rising=True):
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE etf_price_history (code TEXT, date TEXT, adj_close REAL)")
    conn.execute("CREATE TABLE fund_flows (code TEXT, date TEXT, net_inflow REAL)")
    conn.execute("CREATE TABLE index_quotes (code TEXT, date TEXT, close REAL)")
    conn.execute("CREATE TABLE index_pe_history (index_code TEXT, date TEXT, pe REAL)")
    dates = pd.date_range("2018-01-01", periods=n, freq="B").strftime("%Y-%m-%d")
    prices = np.linspace(1.0, 3.0, n) if rising else np.linspace(3.0, 1.0, n)
    conn.executemany("INSERT INTO etf_price_history VALUES (?,?,?)",
                     [(code, d, float(p)) for d, p in zip(dates, prices)])
    conn.commit()
    return conn


def test_evaluate_with_shared_connection_price_only():
    """只有价格数据时：资金流/估值因子缺失 → 自动剔除并按价格因子归一化。"""
    conn = _mem_db(rising=True)
    try:
        r = evaluate("999999", conn=conn)
    finally:
        conn.close()
    assert r["code"] == "999999" and r["type"] == "equity"
    assert r["n_factors"] == 1, "缺失因子必须被剔除"
    assert r["P"] > 50 and r["label"] == "极高(警惕区)"
    assert r["factors"]["price"]["basis"] == "etf_adj_close"
    # 单因子时一致性惩罚不应生效
    assert r["C_agree"] == pytest.approx(1.0)


def test_evaluate_unknown_code_is_safe():
    """未知代码不应抛异常, 应给出不可用的价格因子。"""
    conn = _mem_db()
    try:
        r = evaluate("000000", conn=conn)
    finally:
        conn.close()
    assert r["P"] == 0.0 and r["C"] == 0.0
    assert r["factors"]["price"]["available"] is False


def test_evaluate_prefers_long_index_history_for_broad_etf():
    """宽基 ETF: index_quotes 长历史(>=1260) 应替换 ETF 自身窗口作为定位基准。"""
    conn = _mem_db(code="510300", n=600, rising=True)
    dates = pd.date_range("2005-01-01", periods=1400, freq="B").strftime("%Y-%m-%d")
    closes = np.concatenate([np.linspace(1000, 6000, 700), np.linspace(6000, 3000, 700)])
    conn.executemany("INSERT INTO index_quotes VALUES (?,?,?)",
                     [("sh000300", d, float(c)) for d, c in zip(dates, closes)])
    conn.commit()
    try:
        r = evaluate("510300", conn=conn)
    finally:
        conn.close()
    assert r["factors"]["price"]["basis"] == "index_close(sh000300)"
    # 指数长历史末端处于中段回落 → 不应再判极高（修正 ETF 窗口偏置）
    assert r["P"] < 60


def test_evaluate_bond_etf_branch():
    conn = _mem_db(code="511520", rising=True)
    try:
        r = evaluate("511520", conn=conn)
    finally:
        conn.close()
    assert r["type"] == "bond" and "note" in r
    assert "valuation" not in r["factors"], "债券 ETF 不应带权益估值因子"
