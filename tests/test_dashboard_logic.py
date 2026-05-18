"""L1 - dashboard.py 提取函数的纯函数单元测试

覆盖 Step 1-4 重构中提取的 5 个顶层函数:
  - get_indicator_color
  - get_risk_color / get_risk_label
  - compute_risk_score
  - get_warnings
  - compute_comprehensive_score
"""
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import numpy as np
import pandas as pd
import pytest

from dashboard import (
    get_indicator_color,
    get_risk_color,
    get_risk_label,
    compute_risk_score,
    get_warnings,
    compute_comprehensive_score,
)


def _make_positions(overrides=None):
    data = {
        "code": ["510300", "510500", "159915", "512880", "159267"],
        "name": ["沪深300ETF", "中证500ETF", "创业板ETF", "证券ETF", "航天ETF"],
        "quantity": [1000, 500, 800, 2000, 300],
        "cost_price": [4.5, 6.2, 2.1, 1.0, 0.8],
        "current_price": [4.8, 6.0, 2.3, 0.9, 0.85],
        "market_value": [4800.0, 3000.0, 1840.0, 1800.0, 255.0],
        "pnl": [300.0, -100.0, 160.0, -200.0, 15.0],
        "pnl_rate": [6.67, -3.23, 9.52, -10.0, 6.25],
        "beta": [0.95, 1.05, 1.15, 1.30, 0.80],
    }
    if overrides:
        data.update(overrides)
    return pd.DataFrame(data)


def _make_summary(days=60, base=100000, seed=42):
    np.random.seed(seed)
    dates = pd.date_range("2025-01-01", periods=days, freq="D")
    values = base + np.cumsum(np.random.normal(0, 500, days))
    return pd.DataFrame({
        "date": dates.strftime("%Y-%m-%d"),
        "total_value": values.round(2),
        "total_pnl": (values - base).round(2),
        "daily_return": np.random.normal(0.001, 0.015, days).round(6),
    })


class TestGetIndicatorColor:
    def test_abs_value_gt_threshold(self):
        """abs(value) > upper -> 匹配"""
        thresholds = [(10, "#ef4444"), (5, "#f59e0b"), (0, "#22c55e")]
        assert get_indicator_color(12, thresholds) == "#ef4444"

    def test_medium_match(self):
        thresholds = [(10, "#ef4444"), (5, "#f59e0b"), (0, "#22c55e")]
        assert get_indicator_color(7, thresholds) == "#f59e0b"

    def test_falls_to_last(self):
        """abs(value) < 所有阈值 -> 返回最后一个阈值颜色"""
        thresholds = [(10, "#ef4444"), (5, "#f59e0b"), (0, "#22c55e")]
        assert get_indicator_color(3, thresholds) == "#22c55e"

    def test_negative_abs(self):
        thresholds = [(10, "#ef4444"), (5, "#f59e0b"), (0, "#22c55e")]
        assert get_indicator_color(-8, thresholds) == "#f59e0b"

    def test_none_returns_default(self):
        thresholds = [(10, "#ef4444"), (0, "#22c55e")]
        assert get_indicator_color(None, thresholds) == "#888"

    def test_nan_returns_default(self):
        thresholds = [(10, "#ef4444"), (0, "#22c55e")]
        assert get_indicator_color(float("nan"), thresholds) == "#888"

    def test_single_threshold_exceed(self):
        thresholds = [(10, "#ef4444")]
        assert get_indicator_color(15, thresholds) == "#ef4444"

    def test_single_threshold_below(self):
        thresholds = [(10, "#ef4444")]
        assert get_indicator_color(5, thresholds) == "#ef4444"


class TestGetRiskColor:
    def test_low_risk(self):
        assert get_risk_color(80) == "#22c55e"

    def test_medium_risk(self):
        assert get_risk_color(55) == "#f59e0b"

    def test_high_risk(self):
        assert get_risk_color(20) == "#ef4444"

    def test_boundary_low(self):
        assert get_risk_color(70) == "#22c55e"

    def test_boundary_medium(self):
        assert get_risk_color(40) == "#f59e0b"

    def test_boundary_high(self):
        assert get_risk_color(39) == "#ef4444"


class TestGetRiskLabel:
    def test_low(self):
        assert get_risk_label(80) == "低风险"

    def test_medium(self):
        assert get_risk_label(55) == "中等风险"

    def test_high(self):
        assert get_risk_label(15) == "高风险"

    def test_boundary(self):
        assert get_risk_label(70) == "低风险"
        assert get_risk_label(69) == "中等风险"
        assert get_risk_label(39) == "高风险"


class TestComputeRiskScore:
    def test_good_vol_low_dd(self):
        """低波动+小回撤+高sharpe -> 高分(低风险)"""
        score = compute_risk_score(volatility=8, max_dd=-3.0, sharpe=1.5)
        assert score >= 80

    def test_high_vol_large_dd(self):
        """高波动+大回撤+负sharpe -> 低分(高风险)"""
        score = compute_risk_score(volatility=30, max_dd=-20.0, sharpe=-1.0)
        assert score <= 40

    def test_none_volatility(self):
        score = compute_risk_score(volatility=None, max_dd=-5.0, sharpe=0.5)
        assert isinstance(score, (int, float))

    def test_nan_volatility(self):
        score = compute_risk_score(volatility=float("nan"), max_dd=-5.0, sharpe=0.5)
        assert isinstance(score, (int, float))

    def test_none_max_dd(self):
        score = compute_risk_score(volatility=15, max_dd=None, sharpe=1.0)
        assert isinstance(score, (int, float))

    def test_nan_max_dd(self):
        score = compute_risk_score(volatility=15, max_dd=float("nan"), sharpe=1.0)
        assert isinstance(score, (int, float))

    def test_zero_sharpe(self):
        score = compute_risk_score(volatility=15, max_dd=-10.0, sharpe=0.0)
        assert isinstance(score, (int, float))

    def test_all_none(self):
        score = compute_risk_score(volatility=None, max_dd=None, sharpe=None)
        assert isinstance(score, (int, float))

    def test_score_range(self):
        for vol in [5, 15, 25, 35]:
            for dd in [-2, -10, -25]:
                for sh in [-2, 0, 2]:
                    score = compute_risk_score(vol, dd, sh)
                    assert 0 <= score <= 100, f"vol={vol}, dd={dd}, sh={sh} -> score={score}"


class TestGetWarnings:
    def test_empty_positions(self):
        pos = pd.DataFrame(columns=[
            "code", "name", "quantity", "cost_price", "current_price",
            "market_value", "pnl", "pnl_rate", "beta",
        ])
        warnings = get_warnings(pos, -10, 15, 1.0, 3, 2)
        assert isinstance(warnings, list)

    def test_concentration_risk_high(self):
        pos = _make_positions({"market_value": [50000.0, 100.0, 100.0, 100.0, 100.0]})
        warnings = get_warnings(pos, -10, 15, 1.0, 3, 2)
        titles = [w[1] for w in warnings]
        assert any("集中度" in t for t in titles)

    def test_concentration_no_warning(self):
        pos = _make_positions({"market_value": [2000.0, 2000.0, 2000.0, 2000.0, 2000.0]})
        warnings = get_warnings(pos, -5, 12, 1.0, 3, 2)
        titles = [w[1] for w in warnings]
        assert not any("集中度" in t for t in titles)

    def test_drawdown_warning_large(self):
        warnings = get_warnings(_make_positions(), -18, 15, 1.0, 3, 2)
        titles = [w[1] for w in warnings]
        assert any("回撤" in t for t in titles)

    def test_volatility_warning_high(self):
        warnings = get_warnings(_make_positions(), -10, 30, 1.0, 3, 2)
        titles = [w[1] for w in warnings]
        assert any("波动率" in t for t in titles)

    def test_volatility_no_warning_normal(self):
        warnings = get_warnings(_make_positions(), -10, 12, 1.0, 3, 2)
        titles = [w[1] for w in warnings]
        assert not any("波动率" in t for t in titles)

    def test_winrate_low(self):
        warnings = get_warnings(_make_positions(), -10, 15, 1.0, profit_count=1, loss_count=5)
        titles = [w[1] for w in warnings]
        assert any("胜率" in t for t in titles)

    def test_beta_warning_high(self):
        pos = _make_positions({"beta": [1.5, 1.5, 1.5, 1.5, 1.5]})
        warnings = get_warnings(pos, -10, 15, 1.0, 3, 2)
        titles = [w[1] for w in warnings]
        assert any("Beta" in t for t in titles)

    def test_loss_position_warning(self):
        pos = _make_positions({
            "pnl": [-500.0, -100.0, -80.0, -60.0, -40.0],
            "pnl_rate": [-20.0, -5.0, -4.0, -3.0, -2.0],
            "market_value": [5000.0, 3000.0, 2000.0, 1500.0, 1000.0],
        })
        warnings = get_warnings(pos, -10, 15, 1.0, 0, 5)
        titles = [w[1] for w in warnings]
        assert any("个股预警" in t for t in titles)

    def test_none_params(self):
        warnings = get_warnings(_make_positions(), None, None, None, None, None)
        assert isinstance(warnings, list)

    def test_warning_tuple_format(self):
        warnings = get_warnings(_make_positions(), -15, 25, -0.5, 1, 4)
        for w in warnings:
            assert len(w) == 3, f"Expected 3-tuple, got {w}"
            assert all(isinstance(x, str) for x in w)


class TestComputeComprehensiveScore:
    def test_returns_all_keys(self):
        pos = _make_positions()
        summary = _make_summary()
        result = compute_comprehensive_score(pos, summary, 15, -10, pd.DataFrame())
        expected_keys = {
            "score_return", "score_risk", "tech_score", "score_health",
            "total_score", "score_color", "score_label", "tech_signals",
        }
        assert set(result.keys()) == expected_keys

    def test_total_score_sum(self):
        pos = _make_positions()
        summary = _make_summary()
        result = compute_comprehensive_score(pos, summary, 15, -10, pd.DataFrame())
        assert result["total_score"] == (
            result["score_return"] + result["score_risk"]
            + result["tech_score"] + result["score_health"]
        )

    def test_max_score_100(self):
        pos = _make_positions()
        summary = _make_summary()
        result = compute_comprehensive_score(pos, summary, 5, -2, pd.DataFrame())
        assert result["total_score"] <= 100

    def test_score_return_good(self):
        np.random.seed(99)
        summary = pd.DataFrame({
            "date": pd.date_range("2025-01-01", periods=60, freq="D").strftime("%Y-%m-%d"),
            "total_value": np.linspace(100000, 120000, 60),
            "total_pnl": np.linspace(0, 20000, 60),
            "daily_return": np.random.normal(0.003, 0.01, 60),
        })
        result = compute_comprehensive_score(_make_positions(), summary, 10, -5, pd.DataFrame())
        assert result["score_return"] == 30

    def test_score_return_bad(self):
        np.random.seed(99)
        summary = pd.DataFrame({
            "date": pd.date_range("2025-01-01", periods=60, freq="D").strftime("%Y-%m-%d"),
            "total_value": np.linspace(100000, 90000, 60),
            "total_pnl": np.linspace(0, -10000, 60),
            "daily_return": np.random.normal(-0.001, 0.01, 60),
        })
        result = compute_comprehensive_score(_make_positions(), summary, 15, -10, pd.DataFrame())
        assert result["score_return"] == 5

    def test_score_risk_good(self):
        result = compute_comprehensive_score(
            _make_positions(), _make_summary(), volatility=8, effective_max_dd=-3, tech_df=pd.DataFrame()
        )
        assert result["score_risk"] >= 24

    def test_score_risk_bad(self):
        result = compute_comprehensive_score(
            _make_positions(), _make_summary(), volatility=30, effective_max_dd=-20, tech_df=pd.DataFrame()
        )
        assert result["score_risk"] <= 12

    def test_score_health_concentrated(self):
        pos = _make_positions({
            "market_value": [50000, 100, 100, 100, 100],
            "pnl": [100, -500, -400, -300, -200],
            "pnl_rate": [2, -30, -25, -20, -15],
        })
        result = compute_comprehensive_score(pos, _make_summary(), 15, -10, pd.DataFrame())
        assert result["score_health"] < 15

    def test_score_health_balanced(self):
        pos = _make_positions({"market_value": [2000, 2000, 2000, 2000, 2000],
                                "pnl": [100, 100, 100, 100, 100]})
        result = compute_comprehensive_score(pos, _make_summary(), 15, -10, pd.DataFrame())
        assert result["score_health"] == 15

    def test_tech_score_with_signals(self):
        tech_df = pd.DataFrame([{
            "code": "510300",
            "ma_signal": "多头排列",
            "macd_signal": "金叉",
            "rsi_status": "正常",
            "trend": "上涨",
        }])
        result = compute_comprehensive_score(_make_positions(), _make_summary(), 15, -10, tech_df)
        assert result["tech_score"] > 0
        assert len(result["tech_signals"]) > 0

    def test_tech_score_empty(self):
        result = compute_comprehensive_score(
            _make_positions(), _make_summary(), 15, -10, pd.DataFrame()
        )
        assert result["tech_score"] == 0
        assert result["tech_signals"] == []

    def test_score_color_low(self):
        result = compute_comprehensive_score(
            _make_positions({"market_value": [50000, 100, 100, 100, 100],
                             "pnl": [-500, -200, -100, -50, -50],
                             "pnl_rate": [-20, -15, -10, -5, -5]}),
            _make_summary(), 35, -25, pd.DataFrame(),
        )
        assert result["score_color"] == "#ef4444"

    def test_score_label_valid(self):
        valid_labels = {"优秀", "良好", "一般", "较差"}
        pos = _make_positions()
        summary = _make_summary()
        result = compute_comprehensive_score(pos, summary, 15, -10, pd.DataFrame())
        assert result["score_label"] in valid_labels

    def test_tech_score_capped_at_25(self):
        tech_df = pd.DataFrame([{
            "code": c, "ma_signal": "多头排列", "macd_signal": "金叉",
            "rsi_status": "超卖", "trend": "上涨",
        } for c in ["510300", "510500", "159915", "512880", "159267"]])
        result = compute_comprehensive_score(_make_positions(), _make_summary(), 15, -10, tech_df)
        assert result["tech_score"] <= 25

    def test_none_volatility(self):
        result = compute_comprehensive_score(
            _make_positions(), _make_summary(), None, -10, pd.DataFrame()
        )
        assert "score_risk" in result

    def test_none_max_dd(self):
        result = compute_comprehensive_score(
            _make_positions(), _make_summary(), 15, None, pd.DataFrame()
        )
        assert result["total_score"] is not None
