"""Tests for src/analysis/signal_score.py"""

import pytest
import pandas as pd
import numpy as np
from src.analysis.signal_score import (
    compute_signal_score,
    compute_signal_scores,
    _rsi_to_score,
    _boll_to_score,
    _atr_to_score,
    _score_trend,
    _score_momentum,
    _score_volatility,
    _score_oversold_overbought,
    _score_volume,
)


def _make_row(**overrides):
    """Create a test row with default values."""
    defaults = {
        "code": "510300",
        "date": "2026-06-12",
        "ma_signal": "中性",
        "macd_signal": "中性",
        "rsi_value": 50.0,
        "rsi_status": "正常",
        "kdj_signal": "中性",
        "bollinger_position": 50.0,
        "atr_pct": 2.0,
        "trend": "震荡整理",
    }
    defaults.update(overrides)
    return pd.Series(defaults)


class TestRSIToScore:
    def test_extreme_oversold(self):
        assert _rsi_to_score(10) == 95

    def test_oversold(self):
        assert _rsi_to_score(25) == 85

    def test_neutral(self):
        assert _rsi_to_score(50) == 50

    def test_overbought(self):
        assert _rsi_to_score(75) == 15

    def test_extreme_overbought(self):
        assert _rsi_to_score(90) == 5

    def test_nan(self):
        assert _rsi_to_score(np.nan) == 50

    def test_boundary_low(self):
        assert _rsi_to_score(20) == 95

    def test_boundary_high(self):
        assert _rsi_to_score(80) == 15


class TestBollToScore:
    def test_near_lower(self):
        assert _boll_to_score(5) == 90

    def test_middle(self):
        assert _boll_to_score(50) == 50

    def test_near_upper(self):
        assert _boll_to_score(95) == 10

    def test_nan(self):
        assert _boll_to_score(np.nan) == 50

    def test_boundaries(self):
        assert _boll_to_score(10) == 90
        assert _boll_to_score(60) == 50
        assert _boll_to_score(90) == 25


class TestATRToScore:
    def test_low_vol(self):
        assert _atr_to_score(0.5) == 80

    def test_normal(self):
        assert _atr_to_score(2.0) == 50

    def test_high_vol(self):
        assert _atr_to_score(5.0) == 10

    def test_nan(self):
        assert _atr_to_score(np.nan) == 50


class TestScoreTrend:
    def test_bullish_alignment(self):
        row = _make_row(ma_signal="多头排列", trend="上升趋势")
        result = _score_trend(row)
        assert result["score"] >= 90
        assert "趋势强劲" in result["detail"]

    def test_bearish_alignment(self):
        row = _make_row(ma_signal="空头排列", trend="下降趋势")
        result = _score_trend(row)
        assert result["score"] <= 15
        assert "疲弱" in result["detail"]

    def test_neutral(self):
        row = _make_row(ma_signal="中性", trend="震荡整理")
        result = _score_trend(row)
        assert result["score"] == 50

    def test_weight(self):
        row = _make_row()
        assert _score_trend(row)["weight"] == 0.30

    def test_mixed_signal(self):
        row = _make_row(ma_signal="多头", trend="下降趋势")
        result = _score_trend(row)
        assert 20 < result["score"] < 80


class TestScoreMomentum:
    def test_bullish(self):
        row = _make_row(macd_signal="金叉", rsi_value=25, kdj_signal="金叉")
        result = _score_momentum(row)
        assert result["score"] > 70

    def test_bearish(self):
        row = _make_row(macd_signal="死叉", rsi_value=80, kdj_signal="死叉")
        result = _score_momentum(row)
        assert result["score"] < 30

    def test_neutral(self):
        row = _make_row(macd_signal="中性", rsi_value=50, kdj_signal="中性")
        result = _score_momentum(row)
        assert result["score"] == 50

    def test_weight(self):
        assert _score_momentum(_make_row())["weight"] == 0.25

    def test_detail_string(self):
        row = _make_row(rsi_value=45.5, rsi_status="正常")
        result = _score_momentum(row)
        assert "MACD" in result["detail"]
        assert "RSI" in result["detail"]
        assert "45.5" in result["detail"]


class TestScoreVolatility:
    def test_boll_lower(self):
        row = _make_row(bollinger_position=5, atr_pct=1.0)
        result = _score_volatility(row)
        assert result["score"] > 80

    def test_boll_upper(self):
        row = _make_row(bollinger_position=95, atr_pct=4.0)
        result = _score_volatility(row)
        assert result["score"] < 25

    def test_neutral(self):
        row = _make_row(bollinger_position=50, atr_pct=2.0)
        result = _score_volatility(row)
        assert result["score"] == 50

    def test_weight(self):
        assert _score_volatility(_make_row())["weight"] == 0.20

    def test_nan_values(self):
        row = _make_row(bollinger_position=np.nan, atr_pct=np.nan)
        result = _score_volatility(row)
        assert result["score"] == 50


class TestScoreOversoldOverbought:
    def test_oversold(self):
        row = _make_row(rsi_value=20, bollinger_position=10, kdj_signal="超卖")
        result = _score_oversold_overbought(row)
        assert result["score"] > 50
        assert "反弹预期" in result["detail"]

    def test_overbought(self):
        row = _make_row(rsi_value=80, bollinger_position=90, kdj_signal="超买")
        result = _score_oversold_overbought(row)
        assert result["score"] < 50
        assert "回调风险" in result["detail"]

    def test_neutral(self):
        row = _make_row(rsi_value=50, bollinger_position=50, kdj_signal="中性")
        result = _score_oversold_overbought(row)
        assert result["score"] == 50
        assert "无明显" in result["detail"]

    def test_conflicting(self):
        row = _make_row(rsi_value=15, bollinger_position=90, kdj_signal="中性")
        result = _score_oversold_overbought(row)
        assert result["score"] == 50
        assert "矛盾" in result["detail"]

    def test_weight(self):
        assert _score_oversold_overbought(_make_row())["weight"] == 0.15


class TestScoreVolume:
    def test_always_neutral(self):
        result = _score_volume(_make_row())
        assert result["score"] == 50
        assert "成交量" in result["detail"]
        assert result["weight"] == 0.10


class TestComputeSignalScore:
    def test_returns_dict(self):
        row = _make_row()
        result = compute_signal_score(row)
        assert isinstance(result, dict)
        assert "total_score" in result
        assert "grade" in result
        assert "signals" in result

    def test_total_score_range(self):
        result = compute_signal_score(_make_row())
        assert 0 <= result["total_score"] <= 100

    def test_grade_strong_buy(self):
        row = _make_row(ma_signal="多头排列", trend="上升趋势",
                        macd_signal="金叉", rsi_value=20, kdj_signal="金叉",
                        bollinger_position=5, atr_pct=0.5)
        result = compute_signal_score(row)
        assert result["grade"] in ("强烈买入", "买入")

    def test_grade_strong_sell(self):
        row = _make_row(ma_signal="空头排列", trend="下降趋势",
                        macd_signal="死叉", rsi_value=85, kdj_signal="死叉",
                        bollinger_position=95, atr_pct=5.0)
        result = compute_signal_score(row)
        assert result["grade"] in ("卖出", "强烈卖出")

    def test_grade_hold(self):
        row = _make_row(ma_signal="中性", trend="震荡整理",
                        macd_signal="中性", rsi_value=50, kdj_signal="中性",
                        bollinger_position=50, atr_pct=2.0)
        result = compute_signal_score(row)
        assert result["grade"] == "持有"

    def test_signal_keys(self):
        result = compute_signal_score(_make_row())
        expected_keys = {"trend", "momentum", "volatility", "oversold_overbought", "volume"}
        assert set(result["signals"].keys()) == expected_keys

    def test_neutral_score(self):
        """All-neutral row should produce score close to 50."""
        result = compute_signal_score(_make_row())
        assert 45 <= result["total_score"] <= 55


class TestComputeSignalScores:
    def test_empty_df(self):
        assert compute_signal_scores(pd.DataFrame()).empty

    def test_batch_output(self):
        df = pd.DataFrame([
            _make_row(code="A"),
            _make_row(code="B", ma_signal="多头排列"),
        ])
        result = compute_signal_scores(df)
        assert len(result) == 2
        assert "total_score" in result.columns
        assert "grade" in result.columns

    def test_batch_sorted(self):
        df = pd.DataFrame([
            _make_row(code="A", ma_signal="空头排列", trend="下降趋势"),
            _make_row(code="B", ma_signal="多头排列", trend="上升趋势"),
        ])
        result = compute_signal_scores(df)
        # B should have higher score than A
        score_a = result[result["code"] == "A"]["total_score"].iloc[0]
        score_b = result[result["code"] == "B"]["total_score"].iloc[0]
        assert score_b > score_a
