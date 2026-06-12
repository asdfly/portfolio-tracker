"""Tests for src/analysis/signal_score.py"""

import pandas as pd
import pytest
from src.analysis.signal_score import (
    compute_signal_score,
    compute_signal_scores,
    _score_trend,
    _score_momentum,
    _score_volatility,
    _score_oversold_overbought,
    _score_volume,
)


def _make_row(**kwargs):
    defaults = {
        "code": "510300", "date": "2026-06-11",
        "ma_signal": "中性", "macd_signal": "中性",
        "rsi_value": 50.0, "rsi_status": "正常",
        "kdj_signal": "中性", "bollinger_position": 50.0,
        "atr_pct": 2.0, "trend": "震荡整理",
    }
    defaults.update(kwargs)
    return pd.Series(defaults)


class TestScoreTrend:
    def test_bullish_alignment(self):
        row = _make_row(ma_signal="多头排列", trend="上升趋势")
        result = _score_trend(row)
        assert result["score"] >= 75
        assert "强劲" in result["detail"]

    def test_bearish_alignment(self):
        row = _make_row(ma_signal="空头排列", trend="下降趋势")
        result = _score_trend(row)
        assert result["score"] <= 35
        assert "疲弱" in result["detail"]

    def test_neutral(self):
        row = _make_row(ma_signal="中性", trend="震荡整理")
        result = _score_trend(row)
        assert 40 <= result["score"] <= 60

    def test_weight(self):
        row = _make_row()
        assert _score_trend(row)["weight"] == 0.30

    def test_all_ma_signals(self):
        for signal in ["多头排列", "多头", "中性", "空头", "空头排列"]:
            row = _make_row(ma_signal=signal, trend="震荡整理")
            result = _score_trend(row)
            assert 0 <= result["score"] <= 100


class TestScoreMomentum:
    def test_golden_cross_low_rsi(self):
        row = _make_row(macd_signal="金叉", rsi_value=25.0, rsi_status="超卖", kdj_signal="金叉")
        result = _score_momentum(row)
        assert result["score"] >= 75

    def test_death_cross_high_rsi(self):
        row = _make_row(macd_signal="死叉", rsi_value=80.0, rsi_status="超买", kdj_signal="死叉")
        result = _score_momentum(row)
        assert result["score"] <= 25

    def test_neutral_momentum(self):
        row = _make_row(macd_signal="中性", rsi_value=50.0, kdj_signal="中性")
        result = _score_momentum(row)
        assert 40 <= result["score"] <= 60

    def test_rsi_extreme_oversold(self):
        row = _make_row(rsi_value=10.0, rsi_status="超卖")
        result = _score_momentum(row)
        assert result["score"] >= 65

    def test_rsi_extreme_overbought(self):
        row = _make_row(rsi_value=90.0, rsi_status="超买")
        result = _score_momentum(row)
        assert result["score"] <= 35

    def test_weight(self):
        row = _make_row()
        assert _score_momentum(row)["weight"] == 0.25


class TestScoreVolatility:
    def test_boll_lower_band(self):
        row = _make_row(bollinger_position=5.0, atr_pct=1.0)
        result = _score_volatility(row)
        assert result["score"] >= 75

    def test_boll_upper_band(self):
        row = _make_row(bollinger_position=95.0, atr_pct=1.0)
        result = _score_volatility(row)
        assert result["score"] <= 40

    def test_normal_boll(self):
        row = _make_row(bollinger_position=50.0, atr_pct=2.0)
        result = _score_volatility(row)
        assert 40 <= result["score"] <= 60

    def test_high_atr(self):
        row = _make_row(bollinger_position=50.0, atr_pct=5.0)
        result = _score_volatility(row)
        assert result["score"] < 50

    def test_weight(self):
        row = _make_row()
        assert _score_volatility(row)["weight"] == 0.20


class TestScoreOversoldOverbought:
    def test_oversold_signals(self):
        row = _make_row(rsi_value=20.0, bollinger_position=10.0, kdj_signal="超卖")
        result = _score_oversold_overbought(row)
        assert result["score"] > 50
        assert "反弹" in result["detail"]

    def test_overbought_signals(self):
        row = _make_row(rsi_value=80.0, bollinger_position=90.0, kdj_signal="超买")
        result = _score_oversold_overbought(row)
        assert result["score"] < 50
        assert "回调" in result["detail"]

    def test_mixed_signals(self):
        row = _make_row(rsi_value=20.0, bollinger_position=90.0, kdj_signal="中性")
        result = _score_oversold_overbought(row)
        assert result["score"] == 50
        assert "矛盾" in result["detail"]

    def test_no_extreme_signals(self):
        row = _make_row(rsi_value=50.0, bollinger_position=50.0, kdj_signal="中性")
        result = _score_oversold_overbought(row)
        assert result["score"] == 50

    def test_weight(self):
        row = _make_row()
        assert _score_oversold_overbought(row)["weight"] == 0.15


class TestScoreVolume:
    def test_always_neutral(self):
        row = _make_row()
        result = _score_volume(row)
        assert result["score"] == 50

    def test_weight(self):
        row = _make_row()
        assert _score_volume(row)["weight"] == 0.10


class TestComputeSignalScore:
    def test_bullish_composite(self):
        row = _make_row(
            ma_signal="多头排列", trend="上升趋势", macd_signal="金叉",
            rsi_value=55.0, rsi_status="正常", kdj_signal="金叉",
            bollinger_position=60.0, atr_pct=1.2,
        )
        result = compute_signal_score(row)
        assert result["total_score"] >= 60
        assert result["grade"] in ("买入", "强烈买入")
        assert "total_score" in result
        assert "grade" in result
        assert "signals" in result
        assert len(result["signals"]) == 5

    def test_bearish_composite(self):
        row = _make_row(
            ma_signal="空头排列", trend="下降趋势", macd_signal="死叉",
            rsi_value=80.0, rsi_status="超买", kdj_signal="死叉",
            bollinger_position=95.0, atr_pct=4.0,
        )
        result = compute_signal_score(row)
        assert result["total_score"] <= 40
        assert result["grade"] in ("卖出", "强烈卖出")

    def test_neutral_composite(self):
        row = _make_row()
        result = compute_signal_score(row)
        assert 30 <= result["total_score"] <= 70
        assert result["grade"] in ("持有", "买入", "卖出")

    def test_score_range(self):
        for ma in ["多头排列", "空头排列"]:
            for rsi in [20.0, 50.0, 80.0]:
                row = _make_row(ma_signal=ma, rsi_value=rsi)
                result = compute_signal_score(row)
                assert 0 <= result["total_score"] <= 100

    def test_grade_boundaries(self):
        assert compute_signal_score(_make_row())["grade"] in (
            "强烈买入", "买入", "持有", "卖出", "强烈卖出")


class TestComputeSignalScores:
    def test_empty_df(self):
        result = compute_signal_scores(pd.DataFrame())
        assert result.empty

    def test_batch(self):
        df = pd.DataFrame([
            _make_row(code="510300", ma_signal="多头排列", trend="上升趋势"),
            _make_row(code="159949", ma_signal="空头排列", trend="下降趋势"),
        ])
        result = compute_signal_scores(df)
        assert len(result) == 2
        assert "total_score" in result.columns
        assert "grade" in result.columns
        assert result.iloc[0]["total_score"] > result.iloc[1]["total_score"]

    def test_columns(self):
        df = pd.DataFrame([_make_row()])
        result = compute_signal_scores(df)
        expected_cols = ["code", "date", "total_score", "grade",
                        "trend_score", "momentum_score",
                        "volatility_score", "ob_score", "volume_score"]
        for col in expected_cols:
            assert col in result.columns
