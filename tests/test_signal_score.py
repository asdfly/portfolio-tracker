"""Tests for src/analysis/signal_score.py"""
import pytest
import pandas as pd
import numpy as np
from src.analysis.signal_score import (
    compute_signal_score, compute_signal_scores,
    _score_trend, _score_momentum, _score_volatility,
    _score_oversold_overbought, _score_volume,
)


def _make_row(**overrides):
    """Helper: create a minimal etf_technical row."""
    defaults = {
        "date": "2026-06-12", "code": "510300",
        "ma_signal": "中性", "macd_signal": "中性",
        "rsi_value": 50.0, "rsi_status": "正常",
        "kdj_signal": "中性", "bollinger_position": 50.0,
        "atr_pct": 2.0, "trend": "震荡整理",
    }
    defaults.update(overrides)
    return pd.Series(defaults)


class TestScoreTrend:
    def test_bullish_alignment(self):
        row = _make_row(ma_signal="多头排列", trend="上升趋势")
        r = _score_trend(row)
        assert r["score"] == 96  # 100*0.6 + 90*0.4
        assert "趋势强劲" in r["detail"]

    def test_bearish_alignment(self):
        row = _make_row(ma_signal="空头排列", trend="下降趋势")
        r = _score_trend(row)
        assert r["score"] == 4  # 0*0.6 + 10*0.4
        assert "趋势疲弱" in r["detail"]

    def test_neutral(self):
        row = _make_row(ma_signal="中性", trend="震荡整理")
        r = _score_trend(row)
        assert r["score"] == 50
        assert "方向不明" in r["detail"]

    def test_weight(self):
        r = _score_trend(_make_row())
        assert r["weight"] == 0.30


class TestScoreMomentum:
    def test_golden_cross_rsi_oversold(self):
        row = _make_row(macd_signal="金叉", rsi_value=25.0, rsi_status="超卖", kdj_signal="超卖")
        r = _score_momentum(row)
        assert r["score"] > 80

    def test_death_cross_rsi_overbought(self):
        row = _make_row(macd_signal="死叉", rsi_value=80.0, rsi_status="超买", kdj_signal="超买")
        r = _score_momentum(row)
        assert r["score"] < 20

    def test_neutral_rsi(self):
        row = _make_row(rsi_value=50.0)
        r = _score_momentum(row)
        assert r["score"] == 50

    def test_weight(self):
        r = _score_momentum(_make_row())
        assert r["weight"] == 0.25


class TestScoreVolatility:
    def test_boll_lower_atr_low(self):
        row = _make_row(bollinger_position=5.0, atr_pct=0.8)
        r = _score_volatility(row)
        assert r["score"] > 80  # near lower band + low ATR

    def test_boll_upper_atr_high(self):
        row = _make_row(bollinger_position=95.0, atr_pct=5.0)
        r = _score_volatility(row)
        assert r["score"] < 20

    def test_midpoint(self):
        row = _make_row(bollinger_position=50.0, atr_pct=2.0)
        r = _score_volatility(row)
        assert 45 <= r["score"] <= 55

    def test_nan_boll(self):
        row = _make_row(bollinger_position=np.nan, atr_pct=2.0)
        r = _score_volatility(row)
        assert r["score"] == 50  # 50*0.65 + 50*0.35

    def test_weight(self):
        r = _score_volatility(_make_row())
        assert r["weight"] == 0.20


class TestScoreOB:
    def test_oversold(self):
        row = _make_row(rsi_value=25.0, bollinger_position=15.0, kdj_signal="超卖")
        r = _score_oversold_overbought(row)
        assert r["score"] > 80
        assert "超卖" in r["detail"]

    def test_overbought(self):
        row = _make_row(rsi_value=80.0, bollinger_position=85.0, kdj_signal="超买")
        r = _score_oversold_overbought(row)
        assert r["score"] < 20
        assert "超买" in r["detail"]

    def test_mixed_signals(self):
        row = _make_row(rsi_value=25.0, bollinger_position=85.0, kdj_signal="中性")
        r = _score_oversold_overbought(row)
        assert r["score"] == 50
        assert "矛盾" in r["detail"]

    def test_weight(self):
        r = _score_oversold_overbought(_make_row())
        assert r["weight"] == 0.15


class TestScoreVolume:
    def test_always_neutral(self):
        r = _score_volume(_make_row())
        assert r["score"] == 50
        assert r["weight"] == 0.10


class TestComputeSignalScore:
    def test_grade_boundaries(self):
        # All bullish -> strong buy
        bullish = _make_row(ma_signal="多头排列", trend="上升趋势",
                              macd_signal="金叉", rsi_value=20.0, kdj_signal="超卖",
                              bollinger_position=5.0, atr_pct=1.0)
        r = compute_signal_score(bullish)
        assert r["grade"] == "强烈买入"
        assert r["total_score"] >= 75

    def test_all_bearish(self):
        bearish = _make_row(ma_signal="空头排列", trend="下降趋势",
                              macd_signal="死叉", rsi_value=90.0, kdj_signal="超买",
                              bollinger_position=95.0, atr_pct=5.0)
        r = compute_signal_score(bearish)
        assert r["grade"] in ("卖出", "强烈卖出")
        assert r["total_score"] < 30

    def test_neutral(self):
        r = compute_signal_score(_make_row())
        assert r["grade"] == "持有"
        assert r["total_score"] == 50.0

    def test_return_structure(self):
        r = compute_signal_score(_make_row())
        assert "total_score" in r
        assert "grade" in r
        assert "signals" in r
        assert len(r["signals"]) == 5

    def test_score_range(self):
        r = compute_signal_score(_make_row())
        assert 0 <= r["total_score"] <= 100
        for dim in r["signals"].values():
            assert 0 <= dim["score"] <= 100


class TestComputeSignalScores:
    def test_empty_df(self):
        r = compute_signal_scores(pd.DataFrame())
        assert r.empty
        assert len(r) == 0

    def test_batch(self):
        df = pd.DataFrame([_make_row(code="A"), _make_row(code="B", rsi_value=20.0)])
        r = compute_signal_scores(df)
        assert len(r) == 2
        assert "total_score" in r.columns
        assert "grade" in r.columns
        # Lower RSI should give higher score
        assert r.loc[r["code"]=="B", "total_score"].values[0] > r.loc[r["code"]=="A", "total_score"].values[0]

    def test_output_columns(self):
        df = pd.DataFrame([_make_row()])
        r = compute_signal_scores(df)
        expected = {"code", "date", "total_score", "grade",
                  "trend_score", "momentum_score", "volatility_score",
                  "ob_score", "volume_score"}
        assert expected.issubset(set(r.columns))
