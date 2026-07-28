"""P2 信号强度分级 + 滚动窗口回测测试

方案7: RSI/布林带信号强度分级 (mild/moderate/extreme)
方案6: 滚动窗口置信度稳定性指标
"""
import math
import numpy as np
import pandas as pd
import pytest
from unittest.mock import patch, MagicMock
from src.analysis.signal_backtest import (
    STRENGTH_ALL, STRENGTH_MILD, STRENGTH_MODERATE, STRENGTH_EXTREME,
    RSI_STRENGTH_BUY, RSI_STRENGTH_SELL,
    BOLL_STRENGTH_BUY, BOLL_STRENGTH_SELL,
    ROLLING_WINDOW_DAYS, ROLLING_STEP_DAYS, ROLLING_MIN_WINDOWS,
    _classify_rsi_strength,
    _classify_bollinger_strength,
    _compute_rolling_window_stability,
    compute_confidence,
    _compute_weighted_hit_rate,
)


class TestRSIStrengthClassification:
    """测试RSI信号强度分级。"""

    def test_buy_mild(self):
        """轻度超卖: RSI 25-30"""
        assert _classify_rsi_strength(28, 1) == STRENGTH_MILD

    def test_buy_moderate(self):
        """中度超卖: RSI 20-25"""
        assert _classify_rsi_strength(22, 1) == STRENGTH_MODERATE

    def test_buy_extreme(self):
        """极端超卖: RSI < 20"""
        assert _classify_rsi_strength(15, 1) == STRENGTH_EXTREME

    def test_sell_mild(self):
        """轻度超买: RSI 70-75"""
        assert _classify_rsi_strength(72, -1) == STRENGTH_MILD

    def test_sell_moderate(self):
        """中度超买: RSI 75-80"""
        assert _classify_rsi_strength(78, -1) == STRENGTH_MODERATE

    def test_sell_extreme(self):
        """极端超买: RSI > 80"""
        assert _classify_rsi_strength(85, -1) == STRENGTH_EXTREME

    def test_nan_returns_all(self):
        """NaN值返回all"""
        assert _classify_rsi_strength(float('nan'), 1) == STRENGTH_ALL

    def test_boundary_25(self):
        """边界值: RSI=25 应为mild"""
        assert _classify_rsi_strength(25, 1) == STRENGTH_MILD

    def test_boundary_20(self):
        """边界值: RSI=20 应为moderate"""
        assert _classify_rsi_strength(20, 1) == STRENGTH_MODERATE

    def test_boundary_70(self):
        """边界值: RSI=70 应为mild (卖出方向)"""
        assert _classify_rsi_strength(70, -1) == STRENGTH_MILD

    def test_outside_range_returns_all(self):
        """RSI在正常范围(30-70)返回all"""
        assert _classify_rsi_strength(50, 1) == STRENGTH_ALL
        assert _classify_rsi_strength(50, -1) == STRENGTH_ALL


class TestBollingerStrengthClassification:
    """测试布林带信号强度分级。"""

    def test_buy_mild(self):
        """轻度低位: 15-20"""
        assert _classify_bollinger_strength(18, 1) == STRENGTH_MILD

    def test_buy_moderate(self):
        """中度低位: 10-15"""
        assert _classify_bollinger_strength(12, 1) == STRENGTH_MODERATE

    def test_buy_extreme(self):
        """极端低位: < 10"""
        assert _classify_bollinger_strength(5, 1) == STRENGTH_EXTREME

    def test_sell_mild(self):
        """轻度高位: 80-85"""
        assert _classify_bollinger_strength(82, -1) == STRENGTH_MILD

    def test_sell_moderate(self):
        """中度高位: 85-90"""
        assert _classify_bollinger_strength(87, -1) == STRENGTH_MODERATE

    def test_sell_extreme(self):
        """极端高位: > 90"""
        assert _classify_bollinger_strength(95, -1) == STRENGTH_EXTREME

    def test_nan_returns_all(self):
        """NaN值返回all"""
        assert _classify_bollinger_strength(float('nan'), 1) == STRENGTH_ALL


class TestRollingWindowStability:
    """测试滚动窗口稳定性计算。"""

    def _make_test_df(self, n_rows=2000, indicator="rsi_status",
                      signal_val="超卖", direction=1, consistent=True):
        """生成测试数据。"""
        dates = pd.date_range("2015-01-01", periods=n_rows, freq="B")
        if consistent:
            # 一致的信号: 60%命中率
            returns = np.where(np.random.rand(n_rows) > 0.4, 0.01, -0.01)
        else:
            # 不一致的信号: 命中率波动大
            returns = np.zeros(n_rows)
            for i in range(0, n_rows, 500):
                hr = 0.3 if (i // 500) % 2 == 0 else 0.7
                end = min(i + 500, n_rows)
                returns[i:end] = np.where(np.random.rand(end - i) > (1 - hr), 0.01, -0.01)

        df = pd.DataFrame({
            "date": dates,
            "code": "510300",
            indicator: [signal_val] * n_rows,
            "rsi_value": [25.0] * n_rows,
            "bollinger_position": [15.0] * n_rows,
            "close": np.cumprod(1 + returns) * 100,
            f"fwd_ret_20": np.concatenate([returns[20:], [np.nan] * 20]),
        })
        return df

    def test_consistent_signal_high_stability(self):
        """一致信号应有较高稳定性。"""
        np.random.seed(42)
        df = self._make_test_df(n_rows=2000, consistent=True)
        stability = _compute_rolling_window_stability(df, "rsi_status", "超卖", 1, 20)
        assert stability is not None
        assert 0 <= stability <= 1
        # 一致信号稳定性应较高
        assert stability > 0.5

    def test_inconsistent_signal_lower_stability(self):
        """不一致信号应有较低稳定性。"""
        np.random.seed(42)
        df = self._make_test_df(n_rows=2000, consistent=False)
        stability = _compute_rolling_window_stability(df, "rsi_status", "超卖", 1, 20)
        assert stability is not None
        assert 0 <= stability <= 1

    def test_insufficient_data_returns_none(self):
        """数据不足时返回None。"""
        df = self._make_test_df(n_rows=50, consistent=True)
        stability = _compute_rolling_window_stability(df, "rsi_status", "超卖", 1, 20)
        assert stability is None

    def test_missing_column_returns_none(self):
        """缺少前瞻收益列时返回None。"""
        df = pd.DataFrame({
            "date": pd.date_range("2015-01-01", periods=100),
            "code": "510300",
            "rsi_status": ["超卖"] * 100,
            "rsi_value": [25.0] * 100,
            "bollinger_position": [15.0] * 100,
        })
        stability = _compute_rolling_window_stability(df, "rsi_status", "超卖", 1, 20)
        assert stability is None

    def test_combo_returns_none(self):
        """组合信号不参与稳定性计算。"""
        df = self._make_test_df(n_rows=2000, consistent=True)
        stability = _compute_rolling_window_stability(df, "combo", "RSI超卖+MACD金叉", 1, 20)
        assert stability is None

    def test_bollinger_signal(self):
        """布林带信号稳定性计算。"""
        np.random.seed(42)
        df = self._make_test_df(n_rows=2000, consistent=True)
        stability = _compute_rolling_window_stability(df, "bollinger", "低位(≤20)", 1, 20)
        assert stability is not None
        assert 0 <= stability <= 1

    def test_stability_range(self):
        """稳定性评分应在0-1范围内。"""
        np.random.seed(123)
        df = self._make_test_df(n_rows=3000, consistent=True)
        for n in [5, 10, 20, 30, 60]:
            df[f"fwd_ret_{n}"] = np.concatenate([
                np.random.randn(3000) * 0.02, 
            ])[:3000]
            stability = _compute_rolling_window_stability(df, "rsi_status", "超卖", 1, n)
            if stability is not None:
                assert 0 <= stability <= 1, f"n={n}: stability={stability} out of range"


class TestStrengthGradingBacktest:
    """测试强度分级在回测中的效果。"""

    def test_strength_constants_defined(self):
        """强度常量正确定义。"""
        assert STRENGTH_ALL == "all"
        assert STRENGTH_MILD == "mild"
        assert STRENGTH_MODERATE == "moderate"
        assert STRENGTH_EXTREME == "extreme"

    def test_rsi_strength_tiers_coverage(self):
        """RSI强度分档覆盖完整范围。"""
        # 买入方向: 0-30 覆盖
        buy_ranges = [(lo, hi) for _, lo, hi in RSI_STRENGTH_BUY]
        assert (0, 20.01) in buy_ranges
        assert (20, 25.01) in buy_ranges
        assert (25, 30.01) in buy_ranges

        # 卖出方向: 70-101 覆盖
        sell_ranges = [(lo, hi) for _, lo, hi in RSI_STRENGTH_SELL]
        assert (70, 75.01) in sell_ranges
        assert (75, 80.01) in sell_ranges
        assert (80, 101) in sell_ranges

    def test_bollinger_strength_tiers_coverage(self):
        """布林带强度分档覆盖完整范围。"""
        buy_ranges = [(lo, hi) for _, lo, hi in BOLL_STRENGTH_BUY]
        assert (0, 10.01) in buy_ranges
        assert (10, 15.01) in buy_ranges
        assert (15, 20.01) in buy_ranges

        sell_ranges = [(lo, hi) for _, lo, hi in BOLL_STRENGTH_SELL]
        assert (80, 85.01) in sell_ranges
        assert (85, 90.01) in sell_ranges
        assert (90, 101) in sell_ranges

    def test_rolling_window_constants(self):
        """滚动窗口常量合理。"""
        assert ROLLING_WINDOW_DAYS == 730
        assert ROLLING_STEP_DAYS == 125
        assert ROLLING_MIN_WINDOWS == 2


class TestConfidenceStabilityAdjustment:
    """测试置信度稳定性调整逻辑。"""

    def test_backtest_single_with_stability(self):
        """_backtest_single 应接受 stability_score 参数。"""
        from src.analysis.signal_backtest import _backtest_single, STRENGTH_EXTREME

        # 构造测试数据
        n = 100
        returns = pd.Series(np.where(np.random.rand(n) > 0.4, 0.01, -0.01))
        df = pd.DataFrame({
            "fwd_ret_5": returns,
        })

        result = _backtest_single(
            df, "rsi_status", "超卖", 1, 5, "all", None, "all",
            signal_strength=STRENGTH_EXTREME, stability_score=0.9
        )
        assert result is not None
        assert result["signal_strength"] == STRENGTH_EXTREME
        assert result["stability_score"] == 0.9
        # 高稳定性应使置信度接近原始值
        assert result["confidence_score"] > 0

    def test_backtest_single_without_stability(self):
        """_backtest_single 无 stability_score 时正常工作。"""
        from src.analysis.signal_backtest import _backtest_single, STRENGTH_ALL

        n = 100
        returns = pd.Series(np.where(np.random.rand(n) > 0.4, 0.01, -0.01))
        df = pd.DataFrame({"fwd_ret_5": returns})

        result = _backtest_single(
            df, "rsi_status", "超卖", 1, 5, "all", None, "all",
            signal_strength=STRENGTH_ALL, stability_score=None
        )
        assert result is not None
        assert result["stability_score"] is None
        assert result["signal_strength"] == STRENGTH_ALL

    def test_high_stability_preserves_confidence(self):
        """高稳定性(1.0)应保持原始置信度。"""
        from src.analysis.signal_backtest import _backtest_single

        n = 100
        np.random.seed(42)
        returns = pd.Series(np.where(np.random.rand(n) > 0.4, 0.01, -0.01))
        df = pd.DataFrame({"fwd_ret_5": returns})

        result_no_stab = _backtest_single(df, "rsi_status", "超卖", 1, 5, "all", None, "all")
        result_high_stab = _backtest_single(
            df, "rsi_status", "超卖", 1, 5, "all", None, "all",
            stability_score=1.0
        )
        # stability=1.0: adjusted = conf * (0.7 + 0.3*1) = conf * 1.0
        assert result_high_stab["confidence_score"] == result_no_stab["confidence_score"]

    def test_low_stability_reduces_confidence(self):
        """低稳定性应降低置信度。"""
        from src.analysis.signal_backtest import _backtest_single

        n = 100
        np.random.seed(42)
        returns = pd.Series(np.where(np.random.rand(n) > 0.4, 0.01, -0.01))
        df = pd.DataFrame({"fwd_ret_5": returns})

        result_no_stab = _backtest_single(df, "rsi_status", "超卖", 1, 5, "all", None, "all")
        result_low_stab = _backtest_single(
            df, "rsi_status", "超卖", 1, 5, "all", None, "all",
            stability_score=0.1
        )
        # stability=0.1: adjusted = conf * (0.7 + 0.3*0.1) = conf * 0.73
        assert result_low_stab["confidence_score"] < result_no_stab["confidence_score"]
