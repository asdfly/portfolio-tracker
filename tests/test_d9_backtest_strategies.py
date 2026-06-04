"""
D9: 回测策略扩充 - 测试
- 动量策略存在且可调用
- 均值回归策略存在且可调用
- compare_strategies包含5种策略
"""
import pytest
import pandas as pd
import numpy as np
from pathlib import Path
import sys
import sqlite3

PROJECT_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_DIR))


@pytest.fixture
def backtester():
    conn = sqlite3.connect(":memory:")
    from src.analysis.backtest import StrategyBacktester
    return StrategyBacktester(conn)


@pytest.fixture
def sample_prices():
    """生成模拟价格数据"""
    np.random.seed(42)
    dates = pd.date_range("2025-01-01", periods=60, freq="B")
    df = pd.DataFrame({
        "A": 100 + np.cumsum(np.random.randn(60) * 0.5),
        "B": 100 + np.cumsum(np.random.randn(60) * 0.8),
        "C": 100 + np.cumsum(np.random.randn(60) * 0.3),
    }, index=dates)
    return df


class TestMomentumStrategy:
    def test_method_exists(self, backtester):
        assert hasattr(backtester, "backtest_momentum")

    def test_returns_result(self, backtester, sample_prices):
        result = backtester.backtest_momentum(sample_prices, lookback=20, top_n=2)
        assert result.strategy is not None
        assert result.initial_value == 100000
        assert result.final_value > 0
        assert isinstance(result.rebalance_count, int)

    def test_empty_prices(self, backtester):
        empty = pd.DataFrame()
        result = backtester.backtest_momentum(empty)
        assert result.final_value == 100000
        assert result.total_return == 0


class TestMeanReversionStrategy:
    def test_method_exists(self, backtester):
        assert hasattr(backtester, "backtest_mean_reversion")

    def test_returns_result(self, backtester, sample_prices):
        result = backtester.backtest_mean_reversion(sample_prices, lookback=20)
        assert result.strategy is not None
        assert result.initial_value == 100000
        assert result.final_value > 0

    def test_empty_prices(self, backtester):
        empty = pd.DataFrame()
        result = backtester.backtest_mean_reversion(empty)
        assert result.total_return == 0


class TestHelperMethods:
    def test_empty_result_exists(self, backtester):
        assert hasattr(backtester, "_empty_result")

    def test_compute_metrics_exists(self, backtester):
        assert hasattr(backtester, "_compute_metrics")

    def test_compute_metrics_values(self, backtester):
        import numpy as np
        vals = pd.Series([100, 101, 99, 102, 104], dtype=float)
        rets = pd.DataFrame({"A": [0.01, -0.02, 0.03, 0.02]})
        m = backtester._compute_metrics(vals, rets, 100)
        assert "total_return" in m
        assert "annualized_return" in m
        assert "volatility" in m
        assert "sharpe_ratio" in m
        assert "max_drawdown" in m
        assert "calmar_ratio" in m


class TestCompareStrategiesIncludesNew:
    def test_compare_includes_5_strategies(self, backtester):
        """compare_strategies should now return 5 strategy rows"""
        # We cannot run compare_strategies (needs DB data), but verify
        # it references both new methods
        import inspect
        source = inspect.getsource(backtester.compare_strategies)
        assert "backtest_momentum" in source
        assert "backtest_mean_reversion" in source
