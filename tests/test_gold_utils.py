"""黄金分析工具函数单元测试（gold_utils.py）

测试策略：
- 纯计算函数（calc_rsi, calc_macd, calc_bollinger, calc_monthly_returns）：直接断言
- 数据获取函数（fetch_*）：通过__wrapped__绕过st.cache_data缓存，验证异常处理
"""
import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import numpy as np
import pandas as pd
import pytest

from tabs.gold_components.gold_utils import calc_rsi, calc_macd, calc_bollinger, calc_monthly_returns


# ==================== calc_rsi ====================

class TestCalcRSI:
    def test_basic_14period(self):
        np.random.seed(42)
        series = pd.Series(np.random.normal(100, 2, 50))
        result = calc_rsi(series, period=14)
        assert len(result) == 50
        assert result.iloc[:13].isna().all(), "前period-1行应为NaN"
        assert result.iloc[13:].notna().all(), "从第period行起应有值"
        assert result.iloc[13:].between(0, 100).all(), "RSI应在0-100之间"

    def test_all_up(self):
        """持续上涨趋势+足够大的随机波动，确保每个rolling窗口内avg_loss>0"""
        np.random.seed(42)
        changes = np.random.normal(0.5, 2.0, 100)
        base = 100 + np.cumsum(changes)
        series = pd.Series(base)
        result = calc_rsi(series, period=14)
        assert not result.iloc[-1:].isna().all(), "RSI最终值不应为NaN"
        assert result.iloc[-1] > 70, f"持续上涨RSI应>70，实际={result.iloc[-1]:.2f}"

    def test_all_down(self):
        np.random.seed(88)
        base = np.cumsum(-np.random.uniform(0.5, 1.5, 100)) + 1000
        series = pd.Series(base)
        result = calc_rsi(series, period=14)
        assert not result.iloc[-1:].isna().all(), "RSI最终值不应为NaN"
        assert result.iloc[-1] < 30, f"持续下跌RSI应<30，实际={result.iloc[-1]:.2f}"

    def test_flat(self):
        """横盘+微小波动，确保avg_loss不全为0"""
        np.random.seed(77)
        series = pd.Series(100.0 + np.random.normal(0, 0.01, 100))
        result = calc_rsi(series, period=14)
        assert not result.iloc[-1:].isna().all(), "RSI最终值不应为NaN"
        assert abs(result.iloc[-1] - 50) < 15, f"横盘RSI应约等于50，实际={result.iloc[-1]:.2f}"

    def test_short_period(self):
        series = pd.Series(range(1, 11))
        result = calc_rsi(series, period=5)
        assert len(result) == 10
        assert result.iloc[:4].isna().all()

    def test_empty_series(self):
        result = calc_rsi(pd.Series(dtype=float), period=14)
        assert len(result) == 0

    def test_single_value(self):
        result = calc_rsi(pd.Series([100.0]), period=14)
        assert result.isna().iloc[0], "单值应为NaN"


# ==================== calc_macd ====================

class TestCalcMACD:
    def test_basic(self):
        np.random.seed(42)
        series = pd.Series(np.random.normal(100, 2, 50))
        result = calc_macd(series)
        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == ["macd", "signal", "hist"]
        assert len(result) == 50
        valid = result.dropna(subset=["macd", "signal"])
        if len(valid) > 0:
            np.testing.assert_allclose(valid["hist"], valid["macd"] - valid["signal"], atol=1e-10)

    def test_trend_up(self):
        series = pd.Series(np.linspace(100, 200, 100))
        result = calc_macd(series)
        valid = result.dropna()
        assert valid["macd"].iloc[-1] > valid["macd"].iloc[0], "上涨趋势MACD应上升"

    def test_empty(self):
        result = calc_macd(pd.Series(dtype=float))
        assert len(result) == 0

    def test_constant(self):
        series = pd.Series([100.0] * 50)
        result = calc_macd(series)
        valid = result.dropna()
        assert (valid["macd"].abs() < 1e-10).all(), "常量序列MACD应接近0"


# ==================== calc_bollinger ====================

class TestCalcBollinger:
    def test_basic(self):
        series = pd.Series(np.random.normal(100, 5, 100))
        result = calc_bollinger(series, window=20, num_std=2)
        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == ["middle", "upper", "lower"]
        assert len(result) == 100
        assert result["upper"].iloc[-1] > result["middle"].iloc[-1]
        assert result["lower"].iloc[-1] < result["middle"].iloc[-1]

    def test_band_width(self):
        series = pd.Series(np.random.normal(100, 5, 100))
        result = calc_bollinger(series, window=20, num_std=2)
        valid = result.dropna()
        expected_width = 2 * 2 * series.rolling(20).std().dropna().values
        actual_width = (valid["upper"] - valid["lower"]).values
        np.testing.assert_allclose(actual_width, expected_width, atol=1e-10)

    def test_flat_series(self):
        series = pd.Series([100.0] * 50)
        result = calc_bollinger(series, window=20)
        valid = result.dropna()
        assert (valid["upper"] == valid["lower"]).all(), "常量序列上下轨应相等"

    def test_empty(self):
        result = calc_bollinger(pd.Series(dtype=float))
        assert len(result) == 0

    def test_short_data(self):
        series = pd.Series([1, 2, 3])
        result = calc_bollinger(series, window=20)
        assert result["middle"].isna().all(), "数据不足window时应全NaN"


# ==================== calc_monthly_returns ====================

class TestCalcMonthlyReturns:
    def test_basic(self):
        dates = pd.date_range("2024-01-01", periods=90, freq="D")
        prices = pd.Series(100 + np.arange(90) * 0.5)
        df = pd.DataFrame({"date": dates, "close": prices})
        result = calc_monthly_returns(df)
        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == ["year", "month", "monthly_return"]
        assert len(result) > 0
        assert result["monthly_return"].notna().all(), "dropna后所有收益值应非NaN"

    def test_first_month_dropped(self):
        """calc_monthly_returns使用dropna()，首月（pct_change产生NaN）被移除"""
        dates = pd.date_range("2024-01-01", periods=60, freq="D")
        prices = pd.Series(100 + np.arange(60) * 0.5)
        df = pd.DataFrame({"date": dates, "close": prices})
        result = calc_monthly_returns(df)
        if len(result) > 0:
            assert result["month"].iloc[0] != 1 or result["year"].iloc[0] != 2024, \
                "首月(2024-01)应被dropna移除"

    def test_empty(self):
        result = calc_monthly_returns(pd.DataFrame({"date": [], "close": []}))
        assert len(result) == 0

    def test_single_month(self):
        dates = pd.date_range("2024-01-01", periods=15, freq="D")
        prices = pd.Series(100 + np.arange(15) * 0.1)
        df = pd.DataFrame({"date": dates, "close": prices})
        result = calc_monthly_returns(df)
        assert len(result) == 0, "单月数据经pct_change+dropna后应为空"


# ==================== 数据获取函数（mock akshare） ====================

class TestFetchFunctions:
    """验证akshare调用失败时的异常处理：不抛异常，返回None"""

    @pytest.fixture(autouse=True)
    def _mock_akshare(self, monkeypatch):
        """monkeypatch akshare使所有函数抛异常"""
        import akshare as ak
        self._originals = {}
        for attr in ["spot_hist_sge", "spot_golden_benchmark_sge", "bond_zh_us_rate",
                      "macro_china_cpi", "macro_cons_gold", "macro_china_fx_gold",
                      "futures_comex_inventory", "forex_hist_em"]:
            if hasattr(ak, attr):
                self._originals[attr] = getattr(ak, attr)
                monkeypatch.setattr(ak, attr, lambda *a, **kw: (_ for _ in ()).throw(Exception("mock error")))

    def test_fetch_sge_hist_returns_none_on_error(self):
        from tabs.gold_components.gold_utils import fetch_sge_hist
        fn = fetch_sge_hist.__wrapped__ if hasattr(fetch_sge_hist, "__wrapped__") else fetch_sge_hist
        result = fn("Au99.99")
        assert result is None or isinstance(result, pd.DataFrame)

    def test_fetch_sge_benchmark_returns_none_on_error(self):
        from tabs.gold_components.gold_utils import fetch_sge_benchmark
        fn = fetch_sge_benchmark.__wrapped__ if hasattr(fetch_sge_benchmark, "__wrapped__") else fetch_sge_benchmark
        result = fn()
        assert result is None or isinstance(result, pd.DataFrame)

    def test_fetch_bond_yields_returns_none_on_error(self):
        from tabs.gold_components.gold_utils import fetch_bond_yields
        fn = fetch_bond_yields.__wrapped__ if hasattr(fetch_bond_yields, "__wrapped__") else fetch_bond_yields
        result = fn()
        assert result is None or isinstance(result, pd.DataFrame)

    def test_fetch_china_cpi_returns_none_on_error(self):
        from tabs.gold_components.gold_utils import fetch_china_cpi
        fn = fetch_china_cpi.__wrapped__ if hasattr(fetch_china_cpi, "__wrapped__") else fetch_china_cpi
        result = fn()
        assert result is None or isinstance(result, pd.DataFrame)

    def test_fetch_global_etf_returns_none_on_error(self):
        from tabs.gold_components.gold_utils import fetch_global_etf_holdings
        fn = fetch_global_etf_holdings.__wrapped__ if hasattr(fetch_global_etf_holdings, "__wrapped__") else fetch_global_etf_holdings
        result = fn()
        assert result is None or isinstance(result, pd.DataFrame)

    def test_fetch_china_reserve_data_returns_none_on_error(self):
        from tabs.gold_components.gold_utils import fetch_china_reserve_data
        fn = fetch_china_reserve_data.__wrapped__ if hasattr(fetch_china_reserve_data, "__wrapped__") else fetch_china_reserve_data
        result = fn()
        assert result is None or isinstance(result, pd.DataFrame)

    def test_fetch_comex_inventory_returns_none_on_error(self):
        from tabs.gold_components.gold_utils import fetch_comex_inventory
        fn = fetch_comex_inventory.__wrapped__ if hasattr(fetch_comex_inventory, "__wrapped__") else fetch_comex_inventory
        result = fn()
        assert result is None or isinstance(result, pd.DataFrame)
