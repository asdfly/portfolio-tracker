"""Tests for tabs/tab5_advanced.py pure functions."""
import pytest, pandas as pd, numpy as np
from tabs.tab5_advanced import format_value, _cleanse_daily_returns

class TestFormatValue:
    def test_basic(self):
        assert format_value(1234.5) == "1,234.50"

    def test_none(self):
        assert format_value(None) == "N/A"

    def test_nan(self):
        assert format_value(float("nan")) == "N/A"

    def test_string(self):
        assert format_value("abc") == "abc"

    def test_prefix_suffix(self):
        assert "$" in format_value(100, prefix="$")
        assert "%" in format_value(50, suffix="%")

    def test_decimals(self):
        r = format_value(0.1234, decimals=4)
        assert "0.1234" in r

    def test_negative(self):
        r = format_value(-1234.5)
        assert "-" in r and "1,234.50" in r

class TestCleanseDailyReturns:
    def test_no_outliers(self):
        df = pd.DataFrame({"date": pd.date_range("2025-01-01", periods=3, freq="B"),
                           "daily_return": [0.01, -0.02, 0.03]})
        result = _cleanse_daily_returns(df, suspect_dates=set())
        assert isinstance(result, tuple) and len(result) == 2
        stats = result[1]
        assert stats["filtered"] == 0

    def test_returns_tuple(self):
        df = pd.DataFrame({"date": pd.date_range("2025-01-01", periods=1, freq="B"),
                           "daily_return": [0.01]})
        result = _cleanse_daily_returns(df)
        assert isinstance(result[0], pd.DataFrame)
        assert isinstance(result[1], dict)

    def test_large_dataset_passthrough(self):
        df = pd.DataFrame({"date": pd.date_range("2025-01-01", periods=1000, freq="B"),
                           "daily_return": np.random.normal(0, 0.02, 1000)})
        result = _cleanse_daily_returns(df)
        assert result[1]["tailed"] == 0 and len(result[0]) == 1000

    def test_all_zeros(self):
        df = pd.DataFrame({"date": pd.date_range("2025-01-01", periods=10, freq="B"),
                           "daily_return": [0.0] * 10})
        result = _cleanse_daily_returns(df)
        assert len(result[0]) == 10