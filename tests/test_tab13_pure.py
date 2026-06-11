"""Tests for tabs/tab13_data_quality.py pure functions."""
import pytest, pandas as pd
from tabs.tab13_data_quality import _backtest_summary, _coverage_table, _score_ring, _freshness_heatmap

class TestBacktestSummary:
    def test_basic(self):
        data = {"sharpe": 1.5, "max_dd": -0.1, "total_return": 0.2}
        r = _backtest_summary(data)
        assert isinstance(r, pd.DataFrame)

    def test_empty(self):
        r = _backtest_summary({})
        assert isinstance(r, pd.DataFrame)

class TestCoverageTable:
    def test_basic(self):
        data = {"positions": {"total_rows": 100, "distinct_codes": 50, "date_range": "2024-01~2024-12"}}
        r = _coverage_table(data)
        assert isinstance(r, pd.DataFrame)

    def test_empty(self):
        r = _coverage_table({})
        assert isinstance(r, pd.DataFrame)

class TestScoreRing:
    def test_basic(self):
        r = _score_ring(85.5, "A")
        assert r is not None

    def test_zero_score(self):
        r = _score_ring(0, "F")
        assert r is not None

class TestFreshnessHeatmap:
    def test_basic(self):
        data = [{"label": "DB", "status": "OK", "days_lag": 0}, {"label": "API", "status": "WARN", "days_lag": 2}]
        r = _freshness_heatmap(data)
        assert r is not None

    def test_empty(self):
        r = _freshness_heatmap([])
        assert r is not None