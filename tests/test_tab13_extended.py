"""Extended tests for tab13_data_quality.py."""
import pytest
import pandas as pd
import plotly.graph_objects as go


class TestScoreRing:
    def test_basic(self):
        from tabs.tab13_data_quality import _score_ring
        fig = _score_ring(85.5, "A-")
        assert isinstance(fig, go.Figure)

    def test_zero_score(self):
        from tabs.tab13_data_quality import _score_ring
        fig = _score_ring(0, "F")
        assert isinstance(fig, go.Figure)

    def test_full_score(self):
        from tabs.tab13_data_quality import _score_ring
        fig = _score_ring(100, "A+")
        assert isinstance(fig, go.Figure)


class TestFreshnessHeatmap:
    def test_mixed_status(self):
        from tabs.tab13_data_quality import _freshness_heatmap
        data = [
            {"label": "DB1", "status": "OK", "days_lag": 0},
            {"label": "DB2", "status": "WARN", "days_lag": 3},
            {"label": "DB3", "status": "STALE", "days_lag": 7},
        ]
        fig = _freshness_heatmap(data)
        assert isinstance(fig, go.Figure)

    def test_all_ok(self):
        from tabs.tab13_data_quality import _freshness_heatmap
        data = [{"label": f"DB{i}", "status": "OK", "days_lag": 0} for i in range(5)]
        fig = _freshness_heatmap(data)
        assert isinstance(fig, go.Figure)

    def test_empty(self):
        from tabs.tab13_data_quality import _freshness_heatmap
        fig = _freshness_heatmap([])
        assert isinstance(fig, go.Figure)


class TestCoverageTable:
    def test_basic(self):
        from tabs.tab13_data_quality import _coverage_table
        data = {
            "snapshots": {"total_rows": 1000, "distinct_codes": 5, "date_range": "30 days"},
            "prices": {"total_rows": 500, "distinct_codes": 3, "date_range": "60 days"},
        }
        df = _coverage_table(data)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert "数据表" in df.columns

    def test_empty(self):
        from tabs.tab13_data_quality import _coverage_table
        df = _coverage_table({})
        assert isinstance(df, pd.DataFrame) and df.empty


class TestBacktestSummary:
    def test_basic(self):
        from tabs.tab13_data_quality import _backtest_summary
        data = {"periods_per_indicator": {"MA5": 30, "MA20": 20, "RSI": 10}}
        df = _backtest_summary(data)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert "指标名称" in df.columns and "覆盖周期数" in df.columns

    def test_empty(self):
        from tabs.tab13_data_quality import _backtest_summary
        df = _backtest_summary({"periods_per_indicator": {}})
        assert isinstance(df, pd.DataFrame) and df.empty

    def test_no_periods_key(self):
        from tabs.tab13_data_quality import _backtest_summary
        df = _backtest_summary({})
        assert isinstance(df, pd.DataFrame) and df.empty
