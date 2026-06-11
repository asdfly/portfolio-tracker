"""测试 tabs/tab1_net_value.py 纯数据函数"""

import numpy as np
import pandas as pd
import pytest


class TestCalcRangeMetrics:
    """测试 _calc_range_metrics 纯数据函数"""

    def _import_func(self):
        from tabs.tab1_net_value import _calc_range_metrics
        return _calc_range_metrics

    def _make_range_data(self, values, dates=None):
        """构造 range_data DataFrame"""
        n = len(values)
        if dates is None:
            dates = pd.date_range("2025-01-01", periods=n, freq="B")
        return pd.DataFrame({"total_value": values, "date": dates})

    def test_basic_metrics(self):
        _calc_range_metrics = self._import_func()
        # 每天涨 0.1% 的稳定增长
        values = [100.0 + i * 0.1 for i in range(60)]
        df = self._make_range_data(values)
        r = _calc_range_metrics(df)

        assert r["cum_ret"] > 0  # 正收益
        assert r["ann_ret"] > 0
        assert r["vol"] > 0
        assert r["dd"] <= 0  # 回撤为负
        assert r["n_days"] == 59  # 60个点, 59个日收益率
        assert 0 <= r["wr"] <= 100
        assert r["pnl_ratio"] > 0

    def test_declining_values(self):
        _calc_range_metrics = self._import_func()
        values = [100.0 - i * 0.2 for i in range(60)]
        df = self._make_range_data(values)
        r = _calc_range_metrics(df)

        assert r["cum_ret"] < 0
        assert r["dd"] < 0  # 有回撤
        assert r["wr"] < 100  # 不会全涨

    def test_flat_values(self):
        _calc_range_metrics = self._import_func()
        values = [100.0] * 30
        df = self._make_range_data(values)
        r = _calc_range_metrics(df)

        assert r["cum_ret"] == 0.0
        assert r["vol"] == 0.0
        assert r["ann_ret"] == 0.0

    def test_single_day_data(self):
        _calc_range_metrics = self._import_func()
        # 2个数据点 = 1个交易日
        values = [100.0, 101.0]
        df = self._make_range_data(values)
        r = _calc_range_metrics(df)

        assert r["n_days"] == 1
        assert r["cum_ret"] > 0

    def test_return_dict_keys(self):
        _calc_range_metrics = self._import_func()
        values = [100.0 + i * 0.1 for i in range(10)]
        df = self._make_range_data(values)
        r = _calc_range_metrics(df)

        expected_keys = {"cum_ret", "ann_ret", "vol", "sharpe", "dd",
                         "best_day", "worst_day", "wr", "pnl_ratio", "n_days"}
        assert set(r.keys()) == expected_keys

    def test_best_worst_day(self):
        _calc_range_metrics = self._import_func()
        values = [100, 101, 99, 102, 98]  # +1%, -1.98%, +3.03%, -3.92%
        df = self._make_range_data(values)
        r = _calc_range_metrics(df)

        assert r["worst_day"] < 0
        assert r["best_day"] > 0


class TestResolveBenchmarkCode:
    def _import_func(self):
        from tabs.tab1_net_value import _resolve_benchmark_code
        return _resolve_benchmark_code

    def test_known_code(self):
        f = self._import_func()
        result = f("沪深300")
        assert result == "sh000300"

    def test_already_code_format(self):
        f = self._import_func()
        result = f("sh000300")
        assert result == "sh000300"

    def test_unknown_returns_input(self):
        f = self._import_func()
        result = f("UNKNOWN_CODE")
        assert result == "UNKNOWN_CODE"


class TestFmtCell:
    def _import_func(self):
        from tabs.tab1_net_value import _fmt_cell
        return _fmt_cell

    def test_positive_value(self):
        _fmt_cell = self._import_func()
        result = _fmt_cell(5.25, "%")
        assert "+5.25%" in result or "5.25" in result

    def test_negative_value(self):
        _fmt_cell = self._import_func()
        result = _fmt_cell(-3.14, "%")
        assert "3.14" in result or "3.14" in result

    def test_zero(self):
        _fmt_cell = self._import_func()
        result = _fmt_cell(0.0, "%")
        assert "0" in result
