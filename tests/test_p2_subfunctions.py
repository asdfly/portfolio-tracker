"""P2 sub-function refactor tests for tab2/tab4/tab7/tab8."""
import ast
import importlib, sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest


def _get_subfunc_lengths(module_name, render_func):
    """Return {func_name: line_count} for all _render_* sub-functions."""
    mod = importlib.import_module(module_name)
    source = open(mod.__file__, encoding="utf-8").read()
    tree = ast.parse(source)
    result = {}
    for node in ast.iter_child_nodes(tree):
        if isinstance(node, ast.FunctionDef) and node.name.startswith("_render_"):
            result[node.name] = node.end_lineno - node.lineno + 1
    # Also get render_func length
    for node in ast.iter_child_nodes(tree):
        if isinstance(node, ast.FunctionDef) and node.name == render_func:
            result[render_func] = node.end_lineno - node.lineno + 1
    return result


class TestTab2Refactor:
    """Verify tab2_position.py refactor."""
    EXPECTED_SUBFUNCS = {
        "_render_etf_filter": 296,
        "_render_sector_weights": 46,
        "_render_correlation_matrix": 38,
        "_render_deep_analysis": 90,
        "_render_cumulative_pnl": 54,
        "_render_etf_f10_panel": 43,
        "_render_f10_metrics": 66,
        "_render_f10_industry_alloc": 60,
        "_render_f10_top_holdings": 56,
        "_render_f10_index_valuation": 61,
    }

    def test_subfunc_exist(self):
        lengths = _get_subfunc_lengths("tabs.tab2_position", "render_tab2")
        for name in self.EXPECTED_SUBFUNCS:
            assert name in lengths, f"{name} missing"

    def test_subfunc_lengths(self):
        lengths = _get_subfunc_lengths("tabs.tab2_position", "render_tab2")
        for name, expected in self.EXPECTED_SUBFUNCS.items():
            assert abs(lengths[name] - expected) <= 3, f"{name}: {lengths[name]} != {expected}"

    def test_orchestrator_short(self):
        lengths = _get_subfunc_lengths("tabs.tab2_position", "render_tab2")
        assert lengths["render_tab2"] <= 40, f"render_tab2 too long: {lengths['render_tab2']}"

    def test_render_runs_empty(self):
        import pandas as pd
        from tabs.tab2_position import render_tab2
        kw = dict(technical=pd.DataFrame(), volatility=None, max_dd=None,
                  sharpe=None, cal_data=pd.DataFrame(), tech_signals=pd.DataFrame())
        render_tab2()


class TestTab4Refactor:
    """Verify tab4_calendar.py refactor."""
    EXPECTED_SUBFUNCS = {
        "_render_year_overview": 131,
        "_render_monthly_view": 121,
        "_render_heatmap": 41,
        "_render_annual_trend": 52,
        "_render_boxplot": 33,
        # 146 → 176（2026-10-14 更新基准）：该函数在 09 月后累增约 30 行，
        # 已明显高于其余子函数，属**待重构**信号（建议拆「事件分组」与「渲染」两段）。
        # 本次仅同步基准值并补硬闸(test_subfunc_hard_cap)，不擅自重构 UI 行为。
        "_render_event_calendar": 176,
    }
    # 硬上限：任何子函数都不得突破。记录精确行数本就是脆弱断言（加一行注释就要
    # 回来改数字），但完全放开会让 tab4 重回"巨石函数"。故上方 ±3 比对用于发现
    # 「意料之外的变动」，这里再设一道红线，防止继续膨胀而无人为此负责。
    SUBFUNC_HARD_CAP = 200

    def test_subfunc_exist(self):
        lengths = _get_subfunc_lengths("tabs.tab4_calendar", "render_tab4")
        for name in self.EXPECTED_SUBFUNCS:
            assert name in lengths, f"{name} missing"

    def test_subfunc_lengths(self):
        lengths = _get_subfunc_lengths("tabs.tab4_calendar", "render_tab4")
        for name, expected in self.EXPECTED_SUBFUNCS.items():
            assert abs(lengths[name] - expected) <= 3, f"{name}: {lengths[name]} != {expected}"

    def test_subfunc_hard_cap(self):
        lengths = _get_subfunc_lengths("tabs.tab4_calendar", "render_tab4")
        for name, ln in lengths.items():
            assert ln <= self.SUBFUNC_HARD_CAP, \
                f"{name} 过长 {ln} > {self.SUBFUNC_HARD_CAP}，应拆分而非放宽"

    def test_orchestrator_short(self):
        lengths = _get_subfunc_lengths("tabs.tab4_calendar", "render_tab4")
        assert lengths["render_tab4"] <= 30, f"render_tab4 too long: {lengths['render_tab4']}"

    def test_year_overview_returns_tuple(self):
        import ast
        mod = __import__("tabs.tab4_calendar", fromlist=["_render_year_overview"])
        source = open(mod.__file__, encoding="utf-8").read()
        tree = ast.parse(source)
        for node in ast.iter_child_nodes(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "_render_year_overview":
                last = node.body[-1]
                assert isinstance(last, ast.Return), "Last statement should be Return"

    def test_render_runs_empty(self):
        import pandas as pd
        from tabs.tab4_calendar import render_tab4
        kw = dict(technical=pd.DataFrame(), volatility=None, max_dd=None,
                  sharpe=None, cal_data=pd.DataFrame(), tech_signals=pd.DataFrame())
        render_tab4()


class TestTab7Refactor:
    """Verify tab7_news.py refactor."""
    EXPECTED_SUBFUNCS = {
        "_render_news_panel": 91,
        "_render_comprehensive_assessment": 187,
        "_render_market_sentiment": 165,
        "_render_news_sentiment": 118,
    }

    def test_subfunc_exist(self):
        lengths = _get_subfunc_lengths("tabs.tab7_news", "render_tab7")
        for name in self.EXPECTED_SUBFUNCS:
            assert name in lengths, f"{name} missing"

    def test_subfunc_lengths(self):
        lengths = _get_subfunc_lengths("tabs.tab7_news", "render_tab7")
        for name, expected in self.EXPECTED_SUBFUNCS.items():
            assert abs(lengths[name] - expected) <= 3, f"{name}: {lengths[name]} != {expected}"

    def test_orchestrator_short(self):
        lengths = _get_subfunc_lengths("tabs.tab7_news", "render_tab7")
        assert lengths["render_tab7"] <= 20, f"render_tab7 too long: {lengths['render_tab7']}"

    def test_render_runs_empty(self):
        import pandas as pd
        from tabs.tab7_news import render_tab7
        kw = dict(technical=pd.DataFrame(), volatility=None, max_dd=None, sharpe=None)
        render_tab7()


class TestTab8Refactor:
    """Verify tab8_advice.py refactor."""
    EXPECTED_SUBFUNCS = {
        "_render_suggestions_compute": 155,
        "_render_suggestion_cards": 24,
        "_render_suggestion_pie": 120,
        "_render_suggestion_details": 126,
        "_render_signal_confidence": 348,
        "_render_backtest_heatmap": 234,
        "_render_market_events": 62,
        "_render_data_export": 48,
        "_render_feedback_tracking": 176,
    }

    def test_subfunc_exist(self):
        lengths = _get_subfunc_lengths("tabs.tab8_advice", "render_tab8")
        for name in self.EXPECTED_SUBFUNCS:
            assert name in lengths, f"{name} missing"

    def test_subfunc_lengths(self):
        lengths = _get_subfunc_lengths("tabs.tab8_advice", "render_tab8")
        for name, expected in self.EXPECTED_SUBFUNCS.items():
            assert abs(lengths[name] - expected) <= 3, f"{name}: {lengths[name]} != {expected}"

    def test_orchestrator_short(self):
        lengths = _get_subfunc_lengths("tabs.tab8_advice", "render_tab8")
        assert lengths["render_tab8"] <= 37, f"render_tab8 too long: {lengths['render_tab8']}"

    def test_suggestions_compute_returns_tuple(self):
        import ast
        mod = __import__("tabs.tab8_advice", fromlist=["_render_suggestions_compute"])
        source = open(mod.__file__, encoding="utf-8").read()
        tree = ast.parse(source)
        for node in ast.iter_child_nodes(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "_render_suggestions_compute":
                last = node.body[-1]
                assert isinstance(last, ast.Return), "Last statement should be Return"

    def test_render_runs_empty(self):
        import pandas as pd
        from tabs.tab8_advice import render_tab8
        kw = dict(technical=pd.DataFrame(), volatility=None, max_dd=None, sharpe=None)
        render_tab8()
