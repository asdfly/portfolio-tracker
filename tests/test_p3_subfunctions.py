"""P3 sub-function refactor tests for tab6/tab9."""
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
    for node in ast.iter_child_nodes(tree):
        if isinstance(node, ast.FunctionDef) and node.name == render_func:
            result[render_func] = node.end_lineno - node.lineno + 1
    return result


class TestTab6Refactor:
    """Verify tab6_technical.py refactor."""
    EXPECTED_SUBFUNCS = {
        "_render_signal_overview": 55,
        "_render_signal_charts": 101,
        "_render_signal_table": 80,
        "_render_bollinger_chart": 59,
        "_render_rsi_chart": 54,
    }

    def test_subfunc_exist(self):
        lengths = _get_subfunc_lengths("tabs.tab6_technical", "render_tab6")
        for name in self.EXPECTED_SUBFUNCS:
            assert name in lengths, f"{name} missing"

    def test_subfunc_lengths(self):
        lengths = _get_subfunc_lengths("tabs.tab6_technical", "render_tab6")
        for name, expected in self.EXPECTED_SUBFUNCS.items():
            assert abs(lengths[name] - expected) <= 5, f"{name}: {lengths[name]} != {expected}"

    def test_orchestrator_short(self):
        lengths = _get_subfunc_lengths("tabs.tab6_technical", "render_tab6")
        assert lengths["render_tab6"] <= 40, f"render_tab6 too long: {lengths['render_tab6']}"

    def test_render_runs_empty(self):
        import pandas as pd
        from tabs.tab6_technical import render_tab6
        kw = dict(technical=pd.DataFrame())
        render_tab6()


class TestTab9Refactor:
    """Verify tab9_custom.py refactor."""
    EXPECTED_SUBFUNCS = {
        "_render_indicator_backtest": 107,
        "_render_candlestick_patterns": 122,
        "_render_backtest_history": 58,
    }

    def test_subfunc_exist(self):
        lengths = _get_subfunc_lengths("tabs.tab9_custom", "render_tab9")
        for name in self.EXPECTED_SUBFUNCS:
            assert name in lengths, f"{name} missing"

    def test_subfunc_lengths(self):
        lengths = _get_subfunc_lengths("tabs.tab9_custom", "render_tab9")
        for name, expected in self.EXPECTED_SUBFUNCS.items():
            assert abs(lengths[name] - expected) <= 5, f"{name}: {lengths[name]} != {expected}"

    def test_orchestrator_short(self):
        lengths = _get_subfunc_lengths("tabs.tab9_custom", "render_tab9")
        assert lengths["render_tab9"] <= 25, f"render_tab9 too long: {lengths['render_tab9']}"

    def test_tab_obj_pattern(self):
        """Verify sub-functions use tab_obj parameter pattern."""
        mod = importlib.import_module("tabs.tab9_custom")
        source = open(mod.__file__, encoding="utf-8").read()
        tree = ast.parse(source)
        for node in ast.iter_child_nodes(tree):
            if isinstance(node, ast.FunctionDef) and node.name.startswith("_render_"):
                params = [a.arg for a in node.args.args]
                assert "tab_obj" in params, f"{node.name} should have tab_obj param"
                # Check first body statement is a With
                assert isinstance(node.body[0], ast.With), f"{node.name} first stmt should be With"

    def test_render_runs_empty(self):
        import pandas as pd
        from tabs.tab9_custom import render_tab9
        kw = dict(technical=pd.DataFrame(), volatility=None, max_dd=None, sharpe=None)
        render_tab9()
