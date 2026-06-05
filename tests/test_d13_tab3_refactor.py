"""D13 tests: Dashboard tab3_risk refactoring validation."""

import ast
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
TAB3_PATH = PROJECT_ROOT / "tabs" / "tab3_risk.py"


def _get_functions(source_code):
    tree = ast.parse(source_code)
    return [(n.name, n.end_lineno - n.lineno + 1) for n in ast.walk(tree)
            if isinstance(n, ast.FunctionDef)]


class TestTab3Structure:
    """Verify render_tab3 was split into sub-functions."""

    def test_tab3_file_exists(self):
        assert TAB3_PATH.exists()

    def test_render_tab3_exists(self):
        fns = _get_functions(TAB3_PATH.read_text(encoding="utf-8"))
        names = [f[0] for f in fns]
        assert "render_tab3" in names

    def test_sub_functions_exist(self):
        expected = [
            "_render_risk_gauge_and_metrics",
            "_render_drawdown_chart",
            "_render_brinson_attribution",
            "_render_multi_factor_attribution",
            "_render_risk_warnings",
            "_render_style_exposure",
            "_render_sector_rotation",
            "_render_alert_center",
        ]
        fns = _get_functions(TAB3_PATH.read_text(encoding="utf-8"))
        names = [f[0] for f in fns]
        for name in expected:
            assert name in names, f"Missing sub-function: {name}"

    def test_render_tab3_under_50_lines(self):
        tree = ast.parse(TAB3_PATH.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "render_tab3":
                length = node.end_lineno - node.lineno + 1
                assert length < 50, f"render_tab3 still {length} lines, target <50"
                break

    def test_no_function_over_300_lines(self):
        fns = _get_functions(TAB3_PATH.read_text(encoding="utf-8"))
        for name, length in fns:
            assert length <= 300, f"{name}() is {length} lines, exceeds 300 limit"

    def test_sub_functions_have_docstrings(self):
        tree = ast.parse(TAB3_PATH.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name.startswith("_render_"):
                doc = ast.get_docstring(node)
                assert doc is not None, f"{node.name}() missing docstring"

    def test_total_function_count(self):
        fns = _get_functions(TAB3_PATH.read_text(encoding="utf-8"))
        assert len(fns) == 13, f"Expected 13 functions, got {len(fns)}"

    def test_original_helpers_preserved(self):
        expected_helpers = [
            "compute_extended_risk_metrics",
            "compute_return_attribution",
            "load_alerts",
            "get_sector",
        ]
        fns = _get_functions(TAB3_PATH.read_text(encoding="utf-8"))
        names = [f[0] for f in fns]
        for name in expected_helpers:
            assert name in names, f"Missing original helper: {name}"
