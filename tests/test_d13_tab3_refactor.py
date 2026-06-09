"""D13 tests: Dashboard tab3_risk refactoring validation.

After tab3_risk.py was split into sub-modules (dashboard/attribution/warnings/alerts),
the main file is a pure orchestration file (~72 lines).
Tests verify structure integrity across main file + sub-modules.
"""

import ast
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
TAB3_PATH = PROJECT_ROOT / "tabs" / "tab3_risk.py"
TAB3_DIR = PROJECT_ROOT / "tabs"

# Sub-module paths
TAB3_SUBMODULES = {
    "dashboard": TAB3_DIR / "tab3_risk_dashboard.py",
    "attribution": TAB3_DIR / "tab3_risk_attribution.py",
    "warnings": TAB3_DIR / "tab3_risk_warnings.py",
    "alerts": TAB3_DIR / "tab3_risk_alerts.py",
}


def _get_functions(source_code):
    tree = ast.parse(source_code)
    return [(n.name, n.end_lineno - n.lineno + 1) for n in ast.walk(tree)
            if isinstance(n, ast.FunctionDef)]


class TestTab3Structure:
    """Verify render_tab3 was split into sub-functions and sub-modules."""

    def test_tab3_file_exists(self):
        assert TAB3_PATH.exists()

    def test_render_tab3_exists(self):
        fns = _get_functions(TAB3_PATH.read_text(encoding="utf-8"))
        names = [f[0] for f in fns]
        assert "render_tab3" in names

    def test_sub_modules_exist(self):
        """All 4 sub-module files exist."""
        for name, path in TAB3_SUBMODULES.items():
            assert path.exists(), f"Missing sub-module: {name}"

    def test_sub_functions_exist(self):
        """Sub-functions exist across sub-modules (not in main file)."""
        expected_locations = {
            "_render_risk_gauge_and_metrics": "dashboard",
            "_render_drawdown_chart": "dashboard",
            "_render_brinson_attribution": "attribution",
            "_render_multi_factor_attribution": "attribution",
            "_render_risk_warnings": "warnings",
            "_render_style_exposure": "warnings",
            "_render_sector_rotation": "warnings",
            "_render_alert_center": "alerts",
        }
        for fn_name, module_key in expected_locations.items():
            mod_path = TAB3_SUBMODULES[module_key]
            fns = _get_functions(mod_path.read_text(encoding="utf-8"))
            names = [f[0] for f in fns]
            assert fn_name in names, f"Missing {fn_name} in {mod_path.name}"

    def test_render_tab3_under_50_lines(self):
        tree = ast.parse(TAB3_PATH.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "render_tab3":
                length = node.end_lineno - node.lineno + 1
                assert length < 50, f"render_tab3 still {length} lines, target <50"
                break

    def test_no_function_over_300_lines(self):
        """No function in any tab3 module exceeds 300 lines."""
        for path in [TAB3_PATH] + list(TAB3_SUBMODULES.values()):
            fns = _get_functions(path.read_text(encoding="utf-8"))
            for name, length in fns:
                assert length <= 300, f"{name}() in {path.name} is {length} lines, exceeds 300 limit"

    def test_sub_functions_have_docstrings(self):
        """All _render_ sub-functions across sub-modules have docstrings."""
        for path in list(TAB3_SUBMODULES.values()):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef) and node.name.startswith("_render_"):
                    doc = ast.get_docstring(node)
                    assert doc is not None, f"{node.name}() in {path.name} missing docstring"

    def test_main_file_is_orchestration(self):
        """Main tab3_risk.py contains delegate functions + render_tab3 + imports."""
        fns = _get_functions(TAB3_PATH.read_text(encoding="utf-8"))
        names = [f[0] for f in fns]
        # Should have render_tab3 + 3 delegate functions
        assert "render_tab3" in names
        assert "compute_extended_risk_metrics" in names
        assert "compute_return_attribution" in names
        assert "load_alerts" in names

    def test_original_helpers_preserved(self):
        """Delegate functions (compute_extended_risk_metrics, etc.) preserved in main file."""
        expected_helpers = [
            "compute_extended_risk_metrics",
            "compute_return_attribution",
            "load_alerts",
        ]
        fns = _get_functions(TAB3_PATH.read_text(encoding="utf-8"))
        names = [f[0] for f in fns]
        for name in expected_helpers:
            assert name in names, f"Missing delegate function: {name}"
