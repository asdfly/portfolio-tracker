"""测试 tab5_advanced.py 子函数拆分后的结构完整性"""
import ast
import pytest

TAB_FILE = __import__("pathlib").Path(__file__).resolve().parent.parent / "tabs" / "tab5_advanced.py"


class TestTab5SubFunctionExtraction:
    """验证 render_tab5 已被拆分为子函数"""

    def test_render_tab5_is_orchestrator_only(self):
        """render_tab5 应为轻量编排函数，不超过 30 行"""
        source = TAB_FILE.read_text(encoding="utf-8")
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "render_tab5":
                size = (node.end_lineno or node.lineno) - node.lineno + 1
                assert size <= 30, f"render_tab5 仍有 {size} 行，应为编排入口"
                break

    @pytest.mark.parametrize("func_name,expected_params", [
        ("_render_monte_carlo", ["summary", "selected_date"]),
        ("_render_risk_attribution", ["positions", "summary", "volatility"]),
        ("_render_stress_test", ["positions", "summary"]),
        ("_render_rebalance_advice", []),
        ("_render_tech_and_advice", ["technical"]),
        ("_render_investment_review", ["volatility", "tech_signals", "summary"]),
        ("_render_data_export", ["positions", "summary", "selected_benchmark", "selected_date", "technical"]),
    ])
    def test_sub_function_exists(self, func_name, expected_params):
        """每个子函数必须存在且参数签名正确"""
        source = TAB_FILE.read_text(encoding="utf-8")
        tree = ast.parse(source)
        found = False
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == func_name:
                actual_params = [a.arg for a in node.args.args]
                assert actual_params == expected_params, (
                    f"{func_name} 参数不匹配: 期望 {expected_params}, 实际 {actual_params}"
                )
                found = True
                break
        assert found, f"{func_name} 不存在于 {TAB_FILE.name}"

    def test_no_sub_function_over_300_lines(self):
        """每个子函数不应超过 300 行"""
        source = TAB_FILE.read_text(encoding="utf-8")
        tree = ast.parse(source)
        violations = []
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name.startswith("_render_"):
                size = (node.end_lineno or node.lineno) - node.lineno + 1
                if size > 300:
                    violations.append(f"{node.name}: {size}L")
        if violations:
            pytest.fail("以下子函数超过 300 行: " + ", ".join(violations))

    def test_render_tab5_calls_all_sub_functions(self):
        """render_tab5 编排函数必须调用所有 7 个子函数"""
        source = TAB_FILE.read_text(encoding="utf-8")
        tree = ast.parse(source)
        expected_calls = {
            "_render_monte_carlo", "_render_risk_attribution", "_render_stress_test",
            "_render_rebalance_advice", "_render_tech_and_advice",
            "_render_investment_review", "_render_data_export",
        }
        actual_calls = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "render_tab5":
                for sub in ast.walk(node):
                    if isinstance(sub, ast.Call) and isinstance(sub.func, ast.Name):
                        if sub.func.id in expected_calls:
                            actual_calls.add(sub.func.id)
                break
        missing = expected_calls - actual_calls
        assert not missing, f"render_tab5 未调用: {missing}"

    @pytest.mark.parametrize("func_name", [
        "_render_monte_carlo", "_render_risk_attribution", "_render_stress_test",
        "_render_rebalance_advice", "_render_tech_and_advice",
        "_render_investment_review", "_render_data_export",
    ])
    def test_sub_function_has_streamlit_calls(self, func_name):
        """每个子函数应包含至少一个 st.xxx 调用"""
        source = TAB_FILE.read_text(encoding="utf-8")
        tree = ast.parse(source)
        has_st = False
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == func_name:
                for sub in ast.walk(node):
                    if isinstance(sub, ast.Call) and isinstance(sub.func, ast.Attribute):
                        if isinstance(sub.func.value, ast.Name) and sub.func.value.id == "st":
                            has_st = True
                            break
                break
        assert has_st, f"{func_name} 不包含任何 st.xxx 调用"
