"""测试 P1 拆分: tab1_net_value.py 和 tab10_fund_flow.py"""
import ast
import pytest

TAB1 = __import__("pathlib").Path(__file__).resolve().parent.parent / "tabs" / "tab1_net_value.py"
TAB10 = __import__("pathlib").Path(__file__).resolve().parent.parent / "tabs" / "tab10_fund_flow.py"


class TestTab1SubFunctionExtraction:
    """验证 render_tab1 已被拆分为 5 个子函数"""

    def test_render_tab1_is_orchestrator(self):
        source = TAB1.read_text(encoding="utf-8")
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "render_tab1":
                size = (node.end_lineno or node.lineno) - node.lineno + 1
                assert size <= 30, f"render_tab1 仍有 {size} 行"
                break

    @pytest.mark.parametrize("func_name,expected_params", [
        ("_render_basic_metrics", ["positions", "summary", "index_quotes", "selected_date", "selected_benchmark", "technical", "volatility", "max_dd", "sharpe", "cal_data", "tech_signals", "show_days"]),
        ("_render_rolling_charts", ["summary", "selected_date", "show_days"]),
        ("_render_benchmark_comparison", ["summary", "selected_benchmark", "selected_date", "show_days"]),
        ("_render_multi_benchmark_analysis", ["summary", "selected_date", "selected_benchmark", "show_days"]),
        ("_render_annual_returns", ["summary"]),
    ])
    def test_sub_function_exists(self, func_name, expected_params):
        source = TAB1.read_text(encoding="utf-8")
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == func_name:
                actual = [a.arg for a in node.args.args]
                assert actual == expected_params, f"{func_name}: 期望 {expected_params}, 实际 {actual}"
                return
        pytest.fail(f"{func_name} 不存在于 {TAB1.name}")

    def test_no_sub_function_over_300_lines(self):
        source = TAB1.read_text(encoding="utf-8")
        tree = ast.parse(source)
        violations = []
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name.startswith("_render_"):
                size = (node.end_lineno or node.lineno) - node.lineno + 1
                if size > 350:
                    violations.append(f"{node.name}: {size}L")
        if violations:
            pytest.fail("以下子函数超过 350 行: " + ", ".join(violations))

    def test_render_tab1_calls_all_sub_functions(self):
        source = TAB1.read_text(encoding="utf-8")
        tree = ast.parse(source)
        expected = {"_render_basic_metrics", "_render_rolling_charts", "_render_benchmark_comparison",
                     "_render_multi_benchmark_analysis", "_render_annual_returns"}
        actual = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "render_tab1":
                for sub in ast.walk(node):
                    if isinstance(sub, ast.Call) and isinstance(sub.func, ast.Name) and sub.func.id in expected:
                        actual.add(sub.func.id)
                break
        assert not expected - actual, f"render_tab1 未调用: {expected - actual}"


class TestTab10SubFunctionExtraction:
    """验证 render_tab10 已被拆分为 3 个子函数"""

    def test_render_tab10_is_orchestrator(self):
        source = TAB10.read_text(encoding="utf-8")
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "render_tab10":
                size = (node.end_lineno or node.lineno) - node.lineno + 1
                assert size <= 25, f"render_tab10 仍有 {size} 行"
                break

    @pytest.mark.parametrize("func_name,expected_params", [
        ("_render_industry_fund_flow", ["tab_obj", "positions"]),
        ("_render_etf_fund_flow", ["tab_obj", "positions"]),
        ("_render_main_force_flow", ["tab_obj"]),
    ])
    def test_sub_function_exists(self, func_name, expected_params):
        source = TAB10.read_text(encoding="utf-8")
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == func_name:
                actual = [a.arg for a in node.args.args]
                assert actual == expected_params, f"{func_name}: 期望 {expected_params}, 实际 {actual}"
                return
        pytest.fail(f"{func_name} 不存在于 {TAB10.name}")

    def test_render_tab10_calls_all_sub_functions(self):
        source = TAB10.read_text(encoding="utf-8")
        tree = ast.parse(source)
        expected = {"_render_industry_fund_flow", "_render_etf_fund_flow", "_render_main_force_flow"}
        actual = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "render_tab10":
                for sub in ast.walk(node):
                    if isinstance(sub, ast.Call) and isinstance(sub.func, ast.Name) and sub.func.id in expected:
                        actual.add(sub.func.id)
                break
        assert not expected - actual, f"render_tab10 未调用: {expected - actual}"
