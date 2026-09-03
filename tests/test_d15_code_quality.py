"""D15 tests: Code quality - advisor helper methods and tab3 structure."""

import ast
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ADVISOR_PATH = PROJECT_ROOT / "src" / "analysis" / "advisor.py"


def _get_functions(source_code):
    tree = ast.parse(source_code)
    return [(n.name, n.end_lineno - n.lineno + 1) for n in ast.walk(tree)
            if isinstance(n, ast.FunctionDef)]


class TestAdvisorHelpers:
    def test_advisor_has_query_helpers(self):
        fns = _get_functions(ADVISOR_PATH.read_text(encoding="utf-8"))
        names = [f[0] for f in fns]
        assert "_query_recent_block_trades" in names
        assert "_query_margin_data" in names
        assert "_query_institution_research" in names

    def test_advisor_function_count(self):
        fns = _get_functions(ADVISOR_PATH.read_text(encoding="utf-8"))
        names = [f[0] for f in fns]

        # 旧断言 `assert len(fns) == 23` 是脆性数字断言：advisor 的核心分析能力以
        # `_analyze_*` 系列方法承载，每新增一类分析（如 2026-08 新增 _analyze_market_stage、
        # _analyze_news_sentiment 等）总函数数就会越过 23 而假红；且该断言并不保护任何
        # 真实行为契约——它只数函数个数，与"advisor 能否给出建议"无关。
        #
        # 改为两类有意义的断言：
        #   1) 关键分析函数白名单——这些方法是 advisor 行为契约的核心，缺一不可；
        #      任一被改名/删除都会破坏下游建请逻辑，必须立即报错。
        #   2) 总数下界——仅用于捕捉"重构时误删整片分析能力"的事故（数量骤降即报警），
        #      不设上界：新增分析函数属正常扩展，不应误报。
        REQUIRED_ANALYZERS = [
            "_analyze_valuation",
            "_analyze_add_opportunity",
            "_analyze_position_levels",
            "_analyze_position_score",
            "_analyze_technical_signals",
            "_analyze_fund_flows",
            "_analyze_macro_environment",
            "_analyze_news_sentiment",
            "_analyze_margin_data",
            "_analyze_institution_research",
            "_analyze_block_trade",
        ]
        missing = [n for n in REQUIRED_ANALYZERS if n not in names]
        assert not missing, f"advisor 缺失核心分析函数（行为契约破坏）: {missing}"

        # 下界略低于当前实际规模（32），给正常精简留余量，但远低于正常规模即报警。
        assert len(fns) >= 20, f"advisor 函数数量异常偏低（疑似误删整片逻辑）: {len(fns)}"

    def test_advisor_no_function_over_200(self):
        fns = _get_functions(ADVISOR_PATH.read_text(encoding="utf-8"))
        for name, length in fns:
            assert length <= 200, f"{name}() is {length} lines"

    def test_advisor_helpers_use_self_db(self):
        source = ADVISOR_PATH.read_text(encoding="utf-8")
        assert source.count("self.db") >= 5


class TestTab3Refactor:
    def test_tab3_max_under_350(self):
        tab3 = PROJECT_ROOT / "tabs" / "tab3_risk.py"
        fns = _get_functions(tab3.read_text(encoding="utf-8"))
        for name, length in fns:
            assert length <= 350, f"{name}() is {length} lines"

    def test_tab3_render_under_50(self):
        tab3 = PROJECT_ROOT / "tabs" / "tab3_risk.py"
        tree = ast.parse(tab3.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "render_tab3":
                length = node.end_lineno - node.lineno + 1
                assert length < 50
                break


class TestProjectStructure:
    def test_backup_script_exists(self):
        assert (PROJECT_ROOT / "scripts" / "backup_db.py").exists()

    def test_changelog_exists(self):
        assert (PROJECT_ROOT / "CHANGELOG.md").exists()

    def test_license_exists(self):
        assert (PROJECT_ROOT / "LICENSE").exists()

    def test_dockerfile_exists(self):
        assert (PROJECT_ROOT / "Dockerfile").exists()

    def test_ci_workflow_exists(self):
        assert (PROJECT_ROOT / ".github" / "workflows" / "ci.yml").exists()
