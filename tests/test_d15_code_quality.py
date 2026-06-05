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
        assert len(fns) == 20

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
