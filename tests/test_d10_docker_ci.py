"""
D10: Dockerfile + GitHub Actions CI - 测试
- Dockerfile存在且格式正确
- .github/workflows/ci.yml存在
- requirements.txt存在
"""
import pytest
from pathlib import Path
import sys

PROJECT_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_DIR))


class TestDockerfileExists:
    def test_dockerfile_exists(self):
        assert (PROJECT_DIR / "Dockerfile").exists()

    def test_dockerfile_has_from(self):
        content = (PROJECT_DIR / "Dockerfile").read_text(encoding="utf-8")
        assert "FROM python:" in content

    def test_dockerfile_has_streamlit(self):
        content = (PROJECT_DIR / "Dockerfile").read_text(encoding="utf-8")
        assert "streamlit" in content

    def test_dockerfile_exposes_8501(self):
        content = (PROJECT_DIR / "Dockerfile").read_text(encoding="utf-8")
        assert "8501" in content


class TestGitHubActionsCI:
    def test_ci_yml_exists(self):
        assert (PROJECT_DIR / ".github" / "workflows" / "ci.yml").exists()

    def test_ci_runs_pytest(self):
        content = (PROJECT_DIR / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
        assert "pytest" in content

    def test_ci_triggers_on_push(self):
        content = (PROJECT_DIR / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
        assert "push:" in content

    def test_ci_triggers_on_pull_request(self):
        content = (PROJECT_DIR / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
        assert "pull_request:" in content

    def test_ci_uses_python_312(self):
        content = (PROJECT_DIR / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
        assert "3.12" in content


class TestRequirementsTxt:
    def test_requirements_exists(self):
        assert (PROJECT_DIR / "requirements.txt").exists()

    def test_requirements_has_streamlit(self):
        content = (PROJECT_DIR / "requirements.txt").read_text(encoding="utf-8")
        assert "streamlit" in content.lower() or "Streamlit" in content
