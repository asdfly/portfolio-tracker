"""D11 tests: Version release - CHANGELOG, LICENSE, README validation."""

import os
from pathlib import Path


class TestChangelog:
    """CHANGELOG.md structure and content validation."""

    @property
    def changelog_path(self):
        return Path(__file__).resolve().parent.parent / "CHANGELOG.md"

    def test_changelog_exists(self):
        assert self.changelog_path.exists()

    def test_changelog_has_header(self):
        content = self.changelog_path.read_text(encoding="utf-8")
        assert "# Changelog" in content

    def test_changelog_has_v22(self):
        content = self.changelog_path.read_text(encoding="utf-8")
        assert "[v2.2]" in content

    def test_changelog_v22_has_date(self):
        content = self.changelog_path.read_text(encoding="utf-8")
        assert "2026-06-04" in content

    def test_changelog_has_v21(self):
        content = self.changelog_path.read_text(encoding="utf-8")
        assert "[v2.1]" in content

    def test_changelog_has_sections(self):
        content = self.changelog_path.read_text(encoding="utf-8")
        for section in ["### 新增", "### 修复", "### 变更"]:
            assert section in content, f"Missing section: {section}"

    def test_changelog_v22_mentions_d1_d10(self):
        content = self.changelog_path.read_text(encoding="utf-8")
        for d in ["D1", "D2", "D3", "D4", "D5", "D6", "D8", "D9", "D10"]:
            assert d in content, f"Missing D-reference: {d}"

    def test_changelog_v22_has_test_count(self):
        content = self.changelog_path.read_text(encoding="utf-8")
        assert "655" in content

    def test_changelog_keeps_v21_content(self):
        content = self.changelog_path.read_text(encoding="utf-8")
        assert "黄金市场分析" in content
        assert "资金流历史回填" in content


class TestLicense:
    """LICENSE file validation."""

    @property
    def license_path(self):
        return Path(__file__).resolve().parent.parent / "LICENSE"

    def test_license_exists(self):
        assert self.license_path.exists()

    def test_license_is_mit(self):
        content = self.license_path.read_text(encoding="utf-8")
        assert "MIT License" in content

    def test_license_has_copyright(self):
        content = self.license_path.read_text(encoding="utf-8")
        assert "Copyright" in content

    def test_license_has_permission(self):
        content = self.license_path.read_text(encoding="utf-8")
        assert "Permission is hereby granted" in content


class TestReadme:
    """README.md reflects v2.2 current state."""

    @property
    def readme_path(self):
        return Path(__file__).resolve().parent.parent / "README.md"

    def test_readme_exists(self):
        assert self.readme_path.exists()

    def test_readme_has_14_tabs(self):
        content = self.readme_path.read_text(encoding="utf-8")
        assert "14" in content  # 14 tabs

    def test_readme_has_655_tests(self):
        content = self.readme_path.read_text(encoding="utf-8")
        assert "655" in content

    def test_readme_has_docker(self):
        content = self.readme_path.read_text(encoding="utf-8")
        assert "Docker" in content
        assert "Dockerfile" in content

    def test_readme_has_ci(self):
        content = self.readme_path.read_text(encoding="utf-8")
        assert "GitHub Actions" in content

    def test_readme_has_env_config(self):
        content = self.readme_path.read_text(encoding="utf-8")
        assert ".env" in content
        assert ".env.example" in content

    def test_readme_has_9_alert_rules(self):
        content = self.readme_path.read_text(encoding="utf-8")
        assert "9" in content

    def test_readme_has_20_tables(self):
        content = self.readme_path.read_text(encoding="utf-8")
        assert "20" in content

    def test_readme_has_license_link(self):
        content = self.readme_path.read_text(encoding="utf-8")
        assert "MIT" in content
