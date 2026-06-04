"""
D6: 根目录清理 + backfill统一入口 + 脚本归档 - 测试
"""
import pytest
from pathlib import Path
import sys

PROJECT_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_DIR))


class TestRootDirectoryClean:

    def test_root_only_has_core_py(self):
        py_files = [f.name for f in PROJECT_DIR.glob("*.py")]
        assert set(py_files) == {"dashboard.py", "run_analysis.py"}, \
            f"Unexpected: {py_files}"

    def test_root_no_test_py(self):
        for f in PROJECT_DIR.glob("*.py"):
            assert not f.name.startswith("test_"), f"Test in root: {f.name}"

    def test_root_no_backfill_py(self):
        for f in PROJECT_DIR.glob("*.py"):
            assert not f.name.startswith("backfill_"), f"Backfill in root: {f.name}"

    def test_root_no_temp_py(self):
        for f in PROJECT_DIR.glob("*.py"):
            assert not f.name.startswith("temp_"), f"Temp in root: {f.name}"


class TestScriptsBackfillExists:

    def test_backfill_dir_exists(self):
        assert (PROJECT_DIR / "scripts" / "backfill").is_dir()

    def test_backfill_history_exists(self):
        assert (PROJECT_DIR / "scripts" / "backfill" / "backfill_history.py").exists()

    def test_backfill_indicators_exists(self):
        assert (PROJECT_DIR / "scripts" / "backfill" / "backfill_indicators.py").exists()

    def test_backfill_macro_exists(self):
        assert (PROJECT_DIR / "scripts" / "backfill" / "backfill_macro.py").exists()

    def test_backfill_news_exists(self):
        assert (PROJECT_DIR / "scripts" / "backfill" / "backfill_news.py").exists()

    def test_backfill_sector_exists(self):
        assert (PROJECT_DIR / "scripts" / "backfill" / "backfill_sector_enhanced.py").exists()

    def test_backfill_full_exists(self):
        assert (PROJECT_DIR / "scripts" / "backfill" / "backfill_full_history.py").exists()

    def test_backfill_count(self):
        scripts = list((PROJECT_DIR / "scripts" / "backfill").glob("*.py"))
        assert len(scripts) == 6


class TestScriptsSetupExists:

    def test_setup_dir_exists(self):
        assert (PROJECT_DIR / "scripts" / "setup").is_dir()

    def test_setup_notification_exists(self):
        assert (PROJECT_DIR / "scripts" / "setup" / "setup_notification.py").exists()


class TestArchiveExists:

    def test_archive_dir_exists(self):
        assert (PROJECT_DIR / "archive").is_dir()

    def test_archive_has_old_test_files(self):
        assert (PROJECT_DIR / "archive" / "test_import.py").exists()
        assert (PROJECT_DIR / "archive" / "test_modular_structure.py").exists()

    def test_archive_has_old_run_scripts(self):
        assert (PROJECT_DIR / "archive" / "run_enhanced.py").exists()
        assert (PROJECT_DIR / "archive" / "run_smart.py").exists()


class TestUnifiedBackfillEntry:

    def test_run_backfill_exists(self):
        assert (PROJECT_DIR / "scripts" / "run_backfill.py").exists()

    def test_run_backfill_importable(self):
        import scripts.run_backfill as rb
        assert hasattr(rb, "BACKFILL_MODULES")
        assert len(rb.BACKFILL_MODULES) == 6

    def test_run_backfill_has_main(self):
        import scripts.run_backfill as rb
        assert hasattr(rb, "main")


class TestGitignoreArchiveExclusion:

    def test_gitignore_has_archive(self):
        gi = PROJECT_DIR / ".gitignore"
        content = gi.read_text(encoding="utf-8")
        assert "archive/" in content
