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
        # 根目录允许的 .py：核心模块 + 合法的根级运行脚本
        allowed = {"dashboard.py", "run_analysis.py", "data_loader.py", "sidebar.py",
                   "run_morning.py", "run_supplemental.py"}
        assert set(py_files) == allowed, \
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
        """backfill 目录须容纳全部归档脚本，且随新增脚本单调增长。

        ⚠ 2026-10-14 修复：原来断言 ``== 6``。那只是 D6 当时的快照值，此后脚本
        增至 35 个（每次新增都要回来改数字），属典型的脆弱计数断言。
        真正要约束的是「该归档的都已归档」——6 项必要性已由上方各 ``*_exists``
        用例逐个锁死，这里只校验总数为下界，与时点无关。
        """
        scripts = list((PROJECT_DIR / "scripts" / "backfill").glob("*.py"))
        assert len(scripts) >= 6, f"backfill 脚本过少: {len(scripts)}"


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
