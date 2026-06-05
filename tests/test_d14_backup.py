"""D14 tests: Database backup utility validation."""

import sqlite3
import os
from datetime import datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BACKUP_SCRIPT = PROJECT_ROOT / "scripts" / "backup_db.py"


class TestBackupScriptExists:
    def test_backup_script_exists(self):
        assert BACKUP_SCRIPT.exists()

    def test_backup_dir_in_settings(self):
        from config.settings import BACKUP_DIR
        assert "backups" in str(BACKUP_DIR)


class TestBackupFunction:
    def _make_db(self, tmp_path):
        db = tmp_path / "test.db"
        conn = sqlite3.connect(str(db))
        conn.execute('CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)')
        conn.execute('INSERT INTO t1 VALUES(1, ?)', ('hello',))
        conn.commit()
        conn.close()
        return db

    def test_backup_creates_file(self, tmp_path):
        db = self._make_db(tmp_path)
        bak_dir = tmp_path / "backups"
        from scripts.backup_db import backup_database
        result = backup_database(str(db), str(bak_dir))
        assert result.exists()
        assert result.suffix == '.db'

    def test_backup_preserves_data(self, tmp_path):
        db = self._make_db(tmp_path)
        bak_dir = tmp_path / "backups"
        from scripts.backup_db import backup_database
        result = backup_database(str(db), str(bak_dir))
        conn = sqlite3.connect(str(result))
        rows = conn.execute('SELECT * FROM t1').fetchall()
        conn.close()
        assert rows == [(1, 'hello')]

    def test_backup_raises_on_missing(self, tmp_path):
        import pytest
        from scripts.backup_db import backup_database
        with pytest.raises(FileNotFoundError):
            backup_database(str(tmp_path / 'no.db'), str(tmp_path / 'bak'))


class TestCleanupFunction:
    def _make_backups(self, tmp_path, count=5):
        bak_dir = tmp_path / "backups"
        bak_dir.mkdir()
        for i in range(count):
            p = bak_dir / f"bak_{i}.db"
            p.write_text("fake")
            mtime = (datetime.now() - timedelta(days=i)).timestamp()
            os.utime(str(p), (mtime, mtime))
        return bak_dir

    def test_keep_min_respected(self, tmp_path):
        bak_dir = self._make_backups(tmp_path, count=5)
        from scripts.backup_db import cleanup_old_backups
        cleanup_old_backups(str(bak_dir), max_age_days=1, keep_min=3)
        assert len(list(bak_dir.glob('*.db'))) == 3

    def test_no_cleanup_when_recent(self, tmp_path):
        bak_dir = self._make_backups(tmp_path, count=3)
        from scripts.backup_db import cleanup_old_backups
        cleanup_old_backups(str(bak_dir), max_age_days=30, keep_min=1)
        assert len(list(bak_dir.glob('*.db'))) == 3

    def test_cleanup_removes_old(self, tmp_path):
        bak_dir = self._make_backups(tmp_path, count=5)
        from scripts.backup_db import cleanup_old_backups
        cleanup_old_backups(str(bak_dir), max_age_days=1, keep_min=1)
        assert len(list(bak_dir.glob('*.db'))) <= 2