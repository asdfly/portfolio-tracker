"""SQLite database backup utility."""

import argparse
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import DATABASE_PATH, BACKUP_DIR


def get_backup_dir() -> Path:
    """Get or create backup directory."""
    backup = Path(BACKUP_DIR)
    backup.mkdir(parents=True, exist_ok=True)
    return backup


def backup_database(db_path=None, backup_dir=None) -> Path:
    """Create a timestamped backup using SQLite online backup API."""
    db = Path(db_path or DATABASE_PATH)
    bak_dir = get_backup_dir() if backup_dir is None else Path(backup_dir)
    bak_dir.mkdir(parents=True, exist_ok=True)
    if not db.exists():
        raise FileNotFoundError(f"Database not found: {db}")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = bak_dir / f"{db.stem}_{timestamp}.db"
    source = sqlite3.connect(str(db))
    dest = sqlite3.connect(str(backup_path))
    try:
        source.backup(dest)
    finally:
        source.close()
        dest.close()
    size_mb = backup_path.stat().st_size / (1024 * 1024)
    print(f"Backup created: {backup_path} ({size_mb:.1f} MB)")
    return backup_path


def cleanup_old_backups(backup_dir=None, max_age_days=7, keep_min=3):
    """Delete backup files older than max_age_days, keeping at least keep_min."""
    bak_dir = Path(backup_dir or BACKUP_DIR) if backup_dir else get_backup_dir()
    if not bak_dir.exists():
        return
    cutoff = datetime.now() - timedelta(days=max_age_days)
    backups = sorted(bak_dir.glob("*.db"), key=lambda p: p.stat().st_mtime, reverse=True)
    deleted = 0
    for i, bp in enumerate(backups):
        if i < keep_min:
            continue
        mtime = datetime.fromtimestamp(bp.stat().st_mtime)
        if mtime < cutoff:
            bp.unlink()
            deleted += 1
            print(f"Deleted old backup: {bp.name}")
    print(f"Cleanup: {deleted} old backups removed, {len(backups) - deleted} retained")


def list_backups(backup_dir=None):
    """List all backup files with size and age."""
    bak_dir = Path(backup_dir or BACKUP_DIR) if backup_dir else get_backup_dir()
    if not bak_dir.exists():
        print("No backup directory found.")
        return
    backups = sorted(bak_dir.glob("*.db"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not backups:
        print("No backups found.")
        return
    print(f"Backup File{" "*30} Size       Age")
    print("-" * 60)
    for bp in backups:
        size_mb = bp.stat().st_size / (1024 * 1024)
        age = datetime.now() - datetime.fromtimestamp(bp.stat().st_mtime)
        print(f"  {bp.name:<40} {size_mb:>7.1f} MB  {age.days:>5} days ago")
    print(f"Total: {len(backups)} backups")


def main():
    parser = argparse.ArgumentParser(description="SQLite database backup")
    parser.add_argument("--max-age", type=int, default=7)
    parser.add_argument("--keep-min", type=int, default=3)
    parser.add_argument("--no-cleanup", action="store_true")
    parser.add_argument("--list", action="store_true")
    args = parser.parse_args()
    if args.list:
        list_backups()
        return
    try:
        backup_database()
        if not args.no_cleanup:
            cleanup_old_backups(max_age_days=args.max_age, keep_min=args.keep_min)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()