@echo off
chcp 65001 >nul
cd /d "%~dp0"

:: ========== Stage 0: Database Backup ==========
set PYTHON="C:\Users\HUAWEI\AppData\Roaming\WPS 灵犀\python-env\python.exe"
set DB=data\database\portfolio.db
set BACKUP_DIR=data\backups

if exist %DB% (
    echo [%date% %time%] Starting daily backup...
    %PYTHON% -c "
import sqlite3, shutil, os, hashlib, sys
from pathlib import Path
from datetime import date
db = Path(r'data\database\portfolio.db')
bak_dir = Path(r'data\backups')
bak_dir.mkdir(parents=True, exist_ok=True)
today = date.today().strftime('%%Y%%m%%d')
bak_path = bak_dir / f'backup_{today}.db'
if not bak_path.exists():
    conn = sqlite3.connect(str(db))
    conn.execute(f'VACUUM INTO \"{bak_path}\"')
    conn.close()
    sz = bak_path.stat().st_size / 1024 / 1024
    print(f'  Backup OK: {bak_path.name} ({sz:.1f} MB)')
    # cleanup: keep last 7 days
    kept = 0
    for f in sorted(bak_dir.glob('backup_*.db'), reverse=True):
        kept += 1
        if kept > 7:
            f.unlink()
            print(f'  Removed old: {f.name}')
else:
    print(f'  Backup already exists: {bak_path.name}')
" >> logs\scheduled_run.log 2>&1
) else (
    echo [%date% %time%] Database not found, skipping backup >> logs\scheduled_run.log
)

:: ========== Stage 1-5: Daily Analysis ==========
%PYTHON% run_analysis.py >> logs\scheduled_run.log 2>&1
