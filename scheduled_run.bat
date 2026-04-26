@echo off
chcp 65001 >nul
cd /d "%~dp0"
"C:\Users\HUAWEI\AppData\Roaming\WPS 灵犀\python-env\python.exe" run_analysis.py >> logs\scheduled_run.log 2>&1
