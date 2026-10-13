@echo off
chcp 65001 >nul

set PROJECT_DIR=%~dp0
set PYTHON="%~dp0venv313\Scripts\python.exe" -E

cd /d "%PROJECT_DIR%"

:: 日志重定向放在 bat 内部，避免任务计划器对 Action 参数中 '>' 的解析怪癖
%PYTHON% scripts/run_sentinel.py %* >> "%PROJECT_DIR%logs\sentinel.log" 2>&1
set rc=%ERRORLEVEL%
echo. >> "%PROJECT_DIR%logs\sentinel.log"

if %rc% NEQ 0 (
    echo [WARN] sentinel rc=%rc% >> "%PROJECT_DIR%logs\sentinel.log"
    exit /b %rc%
) else (
    echo [OK] sentinel done >> "%PROJECT_DIR%logs\sentinel.log"
)
