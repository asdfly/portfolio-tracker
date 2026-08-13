@echo off
chcp 65001 >nul

set PROJECT_DIR=%~dp0
set PYTHON="%~dp0venv313\Scripts\python.exe" -E

cd /d "%PROJECT_DIR%"

%PYTHON% scripts/send_report_email.py

if %ERRORLEVEL% NEQ 0 (
    echo.
    echo [WARN] report email send FAILED, rc=%ERRORLEVEL%
    exit /b %ERRORLEVEL%
) else (
    echo.
    echo [OK] daily report email sent
)
