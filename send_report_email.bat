@echo off
chcp 65001 >nul

set PROJECT_DIR=%~dp0
set PYTHON="%~dp0venv313\Scripts\python.exe" -E

cd /d "%PROJECT_DIR%"

%PYTHON% scripts/send_report_email.py

if %ERRORLEVEL% NEQ 0 (
    echo.
    echo [WARN] report email send failed, rc=%ERRORLEVEL%
    exit /b %ERRORLEVEL%
) else (
    echo.
    echo [成功] 日报邮件推送完成
)
