@echo off
chcp 65001 >nul
title 投资组合Dashboard - Streamlit

set PROJECT_DIR=%~dp0
set PYTHON_ENV=C:\Users\HUAWEI\AppData\Roaming\WPS 灵犀\python-env\python.exe

echo.
echo ============================================
echo      投资组合Dashboard
echo ============================================
echo.
echo  启动中，请稍候...
echo  访问地址: http://localhost:8501
echo  按 Ctrl+C 停止服务
echo.

:: 检查Python环境
if not exist "%PYTHON_ENV%" (
    echo [错误] 未找到Python环境: %PYTHON_ENV%
    echo 请确认WPS灵犀已正确安装
    pause
    exit /b 1
)

:: 启动Dashboard
cd /d "%PROJECT_DIR%"
"%PYTHON_ENV%" -m streamlit run dashboard.py --server.port 8501 --server.headless true

pause
