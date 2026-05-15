@echo off
echo 启动投资组合跟踪分析系统（模块化版本）...
echo.

cd /d "%~dp0"

echo 检查Python环境...
python --version
if errorlevel 1 (
    echo 错误: 未找到Python
    pause
    exit /b 1
)

echo.
echo 启动Streamlit应用...
streamlit run dashboard_main.py --server.port 8501

pause
