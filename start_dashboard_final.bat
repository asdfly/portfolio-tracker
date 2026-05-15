@echo off
echo 启动投资组合跟踪分析系统...
echo 使用端口 8501
echo.
cd /d "%~dp0"
echo 检查虚拟环境...
if exist venv\Scripts\activate.bat (
    call venv\Scripts\activate.bat
    echo 虚拟环境已激活。
) else (
    echo 警告：虚拟环境不存在，使用系统Python。
)
echo.
echo 启动Streamlit应用...
"C:\Users\HUAWEI\Documents\lingxi-claw\portfolio_tracker\venv\Scripts\streamlit.exe" run dashboard_main.py --server.port 8501 --server.headless true
pause
