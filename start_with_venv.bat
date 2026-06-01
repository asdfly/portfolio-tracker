@echo off
chcp 65001 >nul
echo 启动投资组合跟踪系统（使用虚拟环境）...
cd /d "%~dp0"
call venv\Scripts\activate.bat
streamlit run dashboard.py --server.port 8501
pause
