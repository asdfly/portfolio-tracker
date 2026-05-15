@echo off
echo 启动Portfolio Tracker Dashboard...
cd /d "%~dp0"
streamlit run dashboard.py --server.port 8501
pause
