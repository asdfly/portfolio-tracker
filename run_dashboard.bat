@echo off
chcp 65001 >nul
title 投资组合跟踪分析 - 可视化Dashboard
echo ============================================================
echo   投资组合跟踪分析系统 - 可视化Dashboard
echo   端口: 8501 | 浏览器访问: http://localhost:8501
echo ============================================================
echo.

cd /d "%~dp0"

python -m streamlit run dashboard.py --server.port 8501 --server.headless true

pause
