@echo off
echo 激活投资组合项目虚拟环境...
cd /d "%~dp0"
call venv\Scripts\activate.bat
echo.
echo 虚拟环境已激活！
echo Python路径: %VIRTUAL_ENV%\Scripts\python.exe
echo.
echo 现在可以运行:
echo   streamlit run dashboard_main.py
echo.
cmd /k
