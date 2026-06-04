@echo off
echo ͶĿ⻷...
cd /d "%~dp0"
call venv\Scripts\activate.bat
echo.
echo ⻷Ѽ
echo Python·: %VIRTUAL_ENV%\Scripts\python.exe
echo.
echo ڿ:
echo   streamlit run dashboard.py
echo.
cmd /k
