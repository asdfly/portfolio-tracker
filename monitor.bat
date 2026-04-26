@echo off
chcp 65001 >nul
title 投资组合监控面板

set PROJECT_DIR=%~dp0
set PYTHON=C:\Users\HUAWEI\AppData\Local\Programs\Python\Python311\python.exe

:menu
echo.
echo ============================================
echo      投资组合监控系统
echo ============================================
echo.
echo  [1] 查看系统健康状态
echo  [2] 查看今日告警
echo  [3] 查看最近7天执行统计
echo  [4] 手动运行分析
echo  [5] 退出
echo.

set /p choice="输入选项 (1-5): "

if "%choice%"=="1" goto health
if "%choice%"=="2" goto today_alerts
if "%choice%"=="3" goto stats
if "%choice%"=="4" goto run
if "%choice%"=="5" goto exit
goto menu

:health
echo.
cd /d "%PROJECT_DIR%"
"%PYTHON%" run_enhanced.py --health
echo.
pause
goto menu

:today_alerts
echo.
cd /d "%PROJECT_DIR%"
"%PYTHON%" run_enhanced.py --alerts 24
echo.
pause
goto menu

:stats
echo.
cd /d "%PROJECT_DIR%"
"%PYTHON%" run_enhanced.py --stats 7
echo.
pause
goto menu

:run
echo.
cd /d "%PROJECT_DIR%"
"%PYTHON%" run_enhanced.py --run
echo.
pause
goto menu

:exit
echo.
timeout /t 1 >nul
exit
