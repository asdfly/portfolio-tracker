@echo off
chcp 65001 >nul

set PROJECT_DIR=%~dp0
set PYTHON=C:\Users\HUAWEI\AppData\Local\Programs\Python\Python311\python.exe

echo ============================================
echo   投资组合智能分析系统 v1.2
echo   执行时间: %date% %time%
echo ============================================
echo.
echo   阶段一: 基础分析 (持仓+行情+技术指标)
echo   阶段二: 风险分析 (夏普/回撤/VaR/Beta)
echo   阶段三: 监控告警 (异常检测+通知推送)
echo   阶段四: 智能分析 (建议生成+报告输出)
echo.
echo ============================================

cd /d "%PROJECT_DIR%"

"%PYTHON%" run_analysis.py

if %ERRORLEVEL% NEQ 0 (
    echo.
    echo [错误] 分析任务执行失败，错误码: %ERRORLEVEL%
    echo [%date% %time%] 请检查日志文件: logs\portfolio_%date:~0,4%%date:~5,2%%date:~8,2%.log
    exit /b 1
) else (
    echo.
    echo [成功] 分析任务执行完成
    echo [%date% %time%] 全部四个阶段执行完毕
)
