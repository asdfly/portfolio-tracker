@echo off
chcp 65001 >nul
title 投资组合智能分析系统 v1.3

set PROJECT_DIR=%~dp0

echo.
echo ============================================
echo      投资组合智能分析系统 v1.3
echo ============================================
echo.
echo  [1] 完整分析 (五阶段: 基础+风险+监控+智能+通知)
echo  [2] 快速分析 (仅阶段一: 基础持仓分析)
echo  [3] 监控面板 (健康检查/告警/统计)
echo  [4] 可视化Dashboard (Streamlit Web界面)
echo  [5] 增强版报告 (生成含图表的HTML报告)
echo  [6] 通知配置向导 (配置邮件/企业微信)
echo  [7] 历史数据回填 (补充历史使风险指标可用)
echo  [8] 退出
echo.
echo ============================================

set /p choice="请选择 (1-8): "

if "%choice%"=="1" goto full
if "%choice%"=="2" goto basic
if "%choice%"=="3" goto monitor
if "%choice%"=="4" goto dashboard
if "%choice%"=="5" goto report
if "%choice%"=="6" goto notify
if "%choice%"=="7" goto backfill
if "%choice%"=="8" goto exit
goto :eof

:full
echo.
echo 正在运行完整五阶段分析...
echo [1/5] 基础分析 - 持仓+行情+技术指标
echo [2/5] 风险分析 - 夏普+回撤+VaR+Beta
echo [3/5] 监控告警 - 异常检测+通知推送
echo [4/5] 智能分析 - 建议生成+报告输出
echo [5/5] 通知报告 - HTML邮件+企业微信
echo.
cd /d "%PROJECT_DIR%"
python run_analysis.py
echo.
pause
goto :eof

:basic
echo.
echo 正在运行快速分析...
cd /d "%PROJECT_DIR%"
python run_enhanced.py --run
echo.
pause
goto :eof

:monitor
echo.
echo ============================================
echo      监控管理面板
echo ============================================
echo.
echo  [1] 系统健康状态
echo  [2] 今日告警
echo  [3] 执行统计 (7天)
echo  [4] 手动触发告警检查
echo  [5] 返回主菜单
echo.

set /p mon="请选择 (1-5): "

if "%mon%"=="1" (
    cd /d "%PROJECT_DIR%"
    python run_enhanced.py --health
    echo.
    pause
    goto monitor
)
if "%mon%"=="2" (
    cd /d "%PROJECT_DIR%"
    python run_enhanced.py --alerts 24
    echo.
    pause
    goto monitor
)
if "%mon%"=="3" (
    cd /d "%PROJECT_DIR%"
    python run_enhanced.py --stats 7
    echo.
    pause
    goto monitor
)
if "%mon%"=="4" (
    cd /d "%PROJECT_DIR%"
    python run_enhanced.py --run
    echo.
    pause
    goto monitor
)
if "%mon%"=="5" goto :eof

:dashboard
echo.
echo 正在启动可视化Dashboard...
echo 访问地址: http://localhost:8501
echo 按 Ctrl+C 停止服务
echo.
cd /d "%PROJECT_DIR%"
set "PYTHON_ENV=C:\Users\HUAWEI\AppData\Roaming\WPS 灵犀\python-env\python.exe"
if exist "%PYTHON_ENV%" (
    "%PYTHON_ENV%" -m streamlit run dashboard.py --server.port 8501
) else (
    echo [错误] 未找到WPS灵犀Python环境，尝试使用系统Python...
    python -m streamlit run dashboard.py --server.port 8501
)
pause
goto :eof

:report
echo.
echo 正在生成增强版HTML报告（含净值走势图+回撤曲线）...
cd /d "%PROJECT_DIR%"
python -c "from src.utils.enhanced_report import EnhancedReportBuilder; from config.settings import DATABASE_PATH; b=EnhancedReportBuilder(str(DATABASE_PATH)); h=b.build_full_report(); p=b.save_report(h); print(f'报告已生成: {p}')"
echo.
pause
goto :eof

:notify
echo.
cd /d "%PROJECT_DIR%"
python setup_notification.py
pause
goto :eof

:backfill
echo.
echo 正在执行历史数据回填...
cd /d "%PROJECT_DIR%"
python backfill_history.py
echo.
pause
goto :eof

:exit
echo.
timeout /t 1 >nul
exit