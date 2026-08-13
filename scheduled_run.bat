@echo off
chcp 65001 >nul
cd /d "%~dp0"

:: ========================================================================
:: 定时任务包装器 - 调用 run_analysis.bat 并把输出追加到 scheduled_run.log
::
:: 说明: 原 Stage 0 内联备份块(产出 backup_YYYYMMDD.db)已退休。
::       数据库备份已并入 run_analysis.py 主流程，由 scripts/backup_db.py
::       统一实现(SQLite 在线备份 API + 7 天保留策略)，全项目只保留一套备份。
:: ========================================================================

if not exist logs mkdir logs

call "%~dp0run_analysis.bat" >> logs\scheduled_run.log 2>&1

:: 收盘分析完成后推送日报邮件（HTML 附件，免确认；未启用 SMTP 时脚本内部自动跳过）
call "%~dp0send_report_email.bat" >> logs\scheduled_run.log 2>&1

exit /b %ERRORLEVEL%
