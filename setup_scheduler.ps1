
# 投资组合智能分析系统 - 定时任务配置脚本
# 以管理员身份运行 PowerShell
# 用法: .\setup_scheduler.ps1

param(
    [switch]$Uninstall
)

$TaskName = "PortfolioDailyAnalysis"
$TaskDescription = "投资组合智能分析系统 v1.2 - 每交易日15:10执行四阶段完整分析"
$ScriptPath = "C:\Users\HUAWEI\Documents\lingxi-claw\portfolio_tracker\run_analysis.bat"

# 检查脚本是否存在
if (-not (Test-Path $ScriptPath)) {
    Write-Error "脚本不存在: $ScriptPath"
    exit 1
}

# 卸载模式
if ($Uninstall) {
    Write-Host "正在卸载定时任务..." -ForegroundColor Yellow
    try {
        Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction Stop
        Write-Host "定时任务已卸载: $TaskName" -ForegroundColor Green
    } catch {
        Write-Warning "任务不存在或卸载失败: $_"
    }
    exit 0
}

# 创建任务动作
$Action = New-ScheduledTaskAction `
    -Execute $ScriptPath `
    -WorkingDirectory "C:\Users\HUAWEI\Documents\lingxi-claw\portfolio_tracker"

# 创建触发器 - 周一至周五 15:10
$Trigger = New-ScheduledTaskTrigger -Daily -At "15:10"
# 注意: Windows任务计划程序 Daily 触发器每天运行
# 在 run_analysis.py 中已做周末判断

# 创建任务主体 - 以当前用户身份运行
$Principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType S4U -RunLevel Highest

# 任务设置
$Settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -ExecutionTimeLimit (New-TimeSpan -Hours 1) `
    -MultipleInstances IgnoreNew

# 注册任务
try {
    # 先删除旧任务（如果存在）
    $existing = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    if ($existing) {
        Write-Host "检测到已有任务，正在更新..." -ForegroundColor Yellow
        Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    }

    Register-ScheduledTask `
        -TaskName $TaskName `
        -Description $TaskDescription `
        -Action $Action `
        -Trigger $Trigger `
        -Principal $Principal `
        -Settings $Settings

    Write-Host ""
    Write-Host "定时任务配置成功!" -ForegroundColor Green
    Write-Host ""
    Write-Host "  任务名称:   $TaskName" 
    Write-Host "  任务描述:   $TaskDescription"
    Write-Host "  执行时间:   每日 15:10 (交易日)"
    Write-Host "  执行脚本:   $ScriptPath"
    Write-Host "  运行用户:   $env:USERNAME"
    Write-Host ""
    Write-Host "分析流程:" -ForegroundColor Cyan
    Write-Host "  [1] 基础分析 - 持仓获取 + 多数据源行情 + 技术指标"
    Write-Host "  [2] 风险分析 - 夏普比率 + 最大回撤 + VaR + Beta/Alpha"
    Write-Host "  [3] 监控告警 - 5项自动告警 + 邮件/企业微信通知"
    Write-Host "  [4] 智能分析 - 策略回测 + 智能建议 + 报告生成"
    Write-Host ""
    Write-Host "任务状态:" -ForegroundColor Cyan
    Get-ScheduledTask -TaskName $TaskName | Select-Object TaskName, State | Format-Table -AutoSize
    Get-ScheduledTask -TaskName $TaskName | Get-ScheduledTaskInfo | Select-Object NextRunTime, LastRunTime | Format-Table -AutoSize

    Write-Host ""
    Write-Host "管理命令:" -ForegroundColor Cyan
    Write-Host "  启动任务:   Start-ScheduledTask -TaskName '$TaskName'"
    Write-Host "  查看状态:   Get-ScheduledTask -TaskName '$TaskName'"
    Write-Host "  卸载任务:   .\setup_scheduler.ps1 -Uninstall"
    Write-Host ""

} catch {
    Write-Error "创建任务失败: $_"
    exit 1
}
