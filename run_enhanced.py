
"""
增强版分析脚本 - 运行完整分析 + 监控管理
"""
import os
import sys
import time
import logging
from datetime import datetime, date
from pathlib import Path

# 添加项目路径
PROJECT_DIR = Path(__file__).parent
sys.path.insert(0, str(PROJECT_DIR))

from config.settings import DATABASE_PATH, MONITOR_CONFIG, NOTIFICATION_CONFIG
from src.utils.monitor import Monitor
from src.utils.notification import NotificationManager
from src.analysis.portfolio import PortfolioAnalyzer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def run_full_analysis():
    """运行完整四阶段分析"""
    start_time = time.time()
    task_name = "manual_analysis"

    try:
        logger.info("开始手动执行完整分析...")

        monitor = Monitor(str(DATABASE_PATH), MONITOR_CONFIG)
        monitor.log_execution(task_name, "running", "手动触发完整分析")

        analyzer = PortfolioAnalyzer()
        results = analyzer.run_daily_analysis()

        summary = results.get("summary", {})

        # 检查告警
        risk_data = results.get("risk", {})
        check_data = {
            "daily_return": summary.get("daily_return", 0),
            "max_drawdown": risk_data.get("max_drawdown", 0),
            "concentration_hhi": risk_data.get("concentration_hhi", 0),
            "volatility": risk_data.get("volatility", 0),
            "sharpe_ratio": risk_data.get("sharpe_ratio", 0),
        }

        alerts = monitor.check_alerts(check_data, check_data)
        if alerts:
            logger.warning(f"触发 {len(alerts)} 条告警")
            notifier = NotificationManager(NOTIFICATION_CONFIG)
            for alert in alerts:
                notifier.send_alert(alert.rule_name, alert.message, alert.level)

        duration = time.time() - start_time
        monitor.log_execution(task_name, "success", f"手动分析完成", duration)

        logger.info(f"分析完成，耗时: {duration:.2f}秒")
        logger.info(f"总市值: {summary.get('total_value', 0):,.2f}")
        logger.info(f"当日收益: {summary.get('daily_return', 0):+.2f}%")
        logger.info(f"告警数: {len(alerts)}")

        return True

    except Exception as e:
        duration = time.time() - start_time
        logger.error(f"分析失败: {e}")
        try:
            monitor = Monitor(str(DATABASE_PATH), MONITOR_CONFIG)
            monitor.log_execution(task_name, "failed", str(e), duration)
        except:
            pass
        return False


def check_health():
    """检查系统健康状态"""
    monitor = Monitor(str(DATABASE_PATH), MONITOR_CONFIG)
    health = monitor.get_health_status()

    print("")
    print("系统健康状态:")
    print(f"  状态:       {health['status']}")
    print(f"  上次执行:   {health['last_execution']}")
    print(f"  执行状态:   {health['last_execution_status']}")
    print(f"  未确认告警: {health['unacknowledged_alerts']}")
    print(f"  今日告警:   {health['today_alerts']}")
    return health


def show_stats(days=7):
    """显示执行统计"""
    monitor = Monitor(str(DATABASE_PATH), MONITOR_CONFIG)
    stats = monitor.get_execution_stats(days)

    print("")
    print(f"最近{days}天执行统计:")
    print(f"  总任务数: {stats['total']}")
    print(f"  成功:     {stats['success']}")
    print(f"  失败:     {stats['failed']}")
    print(f"  成功率:   {stats['success_rate']:.1f}%")
    print(f"  平均耗时: {stats['avg_duration']:.2f}秒")
    return stats


def show_alerts(hours=24):
    """显示告警"""
    monitor = Monitor(str(DATABASE_PATH), MONITOR_CONFIG)
    alerts = monitor.get_recent_alerts(hours)

    print("")
    print(f"最近{hours}小时告警:")
    if not alerts:
        print("  无告警")
    else:
        for alert in alerts:
            status = "[已确认]" if alert.acknowledged else "[未确认]"
            print(f"  {status} [{alert.level}] {alert.rule_name}: {alert.message}")
    return alerts


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="投资组合跟踪分析 - 增强版")
    parser.add_argument("--run", action="store_true", help="运行完整分析")
    parser.add_argument("--health", action="store_true", help="检查健康状态")
    parser.add_argument("--stats", type=int, metavar="DAYS", help="执行统计")
    parser.add_argument("--alerts", type=int, metavar="HOURS", help="查看告警")

    args = parser.parse_args()

    if args.run:
        success = run_full_analysis()
        sys.exit(0 if success else 1)
    elif args.health:
        check_health()
    elif args.stats:
        show_stats(args.stats)
    elif args.alerts:
        show_alerts(args.alerts)
    else:
        parser.print_help()
