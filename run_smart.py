
"""
智能分析脚本 - 基于已有数据生成建议和报告
前置条件: 需要先运行 run_analysis.py（阶段一二生成基础数据）
"""
import os
import sys
import time
import logging
import sqlite3
from datetime import datetime, date
from pathlib import Path

PROJECT_DIR = Path(__file__).parent
sys.path.insert(0, str(PROJECT_DIR))

from config.settings import DATABASE_PATH, MONITOR_CONFIG, NOTIFICATION_CONFIG, SMART_ANALYSIS_CONFIG
from src.utils.database import DatabaseManager
from src.utils.monitor import Monitor
from src.utils.notification import NotificationManager
from src.report.smart_report import SmartReportGenerator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def run_smart_analysis():
    """运行智能分析"""
    start_time = time.time()

    if not SMART_ANALYSIS_CONFIG.get("advice_enabled", True):
        logger.warning("智能分析已关闭 (SMART_ANALYSIS_CONFIG.advice_enabled = False)")
        return {"success": False, "error": "智能分析已关闭"}

    try:
        logger.info("=" * 50)
        logger.info("开始智能分析")
        logger.info("=" * 50)

        monitor = Monitor(str(DATABASE_PATH), MONITOR_CONFIG)
        monitor.log_execution("smart_analysis", "running", "开始智能分析")

        # 获取最新数据
        db = DatabaseManager()
        latest = db.get_latest_portfolio()
        history = db.get_portfolio_history(days=30)

        if not latest:
            raise ValueError("数据库中无持仓数据，请先运行 run_analysis.py")

        # 获取最近汇总数据
        summary = {}
        if history:
            latest_summary = history[0]
            summary = {
                "total_value": latest_summary.get("total_value", 0),
                "total_pnl": latest_summary.get("total_pnl", 0),
                "daily_return": latest_summary.get("daily_return", 0),
                "daily_pnl": latest_summary.get("daily_pnl", 0),
                "sharpe_ratio": latest_summary.get("sharpe_ratio", 0),
                "max_drawdown": latest_summary.get("max_drawdown", 0),
                "volatility": latest_summary.get("volatility", 0),
            }

        # 风险数据
        risk_data = {k: v for k, v in summary.items() if k in ["sharpe_ratio", "max_drawdown", "volatility"]}
        risk_data["concentration_hhi"] = 0  # 由数据库计算

        # 构建positions数据
        positions = []
        total_value = 0
        for row in latest:
            positions.append({
                "code": row.get("code"),
                "name": row.get("name"),
                "quantity": row.get("quantity", 0),
                "cost_price": row.get("cost_price", 0),
                "current_price": row.get("current_price", 0),
                "market_value": row.get("market_value", 0),
            })
            total_value += row.get("market_value", 0) or 0

        if not summary:
            summary["total_value"] = total_value

        # 计算集中度
        if total_value > 0:
            hhi = sum((p["market_value"] / total_value) ** 2 for p in positions if p["market_value"])
            risk_data["concentration_hhi"] = round(hhi, 4)

        # 连接数据库生成智能报告
        conn = sqlite3.connect(str(DATABASE_PATH))
        smart_report = SmartReportGenerator(conn)

        combined_data = {
            "summary": summary,
            "risk": risk_data,
            "technical": {},
            "positions": positions
        }

        # 获取建议
        advice_summary = smart_report.get_advice_summary(combined_data)

        logger.info(f"建议数量: {advice_summary['total']}")
        logger.info(f"高优先级: {advice_summary['high']}")
        logger.info(f"中优先级: {advice_summary['medium']}")
        logger.info(f"低优先级: {advice_summary['low']}")

        for advice in advice_summary.get("advices", []):
            logger.info(f"  [{advice.priority.value}] {advice.title}")

        # 生成报告
        report_dir = PROJECT_DIR / "data" / "reports"
        report_dir.mkdir(parents=True, exist_ok=True)

        report_path = report_dir / f"smart_report_{date.today().strftime('%Y%m%d')}.md"
        smart_report.generate_full_report(combined_data, str(report_path))
        logger.info(f"报告已保存: {report_path}")

        conn.close()

        # 检查告警并通知
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
        monitor.log_execution("smart_analysis", "success", f"智能分析完成", duration)

        logger.info(f"智能分析完成，耗时: {duration:.2f}秒")

        return {
            "success": True,
            "advice_count": advice_summary["total"],
            "high_priority": advice_summary["high"],
            "alert_count": len(alerts),
            "report_path": str(report_path)
        }

    except Exception as e:
        duration = time.time() - start_time
        logger.error(f"智能分析失败: {e}")
        try:
            monitor = Monitor(str(DATABASE_PATH), MONITOR_CONFIG)
            monitor.log_execution("smart_analysis", "failed", str(e), duration)
        except:
            pass
        return {"success": False, "error": str(e)}


if __name__ == "__main__":
    result = run_smart_analysis()

    if result["success"]:
        print("")
        print(f"智能分析完成")
        print(f"  建议数量: {result['advice_count']}")
        print(f"  高优先级: {result['high_priority']}")
        print(f"  告警数量: {result['alert_count']}")
        print(f"  报告路径: {result['report_path']}")
    else:
        print(f"智能分析失败: {result['error']}")
        sys.exit(1)
