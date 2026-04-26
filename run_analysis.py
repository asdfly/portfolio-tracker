#!/usr/bin/env python3
"""
投资组合智能分析系统 - 定时任务入口
整合四阶段完整功能: 基础优化 + 风险分析 + 自动化部署 + 智能分析
"""
import sys
import os
import time
import logging
from datetime import datetime, date
from pathlib import Path

# 添加项目路径
PROJECT_DIR = Path(__file__).parent
sys.path.insert(0, str(PROJECT_DIR))

from config.settings import (
    DATA_SOURCES, DATABASE_PATH, MONITOR_CONFIG,
    NOTIFICATION_CONFIG, SMART_ANALYSIS_CONFIG
)
from src.analysis.portfolio import PortfolioAnalyzer
from src.utils.database import DatabaseManager
from src.utils.monitor import Monitor
from src.utils.notification import NotificationManager
from src.utils.enhanced_report import EnhancedReportBuilder
from src.utils.news_fetcher import NewsFetcher, save_news_to_db
from src.report.smart_report import SmartReportGenerator
from src.analysis.backtest import StrategyBacktester, RebalanceStrategy

# ==================== 日志配置 ====================
def setup_logging():
    """配置日志"""
    log_dir = PROJECT_DIR / "logs"
    log_dir.mkdir(exist_ok=True)

    today_str = date.today().strftime('%Y%m%d')
    log_file = log_dir / f"portfolio_{today_str}.log"

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


def run_stage1_basic(analyzer):
    """阶段一: 基础分析 - 持仓数据获取、技术指标计算"""
    logger = logging.getLogger(__name__)
    logger.info("[阶段一/4] 基础分析 - 获取持仓和行情数据")
    logger.info("-" * 50)

    # PortfolioAnalyzer.run_daily_analysis() 已包含:
    # - 读取持仓数据
    # - 获取实时行情 (多数据源自动切换)
    # - 获取指数行情
    # - 计算技术指标 (MA/MACD/RSI/KDJ/布林带/ATR)
    # - 保存到数据库
    results = analyzer.run_daily_analysis()

    positions = results.get('positions', [])
    summary = results.get('summary', {})
    indices = results.get('indices', {})
    technical = results.get('technical', {})

    logger.info(f"持仓数量: {len(positions)}")
    logger.info(f"总市值: {summary.get('total_value', 0):,.2f}")
    logger.info(f"当日收益: {summary.get('daily_return', 0):+.2f}%")

    return results


def run_stage2_risk(analyzer, results):
    """阶段二: 风险分析 - 已在run_daily_analysis中集成，此处提取结果"""
    logger = logging.getLogger(__name__)
    logger.info("[阶段二/4] 风险分析 - 提取风险指标")
    logger.info("-" * 50)

    risk_data = results.get('risk', {})
    summary = results.get('summary', {})

    sharpe = summary.get('sharpe_ratio', 'N/A')
    max_dd = summary.get('max_drawdown', 'N/A')
    volatility = summary.get('volatility', 'N/A')

    logger.info(f"夏普比率: {sharpe}")
    logger.info(f"最大回撤: {max_dd}%")
    logger.info(f"波动率: {volatility}%")

    return risk_data


def run_stage3_monitor(summary, risk_data):
    """阶段三: 自动化部署 - 告警检测和通知"""
    logger = logging.getLogger(__name__)
    logger.info("[阶段三/4] 监控告警 - 检查异常并通知")
    logger.info("-" * 50)

    monitor = Monitor(str(DATABASE_PATH), MONITOR_CONFIG)
    notifier = NotificationManager(NOTIFICATION_CONFIG)

    # 告警检测
    check_data = {
        'daily_return': summary.get('daily_return', 0),
        'max_drawdown': risk_data.get('max_drawdown', summary.get('max_drawdown', 0)),
        'concentration_hhi': risk_data.get('concentration_hhi', 0),
        'volatility': risk_data.get('volatility', summary.get('volatility', 0)),
        'sharpe_ratio': risk_data.get('sharpe_ratio', summary.get('sharpe_ratio', 0)),
    }

    alerts = monitor.check_alerts(check_data, check_data)

    if alerts:
        logger.warning(f"触发 {len(alerts)} 条告警:")
        for alert in alerts:
            logger.warning(f"  [{alert.level}] {alert.rule_name}: {alert.message}")
            notifier.send_alert(alert.rule_name, alert.message, alert.level)
    else:
        logger.info("无告警触发")

    # 发送日报通知
    if MONITOR_CONFIG.get('auto_notify', True):
        logger.info("发送日报通知...")
        report_data = {
            'summary': summary,
            'risk': risk_data,
            'alerts': alerts
        }
        notifier.send_portfolio_report(report_data)

    return alerts


def run_stage_news(positions, summary, index_quotes=None):
    """阶段3.5: 行业新闻抓取与分析"""
    logger = logging.getLogger(__name__)
    logger.info("[阶段3.5] 行业新闻 - 抓取资讯并分析影响")
    logger.info("-" * 50)

    try:
        fetcher = NewsFetcher()
        news_data = fetcher.fetch_all_news()

        total_news = sum(len(v.get('news', [])) for v in news_data.values())
        logger.info(f"获取新闻: {total_news} 条")

        # 保存到数据库
        save_news_to_db(str(DATABASE_PATH), news_data)
        logger.info("新闻数据已保存到数据库")

        # 新闻影响分析
        impacts = fetcher.analyze_news_impact(news_data, positions)
        pos_impacts = [i for i in impacts if i.get('affected_positions')]
        logger.info(f"影响评估: {len(impacts)}条相关, {len(pos_impacts)}条影响持仓")

        # 行业轮动分析
        rotation = fetcher.generate_rotation_analysis(positions, index_quotes or {})
        if rotation.get('leaders'):
            logger.info(f"领涨: {rotation['leaders'][0]['name']} ({rotation['leaders'][0].get('change_pct', 0):+.2f}%)")
        if rotation.get('laggards'):
            logger.info(f"领跌: {rotation['laggards'][0]['name']} ({rotation['laggards'][0].get('change_pct', 0):+.2f}%)")
        logger.info(f"趋势: {rotation.get('trend', 'N/A')}")

        return {
            'news': news_data,
            'impacts': impacts,
            'rotation': rotation
        }

    except Exception as e:
        logger.warning(f"新闻抓取失败(不影响主流程): {e}")
        return {'news': {}, 'impacts': [], 'rotation': {}}


def run_stage4_smart(results, summary, risk_data):
    """阶段四: 智能分析 - 策略回测和建议生成"""
    logger = logging.getLogger(__name__)

    if not SMART_ANALYSIS_CONFIG.get('advice_enabled', True):
        logger.info("[阶段四/4] 智能分析 - 已跳过（配置关闭）")
        return None

    logger.info("[阶段四/4] 智能分析 - 生成建议和报告")
    logger.info("-" * 50)

    try:
        db = DatabaseManager()
        import sqlite3
        conn = sqlite3.connect(str(DATABASE_PATH))
        smart_report = SmartReportGenerator(conn)

        combined_data = {
            'summary': summary,
            'risk': risk_data,
            'technical': results.get('technical', {}),
            'positions': results.get('positions', [])
        }

        # 获取建议摘要
        advice_summary = smart_report.get_advice_summary(combined_data)
        total = advice_summary.get('total', 0)
        high = advice_summary.get('high', 0)

        logger.info(f"生成建议: {total} 条 (高优先级: {high})")

        for advice in advice_summary.get('advices', [])[:5]:
            logger.info(f"  [{advice.priority.value}] {advice.title}")

        # 生成智能报告
        report_dir = PROJECT_DIR / "data" / "reports"
        report_dir.mkdir(parents=True, exist_ok=True)

        report_path = report_dir / f"smart_report_{date.today().strftime('%Y%m%d')}.md"
        smart_report.generate_full_report(combined_data, str(report_path))
        logger.info(f"智能报告已保存: {report_path}")

        return advice_summary

    except Exception as e:
        logger.error(f"智能分析失败: {e}")
        return None


def send_daily_report(results, alerts, advice_summary, news_result):
    """阶段五: 发送HTML邮件报告"""
    logger = logging.getLogger(__name__)
    logger.info("[阶段五/5] 发送通知报告")

    try:
        # 始终生成本地增强版HTML报告（含图表）
        logger.info("生成增强版HTML报告...")
        try:
            enh_builder = EnhancedReportBuilder(str(DATABASE_PATH))
            enh_html = enh_builder.build_full_report(news_data=news_result)
            enh_report_name = f"enhanced_report_{date.today().strftime('%Y%m%d')}.html"
            enh_builder.save_report(enh_html, enh_report_name, news_data=news_result)
            logger.info(f"增强版报告已保存: {enh_report_name}")
        except Exception as enh_err:
            logger.warning(f"增强版报告生成失败: {enh_err}")

        # 检查是否有notification.json配置
        config_path = PROJECT_DIR / "config" / "notification.json"
        if not config_path.exists():
            logger.info("通知未配置 (缺少 config/notification.json)，跳过发送")
            return

        import json
        with open(config_path, 'r', encoding='utf-8') as f:
            notify_cfg = json.load(f)

        # 检查邮件是否启用
        email_cfg = notify_cfg.get('email', {})
        if not email_cfg.get('enabled', False):
            logger.info("邮件通知未启用，跳过发送")
            return

        # 生成HTML报告
        builder = EmailReportBuilder(str(DATABASE_PATH))
        html = builder.build_daily_report()

        # 保存本地副本
        report_filename = f"daily_report_{date.today().strftime('%Y%m%d')}.html"
        builder.save_report(html, report_filename)

        # 发送邮件
        import smtplib
        from email.mime.text import MIMEText
        from email.mime.multipart import MIMEMultipart

        msg = MIMEMultipart('alternative')
        msg['Subject'] = f"投资组合日报 - {date.today().strftime('%Y-%m-%d')}"
        msg['From'] = email_cfg.get('sender', email_cfg.get('username'))
        msg['To'] = ', '.join(email_cfg.get('recipients', []))
        msg.attach(MIMEText(html, 'html', 'utf-8'))

        port = email_cfg.get('smtp_port', 465)
        if port == 465:
            server = smtplib.SMTP_SSL(email_cfg.get('smtp_server'), port, timeout=15)
        else:
            server = smtplib.SMTP(email_cfg.get('smtp_server'), port, timeout=15)
            server.starttls()

        server.login(email_cfg.get('username'), email_cfg.get('password'))
        server.send_message(msg)
        server.quit()

        logger.info(f"邮件报告发送成功: {', '.join(email_cfg.get('recipients', []))}")

        # 发送企业微信
        wechat_cfg = notify_cfg.get('wechat', {})
        if wechat_cfg.get('enabled', False) and wechat_cfg.get('webhook_url'):
            import requests
            summary = results.get('summary', {})
            wcontent = (
                f"投资组合日报 {date.today().strftime('%Y-%m-%d')}"
                f"\n总市值: {summary.get('total_value', 0):,.0f}"
                f"\n当日收益: {summary.get('daily_return', 0):+.2f}%"
                f"\n夏普比率: {summary.get('sharpe_ratio', 'N/A')}"
                f"\n最大回撤: {summary.get('max_drawdown', 'N/A')}%"
            )
            if alerts:
                wcontent += f"\n告警: {len(alerts)}条"
            if advice_summary:
                wcontent += f"\n建议: {advice_summary.get('total', 0)}条"

            payload = {"msgtype": "text", "text": {"content": wcontent}}
            resp = requests.post(wechat_cfg['webhook_url'], json=payload, timeout=10)
            if resp.json().get('errcode') == 0:
                logger.info("企业微信通知发送成功")
            else:
                logger.warning(f"企业微信发送失败: {resp.json().get('errmsg')}")

    except Exception as e:
        logger.error(f"通知发送失败: {e}")


def print_summary(results, alerts, advice_summary):
    """打印最终摘要"""
    logger = logging.getLogger(__name__)

    summary = results.get('summary', {})
    risk = results.get('risk', {})

    logger.info("")
    logger.info("=" * 60)
    logger.info("               分析结果摘要")
    logger.info("=" * 60)

    logger.info(f"日期:         {summary.get('date', date.today())}")
    logger.info(f"总市值:       {summary.get('total_value', 0):>12,.2f}")
    logger.info(f"当日盈亏:     {summary.get('daily_pnl', 0):>+12,.2f} ({summary.get('daily_return', 0):+.2f}%)")
    logger.info(f"累计盈亏:     {summary.get('total_pnl', 0):>+12,.2f}")
    logger.info(f"vs 沪深300:   {summary.get('vs_hs300', 0):>+12.2f}%")
    logger.info(f"盈/亏品种:    {summary.get('profit_count', 0)}/{summary.get('loss_count', 0)}")

    sharpe = summary.get('sharpe_ratio', risk.get('sharpe_ratio', 'N/A'))
    max_dd = summary.get('max_drawdown', risk.get('max_drawdown', 'N/A'))
    vol = summary.get('volatility', risk.get('volatility', 'N/A'))

    logger.info(f"夏普比率:     {sharpe}")
    logger.info(f"最大回撤:     {max_dd}%")
    logger.info(f"波动率:       {vol}%")

    if alerts:
        logger.info(f"告警:         {len(alerts)} 条")

    if advice_summary:
        logger.info(f"智能建议:     {advice_summary.get('total', 0)} 条 (高优先级: {advice_summary.get('high', 0)})")

    logger.info("=" * 60)


# ==================== 主函数 ====================
def is_trading_day():
    """判断今天是否为交易日（周末跳过，节假日需配合手动关闭）"""
    today = date.today()
    weekday = today.weekday()
    if weekday >= 5:  # 5=周六, 6=周日
        return False
    return True


def main():
    """主函数 - 整合四阶段完整分析流程"""
    setup_logging()
    logger = logging.getLogger(__name__)

    # 交易日判断
    if not is_trading_day():
        logger.info(f"今日为周末，跳过分析: {date.today()}")
        return 0

    start_time = time.time()
    task_name = "portfolio_daily_analysis"

    try:
        logger.info("")
        logger.info("=" * 60)
        logger.info("   投资组合智能分析系统 v1.3")
        logger.info(f"   执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 60)
        logger.info("")

        # 初始化监控
        monitor = Monitor(str(DATABASE_PATH), MONITOR_CONFIG)
        monitor.log_execution(task_name, "running", "开始每日完整分析")

        # 初始化分析器
        analyzer = PortfolioAnalyzer()

        # === 阶段一: 基础分析 ===
        results = run_stage1_basic(analyzer)

        # === 阶段二: 风险分析 ===
        risk_data = run_stage2_risk(analyzer, results)

        # === 阶段三: 监控告警 ===
        summary = results.get('summary', {})
        alerts = run_stage3_monitor(summary, risk_data)

        # === 阶段三.五: 行业资讯与新闻分析 ===
        positions = results.get('positions', [])
        news_result = run_stage_news(positions, summary, results.get('indices', {}))

        # === 阶段四: 智能分析 ===
        advice_summary = run_stage4_smart(results, summary, risk_data)

        # === 打印摘要 ===
        print_summary(results, alerts, advice_summary)

        # === 阶段五: 发送通知报告 ===
        send_daily_report(results, alerts, advice_summary, news_result)

        # 记录执行成功
        duration = time.time() - start_time
        monitor.log_execution(task_name, "success", "每日分析完成", duration)

        logger.info(f"任务执行成功! 耗时: {duration:.2f}秒")
        logger.info("")

        return 0

    except Exception as e:
        duration = time.time() - start_time
        logger.error(f"任务执行失败: {e}", exc_info=True)

        # 记录失败
        try:
            monitor = Monitor(str(DATABASE_PATH), MONITOR_CONFIG)
            monitor.log_execution(task_name, "failed", str(e), duration)

            notifier = NotificationManager(NOTIFICATION_CONFIG)
            notifier.send_alert("Daily Analysis Failed", str(e), "error")
        except:
            pass

        return 1


if __name__ == '__main__':
    sys.exit(main())
