#!/usr/bin/env python3
"""
投资组合智能分析系统 - 定时任务入口
整合四阶段完整功能: 基础优化 + 风险分析 + 自动化部署 + 智能分析
"""
import sys
import os
import time
import logging
import logging.handlers
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
from src.data_sources.macro_daily import fetch_all_macro_daily
from src.report.smart_report import SmartReportGenerator
from src.data_sources.fund_flow import (
    fetch_sector_fund_flow, fetch_etf_fund_flow,
    fetch_main_fund_flow, fetch_north_flow, save_fund_flows,
    backfill_etf_fund_flow_from_kline, backfill_sector_fund_flow,
    check_push2his_available, fetch_etf_fund_flow_batch,
)
from src.data_sources.market_events import run_market_events_collection
from src.analysis.backtest import StrategyBacktester, RebalanceStrategy

# ==================== 日志配置 ====================
def setup_logging():
    """配置日志（含轮转: 10MB/文件, 保留5份, 按天切分）"""
    log_dir = PROJECT_DIR / "logs"
    log_dir.mkdir(exist_ok=True)

    today_str = date.today().strftime('%Y%m%d')
    log_file = log_dir / f"portfolio_{today_str}.log"

    fmt = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # 主文件: RotatingFileHandler (10MB, 5份备份)
    file_handler = logging.handlers.RotatingFileHandler(
        log_file, maxBytes=10*1024*1024, backupCount=5, encoding='utf-8'
    )
    file_handler.setFormatter(fmt)

    # 控制台输出
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(fmt)

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers.clear()
    root.addHandler(file_handler)
    root.addHandler(console_handler)

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

    # 从嵌套结构中正确提取风险指标（与阶段三告警逻辑一致）
    portfolio_metrics = risk_data.get('portfolio_metrics', {})
    risk_adjusted = portfolio_metrics.get('risk_adjusted_metrics', {})
    drawdown = portfolio_metrics.get('drawdown_metrics', {})
    volatility_metrics = portfolio_metrics.get('volatility_metrics', {})
    risk_summary = summary.get('risk_summary', {})

    sharpe = risk_summary.get('sharpe_ratio',
                risk_adjusted.get('sharpe_ratio',
                    summary.get('sharpe_ratio', 'N/A')))
    max_dd = risk_summary.get('max_drawdown',
                drawdown.get('max_drawdown',
                    summary.get('max_drawdown', 'N/A')))
    volatility = risk_summary.get('annual_volatility',
                volatility_metrics.get('annual_volatility',
                    summary.get('volatility', 'N/A')))

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

    # 告警检测 - 从嵌套结构中正确提取风险指标
    portfolio_metrics = risk_data.get('portfolio_metrics', {})
    risk_adjusted = portfolio_metrics.get('risk_adjusted_metrics', {})
    drawdown = portfolio_metrics.get('drawdown_metrics', {})
    volatility = portfolio_metrics.get('volatility_metrics', {})
    concentration = risk_data.get('concentration_risk', {})
    risk_summary = summary.get('risk_summary', {})

    check_data = {
        'daily_return': summary.get('daily_return', 0),
        'max_drawdown': risk_summary.get('max_drawdown',
                    drawdown.get('max_drawdown', 0)),
        'concentration_hhi': concentration.get('hhi', 0),
        'volatility': risk_summary.get('annual_volatility',
                    volatility.get('annual_volatility', 0)),
        'sharpe_ratio': risk_summary.get('sharpe_ratio',
                    risk_adjusted.get('sharpe_ratio',
                        summary.get('sharpe_ratio', 0))),
    }

    # D4: 扩展告警数据源 - 数据新鲜度/数据质量/持仓变化/市值变化
    try:
        import sqlite3 as _sqlite3
        _conn = _sqlite3.connect(str(DATABASE_PATH))
        _cur = _conn.cursor()

        # 1) 数据源陈旧检查: 统计最近N天无更新的数据源数
        _stale_days = MONITOR_CONFIG.get('stale_threshold_days', 7)
        _cur.execute("SELECT COUNT(*) FROM ("             "SELECT 'portfolio_snapshots' AS src, MAX(date) AS last_date FROM portfolio_snapshots "             "UNION ALL SELECT 'fund_flows', MAX(date) FROM fund_flows "             "UNION ALL SELECT 'daily_news', MAX(date) FROM daily_news "             "UNION ALL SELECT 'market_sentiment', MAX(date) FROM market_sentiment "             "UNION ALL SELECT 'macro_daily', MAX(date) FROM macro_daily"             ") WHERE last_date < date('now', ?)", (f'-{_stale_days} days',))
        _stale_count = _cur.fetchone()[0]
        check_data['stale_sources_count'] = _stale_count
        check_data['stale_threshold_days'] = _stale_days

        # 2) 数据质量评分
        check_data['data_quality_score'] = 0
        check_data['data_quality_grade'] = 'N/A'
        _cur.execute("SELECT message FROM execution_logs "             "WHERE task_name = 'data_quality_check' "             "ORDER BY created_at DESC LIMIT 1")
        _dq_row = _cur.fetchone()
        if _dq_row:
            _dq_msg = _dq_row[0] or ''
            import re as _re
            _score_m = _re.search(r'score=([0-9.]+)', _dq_msg)
            _grade_m = _re.search(r'grade=([A-F])', _dq_msg)
            if _score_m:
                check_data['data_quality_score'] = float(_score_m.group(1))
            if _grade_m:
                check_data['data_quality_grade'] = _grade_m.group(1)

        # 3) 持仓数量变化
        _cur.execute("SELECT COUNT(*) FROM ("             "SELECT DISTINCT code FROM portfolio_snapshots "             "WHERE date = (SELECT MAX(date) FROM portfolio_snapshots))")
        _curr_count = _cur.fetchone()[0] or 0
        check_data['position_count_curr'] = _curr_count
        _prev_count = 0
        _cur.execute("SELECT DISTINCT date FROM portfolio_snapshots ORDER BY date DESC LIMIT 2 OFFSET 1")
        _prev_date_row = _cur.fetchone()
        if _prev_date_row:
            _cur.execute("SELECT COUNT(*) FROM ("                 "SELECT DISTINCT code FROM portfolio_snapshots WHERE date = ?)",
                (_prev_date_row[0],))
            _prev_count = _cur.fetchone()[0] or 0
        check_data['position_count_prev'] = _prev_count
        check_data['position_count_change_pct'] = (
            (_curr_count - _prev_count) / _prev_count * 100
        ) if _prev_count > 0 else 0

        # 4) 总市值变化
        _val_change = 0
        _cur.execute("SELECT total_value FROM portfolio_summary ORDER BY date DESC LIMIT 2")
        _value_rows = _cur.fetchall()
        if len(_value_rows) >= 2 and _value_rows[1][0] and _value_rows[1][0] > 0:
            _val_change = (_value_rows[0][0] - _value_rows[1][0]) / _value_rows[1][0] * 100
        check_data['total_value_change_pct'] = _val_change

        _conn.close()
        logger.info(f"D4告警数据扩展: stale={_stale_count}, dq={check_data.get('data_quality_score', 0)}, "
                     f"pos_change={check_data['position_count_change_pct']:+.1f}%, "
                     f"val_change={_val_change:+.1f}%")
    except Exception as e:
        logger.warning(f"D4告警数据扩展失败(使用默认值): {e}")

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



def run_stage_fund_flow():
    """阶段3.2: 资金流数据采集 - 行业/ETF/北向资金
    完全独立容错：任何采集失败均不影响主流程
    """
    logger = logging.getLogger(__name__)
    logger.info("[阶段3.2/5] 资金流数据采集")
    logger.info("-" * 50)

    stats = {"sector": 0, "etf": 0, "main_fund": 0, "errors": []}

    # 获取持仓 ETF 列表（从配置中读取，避免依赖阶段一结果）
    from config.settings import ETF_CATEGORIES
    etf_codes = list(ETF_CATEGORIES.keys())

    # 获取数据库连接
    from config.settings import DATABASE_PATH
    import sqlite3
    conn = sqlite3.connect(str(DATABASE_PATH))

    try:
        # --- 行业资金流 ---
        try:
            sector_df = fetch_sector_fund_flow()
            if not sector_df.empty:
                n = save_fund_flows(conn, sector_df)
                stats["sector"] = n
                logger.info(f"  行业资金流: {n} 条")
            else:
                logger.warning("  行业资金流: 无数据返回")
        except Exception as e:
            stats["errors"].append(f"行业资金流: {e}")
            logger.warning(f"  行业资金流采集失败(不影响主流程): {e}")

        # --- 行业资金流历史回填（基于同花顺多周期排行差值估算） ---
        try:
            bf_sector = backfill_sector_fund_flow(conn)
            if bf_sector > 0:
                logger.info(f"  行业资金流回填: {bf_sector} 条")
        except Exception as e:
            logger.warning(f"  行业资金流回填失败(跳过): {e}")

        # --- ETF 资金流 ---
        # 策略：先探测 push2his 可用性，可用则逐只采集（含历史数据）；
        #       不可用时直接走 fund_etf_spot_em 批量方案（单次请求，含完整字段）
        _push2his_ok = check_push2his_available()
        if _push2his_ok:
            # push2his 可用：逐只采集（返回完整历史资金流，含超大单/大单细分）
            logger.info("  ETF资金流: push2his 可用，逐只采集")
            consecutive_failures = 0
            max_consecutive_failures = 5
            etf_success = 0
            etf_skip = 0
            for code in etf_codes:
                try:
                    name = ETF_CATEGORIES[code].get("name", "")
                    df = fetch_etf_fund_flow(code, name)
                    if not df.empty:
                        n = save_fund_flows(conn, df)
                        stats["etf"] += n
                        etf_success += 1
                        consecutive_failures = 0
                    else:
                        consecutive_failures += 1
                        etf_skip += 1
                except Exception as e:
                    consecutive_failures += 1
                    etf_skip += 1
                    logger.warning(f"  ETF {code} 资金流失败(跳过): {e}")
                    stats["errors"].append(f"ETF {code}: {e}")
                time.sleep(0.5)
                if consecutive_failures >= max_consecutive_failures:
                    remaining = len(etf_codes) - etf_success - etf_skip
                    logger.warning(f"  ETF资金流: 连续{consecutive_failures}只失败，"
                                   f"跳过剩余{remaining}只")
                    break
            if stats["etf"] > 0:
                logger.info(f"  ETF资金流(push2his): {stats['etf']} 条 ({etf_success}/{len(etf_codes)} 只)")
        else:
            # push2his 不可用：走批量方案（单次请求，无逐只等待）
            logger.info("  ETF资金流: push2his 不可用，走批量方案")
            batch_df = fetch_etf_fund_flow_batch(etf_codes)
            if not batch_df.empty:
                n = save_fund_flows(conn, batch_df)
                stats["etf"] = n
                logger.info(f"  ETF资金流(批量): {n} 条 ({len(batch_df)} 只)")
            else:
                logger.warning("  ETF资金流: 批量方案也无数据")

        # --- ETF资金流历史回填（基于K线估算，补充push2his封锁缺失的历史） ---
        try:
            etf_name_map = {c: ETF_CATEGORIES[c].get("name", "") for c in etf_codes}
            bf_stats = backfill_etf_fund_flow_from_kline(conn, etf_name_map, target_days=120)
            bf_count = sum(v for v in bf_stats.values() if v > 0)
            if bf_count > 0:
                logger.info(f"  ETF历史回填: {bf_count} 条")
        except Exception as e:
            logger.warning(f"  ETF历史回填失败(跳过): {e}")

        # --- 主力资金净流入（替代已停更的北向资金） ---
        try:
            main_df = fetch_main_fund_flow(days=120)
            if not main_df.empty:
                n = save_fund_flows(conn, main_df)
                stats["main_fund"] = n
                logger.info(f"  主力资金: {n} 条")
            else:
                logger.warning("  主力资金: 无数据返回")
        except Exception as e:
            stats["errors"].append(f"主力资金: {e}")
            logger.warning(f"  主力资金采集失败(不影响主流程): {e}")

        # --- ETF 当日实时资金流补充 ---
        # 仅在 push2his 可用且逐只采集时作为补充；批量方案已包含完整字段无需重复
        if _push2his_ok:
            try:
                batch_df = fetch_etf_fund_flow_batch(etf_codes)
                if not batch_df.empty:
                    n = save_fund_flows(conn, batch_df)
                    logger.info(f"  ETF实时资金流补充: {n} 条")
            except Exception as e:
                logger.warning(f"  ETF实时资金流补充失败(跳过): {e}")

        logger.info(f"资金流采集完成: 行业{stats['sector']}条 + ETF{stats['etf']}条 + 主力资金{stats['main_fund']}条")
        if stats["errors"]:
            logger.warning(f"失败项: {len(stats['errors'])} 个")

    except Exception as e:
        logger.error(f"资金流采集阶段异常(不影响主流程): {e}")
    finally:
        conn.close()

    return stats

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
    import pandas as pd
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

        # === 闭环反馈: 扩展数据接入 ===
        try:
            ff_query = "SELECT code, name, date as trade_date, net_inflow, " \
                "super_large_inflow, large_inflow, medium_inflow, small_inflow " \
                "FROM fund_flows WHERE date >= date('now','-7 days') " \
                "ORDER BY date DESC, code"
            combined_data['fund_flows'] = pd.read_sql(ff_query, conn)
        except Exception as e:
            logger.warning(f'资金流数据获取失败: {e}')
            combined_data['fund_flows'] = pd.DataFrame()

        try:
            ms_query = "SELECT name, value, date as trade_date " \
                "FROM market_sentiment WHERE date >= date('now','-7 days') " \
                "ORDER BY date DESC"
            combined_data['market_sentiment'] = pd.read_sql(ms_query, conn)
        except Exception as e:
            logger.warning(f'市场情绪数据获取失败: {e}')
            combined_data['market_sentiment'] = pd.DataFrame()

        try:
            md_query = "SELECT name, value, 'N/A' as unit, date as trade_date " \
                "FROM macro_daily WHERE date >= date('now','-7 days') " \
                "ORDER BY date DESC"
            combined_data['macro_daily'] = pd.read_sql(md_query, conn)
        except Exception as e:
            logger.warning(f'宏观数据获取失败: {e}')
            combined_data['macro_daily'] = pd.DataFrame()

        try:
            news_query = "SELECT title, sentiment_score, category, date, source " \
                "FROM daily_news WHERE date >= date('now','-3 days') " \
                "ORDER BY date DESC LIMIT 50"
            combined_data['daily_news'] = pd.read_sql(news_query, conn)
        except Exception as e:
            logger.warning(f'新闻数据获取失败: {e}')
            combined_data['daily_news'] = pd.DataFrame()

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

    # 从嵌套结构中正确提取风险指标
    portfolio_metrics = risk.get('portfolio_metrics', {})
    risk_adjusted = portfolio_metrics.get('risk_adjusted_metrics', {})
    drawdown_metrics = portfolio_metrics.get('drawdown_metrics', {})
    volatility_metrics = portfolio_metrics.get('volatility_metrics', {})
    risk_summary = summary.get('risk_summary', {})

    sharpe = risk_summary.get('sharpe_ratio',
                risk_adjusted.get('sharpe_ratio',
                    summary.get('sharpe_ratio', 'N/A')))
    max_dd = risk_summary.get('max_drawdown',
                drawdown_metrics.get('max_drawdown',
                    summary.get('max_drawdown', 'N/A')))
    vol = risk_summary.get('annual_volatility',
                volatility_metrics.get('annual_volatility',
                    summary.get('volatility', 'N/A')))

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


        # === 阶段零: 数据源健康检测 ===
        try:
            import akshare as ak
            import time as _time
            health_results = []
            # 临时移除代理以直连东方财富API
            _saved_proxies = {}
            for _k in ['http_proxy','https_proxy','HTTP_PROXY','HTTPS_PROXY','ALL_PROXY','all_proxy']:
                _saved_proxies[_k] = os.environ.pop(_k, None)
            _api_checks = [
                ('AKShare-行情', lambda: ak.stock_zh_a_spot_em()),
                ('AKShare-板块', lambda: ak.stock_board_industry_name_em()),
                ('AKShare-北向', lambda: ak.stock_hsgt_hist_em(symbol='沪股通')),
            ]
            for _name, _fn in _api_checks:
                try:
                    _t0 = _time.time()
                    _df = _fn()
                    _ms = (_time.time() - _t0) * 1000
                    _status = 'OK' if _df is not None and len(_df) > 0 else 'EMPTY'
                    health_results.append((_name, _status, f'{_ms:.0f}ms'))
                except Exception as _e:
                    _err_str = str(_e)
                    # ProxyError/ConnectionError = 网络不可达，非数据源故障
                    if 'ProxyError' in _err_str or 'ConnectionError' in _err_str or 'Max retries' in _err_str:
                        health_results.append((_name, 'NETWORK', _err_str[:60]))
                    else:
                        health_results.append((_name, 'FAIL', _err_str[:80]))
            # 恢复代理
            for _k, _v in _saved_proxies.items():
                if _v is not None:
                    os.environ[_k] = _v
            _ok_count = sum(1 for _, s, _ in health_results if s == 'OK')
            _fail_count = sum(1 for _, s, _ in health_results if s == 'FAIL')
            _network_count = sum(1 for _, s, _ in health_results if s == 'NETWORK')
            logger.info(f"数据源健康: {_ok_count}/{len(_api_checks)} OK, {_network_count} NETWORK, {_fail_count} FAIL")
            for _n, _s, _d in health_results:
                _icon = 'V' if _s == 'OK' else 'X' if _s == 'FAIL' else '?' if _s == 'NETWORK' else '!'
                logger.info(f"  [{_icon}] {_n}: {_s} ({_d})")
            # 仅当有 FAIL（非 NETWORK）时才告警
            if _fail_count > 0:
                monitor.log_execution('source_health_check', 'warning',
                    f'{_fail_count} source failures + {_network_count} network issues')
            elif _network_count > 0:
                monitor.log_execution('source_health_check', 'success',
                    f'{_ok_count} OK, {_network_count} network issues (bypassed)')
            else:
                monitor.log_execution('source_health_check', 'success',
                    f'{_ok_count}/{len(_api_checks)} sources OK')
        except Exception as e:
            logger.warning(f"数据源健康检测失败(不影响主流程): {e}")


        # === 阶段一: 基础分析 ===
        results = run_stage1_basic(analyzer)

        # === 阶段二: 风险分析 ===
        risk_data = run_stage2_risk(analyzer, results)

        # === 阶段三: 监控告警 ===
        summary = results.get('summary', {})
        alerts = run_stage3_monitor(summary, risk_data)

        # === 阶段3.2: 资金流数据采集 ===
        fund_flow_stats = run_stage_fund_flow()

        # === 阶段三.五: 行业资讯与新闻分析 ===
        positions = results.get('positions', [])
        news_result = run_stage_news(positions, summary, results.get('indices', {}))


        # === 阶段三.六: 宏观数据采集 ===
        try:
            macro_stats = fetch_all_macro_daily()
            macro_count = sum(macro_stats.values())
            logger.info(f"宏观数据采集完成: {macro_stats}")
        except Exception as e:
            logger.warning(f"宏观数据采集失败: {e}")

        # === 阶段三.七: 市场事件数据采集（龙虎榜/融资融券/股东增减持/机构调研/大宗交易）===
        try:
            me_stats = run_market_events_collection()
            me_total = sum(v for k, v in me_stats.items() if k != 'errors')
            logger.info(f"市场事件采集完成: {me_total} 条 ({me_stats})")
        except Exception as e:
            logger.warning(f"市场事件采集失败(不影响主流程): {e}")

        # === 阶段三.八: 市场事件信号分析 + 告警 ===
        try:
            import sqlite3 as _sqlite3
            from src.analysis.market_event_signals import MarketEventSignalEngine
            _me_conn = _sqlite3.connect(str(DATABASE_PATH))
            _me_engine = MarketEventSignalEngine(_me_conn)
            _me_signals = _me_engine.generate_all_signals(lookback_days=3)
            _me_summary = _me_engine.get_signal_summary(_me_signals)
            _me_conn.close()

            # 持仓关联信号告警
            _positions = results.get('positions', [])
            if _positions:
                _held = [str(p.get('code', '')) for p in _positions if isinstance(p, dict)]
                if _held:
                    _me_conn2 = _sqlite3.connect(str(DATABASE_PATH))
                    _me_engine2 = MarketEventSignalEngine(_me_conn2)
                    _rpt = _me_engine2.get_portfolio_signal_report(_me_signals, _held)
                    _me_conn2.close()
                    if _rpt['portfolio_risk_level'] == 'high':
                        _risk_codes = ", ".join(set(s.code for s in _rpt['related_signals']
                                                     if s.signal_type.value == 'risk'))
                        _notifier.send_alert(
                            "市场事件风险预警",
                            f"持仓标的触发高风险信号: {_risk_codes}。请及时关注。",
                            "error"
                        )
                        logger.warning(f"市场事件风险预警: {_rpt['related_count']}条关联信号")
                    elif _rpt['related_count'] > 0:
                        logger.info(f"市场事件信号: {_rpt['related_count']}条关联, "
                                    f"级别={_rpt['portfolio_risk_level']}")
            logger.info(f"市场事件信号: 总计 {_me_summary['total']} 条 "
                        f"(风险 {_me_summary['by_type']['risk']}, "
                        f"机会 {_me_summary['by_type']['opp']})")
        except Exception as e:
            logger.warning(f"市场事件信号分析失败(不影响主流程): {e}")

        # === 阶段四: 智能分析 ===
        advice_summary = run_stage4_smart(results, summary, risk_data)


        # === 阶段六: 数据质量巡检 ===
        try:
            from src.utils.data_quality import DataQualityChecker
            dq = DataQualityChecker(str(DATABASE_PATH))
            score_data = dq.compute_quality_score()
            total_score = score_data['total_score']
            grade = score_data['grade']
            freshness_txt = dq.get_freshness_summary()
            logger.info(f"数据质量评分: {total_score}/100 ({grade})")
            if freshness_txt:
                for fl in freshness_txt.split('\n'):
                    logger.info(fl)
            dq_alerts = dq.generate_alerts()
            if dq_alerts:
                logger.warning(f"数据质量告警: {len(dq_alerts)} 条")
                for a in dq_alerts:
                    logger.warning(f"  [{a['severity']}] {a['message']}")
                # 写入execution_logs供后续监控
                try:
                    monitor.log_execution(
                        'data_quality_check', 'warning',
                        f"score={total_score}, alerts={len(dq_alerts)}, grade={grade}"
                    )
                except Exception:
                    pass
            else:
                logger.info("数据质量检查通过，无告警")
                monitor.log_execution('data_quality_check', 'success',
                                   f"score={total_score}, grade={grade}")
        except Exception as e:
            logger.warning(f"数据质量巡检失败(不影响主流程): {e}")

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