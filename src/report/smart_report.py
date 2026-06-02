
"""
智能分析报告生成器 - 整合回测和建议生成报告
"""
import os
import sys
from datetime import datetime, timedelta
from typing import List, Dict
import logging

from src.analysis.backtest import StrategyBacktester, RebalanceStrategy
from src.analysis.advisor import SmartAdvisor, AdviceType, AdvicePriority

logger = logging.getLogger(__name__)


class SmartReportGenerator:
    """智能分析报告生成器"""

    def __init__(self, db_connection):
        self.db = db_connection
        self.backtester = StrategyBacktester(db_connection)
        self.advisor = SmartAdvisor(db_connection)

    def generate_full_report(self, portfolio_data: dict, output_path: str = None):
        """生成完整智能分析报告"""

        # 1. 获取建议
        risk_data = portfolio_data.get('risk', {})
        technical_data = portfolio_data.get('technical', {})

        advices = self.advisor.analyze_portfolio(portfolio_data, risk_data, technical_data)

        # 获取回测策略建议（如果数据可用）
        try:
            from src.analysis.backtest import StrategyBacktester
            backtester = StrategyBacktester(self.db)
            backtest_results = backtester.run_all_strategies()
            if backtest_results and not backtest_results.empty:
                strategy_advice = self.advisor.generate_strategy_advice(backtest_results)
                if strategy_advice:
                    advices.append(strategy_advice)
        except Exception as e:
            logger.debug(f'策略建议生成跳过: {e}')

        # 2. 获取回测结果（简化版，使用已有数据）
        backtest_summary = self._generate_backtest_summary(portfolio_data)

        # 3. 生成报告
        report = self._build_report(advices, backtest_summary, portfolio_data)

        # 4. 保存报告
        if output_path:
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(report)
            logger.info(f"智能分析报告已保存: {output_path}")

        # 闭环反馈: 记录建议到数据库
        try:
            import sqlite3 as _sqlite3
            _db_path = 'data/database/portfolio.db'
            _conn_fb = _sqlite3.connect(_db_path)
            _conn_fb.execute("""
                CREATE TABLE IF NOT EXISTS advice_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    advice_type TEXT,
                    priority TEXT,
                    title TEXT,
                    description TEXT,
                    confidence REAL,
                    related_codes TEXT,
                    source TEXT DEFAULT 'auto'
                )
            """)
            _conn_fb.commit()
            for advice in advices:
                _conn_fb.execute(
                    "INSERT INTO advice_history (created_at, advice_type, priority, title, description, confidence, related_codes, source) VALUES (?,?,?,?,?,?,?,?)",
                    (advice.created_at.strftime('%Y-%m-%d %H:%M:%S'),
                     advice.type.value, advice.priority.value,
                     advice.title, advice.description,
                     advice.confidence,
                     ','.join(advice.related_codes),
                     'smart_report')
                )
            _conn_fb.commit()
            logger.info(f'建议历史已记录: {len(advices)}条')
            _conn_fb.close()
        except Exception as e:
            logger.debug(f'建议历史记录跳过: {e}')

        return report

    def _generate_backtest_summary(self, portfolio_data: dict) -> dict:
        """生成回测摘要"""
        summary = portfolio_data.get('summary', {})
        risk = portfolio_data.get('risk', {})

        # risk 数据有两种来源格式：
        # 1. run_analysis.py 传入的是 analyze_portfolio_risk 的原始返回值（嵌套结构）
        # 2. run_smart.py 传入的是扁平化的 dict（含 sharpe_ratio/max_drawdown/volatility 键）
        # 统一从两种格式中提取
        portfolio_metrics = risk.get('portfolio_metrics', {})
        ram = portfolio_metrics.get('risk_adjusted_metrics', {})
        dm = portfolio_metrics.get('drawdown_metrics', {})
        vm = portfolio_metrics.get('volatility_metrics', {})

        return {
            'current_value': summary.get('total_value', 0),
            'total_return': summary.get('total_pnl', 0),
            'sharpe_ratio': ram.get('sharpe_ratio', risk.get('sharpe_ratio', 0)),
            'max_drawdown': dm.get('max_drawdown', risk.get('max_drawdown', 0)),
            'volatility': vm.get('annual_volatility', risk.get('volatility', 0)),
        }

    def _build_report(self, advices: list, backtest: dict, portfolio: dict) -> str:
        """构建报告内容"""

        lines = []
        lines.append("# 投资组合智能分析报告")

        now_str = datetime.now().strftime('%Y-%m-%d %H:%M')
        today_str = datetime.now().strftime('%Y-%m-%d')

        lines.append("**生成时间**: " + now_str)
        lines.append("**报告周期**: " + today_str)
        lines.append("")
        lines.append("---")
        lines.append("")

        # 执行摘要
        lines.append("## 执行摘要")
        lines.append("基于当前持仓和市场数据，系统生成以下关键洞察：")
        lines.append("")

        high_priority = [a for a in advices if a.priority.value == 'high']
        if high_priority:
            lines.append("**高优先级建议**: " + str(len(high_priority)) + " 条")
            for advice in high_priority[:3]:
                lines.append("  - " + advice.title)
        else:
            lines.append("**当前状态**: 投资组合运行正常，无高优先级建议")

        lines.append("")
        lines.append("---")
        lines.append("")

        # 详细建议
        lines.append("## 智能建议")
        lines.append("基于多维度分析，系统提供以下建议：")
        lines.append("")

        for i, advice in enumerate(advices, 1):
            emoji_map = {"high": "[高]", "medium": "[中]", "low": "[低]"}
            emoji = emoji_map.get(advice.priority.value, "[普]")

            lines.append("### " + str(i) + ". " + emoji + " " + advice.title)
            conf_str = "{:.0%}".format(advice.confidence)
            lines.append("**类型**: " + advice.type.value + " | **优先级**: " + advice.priority.value + " | **置信度**: " + conf_str)
            lines.append("")
            lines.append(advice.description)
            lines.append("")

            if advice.action_items:
                lines.append("**建议操作**:")
                for item in advice.action_items:
                    lines.append("- " + item)
                lines.append("")

            if advice.related_codes:
                codes_str = ', '.join(advice.related_codes)
                lines.append("**相关标的**: " + codes_str)
                lines.append("")

        lines.append("---")
        lines.append("")

        # 策略回测
        lines.append("## 策略表现")
        lines.append("当前投资组合关键指标：")
        lines.append("")
        lines.append("| 指标 | 数值 |")
        lines.append("|------|------|")

        cv = backtest['current_value']
        tr = backtest['total_return']
        sr = backtest['sharpe_ratio']
        md = backtest['max_drawdown']
        vol = backtest['volatility']

        lines.append("| 当前市值 | ¥" + "{:,.2f}".format(cv) + " |")
        lines.append("| 累计收益 | ¥" + "{:,.2f}".format(tr) + " |")
        lines.append("| 夏普比率 | " + "{:.2f}".format(sr) + " |")
        lines.append("| 最大回撤 | " + "{:.2f}".format(md) + "% |")
        lines.append("| 波动率 | " + "{:.2f}".format(vol) + "% |")

        lines.append("")
        lines.append("---")
        lines.append("")

        # 风险提示
        # 多维市场环境分析
        lines.append("## 市场环境分析")
        lines.append("")

        fund_flows = portfolio.get('fund_flows', None)
        sentiment = portfolio.get('market_sentiment', None)
        macro = portfolio.get('macro_daily', None)
        news = portfolio.get('daily_news', None)

        sections = []

        if fund_flows is not None and hasattr(fund_flows, 'empty') and not fund_flows.empty:
            if 'code' in fund_flows.columns:
                agg = fund_flows.groupby('code').agg(
                    total_net=('net_inflow', 'sum'),
                    days=('trade_date', 'count')
                ).reset_index()
                top_inflow = agg.nlargest(3, 'total_net')
                top_outflow = agg.nsmallest(3, 'total_net')
                lines.append("### 资金流向")
                lines.append("")
                lines.append("**净流入TOP3**:")
                for _, r in top_inflow.iterrows():
                    lines.append(f"- {r['code']}: {r['total_net']:.2f}亿元 ({int(r['days'])}日)")
                lines.append("")
                lines.append("**净流出TOP3**:")
                for _, r in top_outflow.iterrows():
                    lines.append(f"- {r['code']}: {r['total_net']:.2f}亿元 ({int(r['days'])}日)")
                lines.append("")
                sections.append(True)

        if sentiment is not None and hasattr(sentiment, 'empty') and not sentiment.empty:
            lines.append("### 市场情绪")
            lines.append("")
            latest_s = sentiment.drop_duplicates('indicator_name', keep='first')
            for _, r in latest_s.iterrows():
                lines.append(f"- {r['indicator_name']}: {r['indicator_value']}")
            lines.append("")
            sections.append(True)

        if macro is not None and hasattr(macro, 'empty') and not macro.empty:
            lines.append("### 宏观指标")
            lines.append("")
            latest_m = macro.drop_duplicates('indicator_name', keep='first')
            for _, r in latest_m.iterrows():
                unit = r.get('unit', '')
                val = r['indicator_value']
                lines.append(f"- {r['indicator_name']}: {val} {unit}")
            lines.append("")
            sections.append(True)

        if news is not None and hasattr(news, 'empty') and not news.empty:
            lines.append("### 近期新闻摘要")
            lines.append("")
            sentiment_counts = news['sentiment'].value_counts() if 'sentiment' in news.columns else {}
            total_news = len(news)
            lines.append(f"共{total_news}条新闻: ", )
            for s, c in sentiment_counts.items():
                lines.append(f"{s}({c}条) ", )
            lines.append("")
            if 'category' in news.columns:
                cat_counts = news['category'].value_counts().head(5)
                lines.append("**热点板块**: " + ", ".join(f"{k}({v})" for k, v in cat_counts.items()))
            lines.append("")
            sections.append(True)

        if not sections:
            lines.append("暂无市场环境数据")
            lines.append("")

        lines.append("---")
        lines.append("")

        lines.append("1. 以上建议基于历史数据和技术指标生成，不构成投资建议")
        lines.append("2. 市场有风险，投资需谨慎")
        lines.append("3. 建议定期审查投资组合，根据个人风险承受能力调整")

        lines.append("")
        lines.append("---")
        lines.append("")
        lines.append("*报告由投资组合智能分析系统自动生成*")

        return "\n".join(lines)

    def get_advice_summary(self, portfolio_data: dict) -> dict:
        """获取建议摘要"""
        risk_data = portfolio_data.get('risk', {})
        technical_data = portfolio_data.get('technical', {})

        advices = self.advisor.analyze_portfolio(portfolio_data, risk_data, technical_data)

        return {
            'total': len(advices),
            'high': len([a for a in advices if a.priority.value == 'high']),
            'medium': len([a for a in advices if a.priority.value == 'medium']),
            'low': len([a for a in advices if a.priority.value == 'low']),
            'advices': advices
        }
