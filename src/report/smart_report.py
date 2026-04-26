
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

        # 2. 获取回测结果（简化版，使用已有数据）
        backtest_summary = self._generate_backtest_summary(portfolio_data)

        # 3. 生成报告
        report = self._build_report(advices, backtest_summary, portfolio_data)

        # 4. 保存报告
        if output_path:
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(report)
            logger.info(f"智能分析报告已保存: {output_path}")

        return report

    def _generate_backtest_summary(self, portfolio_data: dict) -> dict:
        """生成回测摘要"""
        summary = portfolio_data.get('summary', {})
        risk = portfolio_data.get('risk', {})

        return {
            'current_value': summary.get('total_value', 0),
            'total_return': summary.get('total_pnl', 0),
            'sharpe_ratio': risk.get('sharpe_ratio', 0),
            'max_drawdown': risk.get('max_drawdown', 0),
            'volatility': risk.get('volatility', 0),
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
        lines.append("## 风险提示")
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
