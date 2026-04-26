"""
智能建议引擎 - 基于规则和数据驱动的投资建议
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class AdviceType(Enum):
    """建议类型"""
    REBALANCE = "rebalance"           # 再平衡建议
    RISK_MANAGEMENT = "risk_mgmt"     # 风险管理建议
    OPPORTUNITY = "opportunity"       # 机会提示
    CAUTION = "caution"               # 风险提示
    STRATEGY = "strategy"             # 策略建议


class AdvicePriority(Enum):
    """建议优先级"""
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


@dataclass
class InvestmentAdvice:
    """投资建议"""
    type: AdviceType
    priority: AdvicePriority
    title: str
    description: str
    action_items: List[str]
    related_codes: List[str]
    confidence: float  # 置信度 0-1
    created_at: datetime


class SmartAdvisor:
    """智能建议引擎"""

    def __init__(self, db_connection):
        self.db = db_connection
        self.advice_history: List[InvestmentAdvice] = []

    def analyze_portfolio(self, portfolio_data: Dict, risk_data: Dict, 
                         technical_data: Dict) -> List[InvestmentAdvice]:
        """分析投资组合并生成建议"""
        advices = []

        # 1. 再平衡建议
        rebalance_advice = self._check_rebalance_needs(portfolio_data)
        if rebalance_advice:
            advices.extend(rebalance_advice)

        # 2. 风险管理建议
        risk_advice = self._check_risk_indicators(risk_data)
        if risk_advice:
            advices.extend(risk_advice)

        # 3. 技术分析建议
        tech_advice = self._analyze_technical_signals(technical_data)
        if tech_advice:
            advices.extend(tech_advice)

        # 4. 集中度建议
        concentration_advice = self._check_concentration(risk_data)
        if concentration_advice:
            advices.append(concentration_advice)

        # 5. 机会识别
        opportunity_advice = self._identify_opportunities(portfolio_data, technical_data)
        if opportunity_advice:
            advices.extend(opportunity_advice)

        # 按优先级排序
        priority_order = {AdvicePriority.HIGH: 0, AdvicePriority.MEDIUM: 1, AdvicePriority.LOW: 2}
        advices.sort(key=lambda x: priority_order.get(x.priority, 3))

        self.advice_history.extend(advices)
        return advices

    def _check_rebalance_needs(self, portfolio_data: Dict) -> List[InvestmentAdvice]:
        """检查再平衡需求"""
        advices = []
        positions = portfolio_data.get('positions', [])

        if not positions:
            return advices

        # 计算当前权重
        total_value = sum(p.get('market_value', 0) for p in positions)
        if total_value == 0:
            return advices

        current_weights = {}
        target_weights = {}
        deviations = {}

        for pos in positions:
            code = pos.get('code', '')
            market_value = pos.get('market_value', 0)
            current_weight = market_value / total_value
            current_weights[code] = current_weight

            # 假设目标权重为等权（可根据配置调整）
            target_weight = 1.0 / len(positions)
            target_weights[code] = target_weight

            deviation = abs(current_weight - target_weight)
            deviations[code] = deviation

            # 如果偏离超过5%，建议再平衡
            if deviation > 0.05:
                direction = "增持" if current_weight < target_weight else "减持"
                advices.append(InvestmentAdvice(
                    type=AdviceType.REBALANCE,
                    priority=AdvicePriority.MEDIUM if deviation > 0.1 else AdvicePriority.LOW,
                    title=f"{pos.get('name', code)} 仓位偏离",
                    description=f"当前权重 {current_weight*100:.1f}%，目标 {target_weight*100:.1f}%，偏离 {deviation*100:.1f}%",
                    action_items=[
                        f"建议{direction} {abs(current_weight - target_weight)*total_value:,.0f}元",
                        f"将权重调整至 {target_weight*100:.1f}% 附近"
                    ],
                    related_codes=[code],
                    confidence=min(deviation * 2, 0.9),
                    created_at=datetime.now()
                ))

        # 检查整体偏离
        max_deviation = max(deviations.values()) if deviations else 0
        if max_deviation > 0.1:
            advices.insert(0, InvestmentAdvice(
                type=AdviceType.REBALANCE,
                priority=AdvicePriority.HIGH,
                title="组合需要再平衡",
                description=f"最大仓位偏离 {max_deviation*100:.1f}%，建议进行组合再平衡",
                action_items=[
                    "审视当前各资产权重",
                    "根据目标配置进行调整",
                    "考虑交易成本和税费"
                ],
                related_codes=list(current_weights.keys()),
                confidence=0.85,
                created_at=datetime.now()
            ))

        return advices

    def _check_risk_indicators(self, risk_data: Dict) -> List[InvestmentAdvice]:
        """检查风险指标"""
        advices = []
        summary = risk_data.get('summary', {})

        # 检查最大回撤
        max_drawdown = summary.get('max_drawdown', 0)
        if max_drawdown < -15:
            advices.append(InvestmentAdvice(
                type=AdviceType.RISK_MANAGEMENT,
                priority=AdvicePriority.HIGH,
                title="最大回撤过大",
                description=f"当前最大回撤 {max_drawdown:.1f}%，超过15%警戒线",
                action_items=[
                    "审视当前持仓结构",
                    "考虑降低高风险资产比例",
                    "评估止损策略"
                ],
                related_codes=[],
                confidence=0.9,
                created_at=datetime.now()
            ))

        # 检查夏普比率
        sharpe = summary.get('sharpe_ratio', 0)
        if sharpe < 0.5:
            advices.append(InvestmentAdvice(
                type=AdviceType.RISK_MANAGEMENT,
                priority=AdvicePriority.MEDIUM,
                title="风险调整后收益偏低",
                description=f"夏普比率 {sharpe:.2f}，低于0.5的合理水平",
                action_items=[
                    "评估当前资产配置效率",
                    "考虑降低波动性或提高收益",
                    "审视低效持仓"
                ],
                related_codes=[],
                confidence=0.75,
                created_at=datetime.now()
            ))

        # 检查VaR
        var_95 = summary.get('var_95', 0)
        if var_95 < -3:
            advices.append(InvestmentAdvice(
                type=AdviceType.RISK_MANAGEMENT,
                priority=AdvicePriority.MEDIUM,
                title="日度风险价值偏高",
                description=f"VaR(95%)为 {var_95:.2f}%，日度潜在损失较大",
                action_items=[
                    "关注市场波动风险",
                    "考虑对冲或降低仓位",
                    "设置止损线"
                ],
                related_codes=[],
                confidence=0.8,
                created_at=datetime.now()
            ))

        return advices

    def _analyze_technical_signals(self, technical_data: Dict) -> List[InvestmentAdvice]:
        """分析技术信号"""
        advices = []

        for code, data in technical_data.items():
            signals = []

            # MACD信号
            macd_signal = data.get('macd_signal', '')
            if macd_signal == '买入':
                signals.append("MACD金叉")
            elif macd_signal == '卖出':
                signals.append("MACD死叉")

            # RSI信号
            rsi = data.get('rsi_value', 50)
            if rsi > 70:
                signals.append(f"RSI超买({rsi:.1f})")
            elif rsi < 30:
                signals.append(f"RSI超卖({rsi:.1f})")

            # KDJ信号
            kdj_signal = data.get('kdj_signal', '')
            if kdj_signal == '买入':
                signals.append("KDJ金叉")
            elif kdj_signal == '卖出':
                signals.append("KDJ死叉")

            # 布林带位置
            boll_pos = data.get('bollinger_position', '')
            if boll_pos == 'upper':
                signals.append("触及布林上轨")
            elif boll_pos == 'lower':
                signals.append("触及布林下轨")

            if signals:
                has_buy = any('金叉' in s or '超卖' in s or '下轨' in s for s in signals)
                has_sell = any('死叉' in s or '超买' in s or '上轨' in s for s in signals)

                if has_buy and not has_sell:
                    advices.append(InvestmentAdvice(
                        type=AdviceType.OPPORTUNITY,
                        priority=AdvicePriority.MEDIUM,
                        title=f"{code} 技术买入信号",
                        description=f"检测到技术买入信号: {', '.join(signals)}",
                        action_items=[
                            "关注买入机会",
                            "结合基本面确认",
                            "控制仓位分批建仓"
                        ],
                        related_codes=[code],
                        confidence=0.65,
                        created_at=datetime.now()
                    ))
                elif has_sell and not has_buy:
                    advices.append(InvestmentAdvice(
                        type=AdviceType.CAUTION,
                        priority=AdvicePriority.MEDIUM,
                        title=f"{code} 技术卖出信号",
                        description=f"检测到技术卖出信号: {', '.join(signals)}",
                        action_items=[
                            "关注回调风险",
                            "考虑获利了结",
                            "设置止盈止损"
                        ],
                        related_codes=[code],
                        confidence=0.65,
                        created_at=datetime.now()
                    ))

        return advices

    def _check_concentration(self, risk_data: Dict) -> Optional[InvestmentAdvice]:
        """检查持仓集中度"""
        summary = risk_data.get('summary', {})
        hhi = summary.get('concentration_hhi', 0)

        if hhi > 0.5:
            return InvestmentAdvice(
                type=AdviceType.RISK_MANAGEMENT,
                priority=AdvicePriority.HIGH if hhi > 0.6 else AdvicePriority.MEDIUM,
                title="持仓过于集中",
                description=f"赫芬达尔指数(HHI)为 {hhi:.2f}，持仓集中度较高",
                action_items=[
                    "分散投资降低单一资产风险",
                    "增加低相关性资产",
                    "定期监控集中度变化"
                ],
                related_codes=[],
                confidence=0.85,
                created_at=datetime.now()
            )
        return None

    def _identify_opportunities(self, portfolio_data: Dict, 
                               technical_data: Dict) -> List[InvestmentAdvice]:
        """识别投资机会"""
        advices = []
        positions = portfolio_data.get('positions', [])

        # 检查超跌反弹机会
        for pos in positions:
            code = pos.get('code', '')
            pnl_rate = pos.get('pnl_rate', 0)
            ytd_return = pos.get('ytd_return', 0)

            # 超跌资产可能有反弹机会
            if pnl_rate < -10 and ytd_return < -15:
                tech = technical_data.get(code, {})
                rsi = tech.get('rsi_value', 50)

                if rsi < 40:  # 未严重超卖但有反弹潜力
                    advices.append(InvestmentAdvice(
                        type=AdviceType.OPPORTUNITY,
                        priority=AdvicePriority.LOW,
                        title=f"{pos.get('name', code)} 超跌关注",
                        description=f"累计跌幅 {pnl_rate:.1f}%，年初至今 {ytd_return:.1f}%，可能存在反弹机会",
                        action_items=[
                            "关注技术企稳信号",
                            "评估基本面是否恶化",
                            "考虑定投摊低成本"
                        ],
                        related_codes=[code],
                        confidence=0.5,
                        created_at=datetime.now()
                    ))

        return advices

    def generate_strategy_advice(self, backtest_results: pd.DataFrame) -> InvestmentAdvice:
        """基于回测结果生成策略建议"""
        if backtest_results.empty:
            return InvestmentAdvice(
                type=AdviceType.STRATEGY,
                priority=AdvicePriority.LOW,
                title="暂无策略建议",
                description="历史数据不足，无法生成策略建议",
                action_items=["积累更多历史数据后再评估"],
                related_codes=[],
                confidence=0.3,
                created_at=datetime.now()
            )

        # 找出夏普比率最高的策略
        best_sharpe = backtest_results.loc[backtest_results['夏普比率'].idxmax()]

        # 找出收益最高的策略
        best_return = backtest_results.loc[backtest_results['年化收益(%)'].idxmax()]

        # 找出回撤最小的策略
        best_dd = backtest_results.loc[backtest_results['最大回撤(%)'].idxmax()]

        return InvestmentAdvice(
            type=AdviceType.STRATEGY,
            priority=AdvicePriority.MEDIUM,
            title="再平衡策略建议",
            description=f"基于回测分析，{best_sharpe['策略']}策略夏普比率最优({best_sharpe['夏普比率']})",
            action_items=[
                f"推荐策略: {best_sharpe['策略']}",
                f"预期年化收益: {best_sharpe['年化收益(%)']}%",
                f"预期最大回撤: {best_sharpe['最大回撤(%)']}%",
                "建议定期评估策略效果"
            ],
            related_codes=[],
            confidence=0.7,
            created_at=datetime.now()
        )

    def format_advice_report(self, advices: List[InvestmentAdvice]) -> str:
        """格式化建议报告"""
        if not advices:
            return "暂无投资建议"

        lines = ["# 智能投资建议\n"]

        # 按类型分组
        by_type = {}
        for advice in advices:
            type_name = advice.type.value
            if type_name not in by_type:
                by_type[type_name] = []
            by_type[type_name].append(advice)

        type_names = {
            'rebalance': '再平衡建议',
            'risk_mgmt': '风险管理',
            'opportunity': '机会提示',
            'caution': '风险提示',
            'strategy': '策略建议'
        }

        for type_key, type_advices in by_type.items():
            lines.append(f"## {type_names.get(type_key, type_key)}\n")

            for advice in type_advices:
                priority_icon = "🔴" if advice.priority == AdvicePriority.HIGH else "🟡" if advice.priority == AdvicePriority.MEDIUM else "🟢"
                lines.append(f"### {priority_icon} {advice.title}")
                lines.append(f"{advice.description}\n")

                if advice.action_items:
                    lines.append("**建议操作:**")
                    for item in advice.action_items:
                        lines.append(f"- {item}")
                    lines.append("")

                if advice.related_codes:
                    lines.append(f"**相关标的:** {', '.join(advice.related_codes)}\n")

                lines.append(f"*置信度: {advice.confidence*100:.0f}%*\n")

        return "\n".join(lines)
