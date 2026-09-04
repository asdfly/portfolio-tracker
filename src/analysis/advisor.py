"""
智能建议引擎 - 基于规则和数据驱动的投资建议
"""
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional
from dataclasses import dataclass
from enum import Enum
import logging
import sqlite3

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

        # 1. 再平衡建议（分层引擎为唯一权威，避免与战略/战术目标自相矛盾）
        engine_plan = self.generate_rebalance_plan()
        if engine_plan is not None:
            advices.insert(0, self._plan_to_advice(engine_plan))

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

        # 4b. 重复/同质敞口建议（P1 冗余 + P2 相关性）
        overlap_advice = self._check_overlapping_exposure()
        if overlap_advice:
            advices.append(overlap_advice)

        # 4c. 组合风险预算建议（P2 风险预算 + 集中度）
        risk_budget_advice = self._check_risk_budget()
        if risk_budget_advice:
            advices.append(risk_budget_advice)

        # 4d. 持仓盈亏分层建议（浮盈减仓 / 亏损风控 + 回本测算）
        level_advice = self._analyze_position_levels(portfolio_data)
        if level_advice:
            advices.extend(level_advice)

        # 4e. 估值分位分析（跟踪指数 PE 历史分位 + 绝对 PB 兜底）
        valuation_advice = self._analyze_valuation(portfolio_data)
        if valuation_advice:
            advices.extend(valuation_advice)

        # 4e-2. 高低位定位（三因子集成引擎：价格分布 + 资金流 +（就绪后）估值分位）
        position_advice = self._analyze_position_score(portfolio_data)
        if position_advice:
            advices.extend(position_advice)

        # 4f. 低配加仓机会测算（分层战略基准 + 战术超配，仅机会测算不自动调仓）
        add_advice = self._analyze_add_opportunity(portfolio_data)
        if add_advice:
            advices.extend(add_advice)

        # 4g. 市场阶段定位（周线七阶段，情景取向）
        stage_advice = self._analyze_market_stage()
        if stage_advice:
            advices.extend(stage_advice)

        # 4h. 市场情绪温度计 + 极端情绪预警（涨停/跌停 + 连板 + 融资）
        senti_advice = self._analyze_market_sentiment_extremes()
        if senti_advice:
            advices.extend(senti_advice)

        # 5. 机会识别
        opportunity_advice = self._identify_opportunities(portfolio_data, technical_data)
        if opportunity_advice:
            advices.extend(opportunity_advice)

        # 按优先级排序

        # 6. 市场事件信号建议（如果传入）
        event_signals = portfolio_data.get('market_event_signals', [])
        if event_signals:
            event_advice = self.analyze_market_event_signals(event_signals)
            if event_advice:
                advices.extend(event_advice)

        # 7. 资金流建议
        fund_flow_advice = self._analyze_fund_flows(portfolio_data)
        if fund_flow_advice:
            advices.extend(fund_flow_advice)

        # 8. 市场情绪建议
        sentiment_advice = self._analyze_market_sentiment(portfolio_data)
        if sentiment_advice:
            advices.extend(sentiment_advice)

        # 9. 宏观环境建议
        macro_advice = self._analyze_macro_environment(portfolio_data)
        if macro_advice:
            advices.extend(macro_advice)

        # 10. 新闻事件建议
        news_advice = self._analyze_news_sentiment(portfolio_data)
        if news_advice:
            advices.extend(news_advice)


        # 11. 融资融券数据分析
        margin_advice = self._analyze_margin_data(portfolio_data)
        if margin_advice:
            advices.extend(margin_advice)

        # 12. 机构调研热点分析
        research_advice = self._analyze_institution_research(portfolio_data)
        if research_advice:
            advices.extend(research_advice)

        # 13. 大宗交易异常分析
        block_advice = self._analyze_block_trade(portfolio_data)
        if block_advice:
            advices.extend(block_advice)

        priority_order = {AdvicePriority.HIGH: 0, AdvicePriority.MEDIUM: 1, AdvicePriority.LOW: 2}
        advices.sort(key=lambda x: priority_order.get(x.priority, 3))

        self.advice_history.extend(advices)
        return advices

    # ------------------------------------------------------------------
    #  再平衡引擎接入（P2-B 产品化）
    # ------------------------------------------------------------------
    def generate_rebalance_plan(self, as_of: Optional[str] = None,
                                 strategy: str = "layered") -> "Optional[RebalancePlan]":
        """调用再平衡引擎，对真实持仓生成可执行调仓方案。

        返回 RebalancePlan（仅在需要调仓时非空），供 UI/报告渲染完整方案。
        只读，不写库；缺表或空库等异常时安全返回 None（静默降级）。
        """
        try:
            from src.analysis.rebalance_engine import compute_rebalance_suggestion
            plan = compute_rebalance_suggestion(self.db, as_of_date=as_of, strategy=strategy)
        except Exception as e:  # 引擎依赖持仓快照/日历，缺表或空库时静默降级
            logger.warning(f"再平衡引擎调用失败，跳过: {e}")
            return None
        return plan if getattr(plan, "action_needed", False) else None

    def _plan_to_advice(self, plan: "RebalancePlan") -> "InvestmentAdvice":
        """将 RebalancePlan 转为 InvestmentAdvice（兼容现有建议渲染/序列化）。"""
        items = []
        for t in plan.trades[:12]:
            items.append(
                f"{t.direction} {t.name}({t.code}) {t.trade_value:,.0f}元 / {t.shares}手 "
                f"（{t.current_weight*100:.1f}%→{t.target_weight*100:.1f}%）"
            )
        if len(plan.trades) > 12:
            items.append(f"…其余 {len(plan.trades) - 12} 笔")
        items.append(
            f"预估交易成本 ≈ {plan.estimated_cost:,.0f}元，T+1 执行日 {plan.execution_date}"
        )
        advice = InvestmentAdvice(
            type=AdviceType.REBALANCE,
            priority=AdvicePriority.HIGH if plan.turnover > 0.15 else AdvicePriority.MEDIUM,
            title="组合再平衡方案（可执行）",
            description=(
                f"基于真实持仓快照，最大权重偏离触发再平衡。总市值 {plan.total_value:,.0f}元，"
                f"换手率 {plan.turnover*100:.1f}%，预估成本 {plan.estimated_cost:,.0f}元，"
                f"T+1 执行日 {plan.execution_date}。"
            ),
            action_items=items,
            related_codes=[t.code for t in plan.trades[:10]],
            confidence=min(0.6 + plan.turnover, 0.95),
            created_at=datetime.now(),
        )
        # 挂上完整方案，供 UI 直接渲染（序列化到建议历史时忽略此属性）
        advice.rebalance_plan = plan
        return advice

    def _check_risk_indicators(self, risk_data: Dict) -> List[InvestmentAdvice]:
        """检查风险指标"""
        advices = []
        # risk_data 有两种来源格式，统一提取
        summary = risk_data.get('summary', {})
        portfolio_metrics = risk_data.get('portfolio_metrics', {})
        ram = portfolio_metrics.get('risk_adjusted_metrics', {})
        dm = portfolio_metrics.get('drawdown_metrics', {})
        vm = portfolio_metrics.get('volatility_metrics', {})

        # 检查最大回撤
        max_drawdown = dm.get('max_drawdown', summary.get('max_drawdown', 0))
        if max_drawdown is not None and max_drawdown < -15:
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
        sharpe = ram.get('sharpe_ratio', summary.get('sharpe_ratio', 0))
        if sharpe is not None and sharpe < 0.5:
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
        var_95 = vm.get('var_95', summary.get('var_95', 0))
        if var_95 is not None and var_95 < -3:
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
            if rsi is not None and rsi > 70:
                signals.append(f"RSI超买({rsi:.1f})")
            elif rsi is not None and rsi < 30:
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
        concentration = risk_data.get('concentration_risk', {})
        hhi = concentration.get('hhi', summary.get('concentration_hhi', 0))

        if hhi is not None and hhi > 0.5:
            return InvestmentAdvice(
                type=AdviceType.RISK_MANAGEMENT,
                priority=AdvicePriority.HIGH if (hhi or 0) > 0.6 else AdvicePriority.MEDIUM,
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

    def _check_overlapping_exposure(self) -> Optional[InvestmentAdvice]:
        """检测重复/同质敞口（P1 冗余标的 + P2 相关性管理）。

        同一底层指数/主题被多只 ETF 覆盖时相关度≈1，纯属冗余，浪费权重并推高集中度。
        """
        try:
            from datetime import date
            from src.analysis.rebalance_engine import RebalanceEngine
            from src.utils.trading_calendar import last_trading_day_on_or_before
            eng = RebalanceEngine(self.db)
            as_of = str(last_trading_day_on_or_before(date.today()))
            groups = eng.detect_overlapping_exposure(as_of)
        except Exception as e:
            logger.warning(f"重叠敞口检测失败，跳过: {e}")
            return None
        if not groups:
            return None
        lines = []
        for g in groups:
            codes = "、".join(f"{m['name']}({m['code']})" for m in g["members"])
            lines.append(f"· {g['theme']}：{codes}（合计 {g['total_weight']*100:.1f}%）—— {g['note']}")
        return InvestmentAdvice(
            type=AdviceType.CAUTION,
            priority=AdvicePriority.MEDIUM,
            title="存在重复/同质敞口",
            description="以下主题被多只 ETF 重复覆盖，相关度高、浪费权重：\n" + "\n".join(lines),
            action_items=[
                "合并同一底层指数的 ETF，保留流动性最佳的一只",
                "释放冗余权重至宽基/债券，降低集中度",
            ],
            related_codes=[m["code"] for g in groups for m in g["members"]],
            confidence=0.8,
            created_at=datetime.now(),
        )

    def _check_risk_budget(self) -> Optional[InvestmentAdvice]:
        """组合风险预算与集中度（P2 风险预算 + 集中度管理）。"""
        try:
            from datetime import date
            from src.analysis.rebalance_engine import RebalanceEngine
            from src.utils.trading_calendar import last_trading_day_on_or_before
            eng = RebalanceEngine(self.db)
            as_of = str(last_trading_day_on_or_before(date.today()))
            m = eng.compute_risk_metrics(as_of)
        except Exception as e:
            logger.warning(f"风险指标计算失败，跳过: {e}")
            return None
        if not m:
            return None
        warnings = m.get("warnings", [])
        if not warnings:
            return None
        summary = (f"HHI={m['hhi']:.3f}，前3集中度 {m['top3_concentration']*100:.1f}%，"
                   f"有效持仓数 {m['n_effective']:.1f}，组合加权 Beta={m['portfolio_beta']:.2f}，"
                   f"债券实际 {m['bond_actual']*100:.1f}% / 目标 {m['bond_target']*100:.1f}%")
        return InvestmentAdvice(
            type=AdviceType.RISK_MANAGEMENT,
            priority=AdvicePriority.MEDIUM,
            title="组合风险预算预警",
            description=summary + "\n" + "；".join(warnings),
            action_items=[
                "提升债券至目标权重以压低波动率",
                "降低高 Beta / 超配行业敞口",
                "分散高相关同质标的",
            ],
            related_codes=[],
            confidence=0.85,
            created_at=datetime.now(),
        )

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
            if (pnl_rate or 0) < -10 and (ytd_return or 0) < -15:
                tech = technical_data.get(code, {})
                rsi = tech.get('rsi_value', 50)

                if (rsi or 50) < 40:  # 未严重超卖但有反弹潜力
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

    def _analyze_position_levels(self, portfolio_data):
        """持仓盈亏分层建议：浮盈>15% 获利减仓、亏损>5% 关注风控（附回本测算）。

        借鉴 fund-analysis 的操作建议分类（获利减仓/持仓观望/关注风控）与
        回本测算公式 required_gain = |r| / (1 + r)。获利减仓补充持仓市值与权重维度，
        区分高权重标的（降集中度）与小仓位标的（灵活处理）。
        """
        advices = []
        positions = portfolio_data.get('positions', [])
        if not positions:
            return advices

        total_mv = sum((p.get('market_value', 0) or 0)
                       for p in positions if isinstance(p, dict))
        if total_mv <= 0:
            total_mv = 0.0

        gainers = []  # (name, code, pnl_rate, market_value, weight_pct)
        losers = []   # (name, code, pnl_rate, required, market_value, weight_pct)
        for pos in positions:
            if not isinstance(pos, dict):
                continue
            pnl_rate = pos.get('pnl_rate', 0) or 0
            code = str(pos.get('code', ''))
            name = pos.get('name', code)
            mv = pos.get('market_value', 0) or 0
            weight = (mv / total_mv * 100) if total_mv else 0.0
            if pnl_rate > 15:
                gainers.append((name, code, pnl_rate, mv, weight))
            elif pnl_rate < -5:
                r = pnl_rate / 100.0
                # 回本所需涨幅 = -r / (1 + r)，如 -10% → 需涨 11.1%
                required = -r / (1 + r) * 100
                losers.append((name, code, pnl_rate, required, mv, weight))

        if gainers:
            gainers.sort(key=lambda x: -x[4])  # 权重高者优先提示
            lines = []
            for n, c, p, mv, w in gainers:
                mv_str = f"{mv/10000:.1f}万" if mv >= 10000 else f"{mv:,.0f}元"
                note = ""
                if w >= 10:
                    note = f" · 权重 {w:.1f}% 偏高，建议减至≤10%"
                elif p >= 50:
                    note = f" · 收益丰厚，建议兑现 1/3 约 {mv/3/10000:.1f}万"
                lines.append(f"- {n}({c}): 浮盈 {p:+.1f}%，市值 {mv_str}，权重 {w:.1f}%{note}")
            advices.append(InvestmentAdvice(
                type=AdviceType.CAUTION, priority=AdvicePriority.MEDIUM,
                title=f"获利减仓建议（浮盈超15%，{len(gainers)}只）",
                description="以下持仓浮盈已超15%，结合持仓市值与组合权重考虑兑现部分收益：\n" + "\n".join(lines),
                action_items=["高权重标的优先降集中度", "分批兑现部分收益", "保留底仓跟踪趋势", "设回撤止盈纪律"],
                related_codes=[c for _, c, _, _, _ in gainers], confidence=0.7,
                created_at=datetime.now()
            ))

        if losers:
            losers.sort(key=lambda x: -x[5])
            lines = [f"- {n}({c}): 亏损 {p:.1f}%，回本需涨 {req:.1f}%，市值 {mv/10000:.1f}万"
                     for n, c, p, req, mv, _ in losers]
            advices.append(InvestmentAdvice(
                type=AdviceType.RISK_MANAGEMENT, priority=AdvicePriority.MEDIUM,
                title=f"关注风控（亏损超5%，{len(losers)}只）",
                description="以下持仓亏损超5%，附回本所需涨幅与当前市值：\n" + "\n".join(lines),
                action_items=["评估持仓逻辑是否破坏", "逻辑未破坏可考虑定投摊成本", "逻辑破坏则评估止损切换"],
                related_codes=[c for _, c, _, _, _, _ in losers], confidence=0.65,
                created_at=datetime.now()
            ))

        return advices

    def _analyze_valuation(self, portfolio_data):
        """估值分析：PE 历史分位（history≥100 天）+ 绝对 PB 水平双轨。

        借鉴 fund-analysis 层级三（估值水平 PE/PB 分位）。数据来自 index_pe_history
        （akshare 中证指数 + neodata 指数估值落库）。历史不足时用绝对 PB 兜底：
        PB<1 破净低估、PB>8 估值偏高（宽基/红利/金融常态 PB 1-2，科创/科技 3-8）。
        """
        from src.data_sources.valuation_percentile import load_pe_percentile
        from src.data_sources.neodata_valuation import load_latest_valuation
        advices = []
        positions = portfolio_data.get('positions', [])
        if not positions:
            return advices

        overvalued = []   # (name, code, pct)  PE 分位偏高
        undervalued = []  # (name, code, pct)  PE 分位偏低
        pb_rich = []      # (name, code, pb)   绝对 PB 偏高
        pb_cheap = []     # (name, code, pb)   绝对 PB 破净
        for pos in positions:
            if not isinstance(pos, dict):
                continue
            code = str(pos.get('code', ''))
            name = pos.get('name', code)
            try:
                row = self.db.execute(
                    "SELECT index_code FROM etf_fundamental WHERE code=? ORDER BY rowid DESC LIMIT 1",
                    (code,)).fetchone()
            except Exception:
                continue
            if not row or not row[0]:
                continue
            idx = str(row[0])
            try:
                pct = load_pe_percentile(self.db, idx)
            except Exception:
                pct = {}
            try:
                latest = load_latest_valuation(self.db, idx)
            except Exception:
                latest = {}
            # 1) PE 历史分位（需足够历史）
            p5 = pct.get('percentile_5y')
            if p5 is not None and pct.get('history_count', 0) >= 100:
                if p5 >= 70:
                    overvalued.append((name, code, p5))
                elif p5 <= 30:
                    undervalued.append((name, code, p5))
            # 2) 绝对 PB 兜底（无需长历史）
            pb = latest.get('pb')
            if pb is not None:
                if pb < 1.0:
                    pb_cheap.append((name, code, pb))
                elif pb > 8.0:
                    pb_rich.append((name, code, pb))

        if overvalued:
            lines = [f"- {n}({c}): PE 近5年分位 {p:.0f}%（偏高）" for n, c, p in overvalued]
            advices.append(InvestmentAdvice(
                type=AdviceType.CAUTION, priority=AdvicePriority.MEDIUM,
                title=f"估值偏高提示（PE 分位>70%，{len(overvalued)}只）",
                description="以下持仓跟踪指数 PE 处于近5年历史高位：\n" + "\n".join(lines),
                action_items=["警惕估值回归风险", "考虑分批止盈或降低相关仓位"],
                related_codes=[c for _, c, _ in overvalued], confidence=0.6,
                created_at=datetime.now()
            ))

        if undervalued:
            lines = [f"- {n}({c}): PE 近5年分位 {p:.0f}%（偏低）" for n, c, p in undervalued]
            advices.append(InvestmentAdvice(
                type=AdviceType.OPPORTUNITY, priority=AdvicePriority.MEDIUM,
                title=f"估值偏低机会（PE 分位<30%，{len(undervalued)}只）",
                description="以下持仓跟踪指数 PE 处于近5年历史低位：\n" + "\n".join(lines),
                action_items=["关注估值修复机会", "评估是否逢低布局"],
                related_codes=[c for _, c, _ in undervalued], confidence=0.55,
                created_at=datetime.now()
            ))

        if pb_rich:
            lines = [f"- {n}({c}): PB {pb:.2f}（偏高）" for n, c, pb in pb_rich]
            advices.append(InvestmentAdvice(
                type=AdviceType.CAUTION, priority=AdvicePriority.LOW,
                title=f"绝对估值偏高（PB>8，{len(pb_rich)}只）",
                description="以下持仓跟踪指数市净率处于绝对高位：\n" + "\n".join(lines),
                action_items=["警惕高估值回撤", "评估是否兑现部分收益"],
                related_codes=[c for _, c, _ in pb_rich], confidence=0.5,
                created_at=datetime.now()
            ))

        if pb_cheap:
            lines = [f"- {n}({c}): PB {pb:.2f}（破净）" for n, c, pb in pb_cheap]
            advices.append(InvestmentAdvice(
                type=AdviceType.OPPORTUNITY, priority=AdvicePriority.LOW,
                title=f"绝对估值偏低（PB<1 破净，{len(pb_cheap)}只）",
                description="以下持仓跟踪指数市净率低于 1 倍（破净）：\n" + "\n".join(lines),
                action_items=["关注估值修复机会", "评估是否逢低布局"],
                related_codes=[c for _, c, _ in pb_cheap], confidence=0.5,
                created_at=datetime.now()
            ))

        return advices

    def _analyze_position_score(self, portfolio_data):
        """ETF 高低位定位（三因子集成引擎, Phase A）。

        引擎见 src/analysis/etf_position.py：输出位置分数 P∈[-100,+100]
        （-100 极低/便宜，+100 极高/昂贵）+ 置信度 C = 数据充分度 × 因子一致性。
        与 _analyze_valuation 的分工：后者依赖跟踪指数 PE/PB（自 2026-09-04 csindex 长历史
        回填后，19/20 权益 ETF 的 5 年 PE 分位已可用）；本方法以「价格分布
        （多周期百分位/稳健 z/52周高低距）+ 资金流（反向）」为主力因子，数据现成、
        置信可量化，与估值分位互证。159949/399673 创业板50 因国证无免费单指数 PE 接口仍无估值分位。

        定位 ≠ 预测：项目已用 walk-forward 证伪 ETF 短期方向可预测性（Tier1 IC<0.02
        全线 VETO），故此处只给「现在处在什么位置」，不给涨跌判断，且不自动调仓。
        """
        advices = []
        positions = portfolio_data.get('positions', [])
        if not positions:
            return advices
        try:
            from src.analysis.etf_position import evaluate_all, portfolio_position
        except Exception:
            return advices

        codes, names, mv_map = [], {}, {}
        total_mv = 0.0
        for pos in positions:
            if not isinstance(pos, dict):
                continue
            code = str(pos.get('code', '')).strip()
            if not code:
                continue
            try:
                mv = float(pos.get('market_value') or 0)
            except (TypeError, ValueError):
                mv = 0.0
            codes.append(code)
            names[code] = pos.get('name', code)
            mv_map[code] = mv
            total_mv += mv
        if not codes:
            return advices
        weights = {k: (v / total_mv if total_mv > 0 else 0.0) for k, v in mv_map.items()}

        try:
            results = evaluate_all(conn=self.db, codes=codes)
        except Exception as e:
            logger.warning("高低位定位评估失败: %s", e)
            return advices
        results = [r for r in results if r.get('P') is not None]
        if not results:
            return advices

        # 极端位置阈值：|P|>=60 且 C>=0.5 才提示（避免低置信噪声骚扰）
        P_TH, C_TH = 60.0, 0.50
        hot = [r for r in results if r['P'] >= P_TH and r['C'] >= C_TH]
        cold = [r for r in results if r['P'] <= -P_TH and r['C'] >= C_TH]

        def _fmt(r):
            f = r.get('factors', {})
            pct = f.get('price', {}).get('pct_score')
            zf = f.get('flow', {}).get('z_inflow')
            bits = [f"P={r['P']:+.0f}", f"C={r['C']:.2f}"]
            if pct is not None:
                bits.append(f"价格分位 {pct:+.0f}")
            if zf is not None:
                bits.append(f"资金流z {zf:+.1f}")
            return f"- {names.get(r['code'], r['code'])}({r['code']}): " + "，".join(bits)

        def _split(rs):
            """债券 ETF 与权益 ETF 分流：债券高低位由利率/久期驱动，
            「基本面业绩支撑」类行动项对其无意义，必须分开给结论。"""
            return ([r for r in rs if r.get('type') != 'bond'],
                    [r for r in rs if r.get('type') == 'bond'])

        def _emit(rs, *, high, bond):
            if not rs:
                return
            side = "高位" if high else "低位"
            if bond:
                note = ("\n注：债券 ETF 价格高低位由利率/久期驱动"
                        "（价格新高≈收益率新低），权益估值口径不适用。")
                items = (["利率处于低位时债券价格偏贵，票息保护变薄",
                          "可考虑缩短久期或降低利率敏感度",
                          "不建议在此位置继续追加长久期品种"] if high else
                         ["利率处于高位时债券价格偏便宜，票息保护较厚",
                          "可评估拉长久期以锁定较高票息"])
            else:
                note = ("\n注：定位为描述性结论，不含涨跌预测。" if high else
                        "\n注：低位不等于必然反弹，需先确认持仓逻辑未破坏。")
                items = (["放缓该标的加仓节奏", "可考虑分批止盈锁定部分收益",
                          "结合基本面确认高位是否由业绩支撑"] if high else
                         ["核实基本面逻辑是否仍成立", "逻辑未破坏可考虑分批布局",
                          "逻辑破坏则视为价值陷阱，不宜摊平"])
            advices.append(InvestmentAdvice(
                type=AdviceType.CAUTION if high else AdviceType.OPPORTUNITY,
                priority=AdvicePriority.LOW if bond else AdvicePriority.MEDIUM,
                title=(f"高低位定位{'（债券）' if bond else ''}："
                       f"{len(rs)}只处于历史{side}"
                       f"（P{'≥' if high else '≤-'}{P_TH:.0f}）"),
                description=(f"以下持仓价格处于历史{side}区间（位置分数与置信度）：\n"
                             + "\n".join(_fmt(r) for r in rs) + note),
                action_items=items,
                related_codes=[r['code'] for r in rs],
                confidence=round(sum(r['C'] for r in rs) / len(rs), 2),
                created_at=datetime.now()
            ))

        hot_eq, hot_bond = _split(hot)
        cold_eq, cold_bond = _split(cold)
        _emit(hot_eq, high=True, bond=False)
        _emit(hot_bond, high=True, bond=True)
        _emit(cold_eq, high=False, bond=False)
        _emit(cold_bond, high=False, bond=True)

        # 组合层面整体站位：仅统计权益 ETF（债券由利率驱动，口径不同不可混算）
        pf = portfolio_position([r for r in results if r.get('type') != 'bond'], weights)
        if pf and abs(pf['P']) >= 40 and pf['C'] >= C_TH:
            high = pf['P'] > 0
            advices.append(InvestmentAdvice(
                type=AdviceType.CAUTION if high else AdviceType.OPPORTUNITY,
                priority=AdvicePriority.LOW,
                title=f"权益仓位整体站位{'偏高' if high else '偏低'}（加权 P={pf['P']:+.0f}）",
                description=(f"按市值加权（仅权益 ETF），整体位置 P={pf['P']:+.1f}"
                             f"（{pf['label']}），置信度 {pf['C']:.2f}，"
                             f"覆盖 {pf['coverage']*100:.0f}% 组合市值。"),
                action_items=(["总仓位不宜追高，优先保留现金弹性"] if high
                              else ["整体位置偏低，可评估提升总仓位的空间"]),
                related_codes=[], confidence=pf['C'], created_at=datetime.now()
            ))

        return advices

    def _analyze_add_opportunity(self, portfolio_data):
        """低配类别加仓机会测算：按分层战略基准（含战术超配）找低配板块，量化加仓缺口。

        借鉴 fund-analysis 配置审查（资产均衡）+ 缺失/低配仓位识别。与 rebalance_engine
        的 propose_layered 口径一致（SECTOR_TARGET_WEIGHTS 经 TACTICAL_OVERRIDES 替换、
        受管控池按 keep_sum 归一化）。仅做「机会测算」，不生成实际调仓（不自动调仓红线）。
        """
        from config.settings import ETF_CATEGORIES, SECTOR_TARGET_WEIGHTS, TACTICAL_OVERRIDES
        advices = []
        positions = portfolio_data.get('positions', [])
        if not positions:
            return advices
        total_mv = sum((p.get('market_value', 0) or 0) for p in positions if isinstance(p, dict))
        if total_mv <= 0:
            return advices

        sector_cur: Dict[str, float] = {}
        sector_etfs: Dict[str, list] = {}
        for pos in positions:
            if not isinstance(pos, dict):
                continue
            code = str(pos.get('code', ''))
            mv = pos.get('market_value', 0) or 0
            w = mv / total_mv
            sector = (ETF_CATEGORIES.get(code, {}) or {}).get('sector', '其他')
            sector_cur[sector] = sector_cur.get(sector, 0.0) + w
            sector_etfs.setdefault(sector, []).append(
                (code, pos.get('name', code), w, pos.get('pnl_rate', 0) or 0))

        # 战术留痕：TACTICAL_OVERRIDES 替换战略基准
        effective = dict(SECTOR_TARGET_WEIGHTS)
        for _s, _t in (TACTICAL_OVERRIDES or {}).items():
            if _s in effective:
                effective[_s] = _t

        managed = {s: wt for s, wt in effective.items() if s in sector_cur}
        keep_sum = sum(sector_cur.get(s, 0.0) for s in sector_cur if s not in managed)
        msum = sum(managed.values())
        if msum <= 0:
            return advices
        scale = (1.0 - keep_sum) / msum if keep_sum < 1.0 else 0.0

        for s, tw in managed.items():
            target_w = tw * scale
            cur_w = sector_cur.get(s, 0.0)
            gap = target_w - cur_w
            if gap < 0.02:  # 低配不足 2pp，视为无需提示
                continue
            add_amt = gap * total_mv
            etfs = sorted(sector_etfs.get(s, []), key=lambda x: -x[2])
            lines = []
            for c, n, w, pnl in etfs:
                tag = "浮盈" if pnl > 0 else "亏损"
                lines.append(f"- {n}({c}): 权重 {w*100:.1f}%（{tag} {pnl:+.1f}%）")
            advices.append(InvestmentAdvice(
                type=AdviceType.OPPORTUNITY, priority=AdvicePriority.MEDIUM,
                title=f"低配加仓机会：{s}（低配 {gap*100:.1f}pp）",
                description=(
                    f"「{s}」当前权重 {cur_w*100:.1f}%，低于战略目标 {target_w*100:.1f}%，"
                    f"缺口 {gap*100:.1f}pp，加仓至目标约需 +{add_amt:,.0f} 元。候选标的：\n"
                    + "\n".join(lines)),
                action_items=["优先加仓估值低位/亏损待修复标的", "分步加仓避免单次冲击", "加仓后复核组合波动率与集中度"],
                related_codes=[c for c, _, _, _ in etfs], confidence=0.6,
                created_at=datetime.now()
            ))

        # 缺失核心仓位（战略基准里有、但当前无持仓的类别）
        missing = [s for s in effective if s not in sector_cur and s != "其他"]
        if missing:
            lines = [f"- {s}：战略目标 {effective[s]*100:.0f}%，当前无持仓" for s in missing]
            advices.append(InvestmentAdvice(
                type=AdviceType.OPPORTUNITY, priority=AdvicePriority.LOW,
                title=f"缺失核心仓位（{len(missing)}类）",
                description="以下战略基准类别当前无任何持仓，配置存在缺口：\n" + "\n".join(lines),
                action_items=["评估是否补入对应宽基/主题ETF", "避免为凑仓位而强配不了解的品种"],
                related_codes=[], confidence=0.5,
                created_at=datetime.now()
            ))

        return advices

    def _analyze_market_stage(self, portfolio_data=None):
        """周线七阶段市场定位（market-trend-assessment 路径 B 的量化近似实现）。

        用上证指数周线趋势 + 均线排列 + 距 52 周高低点位置，把大盘映射到七阶段，
        输出「当前阶段 + 仓位取向（情景取向，非指令）+ 证据链」。数据来自 index_quotes
        （本地已有，无需新采集）。红线：只作参考提示，不自动调仓。
        """
        import pandas as pd
        try:
            rows = self.db.execute(
                "SELECT date, close FROM index_quotes WHERE code='sh000001' ORDER BY date"
            ).fetchall()
        except Exception as e:
            logger.warning("市场阶段定位：读取上证指数失败 %s", e)
            return []
        if len(rows) < 60:
            return []
        df = pd.DataFrame(rows, columns=["date", "close"])
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df = df.dropna(subset=["close"])
        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date").sort_index()
        weekly = df["close"].resample("W-FRI").last().dropna()
        if len(weekly) < 25:
            return []

        last = float(weekly.iloc[-1])
        ma5 = float(weekly.rolling(5).mean().iloc[-1])
        ma20 = float(weekly.rolling(20).mean().iloc[-1])
        ma5_prev = float(weekly.rolling(5).mean().iloc[-2])
        high52 = float(weekly.iloc[-52:].max())
        low52 = float(weekly.iloc[-52:].min())
        pct_hi = (last / high52 - 1) * 100
        pct_lo = (last / low52 - 1) * 100
        ret4w = (last / float(weekly.iloc[-5]) - 1) * 100
        ret2w = (last / float(weekly.iloc[-3]) - 1) * 100
        uptrend = ma5 > ma20 and ma5 >= ma5_prev
        downtrend = ma5 < ma20 and ma5 <= ma5_prev

        # 七阶段映射（周线趋势 + 位置 + 近期动能）
        if pct_hi <= -15 and downtrend and ret4w <= -6:
            stage, pos = 7, "极低仓（约0-2成，情景取向）——防守为主，等阶段一再评估入场"
        elif pct_hi <= -15 and ret4w > 0:
            stage, pos = 2, "偏低仓（约3-5成）——超跌反弹参与，快进快出"
        elif pct_hi <= -15:
            stage, pos = 1, "轻仓观察——等主跌企稳，关注补跌的防御板块"
        elif uptrend and pct_hi > -3:
            stage, pos = 5, "偏高仓（约7-9成）——主线持有主升；设失效条件，非拿住不动"
        elif uptrend:
            stage, pos = 4, "中高仓（约5-7成）——主线确认后逐步提高敞口，重点观察"
        elif pct_hi <= -5 and ret4w < -4:
            stage, pos = 6, "中低仓（约3-5成）——降低高位主线敞口，转绩优防御"
        else:
            stage, pos = 3, "中低仓（约3-5成）——震荡观望，等方向"
        stage_names = {1: "下跌到企稳", 2: "超跌反弹", 3: "真空期", 4: "主线初期",
                       5: "主线中期", 6: "主线末期", 7: "进入下跌"}
        name = stage_names[stage]

        evidence = (
            f"上证指数周线：收盘 {last:.0f}，5周均线 {ma5:.0f} / 20周均线 {ma20:.0f}"
            f"（{'多头' if ma5 > ma20 else '空头'}排列），距52周高点 {pct_hi:+.1f}%、"
            f"距52周低点 {pct_lo:+.1f}%，近4周 {ret4w:+.1f}%、近2周 {ret2w:+.1f}%。"
        )
        return [InvestmentAdvice(
            type=AdviceType.OPPORTUNITY, priority=AdvicePriority.MEDIUM,
            title=f"市场阶段定位：阶段{stage}「{name}」",
            description=evidence + f"\n仓位取向（情景参考，非指令）：{pos}",
            action_items=[
                "此为周线趋势的量化近似，未含涨停梯队/主线强度（项目未采集）",
                "结合资金流、估值、情绪指标交叉验证后再定仓位",
                "仓位取向为条件框架，非买卖指令，须自行决策",
            ],
            related_codes=[], confidence=0.55,
            created_at=datetime.now()
        )]

    def _analyze_market_sentiment_extremes(self, portfolio_data=None):
        """市场情绪温度计 + 极端情绪检测（market-trend 路径 A/C 简化实现）。

        用涨停/跌停家数、最高连板数（梯队高度）、涨停最集中行业（主线线索）、
        融资余额近期变化（杠杆拥挤度）交叉判断市场情绪阶段，极端信号出预警。
        数据来自 market_breadth（涨停池/跌停池）+ market_sentiment（两融）。
        红线：情绪判断为情景参考，非买卖指令，不自动调仓。
        """
        from src.data_sources.market_breadth import load_latest_breadth
        try:
            b = load_latest_breadth(self.db)
        except Exception as e:
            logger.warning("市场情绪温度计：读取市场广度失败 %s", e)
            return []
        if not b or b.get("zt_count") is None:
            return []

        zt = int(b.get("zt_count") or 0)
        dt = int(b.get("dt_count") or 0)
        max_lb = int(b.get("max_lianban") or 0)
        top_ind = b.get("top_industry")
        top_ind_n = int(b.get("top_industry_count") or 0)
        avg_to = b.get("zt_avg_turnover")

        # 融资拥挤度（5 日变化率）
        margin_note = ""
        chg5 = None
        try:
            mrows = self.db.execute(
                "SELECT date, value FROM market_sentiment "
                "WHERE indicator_code='MARGIN_TOTAL' AND value IS NOT NULL "
                "ORDER BY date DESC LIMIT 6").fetchall()
            if len(mrows) >= 6:
                cur = float(mrows[0][1]); prev = float(mrows[5][1])
                if prev > 0:
                    chg5 = (cur / prev - 1) * 100
                    margin_note = f"两融余额近5日 {chg5:+.1f}%"
        except Exception as e:
            logger.debug("融资余额读取失败 %s", e)

        # 情绪阶段
        if dt >= 40:
            phase = "退潮/恐慌"
            pos = "防守取向——等跌停潮收敛、冰点信号出现再评估"
        elif zt >= 60:
            phase = "过热"
            pos = "降风险敞口——涨停过密易见情绪顶，警惕高位分歧"
        elif zt >= 40 and max_lb >= 5:
            phase = "主升"
            pos = "顺势持有主线（条件框架）——设失效条件，非拿住不动"
        elif zt >= 20 or max_lb >= 3:
            phase = "修复/升温"
            pos = "低仓试错取向（条件满足时）——关注梯队能否走强"
        else:
            phase = "冰点"
            pos = "观望取向——等右侧确认信号，勿逆势抄底"

        # 涨停梯队高度
        if max_lb >= 5:
            ladder = f"高连板（最高 {max_lb} 板，主升梯队）"
        elif max_lb >= 3:
            ladder = f"中连板（最高 {max_lb} 板，梯队成形）"
        else:
            ladder = f"无梯队（最高 {max_lb} 板）"

        # 主线线索
        mainline = f"涨停最集中行业「{top_ind}」（{top_ind_n} 家）" if top_ind else "无明确主线"

        desc_lines = [
            f"情绪阶段：{phase}",
            f"涨/跌停家数：{zt} / {dt}",
            f"涨停梯队：{ladder}" + (f"，平均换手 {avg_to:.1f}%" if avg_to is not None else ""),
            f"主线线索：{mainline}",
        ]
        if margin_note:
            desc_lines.append(f"杠杆拥挤度：{margin_note}")

        advices = [InvestmentAdvice(
            type=AdviceType.OPPORTUNITY, priority=AdvicePriority.MEDIUM,
            title=f"市场情绪温度计：{phase}",
            description="\n".join(desc_lines) + f"\n仓位取向（情景参考，非指令）：{pos}",
            action_items=[
                "情绪判断为情景参考，须与价格结构/估值/资金流交叉验证",
                "极端信号不单独作为交易依据",
                "仓位取向为条件框架，非买卖指令，须自行决策",
            ],
            related_codes=[], confidence=0.55,
            created_at=datetime.now()
        )]

        # 极端预警（交叉验证：情绪 + 杠杆）
        if phase in ("过热", "退潮/恐慌"):
            if phase == "过热":
                risk = ("涨停过密 + 杠杆快速抬升，警惕情绪见顶" if (chg5 or 0) > 0
                        else "涨停过密，警惕高位分歧与情绪见顶")
            else:
                risk = f"跌停 {dt} 家，退潮信号，防守为主"
            advices.append(InvestmentAdvice(
                type=AdviceType.RISK_MANAGEMENT, priority=AdvicePriority.HIGH,
                title=f"极端情绪预警：{phase}",
                description=f"市场情绪处于「{phase}」极端区（涨/跌停 {zt}/{dt}，{ladder}）。{risk}。",
                action_items=["降低风险敞口/控制回撤", "避免追高或逆势重仓", "等情绪回归中性再评估"],
                related_codes=[], confidence=0.6,
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


    # ============================================================
    #  市场事件驱动建议（Phase 2）
    # ============================================================

    # ============================================================
    #  资金流分析建议
    # ============================================================
    def _analyze_fund_flows(self, portfolio_data):
        """分析ETF资金流异动，关联持仓标的。

        优化: 将逐标的的重复模板合并为「净流入/净流出 TOP N」概览，
        显著降低同质建议数量、提升信号密度（原每个持仓ETF各发一条）。
        """
        advices = []
        ff_df = portfolio_data.get('fund_flows')
        if ff_df is None or (hasattr(ff_df, 'empty') and ff_df.empty):
            return advices

        try:
            if not isinstance(ff_df, pd.DataFrame):
                return advices

            positions = portfolio_data.get('positions', [])
            held_codes = set(str(p.get('code', '')) for p in positions if isinstance(p, dict))
            if not held_codes:
                return advices

            date_col = 'date' if 'date' in ff_df.columns else 'trade_date'
            agg = ff_df.groupby('code').agg(
                total_net_inflow=('net_inflow', 'sum'),
                days=(date_col, 'count')
            ).reset_index()
            agg['code'] = agg['code'].astype(str)

            # 仅保留持仓标的的资金异动，避免板块/主力等无关 code 混入
            held = agg[agg['code'].isin(held_codes)].copy()
            if held.empty:
                return advices

            TOP_N = 5
            name_map = {str(p.get('code', '')): p.get('name') for p in positions if isinstance(p, dict)}
            inflow = held[held['total_net_inflow'] > 100000000].sort_values(
                'total_net_inflow', ascending=False).head(TOP_N)
            outflow = held[held['total_net_inflow'] < -100000000].sort_values(
                'total_net_inflow').head(TOP_N)

            if not inflow.empty:
                lines = [
                    f"- {name_map.get(c) or c}({c}): 近{int(r['days'])}日净流入 {r['total_net_inflow']/1e8:.2f}亿元"
                    for _, r in inflow.iterrows() for c in [str(r['code'])]
                ]
                advices.append(InvestmentAdvice(
                    type=AdviceType.OPPORTUNITY, priority=AdvicePriority.MEDIUM,
                    title=f"持仓ETF资金大幅净流入（TOP {len(inflow)}）",
                    description="以下持仓ETF近期获资金青睐：\n" + "\n".join(lines),
                    action_items=["关注资金持续性", "评估是否跟随主力方向", "结合估值与基本面确认"],
                    related_codes=list(inflow['code']), confidence=0.6,
                    created_at=datetime.now()
                ))

            if not outflow.empty:
                lines = [
                    f"- {name_map.get(c) or c}({c}): 近{int(r['days'])}日净流出 {abs(r['total_net_inflow'])/1e8:.2f}亿元"
                    for _, r in outflow.iterrows() for c in [str(r['code'])]
                ]
                advices.append(InvestmentAdvice(
                    type=AdviceType.CAUTION, priority=AdvicePriority.MEDIUM,
                    title=f"持仓ETF资金大幅净流出（TOP {len(outflow)}）",
                    description="以下持仓ETF近期资金撤离：\n" + "\n".join(lines),
                    action_items=["警惕资金撤离风险", "评估止损或减仓时机"],
                    related_codes=list(outflow['code']), confidence=0.6,
                    created_at=datetime.now()
                ))

            # 持仓ETF整体净流出风险（基于持仓子集汇总，而非全市场）
            held_total = held['total_net_inflow'].sum()
            if held_total < -500000000:  # 整体净流出>5亿
                advices.append(InvestmentAdvice(
                    type=AdviceType.RISK_MANAGEMENT, priority=AdvicePriority.MEDIUM,
                    title="持仓ETF整体资金流出",
                    description=f"持仓ETF近{int(held['days'].sum())}日累计净流出{abs(held_total)/1e8:.2f}亿元",
                    action_items=["关注市场整体风险偏好", "考虑降低仓位防御"],
                    related_codes=[], confidence=0.55,
                    created_at=datetime.now()
                ))
        except ValueError as e:
            logger.warning(f"资金流分析异常: {e}")

        return advices

    # ============================================================
    #  市场情绪分析建议
    # ============================================================
    def _analyze_market_sentiment(self, portfolio_data):
        """分析融资融券/质押等市场情绪指标"""
        advices = []
        ms_df = portfolio_data.get('market_sentiment')
        if ms_df is None or (hasattr(ms_df, 'empty') and ms_df.empty):
            return advices

        try:
            if not isinstance(ms_df, pd.DataFrame):
                return advices

            latest = ms_df.drop_duplicates('name', keep='first')
            indicators = dict(zip(latest['name'], latest['value']))

            margin_total = indicators.get('MARGIN_TOTAL')
            margin_buy_sh = indicators.get('MARGIN_BUY_\u4e0a')
            if margin_total and margin_buy_sh:
                try:
                    mt = float(margin_total)
                    if mt > 18000:
                        advices.append(InvestmentAdvice(
                            type=AdviceType.CAUTION, priority=AdvicePriority.LOW,
                            title="融资余额处于高位",
                            description=f"两市融资余额{mt:.0f}亿元，杠杆水平偏高",
                            action_items=["注意杠杆风险", "关注后续资金动向"],
                            related_codes=[], confidence=0.5,
                            created_at=datetime.now()
                        ))
                except (ValueError, TypeError):
                    pass

            pledge_ratio = indicators.get('PLEDGE_RATIO')
            if pledge_ratio:
                try:
                    pr = float(pledge_ratio)
                    if pr > 5.0:
                        advices.append(InvestmentAdvice(
                            type=AdviceType.CAUTION, priority=AdvicePriority.MEDIUM,
                            title="股权质押比例偏高",
                            description=f"市场整体质押比例{pr:.2f}%，需关注平仓风险",
                            action_items=["关注高质押个股", "警惕连锁平仓风险"],
                            related_codes=[], confidence=0.55,
                            created_at=datetime.now()
                        ))
                except (ValueError, TypeError):
                    pass
        except ValueError as e:
            logger.warning(f"市场情绪分析异常: {e}")

        return advices

    # ============================================================
    #  宏观环境分析建议
    # ============================================================
    def _analyze_macro_environment(self, portfolio_data):
        """分析宏观经济指标对投资组合的影响"""
        advices = []
        md_df = portfolio_data.get('macro_daily')
        if md_df is None or (hasattr(md_df, 'empty') and md_df.empty):
            return advices

        try:
            if not isinstance(md_df, pd.DataFrame):
                return advices

            latest = md_df.drop_duplicates('name', keep='first')
            indicators = dict(zip(latest['name'], latest['value']))

            gold_price = indicators.get('COMEX_GOLD')
            if gold_price:
                try:
                    gp = float(gold_price)
                    if gp > 3200:
                        advices.append(InvestmentAdvice(
                            type=AdviceType.OPPORTUNITY, priority=AdvicePriority.LOW,
                            title="金价处于高位",
                            description=f"COMEX黄金价格{gp:.0f}美元/盎司，避险情绪浓厚",
                            action_items=["关注黄金ETF配置价值", "评估避险资产比例"],
                            related_codes=[], confidence=0.5,
                            created_at=datetime.now()
                        ))
                except (ValueError, TypeError):
                    pass

            usd_cny = indicators.get('USD_CNY')
            if usd_cny:
                try:
                    rate = float(usd_cny)
                    if rate > 7.3:
                        advices.append(InvestmentAdvice(
                            type=AdviceType.CAUTION, priority=AdvicePriority.MEDIUM,
                            title="人民币汇率承压",
                            description=f"美元/人民币{rate:.4f}，贬值压力较大",
                            action_items=["关注外资流向变化", "评估进口成本影响"],
                            related_codes=[], confidence=0.55,
                            created_at=datetime.now()
                        ))
                except (ValueError, TypeError):
                    pass

            shibor = indicators.get('SHIBOR_ON')
            if shibor:
                try:
                    s = float(shibor)
                    if s > 2.5:
                        advices.append(InvestmentAdvice(
                            type=AdviceType.RISK_MANAGEMENT, priority=AdvicePriority.LOW,
                            title="银行间利率偏高",
                            description=f"SHIBOR隔夜{s:.3f}%，短期流动性偏紧",
                            action_items=["关注市场流动性变化", "评估对债券/货基的影响"],
                            related_codes=[], confidence=0.45,
                            created_at=datetime.now()
                        ))
                except (ValueError, TypeError):
                    pass

            us_10y = indicators.get('US_10Y_BOND')
            if us_10y:
                try:
                    y = float(us_10y)
                    if y > 4.5:
                        advices.append(InvestmentAdvice(
                            type=AdviceType.CAUTION, priority=AdvicePriority.MEDIUM,
                            title="美债收益率高企",
                            description=f"美国10年期国债收益率{y:.2f}%，全球资产承压",
                            action_items=["关注外资回流美国风险", "评估对A股估值影响"],
                            related_codes=[], confidence=0.55,
                            created_at=datetime.now()
                        ))
                except (ValueError, TypeError):
                    pass
        except ValueError as e:
            logger.warning(f"宏观环境分析异常: {e}")

        return advices

    # ============================================================
    #  新闻事件分析建议
    # ============================================================
    def _analyze_news_sentiment(self, portfolio_data):
        """分析新闻事件，关联持仓板块"""
        advices = []
        news_df = portfolio_data.get('daily_news')
        if news_df is None or (hasattr(news_df, 'empty') and news_df.empty):
            return advices

        try:
            if not isinstance(news_df, pd.DataFrame):
                return advices

            sentiment_counts = news_df['sentiment_score'].value_counts() if 'sentiment_score' in news_df.columns else {}
            total = len(news_df)

            if total == 0:
                return advices

            neg_count = sentiment_counts.get('negative', sentiment_counts.get(-1, 0))
            neg_ratio = neg_count / total if total > 0 else 0

            if neg_ratio > 0.4 and total >= 5:
                advices.append(InvestmentAdvice(
                    type=AdviceType.CAUTION, priority=AdvicePriority.MEDIUM,
                    title="市场负面新闻占比较高",
                    description=f"最近3天{total}条新闻中{neg_count}条为负面({neg_ratio:.0%})",
                    action_items=["关注负面新闻关联标的", "提高风险意识"],
                    related_codes=[], confidence=0.5,
                    created_at=datetime.now()
                ))

            if 'category' in news_df.columns:
                cat_counts = news_df['category'].value_counts()
                top_cat = cat_counts.index[0] if len(cat_counts) > 0 else None
                if top_cat and cat_counts.iloc[0] >= 3:
                    cat_news = news_df[news_df['category'] == top_cat]
                    cat_neg = len(cat_news[cat_news['sentiment_score'].isin(['negative', -1])])
                    if cat_neg >= 2:
                        advices.append(InvestmentAdvice(
                            type=AdviceType.CAUTION, priority=AdvicePriority.LOW,
                            title=f"[{top_cat}] 板块负面新闻密集",
                            description=f"该板块{len(cat_news)}条新闻中{cat_neg}条为负面",
                            action_items=["审视该板块持仓", "关注后续政策或事件发展"],
                            related_codes=[], confidence=0.45,
                            created_at=datetime.now()
                        ))
        except (KeyError, ValueError, TypeError) as e:
            logger.warning(f"新闻情感分析异常: {e}")

        return advices


    def analyze_market_event_signals(self, signals):
        """基于市场事件信号生成投资建议。

        Args:
            signals: List[MarketSignal] from MarketEventSignalEngine

        Returns:
            建议列表
        """
        from src.analysis.market_event_signals import SignalType, SignalLevel

        advices = []
        seen = set()  # 去重: (title, code)

        for signal in signals:
            key = (signal.title, signal.code)
            if key in seen:
                continue
            seen.add(key)

            if signal.signal_type == SignalType.RISK and signal.level == SignalLevel.HIGH:
                advices.append(InvestmentAdvice(
                    type=AdviceType.CAUTION, priority=AdvicePriority.HIGH,
                    title=signal.title, description=signal.description,
                    action_items=["关注该标的风险变化", "评估是否需要减仓或设置止损"],
                    related_codes=[signal.code], confidence=signal.confidence,
                    created_at=datetime.now()
                ))
            elif signal.signal_type == SignalType.RISK and signal.level == SignalLevel.MEDIUM:
                advices.append(InvestmentAdvice(
                    type=AdviceType.CAUTION, priority=AdvicePriority.MEDIUM,
                    title=signal.title, description=signal.description,
                    action_items=["持续关注市场动态", "结合技术面判断"],
                    related_codes=[signal.code], confidence=signal.confidence,
                    created_at=datetime.now()
                ))
            elif signal.signal_type == SignalType.OPPORTUNITY and signal.level == SignalLevel.HIGH:
                advices.append(InvestmentAdvice(
                    type=AdviceType.OPPORTUNITY, priority=AdvicePriority.HIGH,
                    title=signal.title, description=signal.description,
                    action_items=["深入研究基本面", "评估入场时机和仓位"],
                    related_codes=[signal.code], confidence=signal.confidence,
                    created_at=datetime.now()
                ))
            elif signal.signal_type == SignalType.OPPORTUNITY and signal.level == SignalLevel.MEDIUM:
                advices.append(InvestmentAdvice(
                    type=AdviceType.OPPORTUNITY, priority=AdvicePriority.MEDIUM,
                    title=signal.title, description=signal.description,
                    action_items=["加入观察列表", "等待技术面确认"],
                    related_codes=[signal.code], confidence=signal.confidence,
                    created_at=datetime.now()
                ))

        # 按优先级排序
        po = {AdvicePriority.HIGH: 0, AdvicePriority.MEDIUM: 1, AdvicePriority.LOW: 2}
        advices.sort(key=lambda x: po.get(x.priority, 3))
        return advices

    def _analyze_margin_data(self, portfolio_data: Dict) -> List[InvestmentAdvice]:
        """分析融资融券数据，检测融资余额异常变动和融券异动。

        利用stock_margin表（128,464行）的数据，对持仓标的进行：
        1. 融资余额趋势检测：近5日Z-score > 2.0（急剧增长/萎缩）
        2. 融资买入占比检测：融资买入/余额 > 阈值（资金涌入信号）
        3. 融券余额异动检测：融券量近期突增（空头情绪升温）

        Returns:
            投资建议列表
        """

        advices = []

        try:
            # 获取持仓代码
            positions = portfolio_data.get('positions', [])
            if not positions:
                return advices
            hold_codes = set(p.get('code', '') for p in positions)

            df = self._query_margin_data(list(hold_codes))

            if df.empty or len(df) < 5:
                return advices

            # 四类信号分别收集，循环结束后分组汇总（避免逐标的重复模板）
            surge, decline, short_surge, active_buy = [], [], [], []
            for code in df['code'].unique():
                code_df = df[df['code'] == code].sort_values('date')
                name = code_df['name'].iloc[-1] if 'name' in code_df.columns else code

                if len(code_df) < 5:
                    continue

                # --- 指标1: 融资余额趋势 (Z-score) ---
                recent = code_df.head(5)['margin_balance']
                older = code_df.iloc[5:]['margin_balance'] if len(code_df) > 5 else recent

                mean_val = older.mean()
                std_val = older.std()
                current_balance = recent.iloc[0]

                if std_val > 0 and mean_val > 0:
                    z_score = (current_balance - mean_val) / std_val
                    change_pct = (current_balance - mean_val) / mean_val * 100
                    if z_score > 2.0:
                        surge.append((name, code, z_score, change_pct, current_balance))
                    elif z_score < -2.0:
                        decline.append((name, code, z_score, change_pct, current_balance))

                # --- 指标2: 融券量突增检测（空头情绪） ---
                short_recent = code_df.head(5)['short_volume']
                short_older = code_df.iloc[5:]['short_volume'] if len(code_df) > 5 else short_recent
                short_mean = short_older.mean()
                short_std = short_older.std()
                current_short = short_recent.iloc[0]

                if short_std > 0 and short_mean > 0:
                    short_z = (current_short - short_mean) / short_std
                    if short_z > 2.5 and current_short > 0:
                        short_surge.append((name, code, short_z, current_short))

                # --- 指标3: 融资买入活跃度 (买入/余额比) ---
                if current_balance > 0:
                    recent_buy = code_df.head(5)['margin_buy'].iloc[0]
                    buy_ratio = recent_buy / current_balance
                    if buy_ratio > 0.05:  # 单日买入超余额5%
                        active_buy.append((name, code, buy_ratio, recent_buy))

            # --- 分组汇总：四类信号各合并为一条排名概览，提升信号密度、降低同质噪声 ---
            TOP = 5
            if surge:
                surge.sort(key=lambda x: x[2], reverse=True)
                lines = [f"- {n}({c}): Z={z:.1f}，当前{b/1e8:.1f}亿，较均值+{cp:.1f}%"
                         for n, c, z, cp, b in surge[:TOP]]
                best = max(s[2] for s in surge)
                advices.append(InvestmentAdvice(
                    type=AdviceType.OPPORTUNITY, priority=AdvicePriority.MEDIUM,
                    title=f"融资余额近5日异常增长（{len(surge)}只）",
                    description="杠杆资金积极涌入以下标的：\n" + "\n".join(lines),
                    action_items=["关注融资余额增长持续性", "结合技术面确认趋势方向", "警惕短期获利盘回吐压力"],
                    related_codes=[c for _, c, _, _, _ in surge],
                    confidence=min(0.5 + best * 0.05, 0.85),
                    created_at=datetime.now()
                ))
            if decline:
                decline.sort(key=lambda x: x[2])
                lines = [f"- {n}({c}): Z={z:.1f}，当前{b/1e8:.1f}亿，较均值{cp:.1f}%"
                         for n, c, z, cp, b in decline[:TOP]]
                worst = min(d[2] for d in decline)
                advices.append(InvestmentAdvice(
                    type=AdviceType.CAUTION, priority=AdvicePriority.MEDIUM,
                    title=f"融资余额近5日大幅萎缩（{len(decline)}只）",
                    description="杠杆资金正在撤退：\n" + "\n".join(lines),
                    action_items=["评估资金撤离是否与基本面变化相关", "关注后续企稳信号", "考虑适当降低仓位"],
                    related_codes=[c for _, c, _, _, _ in decline],
                    confidence=min(0.5 + abs(worst) * 0.05, 0.85),
                    created_at=datetime.now()
                ))
            if active_buy:
                active_buy.sort(key=lambda x: x[2], reverse=True)
                lines = [f"- {n}({c}): 买入{b/1e8:.2f}亿，占余额{r:.1%}"
                         for n, c, r, b in active_buy[:TOP]]
                best = max(a[2] for a in active_buy)
                advices.append(InvestmentAdvice(
                    type=AdviceType.OPPORTUNITY, priority=AdvicePriority.LOW,
                    title=f"融资买入活跃度偏高（{len(active_buy)}只）",
                    description="杠杆资金买入积极性较高：\n" + "\n".join(lines),
                    action_items=["关注买入持续性", "配合技术面判断"],
                    related_codes=[c for _, c, _, _ in active_buy],
                    confidence=min(0.4 + best * 0.5, 0.7),
                    created_at=datetime.now()
                ))
            if short_surge:
                short_surge.sort(key=lambda x: x[2], reverse=True)
                lines = [f"- {n}({c}): 融券Z={z:.1f}，当前{s/1e8:.2f}亿"
                         for n, c, z, s in short_surge[:TOP]]
                advices.append(InvestmentAdvice(
                    type=AdviceType.CAUTION, priority=AdvicePriority.LOW,
                    title=f"融券量近期显著增加（{len(short_surge)}只）",
                    description="空头力量明显增强：\n" + "\n".join(lines),
                    action_items=["关注融券变化趋势", "结合价格走势判断是否有做空压力"],
                    related_codes=[c for _, c, _, _ in short_surge],
                    confidence=0.5,
                    created_at=datetime.now()
                ))

        except (pd.errors.DatabaseError, sqlite3.OperationalError, sqlite3.IntegrityError, KeyError, ValueError, TypeError) as e:
            logger.warning(f"融资融券分析异常: {e}")

        return advices

    def _analyze_institution_research(self, portfolio_data: Dict) -> List[InvestmentAdvice]:
        """分析机构调研热点，识别市场关注度集中的方向。

        利用stock_institution_research表（4,300行）的数据：
        1. 近30天机构调研密集标的推荐（调研次数>20次）
        2. 新增机构调研异动（近期突然增多）
        3. 券商/基金集中调研方向

        注: 该表数据为个股，不直接关联ETF持仓，但可反映板块关注度方向。

        Returns:
            投资建议列表
        """
        import pandas as pd

        advices = []

        try:
            query = """
                SELECT code, name, date, institution, inst_type,
                       receive_method, research_date
                FROM stock_institution_research
                WHERE date >= DATE('now', '-45 days')
                ORDER BY date DESC
            """

            df = pd.read_sql_query(query, self.db)

            if df.empty:
                return advices

            df['date'] = pd.to_datetime(df['date'])

            # --- 指标1: 高热度调研标的 ---
            code_stats = df.groupby(['code', 'name']).agg(
                research_count=('institution', 'count'),
                inst_count=('institution', 'nunique'),
                latest_date=('date', 'max')
            ).reset_index()

            hot_targets = code_stats[code_stats['research_count'] >= 20].sort_values(
                'research_count', ascending=False
            )

            if not hot_targets.empty:
                top3 = hot_targets.head(3)
                summaries = []
                for _, row in top3.iterrows():
                    summaries.append(
                        f"{row['name']}({row['code']}): "
                        f"{row['research_count']}次调研/{row['inst_count']}家机构"
                    )

                advices.append(InvestmentAdvice(
                    type=AdviceType.OPPORTUNITY,
                    priority=AdvicePriority.LOW,
                    title="机构调研热度TOP标的",
                    description=(
                        f"近45天机构调研最密集的标的: {'; '.join(summaries)}。"
                        f"机构密集调研通常预示潜在投资机会或重大事项。"
                    ),
                    action_items=[
                        "关注调研热点是否与持仓板块相关",
                        "研究高热度标的对应ETF是否有配置价值",
                        "留意相关公司公告和业绩预期"
                    ],
                    related_codes=[], confidence=0.4,
                    created_at=datetime.now()
                ))

            # --- 指标2: 近7天新增调研异动 ---
            recent_date = df['date'].max()
            week_ago = recent_date - pd.Timedelta(days=7)
            recent_df = df[df['date'] >= week_ago]

            if not recent_df.empty:
                recent_stats = recent_df.groupby(['code', 'name']).agg(
                    week_count=('institution', 'count'),
                ).reset_index()

                # 与之前38天对比
                earlier_df = df[df['date'] < week_ago]
                if not earlier_df.empty:
                    earlier_stats = earlier_df.groupby(['code', 'name']).agg(
                        prior_count=('institution', 'count'),
                    ).reset_index()

                    merged = recent_stats.merge(
                        earlier_stats, on=['code', 'name'], how='left'
                    )
                    merged['prior_count'] = merged['prior_count'].fillna(0)

                    # 调研频次骤增（近7天>之前38天总量）
                    surging = merged[
                        (merged['week_count'] >= 10) &
                        (merged['week_count'] > merged['prior_count'])
                    ]

                    if not surging.empty:
                        surge_summaries = []
                        for _, row in surging.head(3).iterrows():
                            surge_summaries.append(
                                f"{row['name']}({row['code']}): "
                                f"近7天{row['week_count']}次 vs 前38天{int(row['prior_count'])}次"
                            )

                        advices.append(InvestmentAdvice(
                            type=AdviceType.OPPORTUNITY,
                            priority=AdvicePriority.MEDIUM,
                            title="机构调研热度骤增标的",
                            description=(
                                f"近7天调研次数显著超过此前: {'; '.join(surge_summaries)}。"
                                f"关注度突然提升可能伴随催化事件。"
                            ),
                            action_items=[
                                "查阅相关公司近期公告",
                                "判断是否为板块级别信号",
                                "评估对应ETF的配置时机"
                            ],
                            related_codes=[], confidence=0.55,
                            created_at=datetime.now()
                        ))

        except (pd.errors.DatabaseError, sqlite3.OperationalError, sqlite3.IntegrityError, KeyError, ValueError, TypeError) as e:
            logger.warning(f"机构调研分析异常: {e}")

        return advices



    def _query_recent_block_trades(self, days=15):
        """查询近期大宗交易数据。"""
        import pandas as pd
        query = """
            SELECT date, code, name, change_pct, close, trade_price,
                   premium_rate, volume, amount, amount_to_float_mv,
                   buyer_broker
            FROM stock_block_trade
            WHERE date >= DATE('now', '-{} days')
            ORDER BY date DESC
        """.format(days)
        df = pd.read_sql_query(query, self.db)
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])
        return df

    def _query_margin_data(self, codes, days=20):
        """查询指定代码的融资融券数据。"""
        import pandas as pd
        if not codes:
            return pd.DataFrame()
        query = """
            SELECT date, code, name, margin_balance, margin_buy, margin_repay,
                   short_volume, short_sell, short_repay
            FROM stock_margin
            WHERE code IN ({})
            ORDER BY code, date DESC
        """.format(','.join('?' * len(codes)))
        df = pd.read_sql_query(query, self.db, params=list(codes))
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])
        return df

    def _query_institution_research(self, days=45):
        """查询近期机构调研数据。"""
        import pandas as pd
        query = """
            SELECT code, name, date, institution, inst_type,
                   receive_method, research_date
            FROM stock_institution_research
            WHERE date >= DATE('now', '-{} days')
            ORDER BY date DESC
        """.format(days)
        df = pd.read_sql_query(query, self.db)
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])
        return df

    def _analyze_block_trade(self, portfolio_data: Dict) -> List[InvestmentAdvice]:
        """分析大宗交易异常，检测可能的筹码变动信号。

        利用stock_block_trade表（6,651行）的数据：
        1. 大额溢价成交（机构主动吸筹信号）
        2. 大额折价成交（减持/资金出逃信号）
        3. 频繁大宗交易标的（活跃度异常）

        注: 大宗交易数据为个股级别，不直接关联ETF持仓，
        但可反映市场资金流向和板块筹码变化趋势。

        Returns:
            投资建议列表
        """
        import pandas as pd

        advices = []

        try:
            query = """
                SELECT date, code, name, change_pct, close, trade_price,
                       premium_rate, volume, amount, amount_to_float_mv,
                       buyer_broker
                FROM stock_block_trade
                WHERE date >= DATE('now', '-15 days')
                ORDER BY date DESC
            """

            df = pd.read_sql_query(query, self.db)

            if df.empty:
                return advices

            df['date'] = pd.to_datetime(df['date'])

            # --- 指标1: 高溢价大宗交易（溢价率>3%，可能主动吸筹） ---
            premium_trades = df[df['premium_rate'] > 0.03].copy()
            if not premium_trades.empty:
                premium_stats = premium_trades.groupby(['code', 'name']).agg(
                    trade_count=('amount', 'count'),
                    total_amount=('amount', 'sum'),
                    avg_premium=('premium_rate', 'mean'),
                    max_premium=('premium_rate', 'max')
                ).reset_index()

                large_premium = premium_stats[
                    (premium_stats['total_amount'] >= 50_000_000) &
                    (premium_stats['avg_premium'] > 0.05)
                ].sort_values('total_amount', ascending=False)

                if not large_premium.empty:
                    top = large_premium.head(3)
                    items = []
                    for _, row in top.iterrows():
                        items.append(
                            f"{row['name']}({row['code']}): "
                            f"{row['trade_count']}笔/{row['total_amount']/1e4:.0f}万元/"
                            f"平均溢价{row['avg_premium']:.1%}"
                        )

                    advices.append(InvestmentAdvice(
                        type=AdviceType.OPPORTUNITY,
                        priority=AdvicePriority.LOW,
                        title="大宗交易高溢价成交标的",
                        description=(
                            f"近15天出现大额溢价大宗交易: {'; '.join(items)}。"
                            f"溢价成交可能反映机构主动吸筹意愿。"
                        ),
                        action_items=[
                            "关注溢价交易标的是否与持仓板块相关",
                            "查看是否有连续溢价成交趋势",
                            "留意相关公告确认动机"
                        ],
                        related_codes=[], confidence=0.4,
                        created_at=datetime.now()
                    ))

            # --- 指标2: 大额折价成交（折价率>5%，减持信号） ---
            discount_trades = df[df['premium_rate'] < -0.05].copy()
            if not discount_trades.empty:
                discount_stats = discount_trades.groupby(['code', 'name']).agg(
                    trade_count=('amount', 'count'),
                    total_amount=('amount', 'sum'),
                    avg_discount=('premium_rate', 'mean'),
                ).reset_index()

                large_discount = discount_stats[
                    discount_stats['total_amount'] >= 100_000_000
                ].sort_values('total_amount', ascending=False)

                if not large_discount.empty:
                    top_d = large_discount.head(3)
                    items = []
                    for _, row in top_d.iterrows():
                        items.append(
                            f"{row['name']}({row['code']}): "
                            f"{row['trade_count']}笔/{row['total_amount']/1e4:.0f}万元/"
                            f"平均折价{abs(row['avg_discount']):.1%}"
                        )

                    advices.append(InvestmentAdvice(
                        type=AdviceType.CAUTION,
                        priority=AdvicePriority.LOW,
                        title="大宗交易大额折价成交标的",
                        description=(
                            f"近15天出现大额折价大宗交易: {'; '.join(items)}。"
                            f"大额折价成交可能反映股东减持或资金撤离。"
                        ),
                        action_items=[
                            "关注折价标的是否与持仓板块相关",
                            "评估对板块情绪的潜在影响",
                            "警惕持续性减持信号"
                        ],
                        related_codes=[], confidence=0.4,
                        created_at=datetime.now()
                    ))

            # --- 指标3: 大宗交易活跃度异常（amount_to_float_mv>1%且频次高） ---
            active_codes = df[df['amount_to_float_mv'] > 0.01].copy()
            if not active_codes.empty:
                active_stats = active_codes.groupby(['code', 'name']).agg(
                    trade_count=('amount', 'count'),
                    total_to_mv=('amount_to_float_mv', 'sum'),
                    total_amount=('amount', 'sum'),
                ).reset_index()

                highly_active = active_stats[
                    active_stats['trade_count'] >= 5
                ].sort_values('total_amount', ascending=False)

                if not highly_active.empty:
                    top_a = highly_active.head(3)
                    items = []
                    for _, row in top_a.iterrows():
                        items.append(
                            f"{row['name']}({row['code']}): "
                            f"{row['trade_count']}笔/解禁占比{row['total_to_mv']:.1%}"
                        )

                    advices.append(InvestmentAdvice(
                        type=AdviceType.CAUTION,
                        priority=AdvicePriority.LOW,
                        title="大宗交易活跃度异常标的",
                        description=(
                            f"近15天大宗交易成交占比超1%: {'; '.join(items)}。"
                            f"频繁大宗交易可能预示筹码结构变化。"
                        ),
                        action_items=["关注标的是否面临解禁压力", "结合换手率判断筹码稳定性"],
                        related_codes=[], confidence=0.35,
                        created_at=datetime.now()
                    ))

        except (pd.errors.DatabaseError, sqlite3.OperationalError, sqlite3.IntegrityError, KeyError, ValueError, TypeError) as e:
            logger.warning(f"大宗交易分析异常: {e}")

        return advices
