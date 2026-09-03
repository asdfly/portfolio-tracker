
"""
智能分析报告生成器 - 整合回测和建议生成报告
"""
from datetime import datetime
import logging

from src.analysis.backtest import StrategyBacktester
from src.analysis.advisor import SmartAdvisor
from data_loader import get_db_connection
import sqlite3

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
            backtest_results = backtester.compare_strategies(
                codes=[], weights={}, start_date='2024-01-01', end_date='2026-06-01'
            )
            if backtest_results is not None and not backtest_results.empty:
                strategy_advice = self.advisor.generate_strategy_advice(backtest_results)
                if strategy_advice:
                    advices.append(strategy_advice)
        except (ImportError, ModuleNotFoundError) as e:
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
            # 不要硬编码库路径：get_db_connection() 传 None 时走 config 解析，
            # 从而尊重 DATABASE_PATH 覆盖（测试隔离依赖这一点，见 tests/conftest.py）。
            # 历史上这里写死 'data/database/portfolio.db'，导致全量跑测试时
            # 闭环反馈会绕过隔离、真实写入生产库（P0-D 根因）。
            _conn_fb = get_db_connection()
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
                    source TEXT DEFAULT 'auto',
                    status TEXT DEFAULT 'pending'
                )
            """)
            _conn_fb.commit()
            for advice in advices:
                _conn_fb.execute(
                    "INSERT INTO advice_history (created_at, advice_type, priority, title, description, confidence, related_codes, source, status) VALUES (?,?,?,?,?,?,?,?,?)",
                    (advice.created_at.strftime('%Y-%m-%d %H:%M:%S'),
                     advice.type.value, advice.priority.value,
                     advice.title, advice.description,
                     advice.confidence,
                     ','.join(advice.related_codes),
                     'smart_report',
                     'pending')
                )
            _conn_fb.commit()
            logger.info(f'建议历史已记录: {len(advices)}条')
            _conn_fb.close()
        except (sqlite3.OperationalError, sqlite3.IntegrityError) as e:
            logger.warning(f'建议历史记录失败: {e}')

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
            ff = fund_flows.copy()
            # 仅看板块级(sector)与主力资金(main_fund)，避免与逐标的机会建议重复；
            # 且 sector 类别下 code 会碰撞(如 '2' 对应多个板块)，必须按 name(板块名)聚合。
            if 'category' in ff.columns:
                ff = ff[ff['category'].isin(['sector', 'main_fund'])]
            if 'name' in ff.columns:
                ff['name'] = ff['name'].astype(str).str.strip()
                date_col = 'date' if 'date' in ff.columns else 'trade_date'
                agg = ff.groupby('name').agg(
                    total_net=('net_inflow', 'sum'),
                    days=(date_col, 'count')
                ).reset_index()
                top_inflow = agg.nlargest(3, 'total_net')
                top_outflow = agg.nsmallest(3, 'total_net')
                lines.append("### 资金流向")
                lines.append("")
                lines.append("**板块净流入TOP3**:")
                for _, r in top_inflow.iterrows():
                    lines.append(f"- {r['name']}: {self._fmt_amount(r['total_net'])} ({int(r['days'])}日)")
                lines.append("")
                lines.append("**板块净流出TOP3**:")
                for _, r in top_outflow.iterrows():
                    lines.append(f"- {r['name']}: {self._fmt_amount(r['total_net'])} ({int(r['days'])}日)")
                lines.append("")
                sections.append(True)

        if sentiment is not None and hasattr(sentiment, 'empty') and not sentiment.empty:
            lines.append("### 市场情绪")
            lines.append("")
            latest_s = sentiment.drop_duplicates('name', keep='first')
            for _, r in latest_s.iterrows():
                lines.append(f"- {r['name']}: {self._fmt_amount(r['value'])}")
            lines.append("")
            sections.append(True)

        if macro is not None and hasattr(macro, 'empty') and not macro.empty:
            lines.append("### 宏观指标")
            lines.append("")
            latest_m = macro.drop_duplicates('name', keep='first')
            for _, r in latest_m.iterrows():
                lines.append(f"- {r['name']}: {self._fmt_amount(r['value'])}")
            lines.append("")
            sections.append(True)

        if news is not None and hasattr(news, 'empty') and not news.empty:
            lines.append("### 近期新闻摘要")
            lines.append("")
            total_news = len(news)
            if 'sentiment_score' in news.columns:
                labels = news['sentiment_score'].apply(self._sentiment_label)
                sc = labels.value_counts()
                dist = "、".join(f"{k} {int(v)}条" for k, v in sc.items())
                lines.append(f"共 {total_news} 条新闻（情感分布：{dist}）")
            else:
                lines.append(f"共 {total_news} 条新闻")
            lines.append("")
            if 'category' in news.columns:
                cat_counts = news['category'].value_counts().head(5)
                lines.append("**热点板块**: " + ", ".join(f"{k}({v})" for k, v in cat_counts.items()))
                lines.append("")
            if 'title' in news.columns:
                lines.append("**近期要闻**:")
                for _, r in news.head(5).iterrows():
                    cat = r.get('category', '') or ''
                    title = (r.get('title') or '').strip()
                    if title:
                        lines.append(f"- [{cat}] {title}")
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

    # ------------------------------------------------------------------
    # 展示辅助：大数格式化 / 情感分桶
    # ------------------------------------------------------------------
    @staticmethod
    def _fmt_amount(v) -> str:
        """大数友好格式化：万亿 / 亿 / 万 + 千分位，避免裸浮点(如 1283743055996.0)。"""
        try:
            v = float(v)
        except (TypeError, ValueError):
            return str(v)
        if v != v:  # NaN
            return "N/A"
        a = abs(v)
        if a >= 1e12:
            return f"{v / 1e12:.2f}万亿"
        if a >= 1e8:
            return f"{v / 1e8:.2f}亿"
        if a >= 1e4:
            return f"{v / 1e4:.2f}万"
        if float(v).is_integer():
            return f"{int(v):,}"
        return f"{v:,.2f}"

    @staticmethod
    def _sentiment_label(score) -> str:
        """将连续 sentiment_score 分桶为可读标签（数据缺失时统一为中性）。"""
        try:
            s = float(score)
        except (TypeError, ValueError):
            return "中性"
        if s != s:  # NaN
            return "中性"
        if s >= 0.6:
            return "正面"
        if s <= 0.4:
            return "负面"
        return "中性"
