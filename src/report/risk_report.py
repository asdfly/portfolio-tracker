"""
风险分析报告生成器
"""
import json
from typing import Dict, Any
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class RiskReportGenerator:
    """风险分析报告生成器"""

    def __init__(self):
        pass

    def generate_risk_summary(self, risk_results: Dict[str, Any]) -> str:
        """生成风险分析摘要文本"""
        lines = []
        lines.append("=" * 60)
        lines.append("风险分析报告")
        lines.append("=" * 60)

        # 组合风险指标
        portfolio_metrics = risk_results.get('portfolio_metrics', {})

        if 'return_metrics' in portfolio_metrics:
            rm = portfolio_metrics['return_metrics']
            lines.append("\n【收益指标】")
            lines.append(f"  总收益率: {rm.get('total_return', 0):.2f}%")
            lines.append(f"  年化收益率: {rm.get('annual_return', 0):.2f}%")
            lines.append(f"  胜率: {rm.get('win_rate', 0):.1f}%")

        if 'volatility_metrics' in portfolio_metrics:
            vm = portfolio_metrics['volatility_metrics']
            lines.append("\n【波动率指标】")
            lines.append(f"  年化波动率: {vm.get('annual_volatility', 0):.2f}%")
            lines.append(f"  下行波动率: {vm.get('downside_volatility', 0):.2f}%")
            lines.append(f"  波动率等级: {vm.get('volatility_level', '--')}")

        if 'drawdown_metrics' in portfolio_metrics:
            dm = portfolio_metrics['drawdown_metrics']
            lines.append("\n【回撤指标】")
            lines.append(f"  最大回撤: {dm.get('max_drawdown', 0):.2f}%")
            lines.append(f"  当前回撤: {dm.get('current_drawdown', 0):.2f}%")
            lines.append(f"  回撤持续天数: {dm.get('dd_duration_days', 0)}")

        if 'risk_adjusted_metrics' in portfolio_metrics:
            ram = portfolio_metrics['risk_adjusted_metrics']
            lines.append("\n【风险调整收益】")
            lines.append(f"  夏普比率: {ram.get('sharpe_ratio', 0):.4f} ({ram.get('sharpe_grade', '--')})")
            lines.append(f"  索提诺比率: {ram.get('sortino_ratio', 0):.4f}")
            lines.append(f"  卡玛比率: {ram.get('calmar_ratio', 0):.4f}")

        if 'var_metrics' in portfolio_metrics:
            var = portfolio_metrics['var_metrics']
            lines.append("\n【VaR风险价值】")
            if 'var_95' in var:
                lines.append(f"  VaR(95%): {var['var_95'].get('historical', 0):.2f}%")
            if 'var_99' in var:
                lines.append(f"  VaR(99%): {var['var_99'].get('historical', 0):.2f}%")

        # 集中度风险
        concentration = risk_results.get('concentration_risk', {})
        if concentration:
            lines.append("\n【集中度风险】")
            lines.append(f"  赫芬达尔指数: {concentration.get('hhi', 0):.4f}")
            lines.append(f"  等效品种数: {concentration.get('effective_n', 0):.1f}")
            lines.append(f"  最大持仓占比: {concentration.get('max_weight', 0):.2f}%")
            lines.append(f"  前5大持仓占比: {concentration.get('top5_weight', 0):.2f}%")
            lines.append(f"  集中度等级: {concentration.get('concentration_level', '--')}")

            if 'industry_distribution' in concentration:
                lines.append("\n  行业分布:")
                for ind in concentration['industry_distribution'][:5]:
                    lines.append(f"    {ind['industry']}: {ind['weight']:.1f}%")

        # 压力测试
        stress_test = risk_results.get('stress_test', {})
        if stress_test:
            lines.append("\n【压力测试】")
            for scenario, result in stress_test.items():
                lines.append(f"\n  {scenario}:")
                lines.append(f"    市场跌幅: {result.get('market_change', 0):.1f}%")
                lines.append(f"    预计损失: {result.get('total_loss', 0):,.0f}元 ({result.get('loss_pct', 0):.2f}%)")
                lines.append(f"    风险等级: {result.get('risk_level', '--')}")

        # 风险预警
        warnings = risk_results.get('risk_warnings', [])
        if warnings:
            lines.append("\n【风险预警】")
            for warning in warnings:
                lines.append(f"\n  [{warning.get('level', '--')}] {warning.get('type', '--')}")
                lines.append(f"    {warning.get('message', '')}")
        else:
            lines.append("\n【风险预警】无")

        lines.append("\n" + "=" * 60)

        return "\n".join(lines)

    def generate_risk_json(self, risk_results: Dict[str, Any]) -> str:
        """生成风险分析JSON"""
        # 将numpy类型转换为Python原生类型
        def convert(obj):
            if isinstance(obj, dict):
                return {k: convert(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert(i) for i in obj]
            elif hasattr(obj, 'item'):  # numpy类型
                return obj.item()
            return obj

        cleaned_results = convert(risk_results)
        return json.dumps(cleaned_results, ensure_ascii=False, indent=2)
