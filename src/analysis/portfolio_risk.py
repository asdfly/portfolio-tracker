"""
组合风险分析器 - 整合风险指标计算
"""
import numpy as np
import pandas as pd
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import logging

from .risk import RiskAnalyzer
from src.utils.database import DatabaseManager

logger = logging.getLogger(__name__)


class PortfolioRiskAnalyzer:
    """组合风险分析器"""

    def __init__(self, risk_free_rate: float = 0.025):
        self.risk_analyzer = RiskAnalyzer(risk_free_rate)
        self.db = DatabaseManager()

    def analyze_portfolio_risk(self, positions: List[Dict[str, Any]],
                               index_quotes: Dict[str, Dict[str, Any]],
                               days: int = 60) -> Dict[str, Any]:
        """分析组合整体风险"""
        logger.info("开始组合风险分析...")

        results = {
            'portfolio_metrics': self._calculate_portfolio_metrics(positions, days),
            'concentration_risk': self._analyze_concentration(positions),
            'correlation_analysis': self._analyze_correlations(positions, days),
            'stress_test': self._run_stress_test(positions),
            'risk_warnings': []
        }

        # 生成风险预警
        results['risk_warnings'] = self._generate_warnings(results)

        logger.info("组合风险分析完成")
        return results

    def _calculate_portfolio_metrics(self, positions: List[Dict[str, Any]], 
                                     days: int = 60) -> Dict[str, Any]:
        """计算组合风险指标"""
        # 从数据库获取历史净值数据
        history = self._get_portfolio_history(days)

        if len(history) < 20:
            logger.warning("历史数据不足，无法计算完整风险指标")
            return {'error': '历史数据不足'}

        # 计算日收益率
        values = np.array([h['total_value'] for h in history])
        returns = np.diff(values) / values[:-1]

        # 获取沪深300作为基准
        benchmark_returns = self._get_benchmark_returns('sh000300', days)

        # 计算风险指标
        metrics = self.risk_analyzer.calculate_all(returns, values, benchmark_returns)

        # 添加组合特定信息
        metrics['data_period'] = len(history)
        metrics['start_date'] = history[0]['date']
        metrics['end_date'] = history[-1]['date']

        return metrics

    def _analyze_concentration(self, positions: List[Dict[str, Any]]) -> Dict[str, Any]:
        """分析集中度风险"""
        total_value = sum(p['market_value'] for p in positions)

        if total_value == 0:
            return {}

        # 计算各品种权重
        weights = np.array([p['market_value'] / total_value for p in positions])

        # 计算赫芬达尔指数
        concentration = self.risk_analyzer.calculate_concentration_risk(weights)

        # 添加品种明细
        position_weights = []
        for pos in positions:
            position_weights.append({
                'code': pos['code'],
                'name': pos['name'],
                'weight': round(pos['market_value'] / total_value * 100, 2),
                'market_value': pos['market_value']
            })

        # 按权重排序
        position_weights.sort(key=lambda x: x['weight'], reverse=True)

        concentration['positions'] = position_weights

        # 行业集中度
        industry_weights = {}
        industry_map = {
            '512010': '医药', '159992': '医药', '515120': '医药',
            '515010': '券商',
            '159267': '军工', '512810': '军工',
            '159796': '新能源', '561910': '新能源', '516160': '新能源',
            '159819': 'AI', '159770': 'AI',
            '159732': '科技',
            '510300': '宽基', '159300': '宽基', '512100': '宽基', 
            '510500': '宽基', '159949': '宽基', '588000': '宽基',
            '563020': '红利', '159220': '红利',
            '159650': '债券', '511520': '债券', '511380': '债券'
        }

        for pos in positions:
            ind = industry_map.get(pos['code'], '其他')
            industry_weights[ind] = industry_weights.get(ind, 0) + pos['market_value']

        # 计算行业HHI
        ind_values = np.array(list(industry_weights.values()))
        ind_weights = ind_values / np.sum(ind_values)
        ind_hhi = np.sum(ind_weights ** 2)

        concentration['industry_hhi'] = round(ind_hhi, 4)
        concentration['industry_distribution'] = [
            {'industry': k, 'value': v, 'weight': round(v/total_value*100, 2)}
            for k, v in sorted(industry_weights.items(), key=lambda x: x[1], reverse=True)
        ]

        return concentration

    def _analyze_correlations(self, positions: List[Dict[str, Any]], 
                              days: int = 60) -> Dict[str, Any]:
        """分析品种间相关性"""
        # 获取各品种历史收益率
        returns_dict = {}

        for pos in positions:
            code = pos['code']
            history = self.db.get_price_history(code, days)

            if len(history) >= 20:
                values = np.array([h['current_price'] for h in history])
                returns = np.diff(values) / values[:-1]
                returns_dict[f"{code}_{pos['name'][:6]}"] = returns

        if len(returns_dict) < 2:
            return {'error': '数据不足'}

        # 计算相关系数矩阵
        corr_matrix = self.risk_analyzer.calculate_correlation_matrix(returns_dict)

        # 找出高相关性品种对
        high_corr_pairs = []
        for i in range(len(corr_matrix.columns)):
            for j in range(i+1, len(corr_matrix.columns)):
                corr = corr_matrix.iloc[i, j]
                if abs(corr) > 0.8:  # 高相关性阈值
                    high_corr_pairs.append({
                        'asset1': corr_matrix.columns[i],
                        'asset2': corr_matrix.columns[j],
                        'correlation': round(corr, 4),
                        'type': '强正相关' if corr > 0 else '强负相关'
                    })

        # 计算平均相关性
        avg_correlation = np.mean([corr_matrix.iloc[i, j] 
                                   for i in range(len(corr_matrix)) 
                                   for j in range(i+1, len(corr_matrix))])

        return {
            'correlation_matrix': corr_matrix.round(4).to_dict(),
            'high_correlation_pairs': high_corr_pairs,
            'average_correlation': round(avg_correlation, 4),
            'diversification_score': round(1 - abs(avg_correlation), 4)
        }

    def _run_stress_test(self, positions: List[Dict[str, Any]]) -> Dict[str, Any]:
        """运行压力测试"""
        total_value = sum(p['market_value'] for p in positions)

        stress_results = self.risk_analyzer.stress_test(total_value, positions)

        # 添加风险评估
        for scenario, result in stress_results.items():
            loss_pct = result['loss_pct']
            if loss_pct > 20:
                result['risk_level'] = '极高'
            elif loss_pct > 15:
                result['risk_level'] = '高'
            elif loss_pct > 10:
                result['risk_level'] = '中高'
            elif loss_pct > 5:
                result['risk_level'] = '中等'
            else:
                result['risk_level'] = '低'

        return stress_results

    def _generate_warnings(self, risk_results: Dict[str, Any]) -> List[Dict[str, str]]:
        """生成风险预警"""
        warnings = []

        # 1. 集中度预警
        concentration = risk_results.get('concentration_risk', {})
        if concentration.get('max_weight', 0) > 25:
            warnings.append({
                'level': '高',
                'type': '集中度风险',
                'message': f"单一品种占比超过25% ({concentration.get('max_weight')}%)，建议分散投资"
            })

        if concentration.get('hhi', 0) > 0.25:
            warnings.append({
                'level': '中',
                'type': '集中度风险',
                'message': f"赫芬达尔指数较高({concentration.get('hhi')})，组合不够分散"
            })

        # 2. 回撤预警
        metrics = risk_results.get('portfolio_metrics', {})
        drawdown = metrics.get('drawdown_metrics', {})
        if drawdown.get('current_drawdown', 0) > 10:
            warnings.append({
                'level': '中',
                'type': '回撤风险',
                'message': f"当前回撤{drawdown.get('current_drawdown')}%，接近历史最大回撤"
            })

        # 3. 波动率预警
        volatility = metrics.get('volatility_metrics', {})
        if volatility.get('annual_volatility', 0) > 30:
            warnings.append({
                'level': '高',
                'type': '波动率风险',
                'message': f"年化波动率{volatility.get('annual_volatility')}%，属于高波动组合"
            })

        # 4. 相关性预警
        correlation = risk_results.get('correlation_analysis', {})
        high_corr = correlation.get('high_correlation_pairs', [])
        if len(high_corr) > 0:
            warnings.append({
                'level': '中',
                'type': '相关性风险',
                'message': f"发现{len(high_corr)}对高相关性品种，分散化效果有限"
            })

        # 5. 夏普比率预警
        risk_adj = metrics.get('risk_adjusted_metrics', {})
        if risk_adj.get('sharpe_ratio', 0) < 0.5:
            warnings.append({
                'level': '中',
                'type': '收益风险比',
                'message': f"夏普比率{risk_adj.get('sharpe_ratio')}较低，风险补偿不足"
            })

        return warnings

    def _get_portfolio_history(self, days: int) -> List[Dict[str, Any]]:
        """获取组合历史数据"""
        return self.db.get_portfolio_history(days)

    def _get_benchmark_returns(self, index_code: str, days: int) -> np.ndarray:
        """获取基准指数收益率"""
        # 从数据库获取指数历史数据
        # 这里简化处理，实际应从数据库查询
        return np.array([])  # 返回空数组表示无基准数据
