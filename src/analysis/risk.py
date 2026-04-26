"""
风险分析模块 - 计算投资组合风险指标
"""
import numpy as np
import pandas as pd
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


class RiskAnalyzer:
    """风险分析器"""

    def __init__(self, risk_free_rate: float = 0.025, 
                 trading_days_per_year: int = 252):
        self.risk_free_rate = risk_free_rate  # 无风险利率
        self.trading_days = trading_days_per_year

    def calculate_all(self, returns: np.ndarray, prices: np.ndarray,
                     benchmark_returns: Optional[np.ndarray] = None) -> Dict[str, Any]:
        """计算所有风险指标"""
        if len(returns) < 20:
            logger.warning("收益率数据不足，风险指标可能不准确")

        results = {
            'return_metrics': self.calculate_return_metrics(returns),
            'volatility_metrics': self.calculate_volatility_metrics(returns),
            'drawdown_metrics': self.calculate_drawdown_metrics(prices),
            'risk_adjusted_metrics': self.calculate_risk_adjusted_metrics(returns),
            'var_metrics': self.calculate_var_metrics(returns),
        }

        if benchmark_returns is not None and len(benchmark_returns) == len(returns):
            results['beta_alpha'] = self.calculate_beta_alpha(returns, benchmark_returns)

        return results

    def calculate_return_metrics(self, returns: np.ndarray) -> Dict[str, float]:
        """计算收益指标"""
        if len(returns) == 0:
            return {}

        total_return = (1 + returns).prod() - 1

        # 年化收益率
        n = len(returns)
        annual_return = (1 + total_return) ** (self.trading_days / n) - 1

        # 几何平均日收益
        geo_mean_daily = (1 + total_return) ** (1 / n) - 1

        # 算术平均日收益
        arithmetic_mean = np.mean(returns)

        return {
            'total_return': round(total_return * 100, 2),
            'annual_return': round(annual_return * 100, 2),
            'geo_mean_daily': round(geo_mean_daily * 100, 4),
            'arithmetic_mean_daily': round(arithmetic_mean * 100, 4),
            'positive_days': int(np.sum(returns > 0)),
            'negative_days': int(np.sum(returns < 0)),
            'win_rate': round(np.sum(returns > 0) / len(returns) * 100, 2)
        }

    def calculate_volatility_metrics(self, returns: np.ndarray) -> Dict[str, float]:
        """计算波动率指标"""
        if len(returns) < 2:
            return {}

        # 日波动率
        daily_volatility = np.std(returns, ddof=1)

        # 年化波动率
        annual_volatility = daily_volatility * np.sqrt(self.trading_days)

        # 下行波动率（只计算负收益的标准差）
        downside_returns = returns[returns < 0]
        downside_volatility = np.std(downside_returns, ddof=1) if len(downside_returns) > 1 else 0
        annual_downside_vol = downside_volatility * np.sqrt(self.trading_days)

        # 收益分布偏度和峰度
        skewness = pd.Series(returns).skew()
        kurtosis = pd.Series(returns).kurtosis()

        return {
            'daily_volatility': round(daily_volatility * 100, 4),
            'annual_volatility': round(annual_volatility * 100, 2),
            'downside_volatility': round(annual_downside_vol * 100, 2),
            'skewness': round(skewness, 4),
            'kurtosis': round(kurtosis, 4),
            'volatility_level': '高' if annual_volatility > 0.3 else ('中' if annual_volatility > 0.15 else '低')
        }

    def calculate_drawdown_metrics(self, prices: np.ndarray) -> Dict[str, Any]:
        """计算回撤指标"""
        if len(prices) < 2:
            return {}

        # 计算累计最大值
        cumulative_max = np.maximum.accumulate(prices)

        # 计算回撤
        drawdowns = (cumulative_max - prices) / cumulative_max

        # 最大回撤
        max_drawdown = np.max(drawdowns)
        max_dd_idx = np.argmax(drawdowns)

        # 找到最大回撤的起始点（峰值）
        peak_idx = np.argmax(prices[:max_dd_idx+1]) if max_dd_idx > 0 else 0

        # 计算回撤持续天数
        dd_duration = max_dd_idx - peak_idx

        # 计算恢复天数（从最大回撤点到创新高）
        recovery_days = 0
        if max_dd_idx < len(prices) - 1:
            peak_price = prices[peak_idx]
            for i in range(max_dd_idx + 1, len(prices)):
                if prices[i] >= peak_price:
                    recovery_days = i - max_dd_idx
                    break

        # 平均回撤
        avg_drawdown = np.mean(drawdowns[drawdowns > 0]) if np.any(drawdowns > 0) else 0

        # 当前回撤
        current_drawdown = drawdowns[-1]

        return {
            'max_drawdown': round(max_drawdown * 100, 2),
            'max_drawdown_date': max_dd_idx,  # 索引，实际使用时转换为日期
            'peak_date': peak_idx,
            'dd_duration_days': int(dd_duration),
            'recovery_days': int(recovery_days) if recovery_days > 0 else None,
            'avg_drawdown': round(avg_drawdown * 100, 2),
            'current_drawdown': round(current_drawdown * 100, 2),
            'drawdown_count': int(np.sum(np.diff(np.concatenate([[0], (drawdowns > 0).astype(int)])) == 1)),
            'warning': current_drawdown > max_drawdown * 0.8  # 当前回撤接近历史最大回撤
        }

    def calculate_risk_adjusted_metrics(self, returns: np.ndarray) -> Dict[str, float]:
        """计算风险调整收益指标"""
        if len(returns) < 2:
            return {}

        # 年化收益率和波动率
        n = len(returns)
        total_return = (1 + returns).prod() - 1
        annual_return = (1 + total_return) ** (self.trading_days / n) - 1
        annual_vol = np.std(returns, ddof=1) * np.sqrt(self.trading_days)

        # 夏普比率 = (年化收益 - 无风险利率) / 年化波动率
        sharpe_ratio = (annual_return - self.risk_free_rate) / annual_vol if annual_vol > 0 else 0

        # 索提诺比率 = (年化收益 - 无风险利率) / 下行波动率
        downside_returns = returns[returns < 0]
        downside_vol = np.std(downside_returns, ddof=1) * np.sqrt(self.trading_days) if len(downside_returns) > 1 else 0
        sortino_ratio = (annual_return - self.risk_free_rate) / downside_vol if downside_vol > 0 else 0

        # 卡玛比率 = 年化收益 / 最大回撤
        prices = np.cumprod(1 + returns)
        cumulative_max = np.maximum.accumulate(prices)
        max_dd = np.max((cumulative_max - prices) / cumulative_max)
        calmar_ratio = annual_return / max_dd if max_dd > 0 else 0

        return {
            'sharpe_ratio': round(sharpe_ratio, 4),
            'sharpe_grade': self._grade_sharpe(sharpe_ratio),
            'sortino_ratio': round(sortino_ratio, 4),
            'calmar_ratio': round(calmar_ratio, 4),
            'return_risk_ratio': round(annual_return / annual_vol, 4) if annual_vol > 0 else 0
        }

    def calculate_var_metrics(self, returns: np.ndarray, 
                             confidence_levels: List[float] = [0.95, 0.99]) -> Dict[str, Any]:
        """计算VaR (Value at Risk)"""
        if len(returns) < 30:
            return {}

        results = {}

        for conf in confidence_levels:
            # 历史模拟法
            var_hist = np.percentile(returns, (1 - conf) * 100)

            # 参数法（假设正态分布）
            mean = np.mean(returns)
            std = np.std(returns, ddof=1)
            from scipy import stats
            var_param = mean + std * stats.norm.ppf(1 - conf)

            # CVaR (条件VaR，超过VaR的平均损失)
            cvar = np.mean(returns[returns <= var_hist]) if np.any(returns <= var_hist) else var_hist

            results[f'var_{int(conf*100)}'] = {
                'historical': round(var_hist * 100, 4),
                'parametric': round(var_param * 100, 4),
                'cvar': round(cvar * 100, 4)
            }

        return results

    def calculate_beta_alpha(self, returns: np.ndarray, 
                            benchmark_returns: np.ndarray) -> Dict[str, float]:
        """计算Beta和Alpha"""
        if len(returns) != len(benchmark_returns) or len(returns) < 2:
            return {}

        # 计算协方差和方差
        covariance = np.cov(returns, benchmark_returns)[0, 1]
        benchmark_variance = np.var(benchmark_returns, ddof=1)

        # Beta = Cov(组合,基准) / Var(基准)
        beta = covariance / benchmark_variance if benchmark_variance > 0 else 1

        # 计算年化收益
        n = len(returns)
        portfolio_annual = (1 + returns).prod() ** (self.trading_days / n) - 1
        benchmark_annual = (1 + benchmark_returns).prod() ** (self.trading_days / n) - 1

        # Alpha = 实际收益 - (无风险利率 + Beta * (基准收益 - 无风险利率))
        alpha = portfolio_annual - (self.risk_free_rate + beta * (benchmark_annual - self.risk_free_rate))

        # R² (决定系数)
        correlation = np.corrcoef(returns, benchmark_returns)[0, 1]
        r_squared = correlation ** 2

        # 跟踪误差
        tracking_error = np.std(returns - benchmark_returns) * np.sqrt(self.trading_days)

        # 信息比率
        information_ratio = alpha / tracking_error if tracking_error > 0 else 0

        return {
            'beta': round(beta, 4),
            'alpha_annual': round(alpha * 100, 2),
            'r_squared': round(r_squared, 4),
            'correlation': round(correlation, 4),
            'tracking_error': round(tracking_error * 100, 2),
            'information_ratio': round(information_ratio, 4),
            'systematic_risk': '高' if beta > 1.2 else ('低' if beta < 0.8 else '适中')
        }

    def calculate_concentration_risk(self, weights: np.ndarray) -> Dict[str, float]:
        """计算集中度风险（赫芬达尔指数）"""
        if len(weights) == 0 or np.sum(weights) == 0:
            return {}

        # 归一化权重
        weights = weights / np.sum(weights)

        # 赫芬达尔指数 = sum(w_i^2)
        hhi = np.sum(weights ** 2)

        # 等效数量 = 1 / HHI
        effective_n = 1 / hhi if hhi > 0 else len(weights)

        # 最大持仓占比
        max_weight = np.max(weights)

        # 前5大持仓占比
        top5_weight = np.sum(np.sort(weights)[-5:])

        return {
            'hhi': round(hhi, 4),
            'effective_n': round(effective_n, 2),
            'max_weight': round(max_weight * 100, 2),
            'top5_weight': round(top5_weight * 100, 2),
            'concentration_level': '高' if hhi > 0.25 else ('中' if hhi > 0.15 else '低')
        }

    def calculate_correlation_matrix(self, returns_dict: Dict[str, np.ndarray]) -> pd.DataFrame:
        """计算相关系数矩阵"""
        # 构建DataFrame
        df = pd.DataFrame(returns_dict)

        # 计算相关系数
        corr_matrix = df.corr()

        return corr_matrix

    def stress_test(self, current_value: float, positions: List[Dict[str, Any]],
                   scenarios: Optional[Dict[str, float]] = None) -> Dict[str, Any]:
        """压力测试"""
        if scenarios is None:
            scenarios = {
                'market_down_10': -0.10,
                'market_down_20': -0.20,
                'market_crash_30': -0.30,
                'flash_crash': -0.15,
            }

        results = {}

        for scenario_name, market_change in scenarios.items():
            # 根据Beta调整各品种跌幅
            total_loss = 0
            position_details = []

            for pos in positions:
                beta = pos.get('beta', 1.0)
                market_value = pos.get('market_value', 0)

                # 估算跌幅 = 市场跌幅 * Beta
                estimated_drop = market_change * beta
                estimated_loss = market_value * estimated_drop

                total_loss += estimated_loss

                position_details.append({
                    'name': pos.get('name', ''),
                    'market_value': market_value,
                    'beta': beta,
                    'estimated_drop': round(estimated_drop * 100, 2),
                    'estimated_loss': round(estimated_loss, 2)
                })

            results[scenario_name] = {
                'market_change': round(market_change * 100, 1),
                'total_loss': round(total_loss, 2),
                'loss_pct': round(total_loss / current_value * 100, 2),
                'remaining_value': round(current_value + total_loss, 2),
                'positions': position_details
            }

        return results

    def _grade_sharpe(self, sharpe: float) -> str:
        """夏普比率评级"""
        if sharpe < 0:
            return '差'
        elif sharpe < 0.5:
            return '较差'
        elif sharpe < 1.0:
            return '一般'
        elif sharpe < 1.5:
            return '良好'
        elif sharpe < 2.0:
            return '优秀'
        else:
            return '卓越'
