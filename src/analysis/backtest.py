"""
策略回测模块 - 投资组合再平衡策略回测
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class RebalanceStrategy(Enum):
    """再平衡策略类型"""
    BUY_AND_HOLD = "buy_and_hold"      # 买入持有
    PERIODIC = "periodic"               # 定期再平衡
    THRESHOLD = "threshold"             # 阈值再平衡
    RISK_PARITY = "risk_parity"         # 风险平价
    MOMENTUM = "momentum"               # 动量策略


@dataclass
class BacktestResult:
    """回测结果"""
    strategy: str
    start_date: str
    end_date: str
    initial_value: float
    final_value: float
    total_return: float
    annualized_return: float
    volatility: float
    sharpe_ratio: float
    max_drawdown: float
    calmar_ratio: float
    rebalance_count: int
    turnover: float
    trades: List[Dict]
    daily_values: pd.DataFrame


class StrategyBacktester:
    """策略回测器"""

    def __init__(self, db_connection):
        self.db = db_connection

    def get_historical_data(self, codes: List[str], start_date: str, end_date: str) -> pd.DataFrame:
        """获取历史价格数据"""
        import sqlite3

        query = """
            SELECT date, code, current_price as close
            FROM portfolio_snapshots
            WHERE code IN ({}) AND date BETWEEN ? AND ?
            ORDER BY date, code
        """.format(','.join(['?' for _ in codes]))

        df = pd.read_sql_query(query, self.db, params=codes + [start_date, end_date])

        if df.empty:
            return pd.DataFrame()

        # 转换为宽格式
        df = df.pivot(index='date', columns='code', values='close')
        df.index = pd.to_datetime(df.index)

        return df

    def calculate_returns(self, prices: pd.DataFrame) -> pd.DataFrame:
        """计算收益率"""
        return prices.pct_change().dropna()

    def backtest_buy_and_hold(self, prices: pd.DataFrame, 
                              initial_weights: Dict[str, float],
                              initial_value: float = 100000) -> BacktestResult:
        """买入持有策略回测"""
        returns = self.calculate_returns(prices)

        # 计算组合收益
        weights = pd.Series(initial_weights)
        portfolio_returns = (returns * weights).sum(axis=1)

        # 计算组合价值
        cumulative_returns = (1 + portfolio_returns).cumprod()
        portfolio_values = initial_value * cumulative_returns

        # 计算指标
        total_return = (portfolio_values.iloc[-1] / initial_value - 1) * 100
        days = len(portfolio_returns)
        annualized_return = ((1 + total_return/100) ** (252/days) - 1) * 100 if days > 0 else 0
        volatility = portfolio_returns.std() * np.sqrt(252) * 100
        sharpe = annualized_return / volatility if volatility > 0 else 0

        # 计算最大回撤
        cummax = portfolio_values.cummax()
        drawdown = (portfolio_values - cummax) / cummax
        max_drawdown = drawdown.min() * 100

        calmar = annualized_return / abs(max_drawdown) if max_drawdown != 0 else 0

        return BacktestResult(
            strategy="买入持有",
            start_date=str(prices.index[0].date()),
            end_date=str(prices.index[-1].date()),
            initial_value=initial_value,
            final_value=portfolio_values.iloc[-1],
            total_return=total_return,
            annualized_return=annualized_return,
            volatility=volatility,
            sharpe_ratio=sharpe,
            max_drawdown=max_drawdown,
            calmar_ratio=calmar,
            rebalance_count=0,
            turnover=0,
            trades=[],
            daily_values=pd.DataFrame({'value': portfolio_values, 'date': portfolio_values.index})
        )

    def backtest_periodic_rebalance(self, prices: pd.DataFrame,
                                    target_weights: Dict[str, float],
                                    initial_value: float = 100000,
                                    rebalance_days: int = 20) -> BacktestResult:
        """定期再平衡策略回测"""
        returns = self.calculate_returns(prices)

        portfolio_values = [initial_value]
        current_weights = target_weights.copy()
        rebalance_dates = []
        trades = []
        turnover = 0

        for i, (date, daily_returns) in enumerate(returns.iterrows()):
            # 计算当日收益
            daily_portfolio_return = sum(daily_returns.get(code, 0) * weight 
                                        for code, weight in current_weights.items())
            new_value = portfolio_values[-1] * (1 + daily_portfolio_return)
            portfolio_values.append(new_value)

            # 更新权重（随价格变动）
            if i > 0:
                for code in current_weights:
                    if code in daily_returns.index:
                        current_weights[code] *= (1 + daily_returns[code])

                # 归一化权重
                total_weight = sum(current_weights.values())
                current_weights = {k: v/total_weight for k, v in current_weights.items()}

            # 定期再平衡
            if i > 0 and i % rebalance_days == 0:
                rebalance_dates.append(date)

                # 计算换手率
                turnover += sum(abs(current_weights.get(code, 0) - target_weights.get(code, 0)) 
                              for code in set(current_weights) | set(target_weights)) / 2

                # 记录交易
                for code in target_weights:
                    old_w = current_weights.get(code, 0)
                    new_w = target_weights[code]
                    if abs(old_w - new_w) > 0.01:
                        trades.append({
                            'date': date,
                            'code': code,
                            'action': 'rebalance',
                            'old_weight': old_w,
                            'new_weight': new_w
                        })

                current_weights = target_weights.copy()

        portfolio_values = pd.Series(portfolio_values[1:], index=returns.index)

        # 计算指标
        total_return = (portfolio_values.iloc[-1] / initial_value - 1) * 100
        days = len(returns)
        annualized_return = ((1 + total_return/100) ** (252/days) - 1) * 100 if days > 0 else 0
        volatility = returns.mean(axis=1).std() * np.sqrt(252) * 100
        sharpe = annualized_return / volatility if volatility > 0 else 0

        cummax = portfolio_values.cummax()
        drawdown = (portfolio_values - cummax) / cummax
        max_drawdown = drawdown.min() * 100
        calmar = annualized_return / abs(max_drawdown) if max_drawdown != 0 else 0

        return BacktestResult(
            strategy=f"定期再平衡({rebalance_days}天)",
            start_date=str(prices.index[0].date()),
            end_date=str(prices.index[-1].date()),
            initial_value=initial_value,
            final_value=portfolio_values.iloc[-1],
            total_return=total_return,
            annualized_return=annualized_return,
            volatility=volatility,
            sharpe_ratio=sharpe,
            max_drawdown=max_drawdown,
            calmar_ratio=calmar,
            rebalance_count=len(rebalance_dates),
            turnover=turnover,
            trades=trades,
            daily_values=pd.DataFrame({'value': portfolio_values, 'date': portfolio_values.index})
        )

    def backtest_threshold_rebalance(self, prices: pd.DataFrame,
                                     target_weights: Dict[str, float],
                                     initial_value: float = 100000,
                                     threshold: float = 0.05) -> BacktestResult:
        """阈值再平衡策略回测"""
        returns = self.calculate_returns(prices)

        portfolio_values = [initial_value]
        current_weights = target_weights.copy()
        rebalance_dates = []
        trades = []
        turnover = 0

        for i, (date, daily_returns) in enumerate(returns.iterrows()):
            # 计算当日收益
            daily_portfolio_return = sum(daily_returns.get(code, 0) * weight 
                                        for code, weight in current_weights.items())
            new_value = portfolio_values[-1] * (1 + daily_portfolio_return)
            portfolio_values.append(new_value)

            # 更新权重
            if i > 0:
                for code in current_weights:
                    if code in daily_returns.index:
                        current_weights[code] *= (1 + daily_returns[code])

                total_weight = sum(current_weights.values())
                current_weights = {k: v/total_weight for k, v in current_weights.items()}

            # 检查是否需要再平衡
            max_deviation = max(abs(current_weights.get(code, 0) - target_weights.get(code, 0)) 
                              for code in target_weights)

            if max_deviation > threshold:
                rebalance_dates.append(date)

                turnover += sum(abs(current_weights.get(code, 0) - target_weights.get(code, 0)) 
                              for code in set(current_weights) | set(target_weights)) / 2

                for code in target_weights:
                    old_w = current_weights.get(code, 0)
                    new_w = target_weights[code]
                    if abs(old_w - new_w) > 0.01:
                        trades.append({
                            'date': date,
                            'code': code,
                            'action': 'rebalance',
                            'old_weight': old_w,
                            'new_weight': new_w
                        })

                current_weights = target_weights.copy()

        portfolio_values = pd.Series(portfolio_values[1:], index=returns.index)

        # 计算指标
        total_return = (portfolio_values.iloc[-1] / initial_value - 1) * 100
        days = len(returns)
        annualized_return = ((1 + total_return/100) ** (252/days) - 1) * 100 if days > 0 else 0
        volatility = returns.mean(axis=1).std() * np.sqrt(252) * 100
        sharpe = annualized_return / volatility if volatility > 0 else 0

        cummax = portfolio_values.cummax()
        drawdown = (portfolio_values - cummax) / cummax
        max_drawdown = drawdown.min() * 100
        calmar = annualized_return / abs(max_drawdown) if max_drawdown != 0 else 0

        return BacktestResult(
            strategy=f"阈值再平衡(±{threshold*100}%)",
            start_date=str(prices.index[0].date()),
            end_date=str(prices.index[-1].date()),
            initial_value=initial_value,
            final_value=portfolio_values.iloc[-1],
            total_return=total_return,
            annualized_return=annualized_return,
            volatility=volatility,
            sharpe_ratio=sharpe,
            max_drawdown=max_drawdown,
            calmar_ratio=calmar,
            rebalance_count=len(rebalance_dates),
            turnover=turnover,
            trades=trades,
            daily_values=pd.DataFrame({'value': portfolio_values, 'date': portfolio_values.index})
        )

    def compare_strategies(self, codes: List[str], weights: Dict[str, float],
                          start_date: str, end_date: str,
                          initial_value: float = 100000) -> pd.DataFrame:
        """对比多种策略"""
        prices = self.get_historical_data(codes, start_date, end_date)

        if prices.empty:
            logger.warning("无历史数据可供回测")
            return pd.DataFrame()

        results = []

        # 买入持有
        result = self.backtest_buy_and_hold(prices, weights, initial_value)
        results.append(self._result_to_dict(result))

        # 定期再平衡（月度）
        result = self.backtest_periodic_rebalance(prices, weights, initial_value, 20)
        results.append(self._result_to_dict(result))

        # 定期再平衡（季度）
        result = self.backtest_periodic_rebalance(prices, weights, initial_value, 60)
        results.append(self._result_to_dict(result))

        # 阈值再平衡
        result = self.backtest_threshold_rebalance(prices, weights, initial_value, 0.05)
        results.append(self._result_to_dict(result))

        return pd.DataFrame(results)

    def _result_to_dict(self, result: BacktestResult) -> Dict:
        """转换结果为字典"""
        return {
            '策略': result.strategy,
            '总收益(%)': round(result.total_return, 2),
            '年化收益(%)': round(result.annualized_return, 2),
            '波动率(%)': round(result.volatility, 2),
            '夏普比率': round(result.sharpe_ratio, 2),
            '最大回撤(%)': round(result.max_drawdown, 2),
            '卡玛比率': round(result.calmar_ratio, 2),
            '再平衡次数': result.rebalance_count,
            '换手率': round(result.turnover, 2),
            '期末价值': round(result.final_value, 2)
        }
