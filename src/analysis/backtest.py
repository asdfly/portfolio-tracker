"""
策略回测模块 - 投资组合再平衡策略回测

P1-C 现实化改造:
  - 交易成本: 每次再平衡按换手额扣除 双边(买+卖)佣金与滑点 (默认 佣金0.03% + 滑点0.05% 单边)
  - T+1 约束: 再平衡决策在日 i 收盘后做出, 权重与成本在日 i+1 开盘执行 (A股T+1)
  - 参数化: 默认开启真实成本; 设 commission_rate=slippage_rate=0 且 tplus1=False 可回到无成本理想回测
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Callable
from dataclasses import dataclass
from enum import Enum
import logging

from config.settings import RISK_FREE_RATE, TRADING_DAYS_PER_YEAR

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
    cost_paid: float = 0.0             # P1-C: 累计交易成本（含佣金+滑点）
    trades: List[Dict] = None
    daily_values: pd.DataFrame = None

    def __post_init__(self):
        if self.trades is None:
            self.trades = []
        if self.daily_values is None:
            self.daily_values = pd.DataFrame()


class StrategyBacktester:
    """策略回测器

    P1-C 交易成本模型:
      cost_rate = commission_rate + slippage_rate (单边)
      单次再平衡双边成本 = 2 * cost_rate * 换手占比 * 组合市值
    """

    def __init__(self, db_connection,
                 commission_rate: float = 0.0003,   # 单边佣金率 0.03%
                 slippage_rate: float = 0.0005,     # 单边滑点率 0.05%
                 tplus1: bool = True):              # A股T+1: 再平衡次日执行
        self.db = db_connection
        self.commission_rate = commission_rate
        self.slippage_rate = slippage_rate
        self.cost_rate = commission_rate + slippage_rate
        self.tplus1 = tplus1

    def get_historical_data(self, codes: List[str], start_date: str, end_date: str) -> pd.DataFrame:
        """获取历史价格数据"""
        query = """
            SELECT date, code, current_price as close
            FROM portfolio_snapshots
            WHERE code IN ({}) AND date BETWEEN ? AND ?
            ORDER BY date, code
        """.format(','.join(['?' for _ in codes]))

        df = pd.read_sql_query(query, self.db, params=codes + [start_date, end_date])

        if df.empty:
            return pd.DataFrame()

        df = df.pivot(index='date', columns='code', values='close')
        df.index = pd.to_datetime(df.index)
        return df

    def calculate_returns(self, prices: pd.DataFrame) -> pd.DataFrame:
        """计算收益率"""
        return prices.pct_change().dropna()

    # ------------------------------------------------------------------
    # P1-C 统一权重再平衡回测内核（periodic/threshold/momentum/mean_reversion 共用）
    # decide_fn(i, date, daily_returns, current_weights) -> Optional[(new_weights, turnover_inc)]
    #   - 返回 None 表示本日不调仓
    #   - 返回 (new_weights, turnover_inc) 表示决定调仓, 权重在 T+1 生效
    # ------------------------------------------------------------------
    def _run_rebalance_backtest(self, prices: pd.DataFrame,
                                initial_weights: Dict[str, float],
                                decide_fn: Callable,
                                initial_value: float,
                                strategy_name: str) -> BacktestResult:
        returns = self.calculate_returns(prices)
        codes = list(prices.columns)
        n = len(codes)
        if n == 0:
            return self._empty_result(strategy_name, prices, initial_value)

        portfolio_values = [initial_value]
        current_weights = dict(initial_weights)
        pending = None            # (new_weights, turnover_inc) 挂起待 T+1 执行
        rebalance_dates = []
        trades = []
        turnover = 0.0
        cost_paid = 0.0

        for i, (date, daily_returns) in enumerate(returns.iterrows()):
            value = portfolio_values[-1]

            # T+1: 执行上一日挂起的再平衡（扣成本 + 生效新权重）
            if pending is not None:
                new_weights, tinc = pending
                cost = 2 * self.cost_rate * tinc * value
                value = value - cost
                cost_paid += cost
                current_weights = dict(new_weights)
                pending = None

            # 当日收益（用已生效权重）
            daily_ret = sum(daily_returns.get(c, 0) * current_weights.get(c, 0) for c in codes)
            value = value * (1 + daily_ret)
            portfolio_values.append(value)

            # 权重随价格漂移
            if i > 0:
                for c in codes:
                    if c in daily_returns.index and daily_returns[c] != -1.0:
                        current_weights[c] *= (1 + daily_returns[c])
                tw = sum(current_weights.values())
                if tw > 0:
                    current_weights = {k: v / tw for k, v in current_weights.items()}

            # 决策是否再平衡
            decision = decide_fn(i, date, daily_returns, current_weights)
            if decision is not None:
                new_weights, tinc = decision
                turnover += tinc
                for c in codes:
                    if abs(current_weights.get(c, 0) - new_weights.get(c, 0)) > 0.01:
                        trades.append({'date': date, 'code': c, 'action': strategy_name,
                                       'old_weight': current_weights.get(c, 0),
                                       'new_weight': new_weights[c]})
                rebalance_dates.append(date)
                if not self.tplus1:
                    # 无T+1: 当日立即执行
                    cost = 2 * self.cost_rate * tinc * value
                    value = value - cost
                    cost_paid += cost
                    current_weights = dict(new_weights)
                    portfolio_values[-1] = value
                else:
                    pending = (dict(new_weights), tinc)

        portfolio_values = pd.Series(portfolio_values[1:], index=returns.index)
        metrics = self._compute_metrics(portfolio_values, returns, initial_value)
        return BacktestResult(
            strategy=strategy_name,
            start_date=str(prices.index[0].date()),
            end_date=str(prices.index[-1].date()),
            initial_value=initial_value,
            final_value=portfolio_values.iloc[-1],
            cost_paid=round(cost_paid, 2),
            rebalance_count=len(rebalance_dates),
            turnover=round(turnover, 4),
            trades=trades,
            daily_values=pd.DataFrame({'value': portfolio_values, 'date': portfolio_values.index}),
            **metrics)

    @staticmethod
    def _turnover_inc(current_weights, new_weights):
        keys = set(current_weights) | set(new_weights)
        return sum(abs(current_weights.get(c, 0) - new_weights.get(c, 0)) for c in keys) / 2

    def backtest_buy_and_hold(self, prices: pd.DataFrame,
                              initial_weights: Dict[str, float],
                              initial_value: float = 100000) -> BacktestResult:
        """买入持有策略回测（P1-C: 计入建仓单边成本）"""
        returns = self.calculate_returns(prices)
        weights = pd.Series(initial_weights)

        # 建仓成本（单边: 买入佣金+滑点）
        entry_cost = initial_value * self.cost_rate
        start_value = initial_value - entry_cost

        portfolio_returns = (returns * weights).sum(axis=1)
        cumulative_returns = (1 + portfolio_returns).cumprod()
        portfolio_values = start_value * cumulative_returns

        total_return = (portfolio_values.iloc[-1] / initial_value - 1) * 100
        days = len(portfolio_returns)
        annualized_return = ((1 + total_return / 100) ** (252 / days) - 1) * 100 if days > 0 else 0
        volatility = portfolio_returns.std() * np.sqrt(252) * 100
        sharpe = annualized_return / volatility if volatility > 0 else 0

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
            cost_paid=round(entry_cost, 2),
            trades=[],
            daily_values=pd.DataFrame({'value': portfolio_values, 'date': portfolio_values.index})
        )

    def backtest_periodic_rebalance(self, prices: pd.DataFrame,
                                    target_weights: Dict[str, float],
                                    initial_value: float = 100000,
                                    rebalance_days: int = 20) -> BacktestResult:
        """定期再平衡策略回测"""
        def decide(i, date, daily_returns, current_weights):
            if i > 0 and i % rebalance_days == 0:
                tinc = self._turnover_inc(current_weights, target_weights)
                return (dict(target_weights), tinc)
            return None
        return self._run_rebalance_backtest(prices, target_weights, decide, initial_value,
                                            f"定期再平衡({rebalance_days}天)")

    def backtest_threshold_rebalance(self, prices: pd.DataFrame,
                                     target_weights: Dict[str, float],
                                     initial_value: float = 100000,
                                     threshold: float = 0.05) -> BacktestResult:
        """阈值再平衡策略回测"""
        def decide(i, date, daily_returns, current_weights):
            max_dev = max(abs(current_weights.get(c, 0) - target_weights.get(c, 0))
                          for c in target_weights)
            if max_dev > threshold:
                tinc = self._turnover_inc(current_weights, target_weights)
                return (dict(target_weights), tinc)
            return None
        return self._run_rebalance_backtest(prices, target_weights, decide, initial_value,
                                            f"阈值再平衡(±{threshold*100}%)")

    def backtest_momentum(self, prices: pd.DataFrame,
                          lookback: int = 20,
                          top_n: int = 3,
                          initial_value: float = 100000,
                          initial_weights: Optional[Dict[str, float]] = None) -> BacktestResult:
        """动量策略回测 - 买入近期涨幅最大的top_n只，定期调仓"""
        returns = self.calculate_returns(prices)
        codes = list(prices.columns)
        if len(codes) == 0:
            return self._empty_result(f"动量(top{top_n},{lookback}d)", prices, initial_value)
        if initial_weights is None:
            initial_weights = {c: 1.0 / len(codes) for c in codes}

        def decide(i, date, daily_returns, current_weights):
            if i > 0 and i % lookback == 0 and i >= lookback:
                past_returns = {}
                for c in codes:
                    if c in returns.columns:
                        past_ret = returns[c].iloc[max(0, i - lookback):i]
                        past_returns[c] = (1 + past_ret).prod() - 1 if len(past_ret) > 0 else 0
                    else:
                        past_returns[c] = 0
                ranked = sorted(past_returns.items(), key=lambda x: x[1], reverse=True)
                selected = [x[0] for x in ranked[:top_n]]
                new_weights = {c: (1.0 / top_n if c in selected else 0.0) for c in codes}
                tinc = self._turnover_inc(current_weights, new_weights)
                return (new_weights, tinc)
            return None
        return self._run_rebalance_backtest(prices, initial_weights, decide, initial_value,
                                            f"动量(top{top_n},{lookback}d)")

    def backtest_mean_reversion(self, prices: pd.DataFrame,
                                lookback: int = 20,
                                z_threshold: float = 1.0,
                                initial_value: float = 100000,
                                initial_weights: Optional[Dict[str, float]] = None) -> BacktestResult:
        """均值回归策略回测 - 低配高估资产、高配低估资产"""
        returns = self.calculate_returns(prices)
        codes = list(prices.columns)
        if len(codes) == 0:
            return self._empty_result(f"均值回归({lookback}d)", prices, initial_value)
        if initial_weights is None:
            initial_weights = {c: 1.0 / len(codes) for c in codes}

        def decide(i, date, daily_returns, current_weights):
            if i > 0 and i % lookback == 0 and i >= lookback:
                z_scores = {}
                for c in codes:
                    if c in returns.columns:
                        window = returns[c].iloc[max(0, i - lookback):i]
                        if len(window) >= 5:
                            m, s = window.mean(), window.std()
                            z_scores[c] = (window.iloc[-1] - m) / s if s > 0 else 0
                        else:
                            z_scores[c] = 0
                total_inv_z = sum(max(0.1, 1.0 - z) for z in z_scores.values())
                new_weights = {c: max(0.05, (1.0 - z_scores.get(c, 0))) / total_inv_z for c in codes}
                tinc = self._turnover_inc(current_weights, new_weights)
                return (new_weights, tinc)
            return None
        return self._run_rebalance_backtest(prices, initial_weights, decide, initial_value,
                                            f"均值回归({lookback}d,z={z_threshold})")

    def _empty_result(self, name, prices, initial_value):
        return BacktestResult(
            strategy=name, start_date="N/A", end_date="N/A",
            initial_value=initial_value, final_value=initial_value,
            total_return=0, annualized_return=0, volatility=0,
            sharpe_ratio=0, max_drawdown=0, calmar_ratio=0,
            rebalance_count=0, turnover=0, cost_paid=0, trades=[], daily_values=pd.DataFrame())

    def _compute_metrics(self, portfolio_values, returns, initial_value):
        total_return = (portfolio_values.iloc[-1] / initial_value - 1) * 100
        days = len(portfolio_values)
        annualized_return = ((1 + total_return / 100) ** (TRADING_DAYS_PER_YEAR / days) - 1) * 100 if days > 0 else 0
        volatility = returns.mean(axis=1).std(ddof=1) * np.sqrt(TRADING_DAYS_PER_YEAR) * 100
        # P0-1 统一来源: RISK_FREE_RATE=0.025(小数) -> 本函数口径为百分比(×100=2.5%)
        risk_free_annual = RISK_FREE_RATE * 100
        sharpe = (annualized_return - risk_free_annual) / volatility if volatility > 0 else 0
        cummax = portfolio_values.cummax()
        drawdown = (portfolio_values - cummax) / cummax
        max_drawdown = drawdown.min() * 100
        calmar = annualized_return / abs(max_drawdown) if max_drawdown != 0 else 0
        return {'total_return': total_return, 'annualized_return': annualized_return,
                'volatility': volatility, 'sharpe_ratio': sharpe,
                'max_drawdown': max_drawdown, 'calmar_ratio': calmar}

    def compare_strategies(self, codes: List[str], weights: Dict[str, float],
                          start_date: str, end_date: str,
                          initial_value: float = 100000) -> pd.DataFrame:
        """对比多种策略"""
        prices = self.get_historical_data(codes, start_date, end_date)

        if prices.empty:
            logger.warning("无历史数据可供回测")
            return pd.DataFrame()

        results = []
        result = self.backtest_buy_and_hold(prices, weights, initial_value)
        results.append(self._result_to_dict(result))
        result = self.backtest_periodic_rebalance(prices, weights, initial_value, 20)
        results.append(self._result_to_dict(result))
        result = self.backtest_periodic_rebalance(prices, weights, initial_value, 60)
        results.append(self._result_to_dict(result))
        result = self.backtest_threshold_rebalance(prices, weights, initial_value, 0.05)
        results.append(self._result_to_dict(result))
        result = self.backtest_momentum(prices, lookback=20, top_n=min(3, len(codes)))
        results.append(self._result_to_dict(result))
        result = self.backtest_mean_reversion(prices, lookback=20)
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
            '交易成本': round(result.cost_paid, 2),
            '期末价值': round(result.final_value, 2)
        }
