# -*- coding: utf-8 -*-
"""定投回测对比模块 — P3 进阶能力

模拟定期定额投资(DCA)在指定ETF/指数上的历史表现，
对比不同定投频率/金额/策略的收益差异。

策略:
  - 均匀定投: 每期固定金额
  - 均值回归: 低于均线多投，高于均线少投
  - 估值定投: 低于PE分位多投，高于PE分位少投
"""

import pandas as pd
import numpy as np
from dataclasses import dataclass
from typing import List, Optional


@dataclass
class DCARecord:
    """单期定投记录"""
    date: str
    price: float
    amount: float         # 当期投入金额
    shares: float         # 买入份额
    cum_shares: float      # 累计份额
    cum_cost: float        # 累计成本
    cum_value: float       # 当前市值
    cum_return: float     # 累计收益率%


@dataclass
class DCAResult:
    """定投回测结果"""
    strategy: str
    total_periods: int
    total_invest: float
    final_value: float
    total_return_pct: float
    annual_return_pct: float
    max_drawdown_pct: float
    sharpe_ratio: float
    records: list


def backtest_dca_uniform(prices: pd.Series, period_amount: float,
                          freq: str = "W") -> DCAResult:
    """均匀定投回测。

    Parameters
    ----------
    prices : pd.Series — index=date, value=close price
    period_amount : float — 每期投入金额
    freq : str — "W"=每周 "2W"=每两周 "M"=每月

    Returns DCAResult
    """
    resampled = prices.resample(freq).last().dropna()
    if len(resampled) < 2:
        return None

    records = []
    cum_shares = 0.0
    cum_cost = 0.0

    for date, price in resampled.items():
        shares = period_amount / price
        cum_shares += shares
        cum_cost += period_amount
        cum_value = cum_shares * price
        cum_ret = (cum_value / cum_cost - 1) * 100 if cum_cost > 0 else 0
        records.append(DCARecord(
            date=str(date.date()) if hasattr(date, "date") else str(date),
            price=price, amount=period_amount, shares=shares,
            cum_shares=round(cum_shares, 4), cum_cost=round(cum_cost, 2),
            cum_value=round(cum_value, 2), cum_return=round(cum_ret, 2),
        ))

    return _build_result("均匀定投", records, resampled)


def backtest_dca_valuation(prices: pd.Series, pe_series: pd.Series,
                            period_amount: float, freq: str = "ME",
                            low_mult: float = 2.0, high_mult: float = 0.5,
                            low_pctile: float = 30, high_pctile: float = 70) -> DCAResult:
    """估值定投回测：低PE分位多投，高PE分位少投。"""
    resampled_p = prices.resample(freq).last().dropna()
    resampled_pe = pe_series.resample(freq).last().dropna()
    common = resampled_p.index.intersection(resampled_pe.index)
    if len(common) < 2:
        return None

    prices_aligned = resampled_p.loc[common]
    pe_aligned = resampled_pe.loc[common]

    pe_median = pe_aligned.median()
    records = []
    cum_shares = 0.0
    cum_cost = 0.0

    for i in range(len(common)):
        date = common[i]
        price = float(prices_aligned.iloc[i])
        pe = float(pe_aligned.iloc[i])

        if pe > 0:
            # 金额调整: PE越低投入越多
            ratio = pe_median / pe  # >1 when cheap
            amount = period_amount * np.clip(ratio, 0.5, 2.0)
        else:
            amount = period_amount

        shares = amount / price
        cum_shares += shares
        cum_cost += amount
        cum_value = cum_shares * price
        cum_ret = (cum_value / cum_cost - 1) * 100 if cum_cost > 0 else 0
        records.append(DCARecord(
            date=str(date.date()) if hasattr(date, "date") else str(date),
            price=price, amount=round(amount, 2), shares=shares,
            cum_shares=round(cum_shares, 4), cum_cost=round(cum_cost, 2),
            cum_value=round(cum_value, 2), cum_return=round(cum_ret, 2),
        ))

    return _build_result("估值定投", records, prices_aligned)


def _build_result(strategy, records, price_series) -> DCAResult:
    """从记录构建DCAResult。"""
    if not records:
        return None
    total_invest = records[-1].cum_cost
    final_value = records[-1].cum_value
    total_ret = (final_value / total_invest - 1) * 100 if total_invest > 0 else 0

    # 年化收益
    n_years = len(records) / 12  # 假设月频
    annual_ret = ((1 + total_ret / 100) ** (1 / n_years) - 1) * 100 if n_years > 0 else 0

    # 最大回撤
    cum_values = [r.cum_value for r in records]
    max_dd = 0
    peak = cum_values[0]
    for v in cum_values[1:]:
        if v > peak:
            peak = v
        dd = (v / peak - 1) * 100 if peak > 0 else 0
        if dd < max_dd:
            max_dd = dd

    # 简易夏普 (假设无风险利率2.5%)
    returns = pd.Series([r.cum_return for r in records]).diff().dropna()
    sharpe = 0
    if len(returns) > 0 and returns.std() > 0:
        sharpe = round((returns.mean() - 2.5 / 12) / returns.std() * np.sqrt(12), 2)

    return DCAResult(
        strategy=strategy, total_periods=len(records),
        total_invest=round(total_invest, 2), final_value=round(final_value, 2),
        total_return_pct=round(total_ret, 2), annual_return_pct=round(annual_ret, 2),
        max_drawdown_pct=round(max_dd, 2), sharpe_ratio=sharpe,
        records=records,
    )