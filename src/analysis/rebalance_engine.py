"""再平衡引擎（P2-B 产品化）

基于真实持仓快照（portfolio_snapshots）计算当前权重，生成**可执行**的再平衡方案：
- 支持策略：
    * threshold    阈值偏离（默认目标=等权，偏离 > 阈值才调仓）
    * periodic     定期（距上次再平衡达到 period_days 个交易日才调仓）
    * equal_weight 直接以等权为目标
    * custom       给定目标权重向量
- 输出 RebalancePlan：具体交易（买卖金额/手数）、换手率、预估交易成本、T+1 执行日
- T+1 执行日 = 本地日历 next_trading_day(as_of_date)（A股 T+1：收盘决策、次交易日开盘执行）
- 成本模型与 backtest 一致：单边 cost_rate = 佣金 + 滑点；单次双边成本 = 2 * cost_rate * 换手额

与既有原型的衔接：models.RebalanceTrade / RebalanceSuggestion 是结构化返回值，
本模块是其"产品化"落地（前视、可执行、带 T+1 执行日与成本预估）。
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd
import sqlite3

from config.settings import SMART_ANALYSIS_CONFIG
from src.models import RebalanceTrade
from src.utils.trading_calendar import (
    next_trading_day,
    last_trading_day_on_or_before,
    get_trading_days,
    is_trading_day,
)

logger = logging.getLogger(__name__)


@dataclass
class RebalancePlan:
    """一次再平衡方案（前视、可执行）"""

    as_of_date: str
    strategy: str
    action_needed: bool
    reason: str
    current_weights: Dict[str, float] = field(default_factory=dict)
    target_weights: Dict[str, float] = field(default_factory=dict)
    trades: List[RebalanceTrade] = field(default_factory=list)
    turnover: float = 0.0                 # 换手率 = Σ|Δw|/2
    estimated_cost: float = 0.0           # 预估交易成本（双边）
    execution_date: str = ""              # T+1 执行日（交易日）
    total_value: float = 0.0

    def to_dict(self) -> dict:
        return {
            "as_of_date": self.as_of_date,
            "strategy": self.strategy,
            "action_needed": self.action_needed,
            "reason": self.reason,
            "turnover": round(self.turnover, 4),
            "estimated_cost": round(self.estimated_cost, 2),
            "execution_date": self.execution_date,
            "total_value": round(self.total_value, 2),
            "current_weights": {k: round(v, 4) for k, v in self.current_weights.items()},
            "target_weights": {k: round(v, 4) for k, v in self.target_weights.items()},
            "trades": [t.__dict__ for t in self.trades],
        }


class RebalanceEngine:
    """前视再平衡引擎：从真实持仓生成可执行的调仓方案。"""

    def __init__(self, db_connection: sqlite3.Connection,
                 commission_rate: float = 0.0003,   # 单边佣金 0.03%
                 slippage_rate: float = 0.0005):    # 单边滑点 0.05%
        self.db = db_connection
        self.commission_rate = commission_rate
        self.slippage_rate = slippage_rate
        self.cost_rate = commission_rate + slippage_rate  # 单边成本率

    # ------------------------------------------------------------------
    # 当前持仓 / 权重
    # ------------------------------------------------------------------
    def get_current_holdings(self, as_of_date: str):
        """取 as_of_date 或之前最近交易日的持仓快照。
        返回 (code->market_value, total_value, code->name, code->current_price)。"""
        d = last_trading_day_on_or_before(as_of_date)
        df = pd.read_sql_query(
            "SELECT code, name, market_value, current_price "
            "FROM portfolio_snapshots WHERE date=?",
            self.db, params=[str(d)],
        )
        if df.empty:
            # 退一步：取 <= d 最近一个有快照的交易日
            row = pd.read_sql_query(
                "SELECT DISTINCT date FROM portfolio_snapshots WHERE date<=? "
                "ORDER BY date DESC LIMIT 1",
                self.db, params=[str(d)],
            )
            if row.empty:
                return {}, 0.0, {}, {}
            d2 = row.iloc[0, 0]
            df = pd.read_sql_query(
                "SELECT code, name, market_value, current_price "
                "FROM portfolio_snapshots WHERE date=?",
                self.db, params=[str(d2)],
            )
        df = df.dropna(subset=["market_value"])
        if df.empty:
            return {}, 0.0, {}, {}
        total = float(df["market_value"].sum())
        mv = {r.code: float(r.market_value) for r in df.itertuples()}
        names = {r.code: (r.name or "") for r in df.itertuples()}
        prices = {
            r.code: (float(r.current_price) if pd.notna(r.current_price) else None)
            for r in df.itertuples()
        }
        return mv, total, names, prices

    def get_current_weights(self, as_of_date: str) -> Tuple[Dict[str, float], float, Dict[str, str], Dict[str, float]]:
        """返回 (code->weight, total_value, code->name, code->price)。"""
        mv, total, names, prices = self.get_current_holdings(as_of_date)
        weights = {c: (m / total if total > 0 else 0.0) for c, m in mv.items()}
        return weights, total, names, prices

    # ------------------------------------------------------------------
    # 核心：给定目标权重，按阈值决定是否生成交易
    # ------------------------------------------------------------------
    def propose(self, as_of_date: str,
                target_weights: Dict[str, float],
                threshold: Optional[float] = None,
                strategy: str = "custom") -> RebalancePlan:
        """若当前与目标的最大权重偏离 > 阈值，生成调仓方案；否则返回 action_needed=False。"""
        if threshold is None:
            threshold = SMART_ANALYSIS_CONFIG.get("rebalance_threshold", 0.05)
        weights, total, names, prices = self.get_current_weights(as_of_date)
        if not weights:
            return RebalancePlan(
                as_of_date=as_of_date, strategy=strategy, action_needed=False,
                reason="无持仓快照数据，无法生成方案", total_value=0.0,
                execution_date=str(next_trading_day(as_of_date)),
            )
        all_codes = set(weights) | set(target_weights)
        max_dev = max(abs(weights.get(c, 0.0) - target_weights.get(c, 0.0)) for c in all_codes)
        exec_date = str(next_trading_day(as_of_date))
        if max_dev <= threshold:
            return RebalancePlan(
                as_of_date=as_of_date, strategy=strategy, action_needed=False,
                reason=f"最大偏离 {max_dev*100:.1f}% ≤ 阈值 {threshold*100:.1f}%，无需再平衡",
                current_weights=weights, target_weights=target_weights,
                execution_date=exec_date, total_value=total,
            )
        # 生成逐标的交易
        trades: List[RebalanceTrade] = []
        turnover = 0.0
        for c in all_codes:
            cw = weights.get(c, 0.0)
            tw = target_weights.get(c, 0.0)
            diff = cw - tw                       # 正=超配，需卖出
            if abs(diff) < 1e-9:
                continue
            trade_value = abs(diff) * total
            price = prices.get(c)
            shares = int(trade_value / price) if (price and price > 0) else 0
            trades.append(RebalanceTrade(
                code=c, name=names.get(c, ""),
                current_weight=round(cw, 4), target_weight=round(tw, 4),
                diff=round(diff, 4), trade_value=round(trade_value, 2),
                shares=shares, direction="卖出" if diff > 0 else "买入",
                price=round(price, 4) if price else 0.0,
            ))
            turnover += abs(diff)
        turnover = turnover / 2.0               # 换手率 = 单边变动之和 / 2
        est_cost = 2 * self.cost_rate * turnover * total
        return RebalancePlan(
            as_of_date=as_of_date, strategy=strategy, action_needed=True,
            reason=f"最大偏离 {max_dev*100:.1f}% > 阈值 {threshold*100:.1f}%，建议再平衡",
            current_weights=weights, target_weights=target_weights,
            trades=trades, turnover=round(turnover, 4),
            estimated_cost=round(est_cost, 2), execution_date=exec_date,
            total_value=total,
        )

    # ------------------------------------------------------------------
    # 策略封装
    # ------------------------------------------------------------------
    def _equal_weight_target(self, weights: Dict[str, float]) -> Dict[str, float]:
        n = len(weights)
        return {c: 1.0 / n for c in weights}

    def propose_equal_weight(self, as_of_date: str,
                             threshold: Optional[float] = None) -> RebalancePlan:
        weights, _, _, _ = self.get_current_weights(as_of_date)
        if not weights:
            return RebalancePlan(as_of_date=as_of_date, strategy="equal_weight",
                                 action_needed=False, reason="无持仓", total_value=0.0,
                                 execution_date=str(next_trading_day(as_of_date)))
        target = self._equal_weight_target(weights)
        return self.propose(as_of_date, target, threshold=threshold, strategy="equal_weight")

    def propose_threshold(self, as_of_date: str,
                          target_weights: Optional[Dict[str, float]] = None,
                          threshold: Optional[float] = None) -> RebalancePlan:
        if target_weights is None:
            weights, _, _, _ = self.get_current_weights(as_of_date)
            if not weights:
                return RebalancePlan(as_of_date=as_of_date, strategy="threshold",
                                     action_needed=False, reason="无持仓", total_value=0.0,
                                     execution_date=str(next_trading_day(as_of_date)))
            target_weights = self._equal_weight_target(weights)
        return self.propose(as_of_date, target_weights, threshold=threshold, strategy="threshold")

    def propose_periodic(self, as_of_date: str,
                         period_days: int = 20,
                         last_rebalance_date: Optional[str] = None,
                         target_weights: Optional[Dict[str, float]] = None) -> RebalancePlan:
        """定期再平衡：距上次再平衡的交易日数 >= period_days 才执行。"""
        weights, total, _, _ = self.get_current_weights(as_of_date)
        if not weights:
            return RebalancePlan(as_of_date=as_of_date, strategy="periodic",
                                 action_needed=False, reason="无持仓", total_value=0.0,
                                 execution_date=str(next_trading_day(as_of_date)))
        if target_weights is None:
            target_weights = self._equal_weight_target(weights)
        exec_date = str(next_trading_day(as_of_date))
        if last_rebalance_date is None:
            # 无历史记录 → 视为需要再平衡
            return self.propose(as_of_date, target_weights, threshold=0.0, strategy="periodic")
        days = get_trading_days(last_rebalance_date, as_of_date)
        elapsed = max(len(days) - 1, 0)        # 间隔交易日数
        if elapsed < period_days:
            return RebalancePlan(
                as_of_date=as_of_date, strategy="periodic", action_needed=False,
                reason=f"距上次再平衡 {elapsed} 交易日 < 周期 {period_days}，未到调仓日",
                current_weights=weights, target_weights=target_weights,
                execution_date=exec_date, total_value=total,
            )
        return self.propose(as_of_date, target_weights, threshold=0.0, strategy="periodic")

    # ------------------------------------------------------------------
    # 分类 + 分层再平衡（不要完全平均）
    # ------------------------------------------------------------------
    def classify_sector(self, code: str, name: str = "") -> str:
        """返回标的 sector 分类：优先用 ETF_CATEGORIES 配置，未知按名称启发式兜底。"""
        from config.settings import ETF_CATEGORIES
        if code in ETF_CATEGORIES:
            return ETF_CATEGORIES[code].get("sector", "其他")
        n = (name or "").lower()
        rules = [
            ("沪深300", "宽基"), ("中证500", "宽基"), ("中证1000", "宽基"),
            ("创业板", "宽基"), ("科创", "宽基"), ("上证50", "宽基"),
            ("医药", "医药"), ("医疗", "医药"), ("创新药", "医药"), ("药", "医药"),
            ("证券", "金融"), ("银行", "金融"), ("保险", "金融"), ("金融", "金融"),
            ("军工", "军工"), ("国防", "军工"), ("航天", "军工"),
            ("新能源", "新能源"), ("电池", "新能源"), ("光伏", "新能源"), ("碳中和", "新能源"),
            ("人工智能", "科技"), ("ai", "科技"), ("科技", "科技"), ("半导体", "科技"), ("芯片", "科技"),
            ("红利", "红利"), ("低波", "红利"),
            ("债", "债券"), ("转债", "债券"),
            ("货币", "现金管理"), ("现金", "现金管理"),
            ("混合", "混合/灵活配置"),
        ]
        for kw, sec in rules:
            if kw.lower() in n:
                return sec
        return "其他"

    def propose_layered(self, as_of_date: str,
                        threshold: Optional[float] = None,
                        shrinkage: Optional[float] = None) -> RebalancePlan:
        """分层再平衡：类别基准权重 + 类别内按市值分配（类别内也不均），阈值过滤小额偏离。

        不再把所有标的拉向 1/n 等权：
          - 在 SECTOR_TARGET_WEIGHTS 中的 sector -> 目标总权重 = 该基准（自动归一化）
          - 不在表中的 sector（现金管理 / 混合类 / 未知）-> 保持当前占比
          - 每个 sector 内部按当前市值占比分配（类别内不均）
          - 最后用收缩系数向当前权重收缩，避免过度交易
        """
        weights, total, names, prices = self.get_current_weights(as_of_date)
        if not weights:
            return RebalancePlan(as_of_date=as_of_date, strategy="layered",
                                 action_needed=False, reason="无持仓", total_value=0.0,
                                 execution_date=str(next_trading_day(as_of_date)))
        from config.settings import (ETF_CATEGORIES, SECTOR_TARGET_WEIGHTS,  # noqa
                                     REBALANCE_SHRINKAGE)
        if shrinkage is None:
            shrinkage = REBALANCE_SHRINKAGE

        # 1) 分类 + sector 当前总权重
        sectors = {c: self.classify_sector(c, names.get(c, "")) for c in weights}
        sector_cur: Dict[str, float] = {}
        for c, w in weights.items():
            sector_cur[sectors[c]] = sector_cur.get(sectors[c], 0.0) + w

        managed = {s: wt for s, wt in SECTOR_TARGET_WEIGHTS.items() if s in sector_cur}
        keep_sectors = {s: sector_cur[s] for s in sector_cur if s not in managed}
        keep_sum = sum(keep_sectors.values())

        # 2) 归一化：受管控 sector 基准和缩放，使其与"保持类"占比叠加后总和 = 1
        managed_target_sum = sum(managed.values())
        if managed_target_sum <= 0:
            target_weights = dict(weights)          # 无受管控 sector，退化为保持
        else:
            scale = (1.0 - keep_sum) / managed_target_sum if keep_sum < 1.0 else 0.0
            scaled_managed = {s: wt * scale for s, wt in managed.items()}

            # 3) sector 内按当前市值占比分配（类别内不均）
            raw_target: Dict[str, float] = {}
            for c, w in weights.items():
                s = sectors[c]
                if s in scaled_managed and sector_cur[s] > 0:
                    inner_share = w / sector_cur[s]
                    raw_target[c] = scaled_managed[s] * inner_share
                else:
                    raw_target[c] = w               # 保持类维持当前

            # 4) 收缩：目标 = (1-λ)·当前 + λ·分层基准（避免过度交易）
            target_weights = {
                c: (1 - shrinkage) * weights[c] + shrinkage * raw_target.get(c, weights[c])
                for c in weights
            }
            # 5) 归一化确保总和 = 1
            tw_sum = sum(target_weights.values())
            if tw_sum > 0:
                target_weights = {c: v / tw_sum for c, v in target_weights.items()}

        return self.propose(as_of_date, target_weights, threshold=threshold, strategy="layered")


def compute_rebalance_suggestion(db_connection: sqlite3.Connection,
                                  as_of_date: Optional[str] = None,
                                  strategy: str = "threshold",
                                  threshold: Optional[float] = None,
                                  target_weights: Optional[Dict[str, float]] = None,
                                  period_days: int = 20,
                                  last_rebalance_date: Optional[str] = None) -> RebalancePlan:
    """统一入口（对应 models 文档中声明的 compute_rebalance_suggestion）。

    strategy: threshold | periodic | equal_weight | custom
    - custom 必须传 target_weights
    - 其余策略 target_weights 可省略（默认等权目标）
    """
    engine = RebalanceEngine(db_connection)
    if as_of_date is None:
        as_of_date = str(last_trading_day_on_or_before(date.today()))
    if strategy == "periodic":
        return engine.propose_periodic(as_of_date, period_days=period_days,
                                        last_rebalance_date=last_rebalance_date,
                                        target_weights=target_weights)
    if strategy == "layered":
        return engine.propose_layered(as_of_date, threshold=threshold)
    if strategy == "equal_weight":
        return engine.propose_equal_weight(as_of_date, threshold=threshold)
    if strategy == "custom":
        if target_weights is None:
            raise ValueError("strategy='custom' 必须提供 target_weights")
        return engine.propose(as_of_date, target_weights, threshold=threshold, strategy="custom")
    # 默认 threshold
    return engine.propose_threshold(as_of_date, target_weights=target_weights, threshold=threshold)


if __name__ == "__main__":
    # 只读演示：对真实生产库生成再平衡方案
    import os
    from config.settings import DATABASE_PATH
    db_path = str(DATABASE_PATH)
    if not os.path.exists(db_path):
        print(f"未找到生产库: {db_path}")
    else:
        conn = sqlite3.connect(db_path)
        try:
            as_of = str(last_trading_day_on_or_before(date.today()))
            plan = compute_rebalance_suggestion(conn, as_of_date=as_of, strategy="equal_weight")
            print(f"as_of={as_of} 执行日={plan.execution_date} 需调仓={plan.action_needed}")
            print(f"  原因: {plan.reason}")
            print(f"  总市值={plan.total_value:.2f} 换手={plan.turnover:.4f} 预估成本={plan.estimated_cost:.2f}")
            for t in plan.trades[:10]:
                print(f"  {t.direction} {t.code} {t.name} 当前{t.current_weight*100:.1f}%→目标{t.target_weight*100:.1f}% "
                      f"金额={t.trade_value:.0f} 手数={t.shares}")
        finally:
            conn.close()
