"""再平衡引擎（P2-B 产品化）

基于真实持仓快照（portfolio_snapshots）计算当前权重，生成**可执行**的再平衡方案：
- 支持策略：
    * threshold    阈值偏离（默认目标=等权，偏离 > 阈值才调仓）
    * periodic     定期（距上次再平衡达到 period_days 个交易日才调仓）
    * equal_weight 直接以等权为目标
    * layered      分层（按资产类别战略基准 + 类别内市值占比；类别偏离超阈值即触发）
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
                strategy: str = "custom",
                force: bool = False) -> RebalancePlan:
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
        if max_dev <= threshold and not force:
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
            ("可转债", "可转债"), ("债", "债券"), ("转债", "可转债"),
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
        """分层再平衡：类别基准权重 + 类别内按市值分配（类别内也不均），战略偏离触发。

        不再把所有标的拉向 1/n 等权：
          - 在 SECTOR_TARGET_WEIGHTS 中的 sector -> 目标总权重 = 该基准（自动归一化）
          - 不在表中的 sector（现金管理 / 混合类 / 未知）-> 保持当前占比
          - 每个 sector 内部按当前市值占比分配（类别内不均）

        触发逻辑（P0 修复核心）：
          - 计算「类别当前权重 vs 类别战略权重」的最大偏离；
          - 若 最大类别偏离 > SECTOR_DEVIATION_THRESHOLD 或 单标偏离 > threshold，
            则触发再平衡，并以「完整战略目标(raw_target)」生成调仓交易；
          - 旧版用 shrinkage 把目标拉向当前权重、又只看单标偏离，导致大类偏离 24%
            也被判为「最大偏离 4.1% ≤ 5% 无需调仓」，战略纪律形同虚设。
        """
        weights, total, names, prices = self.get_current_weights(as_of_date)
        if not weights:
            return RebalancePlan(as_of_date=as_of_date, strategy="layered",
                                 action_needed=False, reason="无持仓", total_value=0.0,
                                 execution_date=str(next_trading_day(as_of_date)))
        from config.settings import (ETF_CATEGORIES, SECTOR_TARGET_WEIGHTS,  # noqa
                                     REBALANCE_SHRINKAGE, SECTOR_DEVIATION_THRESHOLD,
                                     TACTICAL_OVERRIDES)
        if shrinkage is None:
            shrinkage = REBALANCE_SHRINKAGE
        if threshold is None:
            threshold = SMART_ANALYSIS_CONFIG.get("rebalance_threshold", 0.05)

        # 1) 分类 + sector 当前总权重
        sectors = {c: self.classify_sector(c, names.get(c, "")) for c in weights}
        sector_cur: Dict[str, float] = {}
        for c, w in weights.items():
            sector_cur[sectors[c]] = sector_cur.get(sectors[c], 0.0) + w

        # 战术留痕：用 TACTICAL_OVERRIDES 替换对应类别的战略基准，
        # 从而尊重主观战术超配、只对「漂移」触发再平衡（而非与战术观点对着干）。
        effective_targets = dict(SECTOR_TARGET_WEIGHTS)
        tactical_sectors = set()
        for _s, _t in (TACTICAL_OVERRIDES or {}).items():
            if _s in effective_targets:
                effective_targets[_s] = _t
                tactical_sectors.add(_s)

        managed = {s: wt for s, wt in effective_targets.items() if s in sector_cur}
        keep_sectors = {s: sector_cur[s] for s in sector_cur if s not in managed}
        keep_sum = sum(keep_sectors.values())

        # 2) 归一化：受管控 sector 基准和缩放，使其与"保持类"占比叠加后总和 = 1
        managed_target_sum = sum(managed.values())
        raw_target: Dict[str, float] = dict(weights)   # 兜底：保持当前
        sector_tgt: Dict[str, float] = {}
        if managed_target_sum > 0:
            scale = (1.0 - keep_sum) / managed_target_sum if keep_sum < 1.0 else 0.0
            scaled_managed = {s: wt * scale for s, wt in managed.items()}
            # 3) sector 内按当前市值占比分配（类别内不均）
            for c, w in weights.items():
                s = sectors[c]
                if s in scaled_managed and sector_cur[s] > 0:
                    inner_share = w / sector_cur[s]
                    raw_target[c] = scaled_managed[s] * inner_share
                # 保持类维持当前
            sector_tgt = dict(scaled_managed)
        else:
            sector_tgt = {s: sector_cur[s] for s in keep_sectors}
        # 4) 归一化确保 raw_target 总和 = 1
        tw_sum = sum(raw_target.values())
        if tw_sum > 0:
            raw_target = {c: v / tw_sum for c, v in raw_target.items()}

        # 5) 触发判断：类别偏离 或 单标偏离（关键修复点）
        max_cat_dev = 0.0
        worst_cat = ""
        for s in sector_tgt:
            dev = abs(sector_cur.get(s, 0.0) - sector_tgt.get(s, 0.0))
            if dev > max_cat_dev:
                max_cat_dev = dev
                worst_cat = s
        max_sec_dev = max(
            abs(weights.get(c, 0.0) - raw_target.get(c, 0.0))
            for c in set(weights) | set(raw_target)
        )
        cat_trigger = max_cat_dev > SECTOR_DEVIATION_THRESHOLD
        sec_trigger = max_sec_dev > threshold

        if not (cat_trigger or sec_trigger):
            return RebalancePlan(
                as_of_date=as_of_date, strategy="layered", action_needed=False,
                reason=f"战略配置无显著偏离（类别最大偏离 {max_cat_dev*100:.1f}% ≤ "
                       f"{SECTOR_DEVIATION_THRESHOLD*100:.0f}%，单标最大偏离 {max_sec_dev*100:.1f}% ≤ "
                       f"{threshold*100:.0f}%），无需再平衡",
                current_weights=weights, target_weights=raw_target,
                execution_date=str(next_trading_day(as_of_date)), total_value=total,
            )

        # 6) 触发：以完整战略目标为基准，按 shrinkage 决定移动幅度（默认 1.0=一步到位）
        target_weights = {
            c: (1 - shrinkage) * weights[c] + shrinkage * raw_target.get(c, weights[c])
            for c in weights
        }
        tw_sum2 = sum(target_weights.values())
        if tw_sum2 > 0:
            target_weights = {c: v / tw_sum2 for c, v in target_weights.items()}

        reason_parts = []
        if cat_trigger:
            reason_parts.append(
                f"类别偏离触发：{worst_cat} 当前 {sector_cur.get(worst_cat, 0)*100:.1f}% → "
                f"目标 {sector_tgt.get(worst_cat, 0)*100:.1f}%（差 {max_cat_dev*100:+.1f}%）"
            )
        if sec_trigger:
            reason_parts.append(f"单标偏离 {max_sec_dev*100:.1f}% > {threshold*100:.0f}%")
        reason = "分层再平衡：" + "；".join(reason_parts)
        if tactical_sectors:
            reason += f"；战术留痕类别：{', '.join(sorted(tactical_sectors))}（按意图目标执行）"

        plan = self.propose(as_of_date, target_weights, threshold=threshold,
                            strategy="layered", force=True)
        plan.reason = reason
        return plan

    # ------------------------------------------------------------------
    # 重叠敞口检测（P1 重复标的 + P2 相关性管理）
    # ------------------------------------------------------------------
    def detect_overlapping_exposure(self, as_of_date: str) -> List[Dict[str, object]]:
        """检测同一底层指数/主题被多只 ETF 重复覆盖的敞口（相关度≈1，纯冗余）。

        返回 list[dict]: {theme, members:[{code,name,weight}], total_weight, note}
        仅包含成员 >= 2 的组。供 advisor 生成「重复/同质敞口」建议。
        """
        # 主题 -> 名称关键词（按特异性从高到低，先匹配者生效）
        THEME_KEYWORDS = [
            ("沪深300", ["沪深300"]),
            ("中证500", ["中证500"]),
            ("中证1000", ["中证1000"]),
            ("创业板", ["创业板"]),
            ("科创", ["科创"]),
            ("医药/创新药", ["医药", "创新药", "医疗"]),
            ("新能源/电池", ["新能源", "电池", "光伏"]),
            ("军工", ["军工", "国防", "航天"]),
            ("金融", ["证券", "银行", "保险"]),
            ("红利", ["红利", "低波"]),
            ("可转债", ["可转债"]),
            ("科技", ["机器人", "人工智能", "ai", "半导体", "芯片", "科技", "消费电子"]),
        ]
        weights, _, names, _ = self.get_current_weights(as_of_date)
        if not weights:
            return []
        groups: Dict[str, List[Dict[str, object]]] = {}
        for c, w in weights.items():
            n = (names.get(c, "") or "").lower()
            theme = None
            for t, kws in THEME_KEYWORDS:
                if any(kw.lower() in n for kw in kws):
                    theme = t
                    break
            if theme is None:
                continue
            groups.setdefault(theme, []).append(
                {"code": c, "name": names.get(c, ""), "weight": round(w, 4)}
            )
        result = []
        for theme, members in groups.items():
            if len(members) < 2:
                continue
            total = round(sum(m["weight"] for m in members), 4)
            if theme in ("沪深300", "中证500", "中证1000", "创业板", "科创"):
                note = ("同一底层指数被多只 ETF 覆盖，相关度≈1，建议保留流动性最佳的一只、"
                        "释放冗余权重至宽基/债券")
            else:
                note = "同主题 ETF 高度同质、相关性高，建议合并为 1~2 只以降低集中度"
            result.append({"theme": theme, "members": members,
                           "total_weight": total, "note": note})
        result.sort(key=lambda g: -g["total_weight"])
        return result

    # ------------------------------------------------------------------
    # 组合风险指标（P2 风险预算 + 集中度）
    # ------------------------------------------------------------------
    def compute_risk_metrics(self, as_of_date: str) -> Dict[str, object]:
        """计算组合层面风险指标：HHI、前N集中度、加权 Beta、债券敞口 vs 目标。

        返回 dict：hhi, top3_concentration, n_effective, portfolio_beta,
                  bond_actual, bond_target, bond_under_target, equity_weight, warnings
        """
        from config.settings import (SECTOR_TARGET_WEIGHTS,  # noqa
                                     PORTFOLIO_BETA_BUDGET, BOND_UNDER_TARGET_TOL)
        weights, _, names, _ = self.get_current_weights(as_of_date)
        if not weights:
            return {}
        d = last_trading_day_on_or_before(as_of_date)
        betas = {}
        try:
            df = pd.read_sql_query(
                "SELECT code, beta FROM portfolio_snapshots WHERE date=?",
                self.db, params=[str(d)],
            )
            for r in df.itertuples():
                if pd.notna(r.beta):
                    betas[r.code] = float(r.beta)
        except Exception:
            betas = {}
        sectors = {c: self.classify_sector(c, names.get(c, "")) for c in weights}

        hhi = sum(w * w for w in weights.values())
        ranked = sorted(weights.values(), reverse=True)
        top3 = sum(ranked[:3])
        n_eff = (1.0 / hhi) if hhi > 0 else len(weights)
        portfolio_beta = sum(w * betas.get(c, 1.0) for c, w in weights.items())

        bond_actual = sum(w for c, w in weights.items() if sectors[c] == "债券")
        bond_target = SECTOR_TARGET_WEIGHTS.get("债券", 0.0)
        bond_under = bond_actual < (bond_target - BOND_UNDER_TARGET_TOL)
        equity_weight = sum(w for c, w in weights.items()
                            if sectors[c] not in ("债券", "可转债", "现金管理"))

        warnings = []
        if hhi > 0.18:
            warnings.append(f"HHI={hhi:.3f} 偏高（>0.18），前3集中度 {top3*100:.1f}%")
        if portfolio_beta > PORTFOLIO_BETA_BUDGET:
            warnings.append(f"组合加权 Beta={portfolio_beta:.2f} 超过预算 {PORTFOLIO_BETA_BUDGET:.1f}")
        if bond_under:
            warnings.append(f"债券实际 {bond_actual*100:.1f}% 低于目标 {bond_target*100:.1f}%"
                            f"（偏差超 {BOND_UNDER_TARGET_TOL*100:.0f}% 容差），波动率预算未落实")
        # 波动率预警（联动预测底座 risk_lgb：预期年化波动率 >30% 的持仓）
        try:
            df_v = pd.read_sql_query(
                "SELECT code, probability FROM etf_predictions "
                "WHERE model='risk_lgb' AND forward_window=20 AND probability>=0.30 "
                "ORDER BY probability DESC", self.db)
            if not df_v.empty:
                hi_names = [names.get(r.code, r.code) for r in df_v.itertuples()]
                shown = "、".join(hi_names[:5]) + (" 等" if len(hi_names) > 5 else "")
                warnings.append(
                    f"波动率预警：{len(hi_names)} 只持仓预期年化波动率>30%（{shown}），"
                    f"建议关注仓位与回撤风险（仅参考，不自动调仓）")
        except Exception:
            pass
        return {
            "hhi": round(hhi, 4),
            "top3_concentration": round(top3, 4),
            "n_effective": round(n_eff, 1),
            "portfolio_beta": round(portfolio_beta, 3),
            "bond_actual": round(bond_actual, 4),
            "bond_target": round(bond_target, 4),
            "bond_under_target": bond_under,
            "equity_weight": round(equity_weight, 4),
            "warnings": warnings,
        }


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
