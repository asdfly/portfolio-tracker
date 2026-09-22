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

from config.settings import (SMART_ANALYSIS_CONFIG, ETF_LOT_SIZE,
                             is_otc_fund, is_delisted, stale_threshold_days)
from src.models import RebalanceTrade
from src.utils.trading_calendar import (
    CALENDAR_COVERED_YEARS,
    next_trading_day,
    last_trading_day_on_or_before,
    get_trading_days,
    is_trading_day,
    uncovered_years,
)

logger = logging.getLogger(__name__)

# 快照陈旧告警阈值（自然日）：标的最新快照早于目标日期超过该天数即告警
STALE_SNAPSHOT_DAYS = 7
# 快照停更超过该天数 → 升级为「疑似失效」（权重可能已失真，但仍保留在组合里）
STALE_SNAPSHOT_SUSPECT_DAYS = 30
# 年化已实现波动率预警阈值（%），与 Tab16 / risk_report 三档口径一致
VOL_ANN_WARN_PCT = 30.0


def calc_trade_shares(trade_value: float, price: float, code: str = "") -> int:
    """按交易金额与价格折算建议**份额**（份）。

    - 场内 ETF：1 手 = 100 份，向下取整到 100 的整数倍（整手），避免下出零股单。
    - 场外基金：按金额申购，无「手」概念，仅向下取整到 1 份，不做整手取整。
    """
    if not price or price <= 0 or not trade_value:
        return 0
    raw = int(abs(trade_value) / price)
    if is_otc_fund(code):
        return raw
    return (raw // ETF_LOT_SIZE) * ETF_LOT_SIZE


def format_share_display(shares: int, code: str = "") -> str:
    """把份额格式化成给人看的下单量：场内显示「N 手」，场外显示「N 份」。"""
    if is_otc_fund(code):
        return f"{int(shares):,}份"
    return f"{int(shares) // ETF_LOT_SIZE:,}手"


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
    snapshot_dates: Dict[str, str] = field(default_factory=dict)  # code -> 所用快照日期
    stale_snapshots: List[Dict[str, object]] = field(default_factory=list)  # 停更>7天的标的
    dropped_legs: List[Dict[str, object]] = field(default_factory=list)     # 不足最小交易单位被丢弃的腿

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
            "snapshot_dates": self.snapshot_dates,
            "stale_snapshots": self.stale_snapshots,
            "dropped_legs": self.dropped_legs,
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
        # 最近一次 get_current_holdings 的附加信息（兼容旧调用方的旁路通道）
        self.last_snapshot_dates: Dict[str, str] = {}
        self.last_stale: List[Dict[str, object]] = []
        self.last_betas: Dict[str, float] = {}
        self._holdings_cache: Dict[str, tuple] = {}  # 解析后日期 -> 6 元组结果

    # ------------------------------------------------------------------
    # 当前持仓 / 权重
    # ------------------------------------------------------------------
    # 按 code 各取「date <= ? 的最新一条」（等价 GROUP BY code MAX(date)）。
    # 这是 P0-1 的核心修复：旧版取「全局最新快照日期」那一批行，导致 13 只 45 天未
    # 更新的场外基金整批从组合里消失（35 只 → 22 只，总市值蒸发 37.8%）。
    _PER_CODE_LATEST_SQL = """
        SELECT ps.code, ps.name, ps.market_value, ps.current_price, ps.date, ps.beta
        FROM portfolio_snapshots ps
        JOIN (SELECT code, MAX(date) AS md
              FROM portfolio_snapshots
              WHERE date <= ? AND market_value IS NOT NULL
              GROUP BY code) t
          ON ps.code = t.code AND ps.date = t.md
        WHERE ps.id = (SELECT MAX(p2.id) FROM portfolio_snapshots p2
                       WHERE p2.code = ps.code AND p2.date = t.md
                         AND p2.market_value IS NOT NULL)
    """

    def get_current_holdings(self, as_of_date: str, with_meta: bool = False):
        """取 as_of_date 或之前最近交易日的持仓快照。

        **按 code 各自取 `date <= 目标日期` 的最新一条**，而非「全局最新快照日期」
        那一批行——后者会让长期未更新快照的标的（如场外基金）凭空消失。

        已清仓标的（config.DELISTED_CODES / ETF_CATEGORIES 的 delisted 标记）
        即使还有残留快照也被排除，不作为当前持仓。

        返回 (code->market_value, total_value, code->name, code->current_price)；
        with_meta=True 时额外返回 (…, code->snapshot_date, stale_list)，
        stale_list = [{"code","name","snapshot_date","days"}]，days > 7 天即入列。
        """
        d = last_trading_day_on_or_before(as_of_date)
        cached = self._holdings_cache.get(str(d))
        if cached is not None:
            return (cached + ()) if with_meta else cached[:4]
        df = pd.read_sql_query(self._PER_CODE_LATEST_SQL, self.db, params=[str(d)])
        if df.empty:
            # 退一步：目标日期之前完全没有快照 → 取全局最早可用的 <= d 日期
            row = pd.read_sql_query(
                "SELECT DISTINCT date FROM portfolio_snapshots WHERE date<=? "
                "ORDER BY date DESC LIMIT 1",
                self.db, params=[str(d)],
            )
            if row.empty:
                return ({}, 0.0, {}, {}, {}, []) if with_meta else ({}, 0.0, {}, {})
            d2 = row.iloc[0, 0]
            df = pd.read_sql_query(self._PER_CODE_LATEST_SQL, self.db, params=[str(d2)])
        df = df.dropna(subset=["market_value"])
        if df.empty:
            return ({}, 0.0, {}, {}, {}, []) if with_meta else ({}, 0.0, {}, {})

        mv, names, prices, snap_dates, betas = {}, {}, {}, {}, {}
        for r in df.itertuples():
            if is_delisted(r.code):
                # 已清仓标的仍有历史残留快照（159732 停在 2026-07-30），
                # 必须排除，否则会被当成真实持仓生成调仓建议。
                logger.warning(
                    "已清仓标的 %s(%s) 在快照中有残留记录（%s，市值 %.2f），"
                    "已从当前持仓集合排除",
                    r.code, r.name or "", r.date, float(r.market_value),
                )
                continue
            mv[r.code] = float(r.market_value)
            names[r.code] = (r.name or "")
            prices[r.code] = (float(r.current_price) if pd.notna(r.current_price) else None)
            snap_dates[r.code] = str(r.date)
            if pd.notna(getattr(r, "beta", None)):
                betas[r.code] = float(r.beta)

        if not mv:      # 全部是已清仓标的的残留快照
            return ({}, 0.0, {}, {}, {}, []) if with_meta else ({}, 0.0, {}, {})
        total = float(sum(mv.values()))
        stale = self._build_stale_list(snap_dates, names, str(d))
        self.last_snapshot_dates = snap_dates
        self.last_stale = stale
        self.last_betas = betas

        full = (mv, total, names, prices, snap_dates, stale)
        self._holdings_cache[str(d)] = full
        return full if with_meta else full[:4]

    @staticmethod
    def _build_stale_list(snap_dates: Dict[str, str], names: Dict[str, str],
                          target_date: str) -> List[Dict[str, object]]:
        """挑出快照超期的标的并显式告警。

        阈值按标的披露节奏逐只取（见 config.stale_threshold_days），不是一刀切。
        """
        try:
            tgt = datetime.strptime(target_date, "%Y-%m-%d").date()
        except (TypeError, ValueError):
            return []
        stale = []
        for code, sd in snap_dates.items():
            try:
                sdt = datetime.strptime(str(sd), "%Y-%m-%d").date()
            except (TypeError, ValueError):
                continue
            days = (tgt - sdt).days
            # 阈值按标的披露节奏取：场外只有月末导入链路（35 天），
            # 周更净值产品（027293）用显式覆盖值，场内为日更（7 天）。
            # 用统一 7 天阈值会把场外的正常披露节奏整月误报成采集故障。
            thr = stale_threshold_days(code, STALE_SNAPSHOT_DAYS)
            if days > thr:
                # 升级为「疑似失效」：可能已清仓或份额已变，但**不静默丢弃**
                # （静默丢弃正是 P0-1 的病根），只是把告警级别抬高。
                suspect_thr = max(thr, STALE_SNAPSHOT_SUSPECT_DAYS)
                level = "疑似失效" if days > suspect_thr else "陈旧"
                stale.append({"code": code, "name": names.get(code, ""),
                              "snapshot_date": str(sd), "days": days,
                              "level": level, "threshold": thr})
                logger.warning(
                    "持仓快照%s：%s(%s) 最新快照 %s，距目标日期 %s 已 %d 天"
                    "（该标的阈值 %d 天），权重按陈旧快照计算",
                    level, code, names.get(code, ""), sd, target_date, days, thr,
                )
        stale.sort(key=lambda x: -int(x["days"]))
        return stale

    @staticmethod
    def _format_stale_note(stale: List[Dict[str, object]]) -> str:
        """把陈旧快照列表压成一句可展示的提示（疑似失效的优先点名）。"""
        if not stale:
            return ""
        suspect = [s for s in stale if s.get("level") == "疑似失效"]
        normal = [s for s in stale if s.get("level") != "疑似失效"]
        parts = []
        for label, group in (("疑似失效", suspect), ("陈旧", normal)):
            if not group:
                continue
            shown = "、".join(f"{s['code']}({s['days']}天)" for s in group[:5])
            more = f" 等 {len(group)} 只" if len(group) > 5 else ""
            parts.append(f"{label} {len(group)} 只：{shown}{more}")
        return f"；⚠️ 快照停更（{'；'.join(parts)}），其权重按陈旧快照计入"

    def get_current_weights(self, as_of_date: str, with_meta: bool = False):
        """返回 (code->weight, total_value, code->name, code->price)。

        with_meta=True 时额外返回 (…, code->snapshot_date, stale_list)。
        """
        if with_meta:
            mv, total, names, prices, snap_dates, stale = self.get_current_holdings(
                as_of_date, with_meta=True)
        else:
            mv, total, names, prices = self.get_current_holdings(as_of_date)
            snap_dates, stale = {}, []
        weights = {c: (m / total if total > 0 else 0.0) for c, m in mv.items()}
        if with_meta:
            return weights, total, names, prices, snap_dates, stale
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
        (weights, total, names, prices,
         snap_dates, stale) = self.get_current_weights(as_of_date, with_meta=True)
        if not weights:
            return RebalancePlan(
                as_of_date=as_of_date, strategy=strategy, action_needed=False,
                reason="无持仓快照数据，无法生成方案", total_value=0.0,
                execution_date=str(next_trading_day(as_of_date)),
            )
        all_codes = set(weights) | set(target_weights)
        max_dev = max(abs(weights.get(c, 0.0) - target_weights.get(c, 0.0)) for c in all_codes)
        exec_date = str(next_trading_day(as_of_date))
        stale_note = self._format_stale_note(stale)
        if max_dev <= threshold and not force:
            return RebalancePlan(
                as_of_date=as_of_date, strategy=strategy, action_needed=False,
                reason=f"最大偏离 {max_dev*100:.1f}% ≤ 阈值 {threshold*100:.1f}%，无需再平衡"
                       f"{stale_note}",
                current_weights=weights, target_weights=target_weights,
                execution_date=exec_date, total_value=total,
                snapshot_dates=snap_dates, stale_snapshots=stale,
            )
        # 生成逐标的交易
        trades: List[RebalanceTrade] = []
        dropped: List[Dict[str, object]] = []   # 不足最小交易单位被丢弃的腿
        turnover = 0.0
        for c in all_codes:
            cw = weights.get(c, 0.0)
            tw = target_weights.get(c, 0.0)
            diff = cw - tw                       # 正=超配，需卖出
            if abs(diff) < 1e-9:
                continue
            trade_value = abs(diff) * total
            price = prices.get(c)
            lot_traded = not is_otc_fund(c)
            # 场内向下取整到整手（1 手 = 100 份）；场外按金额申购，不取整手
            shares = calc_trade_shares(trade_value, price, c)
            if shares <= 0:
                # 金额不足 1 手（场内 100×price）时整手取整必然归零，
                # 这类腿留在方案里会变成「有金额无份额」的错误下单，直接丢弃并告警。
                min_value = (ETF_LOT_SIZE * price) if (lot_traded and price) else (price or 0)
                logger.warning(
                    "调仓腿已丢弃：%s(%s) 金额 %.2f 元 < 最小交易单位 %.2f 元"
                    "（价 %.4f%s），整手取整后为 0 份",
                    c, names.get(c, ""), trade_value, min_value, price or 0.0,
                    "，1 手=100 份" if lot_traded else "，场外按金额申购",
                )
                dropped.append({"code": c, "name": names.get(c, ""),
                                "trade_value": round(trade_value, 2),
                                "min_value": round(min_value, 2)})
                continue
            trades.append(RebalanceTrade(
                code=c, name=names.get(c, ""),
                current_weight=round(cw, 4), target_weight=round(tw, 4),
                diff=round(diff, 4), trade_value=round(trade_value, 2),
                shares=shares, direction="卖出" if diff > 0 else "买入",
                price=round(price, 4) if price else 0.0,
                lot_traded=lot_traded,
            ))
            turnover += abs(diff)
        turnover = turnover / 2.0               # 换手率 = 单边变动之和 / 2
        est_cost = 2 * self.cost_rate * turnover * total
        if dropped:
            codes = "、".join(f"{d['code']}({d['trade_value']:,.0f}元<{d['min_value']:,.0f}元)"
                              for d in dropped[:3])
            stale_note += (f"；已丢弃 {len(dropped)} 笔不足最小交易单位的调仓腿：{codes}"
                           + (" 等" if len(dropped) > 3 else ""))
        return RebalancePlan(
            as_of_date=as_of_date, strategy=strategy, action_needed=True,
            reason=f"最大偏离 {max_dev*100:.1f}% > 阈值 {threshold*100:.1f}%，建议再平衡"
                   f"{stale_note}",
            current_weights=weights, target_weights=target_weights,
            trades=trades, turnover=round(turnover, 4),
            estimated_cost=round(est_cost, 2), execution_date=exec_date,
            total_value=total, snapshot_dates=snap_dates, stale_snapshots=stale,
            dropped_legs=dropped,
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
            # 🔴 (b) 显式拒绝（项目所有者 2026-09-16 裁定）：**不得**静默退化成「立即再平衡」。
            # 原实现是 `return self.propose(..., threshold=0.0, strategy="periodic")` ——
            # 后果：用户在 UI 选 periodic 时，系统做的是「永远立即再平衡」（策略 A 的皮、
            # 策略 B 的里），且盘上没有任何提示；同时下面 :409-438 的「交易日历退化留痕」
            # 整条链因提前返回而**永不执行**（task #66 的留痕在现网不可达）。
            # 现在改为抛错：缺前提必须显式可见，由调用方处置
            # （生产调用方一律先走 resolve_last_rebalance_date）。
            raise ValueError(
                "strategy='periodic' 缺少 last_rebalance_date：无法计算「距上次再平衡的交易日数」，"
                "因此也无法判定是否到期。本仓目前**没有**「实际调仓日」数据源"
                "（候选与否决理由见 resolve_last_rebalance_date 的文档串），"
                "该基期必须由调用方显式提供；**禁止**静默按「立即再平衡」处理。"
            )
        days = get_trading_days(last_rebalance_date, as_of_date)
        elapsed = max(len(days) - 1, 0)        # 间隔交易日数
        # 退化口径留痕（task #66）：区间跨无官方休市表的年份时，get_trading_days 用的是
        # 「仅周末」规则，落在工作日的节假日会被算成交易日，于是 elapsed 系统性偏大
        # （实测 2023 全年退化 260 天 vs index_quotes 真实 242 天；11 天春节间隔被算成 7~8 个
        # 交易日）。这条链的终点是 action_needed=False —— 调仓被静默延后，日志里只看得到
        # 「未到调仓日」，看不出日历本身不可信。故在调用点把「数字 + 决策」一起打出来。
        missing_years = uncovered_years(last_rebalance_date, as_of_date)
        calendar_note = ""
        if missing_years:
            calendar_note = (
                f"（注意：交易日历缺 {missing_years} 年官方休市表，"
                f"该间隔为退化口径、偏大；已覆盖年份仅 {list(CALENDAR_COVERED_YEARS)}）"
            )
            logger.warning(
                "propose_periodic(as_of=%s, last_rebalance=%s): 交易日历缺 %s 年官方休市表，"
                "间隔交易日数 %d 为退化口径（仅周末）会偏大；判定 elapsed=%d %s 周期 %d → "
                "action_needed=%s。该数字与结论均不可信，请补 _HOLIDAY_RANGES 后再采信。",
                as_of_date, last_rebalance_date, missing_years, elapsed,
                elapsed, ">=" if elapsed >= period_days else "<", period_days,
                elapsed >= period_days,
            )
        if elapsed < period_days:
            return RebalancePlan(
                as_of_date=as_of_date, strategy="periodic", action_needed=False,
                reason=f"距上次再平衡 {elapsed} 交易日 < 周期 {period_days}，未到调仓日{calendar_note}",
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
        # beta 与各标的所用快照对齐（同样按 code 取最新一条），
        # 避免「权重含 36 只、beta 只有当天 22 只」的口径错配。
        betas = dict(self.last_betas)
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
        # 波动率预警：纯历史统计，读 etf_features 最新特征日的 vol_20d 并年化
        # （口径与 Tab16 / src/utils/risk_report.py 一致：日波动率 × √252 × 100）。
        # risk_lgb 模型已因样本外排序能力未跑赢零成本 vol_20d 基线
        # （Spearman IC 0.613 vs 0.743，ΔIC 的 HAC t=−4.51，6/6 全 VETO）而下线，
        # 故不再读 etf_predictions(model='risk_lgb')。
        try:
            row = self.db.execute("SELECT MAX(date) FROM etf_features").fetchone()
            latest_feat = row[0] if row else None
            df_v = pd.DataFrame()
            if latest_feat:
                df_v = pd.read_sql_query(
                    "SELECT code, vol_20d FROM etf_features WHERE date=?",
                    self.db, params=[latest_feat])
            if not df_v.empty:
                df_v = df_v.dropna(subset=["vol_20d"])
                df_v["vol_ann"] = df_v["vol_20d"] * (252 ** 0.5) * 100.0
                hi = df_v[df_v["vol_ann"] > VOL_ANN_WARN_PCT].sort_values(
                    "vol_ann", ascending=False)
                if not hi.empty:
                    hi_names = [names.get(r.code, r.code) for r in hi.itertuples()]
                    shown = "、".join(hi_names[:5]) + (" 等" if len(hi_names) > 5 else "")
                    warnings.append(
                        f"波动率预警：{len(hi_names)} 只持仓近20日已实现年化波动率"
                        f">{VOL_ANN_WARN_PCT:.0f}%（{shown}），"
                        f"建议关注仓位与回撤风险（纯历史统计，不自动调仓）")
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


# ---------------------------------------------------------------------------
# 「上次再平衡日」(last_rebalance_date) 的解析 —— task #77 修法 (b)
# ---------------------------------------------------------------------------
# 🔴 结论：本仓**没有**合格的「实际调仓日」数据源。本函数当前**恒返回 None**。
#    这是**显式**的「没有来源」，不是漏改。候选与否决理由（2026-09-16 生产库实测，只读）：
#
#   1. `rebalance_history`           —— **表不存在**（全库 42 张表，无此表）。裁定候选 (a) 即为此路，已排除。
#   2. `execution_logs`              —— 任务级日志（`task_name`/`status`/`message`/`created_at`），
#                                       粒度是「某任务某次是否成功」，**无任何「调仓」语义**。
#   3. `trade_records`               —— 达信**全账本流水**，1256 行 / 2023-06-28~2026-08-31。
#                                       action 共 8 类，只有「证券买入」(548) 与「证券卖出」(32) 是证券交易，
#                                       其余为「产品定时定额投资确认」(210)、「产品赎回确认」(155)、
#                                       「产品申购确认」(133)、「银行转存」(94) 等。
#                                       取 `MAX(date)` 会把**银行转存日**当成调仓日 ⇒ 语义错误。
#   4. `advice_history`(advice_type='rebalance') —— 有 219 行（最新 2026-09-14），
#                                       但那是**建议推送日**、不是**执行日**：管线几乎每个交易日都可能推。
#                                       用它当基期 ⇒ `elapsed` 恒为 1 ⇒ 周期策略**永不触发**，
#                                       只是把「永远立即再平衡」换成「永远不再平衡」，**同样静默**。
#
# ⇒ 在引入真正的「执行台账」之前，periodic 的基期**只能由调用方显式提供**（UI 输入 / 上游传入）。
#    调用方拿到 None 时**必须显式告警**，不得回退到「立即再平衡」。
_REBALANCE_HISTORY_TABLE = "rebalance_history"


def resolve_last_rebalance_date(db_connection,
                                as_of_date: Optional[str] = None) -> Optional[str]:
    """尝试解析「上次再平衡日」；当前**恒返回 None**（理由见本区块顶部注释）。

    存在的意义有两条，都为了对抗静默：
      1. 把「没有来源」这件事**变成一个可调用、可测试、可日志**的事实，
         而不是散落在各调用点的 `None` 默认值；
      2. 将来若新增真正的执行台账，**只需改这一处**，所有调用点自动生效。

    调用方契约：返回 None 且 `strategy == "periodic"` ⇒ **必须显式可见**。
    见 `SmartAdvisor.generate_rebalance_plan` 与 `tabs/tab8_advice.py` 的处置。
    """
    candidates = []
    try:
        cur = db_connection.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    (_REBALANCE_HISTORY_TABLE,))
        candidates.append(_REBALANCE_HISTORY_TABLE if cur.fetchone() else None)
    except Exception:                                    # 连接不可用 / 空库
        candidates.append(None)
    if candidates and candidates[0]:
        # 走到这里说明有人建了目标表：**先不要猜列名**，显式留痕让人来定口径。
        logger.error(
            "[periodic] 检测到 %s 表存在，但本函数尚未定义其「调仓日」列口径；"
            "仍按「无来源」处理（返回 None）。请补口径后再启用。",
            _REBALANCE_HISTORY_TABLE)
    logger.warning(
        "[periodic] 无法解析「上次再平衡日」(as_of=%s)：本仓无合格的执行台账"
        "（rebalance_history 表不存在；execution_logs 为任务级；trade_records 为全账本流水含转账；"
        "advice_history 记的是建议推送日而非执行日）⇒ 返回 None。"
        "调用方必须显式处置，禁止回退到「立即再平衡」。",
        as_of_date or "(未指定)")
    return None


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
        from src.utils.database import get_db_connection
        conn = get_db_connection(db_path)
        try:
            as_of = str(last_trading_day_on_or_before(date.today()))
            plan = compute_rebalance_suggestion(conn, as_of_date=as_of, strategy="equal_weight")
            print(f"as_of={as_of} 执行日={plan.execution_date} 需调仓={plan.action_needed}")
            print(f"  原因: {plan.reason}")
            print(f"  总市值={plan.total_value:.2f} 换手={plan.turnover:.4f} 预估成本={plan.estimated_cost:.2f}")
            for t in plan.trades[:10]:
                qty = format_share_display(t.shares, t.code)
                extra = "" if is_otc_fund(t.code) else f"（={t.shares:,}份）"
                print(f"  {t.direction} {t.code} {t.name} 当前{t.current_weight*100:.1f}%→目标{t.target_weight*100:.1f}% "
                      f"金额={t.trade_value:.0f} {qty}{extra}")
            if plan.stale_snapshots:
                print(f"  ⚠️ 快照停更标的：{plan.stale_snapshots}")
        finally:
            conn.close()
