"""拆分/合并伪收益判别器（数据问题十二 · 消费侧闸门）

问题：13 只 ETF/场外基金在拆分/合并日，由于价格未复权而数量已变，
portfolio_summary.daily_return 会出现 ±25% 以上的伪收益。该伪收益被下游
(portfolio_nav 的 TWR 累乘、portfolio_risk 的波动率/Sharpe/回撤、
factor_attribution 的因子回归) 静默吸收，污染全部衍生特征。

判据来源：docs/handover/07_known_data_issues.md §三 的 qty_ratio 判别
    qty_ratio = quantity(date) / quantity(prev_date)
    - A族：qty_ratio ≈ 1.0 但 market_value 跳变（价格未复权、数量未复权）→ 伪收益
    - B族：qty_ratio ≈ 1/r（干净拆分，数量已复权）→ 伪收益
    - C族：qty_ratio ≈ 1.058~1.262 → 月末估值切换（非拆分/合并）→ 必须跳过

实库复核（scripts 只读 SQL，file:...portfolio.db?mode=ro）：
    A族 5 只  qty_ratio ∈ {0.287, 3.835, 0.362, 0.281, 0.311}，mv_ratio ≈ 1.000
    B族 2 只  qty_ratio = 2.000，                       mv_ratio ≈ 1.01~1.02
    C族 6 只  qty_ratio ∈ [1.058, 1.251]，            mv_ratio 不连续（0.72~1.46）
结论：A/B 两族 |qty_ratio-1| 均 > 0.25 且 market_value 连续；C 族落在
[1.058, 1.251] 且 mv 不连续。因此判据（见 classify_split_merge）：
    1) C 族跳过带 [VAL_SWITCH_QTY_MIN, VAL_SWITCH_QTY_MAX] 内 → 返回 False；
    2) |qty_ratio-1| > SPLIT_QTY_DEV 且 |mv_ratio-1| < MV_CONTINUITY
       → 真拆分/合并（A/B）。要求 mv 连续以排除真实申赎：
       买卖会改变 market_value，拆分不改变 market_value（价值守恒）。
    3) §三 A 族变体：|qty_ratio-1| < A_FAMILY_QTY_FLAT 且
       |mv_ratio-1| > A_FAMILY_MV_JUMP → 数量未复权、仅 mv 跳变的伪收益。

本模块只负责"读 + 判别 + 返回标志"，不写库、不改 stored daily_return。
下游按返回的"拆分日集合"对 daily_return 做排除/标记（见 nav_engine /
portfolio_risk / factor_attribution）。公式侧（portfolio.py 的折算闸门）
与消费侧（本模块）是互补的双层防护：公式侧纠正 stored 值，消费侧在
stored 值仍失真（如公式侧尚未落库或被重跑覆盖）时阻止其进入衍生特征。
"""
from __future__ import annotations

import logging
from typing import Iterable, Optional, Set

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 阈值（均由实库 13 例复核得出，非拍脑袋）
# ---------------------------------------------------------------------------
# C 族（月末估值切换 / 分红再投资等）qty_ratio 经验带；实库 6 只落在 1.058~1.251。
# 落在带内一律跳过——它们不是拆分/合并，伪收益判别会把它们误伤。
VAL_SWITCH_QTY_MIN = 1.02
VAL_SWITCH_QTY_MAX = 1.27

# 真拆分/合并：qty 相对变化超过此阈值即视为"折算级"跳变。
# A/B 实测 |qty_ratio-1| ∈ {0.69, 2.83, 0.64, 0.72, 0.69, 1.0}；C 实测 ≤ 0.251。
# 0.25 恰好把 C 族上沿(0.251)与 A/B 下沿(0.69)分开。
SPLIT_QTY_DEV = 0.25

# 拆分/合并日 market_value 应"连续"（价值守恒，份额折算不改变持有市值）。
# 超过此容差说明 mv 变了 → 是真实申赎或估值切换，不是干净拆分，不判伪收益。
# A/B 实测 mv_ratio ∈ {1.000, 1.016, 1.011}（< 0.10）；真实买卖 mv_ratio≈qty_ratio（≫1）。
MV_CONTINUITY = 0.10

# §三 A 族变体：数量几乎不变（< 3%）但市值跳变（> 25%）→ 价格未复权的伪收益。
# （实库 A 族当前已把数量一并复权，故该变体在实库不触发，保留以对齐 §三 口径与
#  未来可能未复权数量的摄入场景。）
A_FAMILY_QTY_FLAT = 0.03
A_FAMILY_MV_JUMP = 0.25

# 消费侧"中性化"阈值：stored daily_return（百分比）绝对值超过此值才在 NAV 累乘中
# 置 0，避免把已是经济真值的较小 corrected 值（如 -4.53%）误清成 0。
# 单只 ETF 组合单日 > 25% 在正常行情下几乎不可能，等价于"仍是伪收益"。
SPLIT_MERGE_NEUTRALIZE_DAILY_RETURN = 0.25


# ---------------------------------------------------------------------------
# 低层 ratio 读取（复用调用方已打开的连接，不新开）
# ---------------------------------------------------------------------------
def _prev_snapshot_date(cur, code: str, date: str) -> Optional[str]:
    """返回 code 在 date 之前最近一行的快照日（用于 qty_ratio 的 prev）。"""
    cur.execute(
        "SELECT date FROM portfolio_snapshots WHERE code=? AND date < ? "
        "ORDER BY date DESC LIMIT 1",
        (code, date),
    )
    row = cur.fetchone()
    return row[0] if row else None


def compute_qty_ratio(cur, code: str, date: str) -> Optional[float]:
    """quantity(date) / quantity(date 之前最近一行)。查不到 prev 或 prev qty 为 0 返回 None。"""
    prev = _prev_snapshot_date(cur, code, date)
    if not prev:
        return None
    cur.execute(
        "SELECT date, quantity FROM portfolio_snapshots WHERE code=? AND date IN (?, ?)",
        (code, date, prev),
    )
    m = {r[0]: r[1] for r in cur.fetchall()}
    q_cur = m.get(date)
    q_prev = m.get(prev)
    if q_cur is None or q_prev is None or q_prev == 0:
        return None
    try:
        return float(q_cur) / float(q_prev)
    except (TypeError, ValueError):
        return None


def compute_mv_ratio(cur, code: str, date: str) -> Optional[float]:
    """market_value(date) / market_value(date 之前最近一行)。查不到返回 None。"""
    prev = _prev_snapshot_date(cur, code, date)
    if not prev:
        return None
    cur.execute(
        "SELECT date, market_value FROM portfolio_snapshots WHERE code=? AND date IN (?, ?)",
        (code, date, prev),
    )
    m = {r[0]: r[1] for r in cur.fetchall()}
    mv_cur = m.get(date)
    mv_prev = m.get(prev)
    if mv_cur is None or mv_prev is None or mv_prev == 0:
        return None
    try:
        return float(mv_cur) / float(mv_prev)
    except (TypeError, ValueError):
        return None


def classify_split_merge(qty_ratio: Optional[float],
                        mv_ratio: Optional[float]) -> bool:
    """纯函数判据（不触库），供单测与批量扫描复用。

    Args:
        qty_ratio: quantity(date)/quantity(prev)；None 表示无法计算。
        mv_ratio:  market_value(date)/market_value(prev)；None 表示无法计算。

    Returns:
        该 (code, date) 是否为拆分/合并伪收益日（True=需排除/标记；False=正常或跳过）。
    """
    if qty_ratio is None:
        return False
    # 1) C 族（月末估值切换等）跳过带
    if VAL_SWITCH_QTY_MIN <= qty_ratio <= VAL_SWITCH_QTY_MAX:
        return False
    # 2) 真拆分/合并：qty 大幅变化 且 market_value 连续（价值守恒）。
    #    mv_ratio 必须为已知且连续，否则无法排除"真实申赎"，保守不判。
    if (abs(qty_ratio - 1.0) > SPLIT_QTY_DEV
            and mv_ratio is not None
            and abs(mv_ratio - 1.0) < MV_CONTINUITY):
        return True
    # 3) §三 A 族变体：qty 几乎不变但 mv 跳变（价格未复权、数量未复权）
    if (abs(qty_ratio - 1.0) < A_FAMILY_QTY_FLAT
            and mv_ratio is not None
            and abs(mv_ratio - 1.0) > A_FAMILY_MV_JUMP):
        return True
    return False


def is_split_merge_pseudo_return(conn, code: str, date: str) -> bool:
    """给定 (code, date)，判别该标的当日是否为拆分/合并伪收益日（读 portfolio_snapshots）。

    这是 Task 1 要求暴露给下游的核心原子接口。下游可逐标的/逐日调用，
    也可直接调用 split_merge_pseudo_return_dates 拿组合级"拆分日集合"。
    """
    cur = conn.cursor()
    qty_ratio = compute_qty_ratio(cur, code, date)
    mv_ratio = compute_mv_ratio(cur, code, date)
    return classify_split_merge(qty_ratio, mv_ratio)


def split_merge_pseudo_return_dates(conn,
                                    codes: Optional[Iterable[str]] = None,
                                    start_date: Optional[str] = None,
                                    end_date: Optional[str] = None) -> Set[str]:
    """扫描 portfolio_snapshots，返回所有"拆分/合并伪收益日"的 date 集合（组合级）。

    一个 date 只要任一持有标的命中 is_split_merge_pseudo_return 即纳入集合——
    这正是消费侧（portfolio_summary.daily_return 是组合级单值）需要排除/标记的日期。

    性能：逐 code 取序列表（一次查询），在 Python 内用 prev 行做相邻比较，
    不逐日回查库。34 只标的 × 数年日频约十万余行，单次重建可接受。
    """
    cur = conn.cursor()
    if codes is not None:
        code_list = list(codes)
    else:
        cur.execute("SELECT DISTINCT code FROM portfolio_snapshots")
        code_list = [r[0] for r in cur.fetchall()]

    out: Set[str] = set()
    for code in code_list:
        q = ("SELECT date, quantity, market_value FROM portfolio_snapshots WHERE code=?")
        params: list = [code]
        if start_date:
            q += " AND date >= ?"
            params.append(start_date)
        if end_date:
            q += " AND date <= ?"
            params.append(end_date)
        q += " ORDER BY date"
        cur.execute(q, params)
        rows = cur.fetchall()
        prev = None  # (date, quantity, market_value)
        for date, qty, mv in rows:
            if prev is not None:
                p_date, p_qty, p_mv = prev
                qty_ratio = None
                if qty is not None and p_qty not in (None, 0):
                    try:
                        qty_ratio = float(qty) / float(p_qty)
                    except (TypeError, ValueError):
                        qty_ratio = None
                mv_ratio = None
                if mv is not None and p_mv not in (None, 0):
                    try:
                        mv_ratio = float(mv) / float(p_mv)
                    except (TypeError, ValueError):
                        mv_ratio = None
                if classify_split_merge(qty_ratio, mv_ratio):
                    out.add(str(date)[:10])
            prev = (date, qty, mv)
    return out
