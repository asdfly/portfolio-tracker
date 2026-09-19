"""单位净值账本引擎（P0-2）

解决的问题与决策：
- portfolio_summary.daily_return 用"共同持仓法"（基于价格/数量，剔除大部分申赎扰动），
  是当前最可靠的日收益口径；但 projects 缺少 unit_nav 账本，无法做基准对比/回撤/归因的公共底座。
- portfolio_summary.total_value 序列存在失真跳变（实测 2026-07-31 +75.6%、2026-06-15 +105%，
  集中在月末/季末，且同日 trade_records 现金流完全无法解释），直接用 Modified Dietz 反推会爆炸
  （实测 TWR 累计算出 62499%）。

因此本模块采用**稳健主口径 + 交叉校验**策略：
- 主口径 r_t = daily_return_t / 100（基于价格/数量，不受 total_value 失真污染），累积得 unit_nav 与 TWR；
- 交叉校验：用 total_value + 当日现金流算 Modified Dietz，若与 daily_return 差异 > 30%，
  标记该日 is_suspect（total_value 失真），并记日志告警；
- net_flow 仍从 trade_records 聚合记录，供展示与后续 MWR（资金加权收益）使用。

待 P0-3 修好 total_value 回填质量后，可平滑切换到纯现金流 TWR（保留同一张表与下游接口）。
"""
from __future__ import annotations

import logging
import sqlite3
from typing import Dict, Optional

import numpy as np
import pandas as pd

from config.settings import DATABASE_PATH

logger = logging.getLogger(__name__)

# 买入/卖出方向识别（与 trade_importer.normalize_direction 对齐）
_BUY_ACTIONS = {"证券买入", "买入", "BUY", "买"}
_SELL_ACTIONS = {"证券卖出", "卖出", "SELL", "卖"}

# 交叉校验阈值：Modified Dietz 与 daily_return 差异超过此值视为 total_value 失真
_SUSPECT_DIVERGENCE = 0.30

# MWR（资金加权收益）合理性区间 —— 超出即判定 IRR 求解器发散，返回 None。
# 下界 -0.99：组合亏到接近清零的物理下限（二分法搜索下界亦为此值）。
# 上界 5.0（+500%）：超过此量级的解几乎必然是现金流符号错误/多根导致的伪根。
#   实测 2026-09-15 重建时解出 10.444292（+1044%），而同期 TWR 仅 +162.62%，
#   差 882pp，明显发散。注意不要用 mwr < 10 这类宽区间——+900% 同样是错的。
# 修复 IRR 算法本身是另一个专项（2026-09-15 已完成）：根因为 NPV 方程漏掉 v0 项，
# 已补回（见 _solve_period_irr）；但全周期 IRR 对 14 年长历史仍病态（补 v0 后仍 +680%），
# 故 mwr_return 列改用滚动 1 年（年化）Modified Dietz（见 _trailing_mwr_annualized），
# 本函数仅保留作诊断用途，不再填充列。
_MWR_MIN = -0.99
_MWR_MAX = 5.0


def get_db_connection() -> sqlite3.Connection:
    """复用项目统一的数据库路径建立连接"""
    return sqlite3.connect(str(DATABASE_PATH))


def _daily_net_cashflow(conn: sqlite3.Connection) -> Dict[str, float]:
    """从 trade_records 聚合每日期望净现金流（流入为正）。

    优先级：
    1. change_amount 若存在且非零，直接采用（实测该列对所有 action 都有值且符号合理：
       买入/申购/回购拆出为负=流出，卖出/赎回/银行转存/红利为正=流入）
    2. 否则按 action/quantity/price/费用估算
    """
    try:
        df = pd.read_sql_query(
            "SELECT date, action, quantity, price, commission, stamp_tax, change_amount "
            "FROM trade_records",
            conn,
        )
    except sqlite3.OperationalError:
        return {}

    if df.empty:
        return {}

    cf: Dict[str, float] = {}
    for _, r in df.iterrows():
        d = str(r["date"])[:10]
        action = str(r["action"] or "").strip()
        qty = float(r["quantity"] or 0)
        price = float(r["price"] or 0)
        comm = float(r["commission"] or 0)
        tax = float(r["stamp_tax"] or 0)
        ca = r["change_amount"]

        if ca is not None and not pd.isna(ca) and float(ca) != 0:
            flow = float(ca)
        elif action in _BUY_ACTIONS:
            flow = -(abs(qty) * price + comm + tax)
        elif action in _SELL_ACTIONS:
            flow = abs(qty) * price - comm - tax
        else:
            flow = 0.0  # 分红/其他非现金流动作暂不计入 net_flow

        cf[d] = cf.get(d, 0.0) + flow

    return cf


def _daily_dividend_cash(conn: sqlite3.Connection) -> Dict[str, float]:
    """从 trade_records 聚合每日分红/红利现金（股息入账、产品红利发放等）。

    返回 {date: 分红现金总额}（仅正流入），用于 P0-5 含分红收益计算。
    与 _daily_net_cashflow 区分：分红是收益分配，计入收益口径而非外部现金流。
    """
    try:
        df = pd.read_sql_query(
            "SELECT date, action, change_amount FROM trade_records", conn
        )
    except sqlite3.OperationalError:
        return {}
    if df.empty:
        return {}
    _DIV_ACTIONS = {"股息入账", "产品红利发放", "红利发放", "股息"}
    total: Dict[str, float] = {}
    for _, r in df.iterrows():
        action = str(r["action"] or "").strip()
        if action not in _DIV_ACTIONS:
            continue
        ca = r["change_amount"]
        if ca is None or pd.isna(ca):
            continue
        d = str(r["date"])[:10]
        total[d] = total.get(d, 0.0) + float(ca)
    return total


def _solve_period_irr(df: pd.DataFrame, cf: Dict[str, float],
                      div: Dict[str, float]) -> Optional[float]:
    """全周期资金加权收益率（IRR，持有期口径，含分红）。

    - 外部现金流 cf：入金(buy/申购/回购) < 0，出金(sell/赎回/转存) > 0（不含分红）。
      转换为投资者视角入金(正)参与 IRR 方程。
    - 分红 div：视为已分配收益，加到终点价值。
    用二分法解 NPV(R)=0。无外部现金流或 IRR 无实根时返回 None（此时 TWR 即足够）。

    护栏：解出的 mwr 若非有限值或超出 [_MWR_MIN, _MWR_MAX]（默认 -0.99 ~ +5），
    判定为 IRR 发散，返回 None 并 logger.warning 打印输入摘要。
    （2026-09-15 加：此前无此护栏，曾把 10.444292 即 +1044% 的伪根写入全表。）

    ⚠️ 2026-09-15 起本函数**仅保留作诊断**：mwr_return 列改由
    `_trailing_mwr_annualized` 填充。全周期 IRR 对 14 年长历史病态（补 v0 后仍 +680%），
    单期 Modified Dietz 年化才是稳健口径。
    """
    n = len(df)
    if n < 2:
        return None
    dates = [d.strftime("%Y-%m-%d") for d in df["date"]]
    values = df["total_value"].astype(float).tolist()
    vt = values[-1]
    v0 = values[0]   # 期初持仓市值：必须作为 t=0 的投资进入 NPV 方程，否则会把
                     # 「初始本金 + 历史收益」全算到中途入金头上，解出差一个数量级的伪根。
                     # 实测缺该项时解出 +1044%（10.444292），补回后仍 +680%——
                     # 说明全周期 IRR 对 14 年长历史本身不可靠（见 _trailing_mwr_annualized）。
                     # 补 v0 是必要正确性修复，但本函数仅作诊断保留，不再用于填充 mwr_return 列。
    t = n - 1
    flows = []  # (投资者入金额>0, 时间权重指数 (T-i)/T)
    div_total = 0.0
    for i in range(1, n):
        dep = -float(cf.get(dates[i], 0.0))  # 投资者入金为正
        if dep != 0:
            flows.append((dep, (t - i) / t))
        div_total += float(div.get(dates[i], 0.0))
    if not flows:
        return None  # 无外部现金流，MWR 退化为 TWR
    vt_adj = vt + div_total

    # 输入摘要（IRR 发散时打印，便于定位是现金流符号还是多根问题）
    net_in = sum(d for d, _ in flows)
    _summary = (
        f"MWR 输入摘要: 期间 {dates[0]}~{dates[-1]} ({n} 天), "
        f"现金流 {len(flows)} 笔, 净流入 {net_in:,.2f}, "
        f"首笔 {flows[0][0]:,.2f}@{dates[1] if n > 1 else '-'}, "
        f"末笔 {flows[-1][0]:,.2f}, 分红合计 {div_total:,.2f}, "
        f"期初市值 {values[0]:,.2f}, 期末市值 {vt:,.2f}"
    )

    def _guard(r: Optional[float], reason: str) -> Optional[float]:
        """护栏：越界/非有限值一律返回 None，并记录输入摘要。"""
        if r is None:
            return None
        if not np.isfinite(r) or not (_MWR_MIN <= r <= _MWR_MAX):
            logger.warning(
                "MWR 求解结果 %.6f 超出合理区间 [%.2f, %.2f]，判定 IRR 发散，本次不产出 MWR。"
                "（%s）原因: %s", r, _MWR_MIN, _MWR_MAX, _summary, reason,
            )
            return None
        return r

    def npv(r: float) -> float:
        s = vt_adj
        s -= v0 * (1 + r) ** 1.0   # 初始持仓视为 t=0 投资（2026-09-15 补，原缺此项）
        for dep, frac in flows:
            s -= dep * (1 + r) ** frac
        return s

    lo, hi = -0.99, 100.0
    f_lo, f_hi = npv(lo), npv(hi)
    if f_lo * f_hi > 0:
        logger.warning("MWR 无实根（NPV 在区间两端同号），本次不产出 MWR。（%s）", _summary)
        return None
    for _ in range(200):
        mid = (lo + hi) / 2
        f_mid = npv(mid)
        if abs(f_mid) < 1e-9:
            return _guard(mid, "精确解")
        if f_lo * f_mid < 0:
            hi, f_hi = mid, f_mid
        else:
            lo, f_lo = mid, f_mid
    return _guard((lo + hi) / 2, "二分未收敛到 1e-9，返回区间中点")


def _trailing_mwr_annualized(df: pd.DataFrame, cf: Dict[str, float], div: Dict[str, float],
                             window: int = 365) -> list:
    """滚动 `window` 个交易日的**年化**资金加权收益（单期 Modified Dietz）。

    逐行填充到 portfolio_nav.mwr_return，这是经纪商口径的「近 1 年收益（资金加权）」。

    **为什么不用全周期 IRR（`_solve_period_irr`）：**
    对 14 年、3477 行、现金流相对早期市值巨大的序列，全周期 IRR 病态：
    早期市值小 + 现金流相对大 + 月末 `total_value` 失真，解出的伪根实测可达
    +680%（补 v0 项后）甚至 +1044%（缺 v0 项时）；链式逐日 MD 更爆炸到 ~+4325%。
    根因不是方程 bug，而是「单期 IRR 对超长历史不可靠」。

    **改用滚动 1 年单期 Modified Dietz 年化：**
    窗口短 → 现金流相对市值小 → 月末失真被摊销 → 稳定可用。
    实测最近数个交易日连续给出 ~+43% 的稳定值（同数据下链式法 ~+270%，故只用单期 MD，不链式）。

    公式（窗口 [start, idx]，投资视角入金为正）：
        md_window = (vt_adj - v_begin - net_in) / (v_begin + net_in * 0.5)
        mwr_annualized = (1 + md_window) ** (365 / days) - 1

    护栏（任一条命中 → 该行 None，不产出错误数字）：
        v_begin <= 0 或分母 <= 0（早期低市值 + 大额出入金导致）
        md / mwr 非有限值
        mwr 年化超出 [_MWR_MIN, _MWR_MAX]
    """
    n = len(df)
    if n < 2:
        return [None] * n
    dates = [d.strftime("%Y-%m-%d") for d in df["date"]]
    values = df["total_value"].astype(float).tolist()
    # 逐日投资者入金（dep = -cf，入金为正）与分红，0-indexed 累计
    dep_arr = np.zeros(n)
    div_arr = np.zeros(n)
    for i in range(1, n):
        dep_arr[i] = -float(cf.get(dates[i], 0.0))
        div_arr[i] = float(div.get(dates[i], 0.0))
    net_in_cum = np.cumsum(dep_arr)
    div_cum = np.cumsum(div_arr)

    out = []
    for idx in range(n):
        start = max(0, idx - window)
        v_begin = values[start]
        vt = values[idx]
        net_in = float(net_in_cum[idx] - net_in_cum[start])
        div_win = float(div_cum[idx] - div_cum[start])
        vt_adj = vt + div_win
        denom = v_begin + net_in * 0.5
        if v_begin <= 0 or denom <= 0:
            out.append(None)
            continue
        md = (vt_adj - v_begin - net_in) / denom
        days = idx - start
        if days <= 0 or not np.isfinite(md):
            out.append(None)
            continue
        try:
            mwr = (1.0 + md) ** (365.0 / days) - 1.0
        except (ValueError, OverflowError):
            out.append(None)
            continue
        if not np.isfinite(mwr) or not (_MWR_MIN <= mwr <= _MWR_MAX):
            out.append(None)
            continue
        out.append(float(mwr))
    return out


def rebuild_portfolio_nav(conn: Optional[sqlite3.Connection] = None) -> int:
    """全量重建 portfolio_nav 单位净值账本（幂等：INSERT OR REPLACE）。

    主口径用 daily_return（稳健），交叉校验 total_value 失真并标记 is_suspect。

    Args:
        conn: 可选外部连接；为 None 时内部自建并在结束时关闭。

    Returns:
        写入/更新的行数
    """
    own = conn is None
    if own:
        conn = get_db_connection()
    try:
        df = pd.read_sql_query(
            "SELECT date, total_value, daily_return FROM portfolio_summary ORDER BY date", conn
        )
        if df.empty or len(df) < 2:
            logger.info("portfolio_nav 重建跳过: portfolio_summary 数据不足(<2行)")
            return 0

        cf = _daily_net_cashflow(conn)
        div = _daily_dividend_cash(conn)
        df["date"] = pd.to_datetime(df["date"])
        df["dr"] = df["daily_return"] / 100.0  # 主口径日收益（小数，基于价格）

        unit_nav = 1.0
        prev_nav = 1.0
        prev_v = float(df["total_value"].iloc[0])
        twr_cum = 0.0  # 含分红再投资的累计时间加权收益（P0-5）

        rows = []
        suspects = []
        for i in range(len(df)):
            d = df["date"].iloc[i]
            v = float(df["total_value"].iloc[i])
            d_str = d.strftime("%Y-%m-%d")
            c = float(cf.get(d_str, 0.0))

            if i == 0:
                r = 0.0
                r_total = 0.0
            else:
                # 主口径：daily_return（共同持仓法，基于价格/数量，不受 total_value 失真污染）
                r = float(df["dr"].iloc[i])
                # 交叉校验：用 total_value + 现金流算 Modified Dietz（仅用价格口径 r）
                denom = prev_v + c * 0.5
                md = (v - prev_v - c) / denom if denom > 0 else 0.0
                if abs(md - r) > _SUSPECT_DIVERGENCE:
                    suspects.append(d_str)
                # 含分红（P0-5）：当日分红收益率 = 分红现金 / 当日市值，再投资口径
                div_yield = (div.get(d_str, 0.0) / v) if v > 0 else 0.0
                r_total = r + div_yield
                twr_cum = (1 + twr_cum) * (1 + r_total) - 1

            unit_nav = prev_nav * (1 + r_total)
            rows.append(
                {
                    "date": d_str,
                    "unit_nav": round(unit_nav, 6),
                    "total_units": round(v / unit_nav, 2) if unit_nav else None,
                    "total_value": round(v, 2),
                    "net_flow": round(c, 2),
                    "twr_cumulative": round(twr_cum, 6),
                    "mwr_return": None,  # 全周期 MWR 在循环后统一计算填充
                    "is_suspect": d_str in suspects,
                }
            )
            prev_v = v
            prev_nav = unit_nav

        # 资金加权收益（MWR）：滚动 1 年（年化）单期 Modified Dietz。
        # 全周期 IRR 对 14 年长历史病态（见 _solve_period_irr / docs 问题四），
        # 改用逐行滚动口径，结果稳定且接近经纪商「近 1 年收益」语义。
        mwr_list = _trailing_mwr_annualized(df, cf, div)
        for i, row in enumerate(rows):
            v = mwr_list[i]
            row["mwr_return"] = round(v, 6) if v is not None else None

        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS portfolio_nav (
                date TEXT PRIMARY KEY,
                unit_nav REAL,
                total_units REAL,
                total_value REAL,
                net_flow REAL,
                twr_cumulative REAL,
                mwr_return REAL,
                is_suspect BOOLEAN DEFAULT 0
            )
            """
        )
        # 兼容旧表（早期版本无 is_suspect 列）：自动补列，避免 INSERT 列不匹配
        try:
            cur.execute("ALTER TABLE portfolio_nav ADD COLUMN is_suspect BOOLEAN DEFAULT 0")
        except sqlite3.OperationalError:
            pass  # 列已存在
        cur.executemany(
            """
            INSERT OR REPLACE INTO portfolio_nav
                (date, unit_nav, total_units, total_value, net_flow, twr_cumulative, mwr_return, is_suspect)
            VALUES (:date, :unit_nav, :total_units, :total_value, :net_flow, :twr_cumulative, :mwr_return, :is_suspect)
            """,
            rows,
        )
        conn.commit()

        if suspects:
            logger.warning(
                f"portfolio_nav: {len(suspects)} 天 total_value 跳变与日收益/现金流不符"
                f"(疑似数据失真), 已标记 is_suspect; TWR 仍按 daily_return 计算。首例: {suspects[:3]}"
            )
        logger.info(
            f"portfolio_nav 重建完成: {len(rows)} 行, 累计TWR {twr_cum*100:.2f}%, suspect {len(suspects)} 天"
        )
        return len(rows)
    finally:
        if own:
            conn.close()


def get_nav_series(conn: Optional[sqlite3.Connection] = None) -> pd.DataFrame:
    """读取 portfolio_nav 序列，便于下游（基准对比/回撤/归因）消费。

    ⚠️ `mwr_return` 现为**滚动 1 年（年化）单期 Modified Dietz**口径
    （见 `_trailing_mwr_annualized`），替代原全周期 IRR（对 14 年长历史病态，
    见 `_solve_period_irr` / docs 问题四）。逐行填入，**早期低市值 + 大额出入金
    时点仍为 NULL**。下游展示前**仍须判空**：`None` / `NaN` 显示"暂不可用"，
    **不要**渲染成 nan / None / 0，也不要用 0 参与任何运算（会被当成"收益为零"）。
    另见 docs/handover/07_known_data_issues.md「问题四」。
    """
    own = conn is None
    if own:
        conn = get_db_connection()
    try:
        df = pd.read_sql_query(
            "SELECT date, unit_nav, total_value, net_flow, twr_cumulative, mwr_return, is_suspect "
            "FROM portfolio_nav ORDER BY date",
            conn,
        )
        return df
    finally:
        if own:
            conn.close()


if __name__ == "__main__":
    n = rebuild_portfolio_nav()
    print(f"rebuilt {n} rows")
    nav = get_nav_series()
    if not nav.empty:
        print(nav.tail(5).to_string(index=False))
