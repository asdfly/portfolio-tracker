"""复制行旁路表 —— 消费侧只读查询封装（问题十一）。

背景（docs/handover/07_known_data_issues.md「问题十一」）：
`portfolio_snapshots` 存在非观测的复制行（fill-forward 陈旧行）。生产库已落地旁路表
`portfolio_snapshots_replica_void(code, date)`，登记被两档判据
（`src/analysis/portfolio_risk._scan_replica_rows`）判为非观测的 (code, date)。

本模块是消费侧的**只读**查询封装。所有消费方（历史回填、日报、报告渲染）据此跳过 / 标记
void 行，**不改不删**历史快照。判据与 `portfolio_risk._scan_replica_rows` 同源，本表是其
结果的持久化落盘，供跨进程 / 跨日稳定复用（避免每次重跑两档扫描的非确定性 Tier2）。

核心语义（务必与 docs 一致，禁止静默篡改）：
- void 行 = 该行装的是更早真实值的复制，不是当日有效观测。
- 对 `total_value`：void 行的市值按「最近一个非 void 观测的市值」向后结转（fill-forward），
  即持仓在该窗口内视为「未有新观测、沿用上次已知值」。本库数据下 void 行本身已持有上次
  已知值，故结转后数值不变（**非破坏性**），但口径被显式固定，未来若 void 值偏离上次已知
  值也能正确结转。
- 对 `daily_return`（共同持仓法）：void 行**不进分子也不进分母**（与口径 B 同理：非观测
  不参与收益比较）。这能消除 06-30 因「陈旧→真值」水平修正被误算成单日收益 +3.78% 的伪
  跳变（排除 void 后约 +0.75%，为正常日收益）。
"""
from __future__ import annotations

import sqlite3
from typing import Dict, Iterable, List, Optional, Set


def void_codes_on(conn: sqlite3.Connection, date_str: str) -> Set[str]:
    """返回 date_str 当日被登记为 void 的 code 集合（空集合表示无 void）。"""
    rows = conn.execute(
        "SELECT code FROM portfolio_snapshots_replica_void WHERE date = ?",
        (str(date_str),),
    ).fetchall()
    return {r[0] for r in rows}


def is_replica_void(conn: sqlite3.Connection, code: str, date_str: str) -> bool:
    """(code, date) 是否被登记为 void。"""
    row = conn.execute(
        "SELECT 1 FROM portfolio_snapshots_replica_void WHERE code = ? AND date = ?",
        (str(code), str(date_str)),
    ).fetchone()
    return row is not None


def effective_market_value(
    conn: sqlite3.Connection, code: str, date_str: str,
    fallback: Optional[float] = None,
) -> Optional[float]:
    """void 行的市值按最近一个非 void 观测向后结转；非 void 直接返回自身市值。

    向后（date<=date_str）逐行找第一个非 void 的市值；找不到则回退到当日自身（可能仍是
    void 值，但已无更优选择），再不行用 fallback。
    """
    rows = conn.execute(
        "SELECT date, market_value FROM portfolio_snapshots "
        "WHERE code = ? AND date <= ? ORDER BY date DESC",
        (str(code), str(date_str)),
    ).fetchall()
    for d, mv in rows:
        if is_replica_void(conn, code, d):
            continue
        return float(mv) if mv is not None else fallback
    self_row = conn.execute(
        "SELECT market_value FROM portfolio_snapshots WHERE code = ? AND date = ?",
        (str(code), str(date_str)),
    ).fetchone()
    if self_row and self_row[0] is not None:
        return float(self_row[0])
    return fallback


def void_aware_total_value(conn: sqlite3.Connection, date_str: str) -> float:
    """当日组合市值：非 void 行用自身市值，void 行用向后结转市值。

    本库数据下 void 行已持有上次已知值，故结果与原始 SUM(market_value) 一致（非破坏性），
    但口径被显式固定为「void 行结转」，未来若 void 值偏离也能正确结转。
    """
    rows = conn.execute(
        "SELECT code, market_value FROM portfolio_snapshots WHERE date = ?",
        (str(date_str),),
    ).fetchall()
    if not rows:
        return 0.0
    voids = void_codes_on(conn, date_str)
    total = 0.0
    for code, mv in rows:
        if code in voids:
            eff = effective_market_value(conn, code, date_str, fallback=0.0)
            total += eff if eff is not None else 0.0
        else:
            total += float(mv) if mv is not None else 0.0
    return total


def void_excluded_common_codes(
    common_codes: Iterable[str],
    void_prev: Set[str],
    void_curr: Set[str],
) -> Set[str]:
    """从共同持仓集合里剔除任一端为 void 的标的（非观测不参与收益比较）。"""
    vp = void_prev or set()
    vc = void_curr or set()
    return {c for c in common_codes if c not in vp and c not in vc}


def snapshot_rows_with_void_flag(
    conn: sqlite3.Connection, date_str: str,
) -> List[Dict]:
    """返回当日快照行（dict），附加 `is_void` 标志与 `eff_market_value`（结转后市值）。

    供报告渲染侧标记「数据陈旧 / 复制行」而非把陈旧值当当日有效值展示。
    """
    rows = conn.execute(
        "SELECT * FROM portfolio_snapshots WHERE date = ? ORDER BY market_value DESC",
        (str(date_str),),
    ).fetchall()
    voids = void_codes_on(conn, date_str)
    out: List[Dict] = []
    for r in rows:
        d = dict(r)
        code = d.get("code")
        is_v = code in voids
        d["is_void"] = is_v
        d["eff_market_value"] = (
            effective_market_value(conn, code, date_str, fallback=0.0)
            if is_v else float(d.get("market_value") or 0.0)
        )
        out.append(d)
    return out
