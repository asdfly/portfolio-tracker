#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""重算指定窗口的 portfolio_summary（12 列），并可选连续重建 portfolio_nav。

背景
----
场外 13 只基金此前只有月末快照，2026-08-03 起整批缺失，导致 portfolio_summary
从 08-03 起只有场内 22 只口径（total_value 蒸发约 57 万，sharpe / max_drawdown /
volatility / profit_count 等指标全部残缺）。
`scripts/fetch_otc_fund_nav.py` 补上 347 行快照后，本脚本重算汇总层。

口径说明
--------
- 前 8 列（total_value / total_cost / total_pnl / daily_pnl / daily_return /
  profit_count / loss_count）：复刻 `src/utils/backfill.py:86-146`，
  用 common_codes（前后两日共同持仓、quantity 取前一日）算 daily_return，
  经 `audit/validate_algo.py` 验证可 30/30 复现现库旧值。
- 后 4 列（vs_hs300 / sharpe_ratio / max_drawdown / volatility）：复刻
  `scripts/backfill/backfill_full_history.py:432-477`，从 portfolio_summary
  自身的 daily_return 序列（前 60 个交易日）+ index_quotes 算，不依赖外部持仓。

⚠️ 已知：重算前这 4 列本身就是双来源混合（08-03~09-01 是回填脚本口径、
09-02 起是日常 risk-analyzer 口径）。本脚本统一为回填脚本口径，窗口内自洽。

用法
----
    # 先备份
    venv313\\Scripts\\python.exe scripts\\recompute_summary_window.py --backup

    # dry-run（默认，只读）
    venv313\\Scripts\\python.exe scripts\\recompute_summary_window.py

    # 落地：重算 summary + 连续重建 portfolio_nav（二三步不停在中问态）
    venv313\\Scripts\\python.exe scripts\\recompute_summary_window.py --apply --rebuild-nav
"""

import argparse
import os
import shutil
import sqlite3
import statistics
import sys
from datetime import datetime

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from config.settings import DATABASE_PATH  # noqa: E402

DEFAULT_START = "2026-08-03"
DEFAULT_END = "2026-09-14"
MIN_RISK_DATE = "2025-08-01"        # 与 backfill_full_history.py:439 一致
RISK_FREE_RATE = 0.025              # 与 backfill_full_history.py:459 一致
HIST_DAYS = 60                      # 与 backfill_full_history.py:442 一致


# --------------------------------------------------------------------------
def backup(db_path: str) -> str:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dst = f"{db_path}.bak_recompute_{stamp}"
    shutil.copyfile(db_path, dst)
    print(f"[备份] {dst}  ({os.path.getsize(dst):,} bytes)")
    return dst


def resolve_dates(cur, start: str, end: str):
    """解析窗口内需要重算的日期 —— 取 portfolio_snapshots 与 portfolio_summary 的**并集**。

    返回 (dates, snapshots_only, summary_only)：

    - `dates`          窗口内应重算的日期（升序）
    - `snapshots_only` 只有快照、没有汇总行的日期（= 缺口日）。**必须纳入**：
      旧实现只取 `portfolio_summary` 自己，缺口日永远进不了清单，
      实测窗口 2026-09-03 ~ 2026-09-16 只覆盖 7 天、漏掉 09-03 与 09-15，
      而"缺行 ⇒ 下一日的 daily_return 变成多日值"正是本脚本要修的那个缺陷。
    - `summary_only`   只有汇总行、没有快照的日期（反方向）。算不出 12 列，
      `compute()` 会跳过它 —— 必须显式报出，不能静默丢掉。
    """
    snaps = {r[0] for r in cur.execute(
        "SELECT DISTINCT date FROM portfolio_snapshots WHERE date BETWEEN ? AND ?",
        (start, end)).fetchall()}
    sums = {r[0] for r in cur.execute(
        "SELECT date FROM portfolio_summary WHERE date BETWEEN ? AND ?",
        (start, end)).fetchall()}
    return sorted(snaps | sums), sorted(snaps - sums), sorted(sums - snaps)


def compute(conn, start: str, end: str):
    """返回 {date: {...12 列...}}，只读，不写库。"""
    cur = conn.cursor()
    dates, snaps_only, sums_only = resolve_dates(cur, start, end)
    if snaps_only:
        print(f"[缺口] 窗口内 {len(snaps_only)} 个日期只有快照、没有汇总行，将补算: {snaps_only}")
    if sums_only:
        print(f"[警告] 窗口内 {len(sums_only)} 个日期只有汇总行、没有快照，"
              f"算不出 12 列、将被跳过（请人工确认是否正常）: {sums_only}")

    # 前驱日期同样取并集：若紧邻窗口起点之前是缺口日（只有快照），
    # 只认 summary 会把 prev_dt 再往前跨一格、把缺口复制到窗口第一天的收益上。
    # 注：对当前窗口（09-03 起点前无缺口）与原实现逐位等价。
    prev_dates = [r[0] for r in cur.execute(
        "SELECT date FROM portfolio_summary WHERE date < ? "
        "UNION SELECT DISTINCT date FROM portfolio_snapshots WHERE date < ? "
        "ORDER BY date", (start, start)).fetchall()]
    hist_all = {r[0]: r[1] for r in cur.execute(
        "SELECT date, daily_return FROM portfolio_summary").fetchall()}

    out = {}
    for dt in dates:
        row = cur.execute(
            "SELECT SUM(market_value), SUM(cost_price*quantity), SUM(pnl),"
            "       SUM(CASE WHEN pnl>=0 THEN 1 ELSE 0 END),"
            "       SUM(CASE WHEN pnl<0 THEN 1 ELSE 0 END) "
            "FROM portfolio_snapshots WHERE date=?", (dt,)).fetchone()
        total_value, total_cost, total_pnl, pc, lc = row
        if not total_value:
            continue

        # --- daily_pnl / daily_return：common_codes 口径 ---
        prev_dt = max([d for d in prev_dates if d < dt], default=None)
        daily_pnl, daily_return = 0.0, 0.0
        if prev_dt:
            prev_s = {r[0]: (r[1], r[2]) for r in cur.execute(
                "SELECT code,quantity,market_value FROM portfolio_snapshots WHERE date=?",
                (prev_dt,)).fetchall()}
            curr_s = {r[0]: (r[1], r[2], r[3]) for r in cur.execute(
                "SELECT code,quantity,market_value,current_price FROM portfolio_snapshots WHERE date=?",
                (dt,)).fetchall()}
            common = set(prev_s) & set(curr_s)
            pamv = sum(curr_s[c][2] * prev_s[c][0] for c in common)
            pcmv = sum(prev_s[c][1] for c in common)
            if pcmv > 0:
                daily_pnl = pamv - pcmv
                daily_return = daily_pnl / pcmv * 100

        # --- vs_hs300 ---
        idx = cur.execute(
            "SELECT change_pct FROM index_quotes WHERE code='sh000300' AND date=?", (dt,)).fetchone()
        vs_hs300 = (daily_return - idx[0]) if (idx and idx[0] is not None) else 0

        # --- 风险三列：前 60 个交易日 daily_return（逆序，与回填脚本一致）---
        hist = [hist_all[d] for d in prev_dates
                if d >= MIN_RISK_DATE and hist_all.get(d) is not None]
        hist = hist[-HIST_DAYS:][::-1]

        sharpe = vol = max_dd = None
        if len(hist) >= 20:
            avg = statistics.mean(hist)
            std = statistics.stdev(hist)
            if std > 0:
                annual_ret = avg / 100 * 252
                annual_vol = std / 100 * (252 ** 0.5)
                sharpe = round((annual_ret - RISK_FREE_RATE) / annual_vol, 4) if annual_vol > 0 else 0
            vol = round(std * (252 ** 0.5), 4)
        if len(hist) >= 5:
            cumret = peak = 1.0
            dd = 0.0
            for x in hist:
                cumret *= (1 + x / 100)
                if cumret > peak:
                    peak = cumret
                if peak > 0:
                    dd = max(dd, (peak - cumret) / peak * 100)
            max_dd = dd

        out[dt] = dict(total_value=total_value, total_cost=total_cost, total_pnl=total_pnl,
                       daily_pnl=daily_pnl, daily_return=daily_return, vs_hs300=vs_hs300,
                       profit_count=pc, loss_count=lc, sharpe_ratio=sharpe,
                       max_drawdown=max_dd, volatility=vol)
        if dt not in prev_dates:
            prev_dates.append(dt)
            prev_dates.sort()
        hist_all[dt] = daily_return      # 递推进 hist，供后续日期使用
    return out


def apply_summary(conn, computed):
    cur = conn.cursor()
    cur.execute("BEGIN")
    for dt, v in sorted(computed.items()):
        cur.execute(
            "INSERT OR REPLACE INTO portfolio_summary "
            "(date, total_value, total_cost, total_pnl, daily_pnl, daily_return,"
            " vs_hs300, profit_count, loss_count, sharpe_ratio, max_drawdown, volatility,"
            " snapshot_type) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,"
            " COALESCE((SELECT snapshot_type FROM portfolio_summary WHERE date=?),'daily'))",
            (dt, v["total_value"], v["total_cost"], v["total_pnl"], v["daily_pnl"],
             v["daily_return"], v["vs_hs300"], v["profit_count"], v["loss_count"],
             v["sharpe_ratio"], v["max_drawdown"], v["volatility"], dt))
    conn.commit()
    return len(computed)


# --------------------------------------------------------------------------
# 对照表渲染
# --------------------------------------------------------------------------
_MISSING = "<无行>"
_TABLE_HEADER = (f"{'date':<12}{'tv旧':>13}{'tv新':>13}{'dr旧%':>9}{'dr新%':>9}"
                 f"{'sharpe旧':>10}{'sharpe新':>10}{'mdd旧':>8}{'mdd新':>8}"
                 f"{'vol旧':>9}{'vol新':>9}{'盈亏旧':>8}{'盈亏新':>8}{'标记':>6}")


def _cell_money(v, w: int) -> str:
    return _MISSING.rjust(w) if v is None else format(v, f">{w},.0f")


def _cell_float3(v, w: int) -> str:
    return _MISSING.rjust(w) if v is None else format(v, f">{w}.3f")


def _cell_str(v, w: int) -> str:
    """整行不存在时由调用方直接给 `<无行>`；本函数用于"行存在"的列。

    该列为 NULL 时原样显示 `None`，与"整个日期在旧库里没有行"的 `<无行>`
    明确区分 —— 两者含义完全不同，不能渲染成同一个字符串。
    """
    return format(str(v), f">{w}")


def format_diff_table(computed: dict, old: dict):
    """渲染窗口重算对照表，返回 (行列表, 新增日期列表)。

    ⚠️ `old` 里【可能没有】某个日期：窗口内的缺口日就是新增日期。
    旧实现直接 `o = old[dt]` 再 `f"{o[1]:...}"`，一旦窗口含新增日期就以
    `TypeError: unsupported format string passed to NoneType.__format__`
    在**写库之前**退出（实测 rc=1）。这里改为 `old.get(dt)` + 新增分支，
    并把新增日期在行尾标 `NEW`、由 main 汇总打印，保证新增行始终可见。
    刻意不使用"整个打印块套 try/except" —— 那只会把崩溃变成静默。
    """
    lines = [_TABLE_HEADER]
    new_dates = []
    for dt in sorted(computed):
        o = old.get(dt)
        n = computed[dt]
        mdd_new = round(n["max_drawdown"], 2) if n["max_drawdown"] is not None else None
        if o is None:
            new_dates.append(dt)
            cells = [
                f"{dt:<12}",
                _MISSING.rjust(13), _cell_money(n["total_value"], 13),
                _MISSING.rjust(9), _cell_float3(n["daily_return"], 9),
                _MISSING.rjust(10), _cell_str(n["sharpe_ratio"], 10),
                _MISSING.rjust(8), _cell_str(mdd_new, 8),
                _MISSING.rjust(9), _cell_str(n["volatility"], 9),
                _MISSING.rjust(8), _cell_str(f"{n['profit_count']}/{n['loss_count']}", 8),
                f"{'NEW':>6}",
            ]
        else:
            cells = [
                f"{dt:<12}",
                _cell_money(o[1], 13), _cell_money(n["total_value"], 13),
                _cell_float3(o[2], 9), _cell_float3(n["daily_return"], 9),
                _cell_str(o[6], 10), _cell_str(n["sharpe_ratio"], 10),
                _cell_str(o[7], 8), _cell_str(mdd_new, 8),
                _cell_str(o[8], 9), _cell_str(n["volatility"], 9),
                _cell_str(f"{o[4]}/{o[5]}", 8),
                _cell_str(f"{n['profit_count']}/{n['loss_count']}", 8),
                f"{'':>6}",
            ]
        lines.append("".join(cells))
    return lines, new_dates


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start-date", default=DEFAULT_START)
    ap.add_argument("--end-date", default=DEFAULT_END)
    ap.add_argument("--backup", action="store_true", help="先备份数据库")
    ap.add_argument("--apply", action="store_true", help="真正写库（默认 dry-run）")
    ap.add_argument("--rebuild-nav", action="store_true",
                    help="重算完 summary 后连续重建 portfolio_nav（避免中间态）")
    args = ap.parse_args()

    db_path = str(DATABASE_PATH)
    print("=" * 100)
    print(f"portfolio_summary 窗口重算  模式={'APPLY' if args.apply else 'DRY-RUN'}  "
          f"区间={args.start_date} ~ {args.end_date}  rebuild_nav={args.rebuild_nav}")
    print(f"数据库: {db_path}")
    print("=" * 100)

    if args.backup:
        backup(db_path)

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA busy_timeout=15000")
    cur = conn.cursor()
    old = {r[0]: r for r in cur.execute(
        "SELECT date,total_value,daily_return,vs_hs300,profit_count,loss_count,"
        "sharpe_ratio,max_drawdown,volatility FROM portfolio_summary "
        "WHERE date BETWEEN ? AND ?", (args.start_date, args.end_date)).fetchall()}

    computed = compute(conn, args.start_date, args.end_date)

    lines, new_dates = format_diff_table(computed, old)
    print("")
    for line in lines:
        print(line)

    if new_dates:
        print(f"\n共 {len(computed)} 天（其中新增 {len(new_dates)} 天: {new_dates}）")
        print("[提示] 新增日期的旧列为 <无行>，属正常：这些就是被补上的缺口日。")
    else:
        print(f"\n共 {len(computed)} 天")

    if not args.apply:
        print("\n[DRY-RUN] 未写库。加 --apply 落地；建议同时加 --rebuild-nav 避免中间态。")
        conn.close()
        return 0

    n = apply_summary(conn, computed)
    print(f"\n[APPLY] portfolio_summary 已重算 {n} 天（12 列全写）")

    if args.rebuild_nav:
        from src.analysis.nav_engine import rebuild_portfolio_nav
        cnt = rebuild_portfolio_nav(conn)
        print(f"[APPLY] portfolio_nav 已重建 {cnt} 行")
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
