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


def compute(conn, start: str, end: str):
    """返回 {date: {...12 列...}}，只读，不写库。"""
    cur = conn.cursor()
    dates = [r[0] for r in cur.execute(
        "SELECT date FROM portfolio_summary WHERE date BETWEEN ? AND ? ORDER BY date",
        (start, end)).fetchall()]

    prev_dates = [r[0] for r in cur.execute(
        "SELECT date FROM portfolio_summary WHERE date < ? ORDER BY date", (start,)).fetchall()]
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

    print(f"\n{'date':<12}{'tv旧':>13}{'tv新':>13}{'dr旧%':>9}{'dr新%':>9}"
          f"{'sharpe旧':>10}{'sharpe新':>10}{'mdd旧':>8}{'mdd新':>8}{'vol旧':>9}{'vol新':>9}{'盈亏旧':>8}{'盈亏新':>8}")
    for dt in sorted(computed):
        o = old[dt]; n = computed[dt]
        print(f"{dt:<12}{o[1]:>13,.0f}{n['total_value']:>13,.0f}{o[2]:>9.3f}{n['daily_return']:>9.3f}"
              f"{str(o[6]):>10}{str(n['sharpe_ratio']):>10}{str(o[7]):>8}"
              f"{str(round(n['max_drawdown'],2) if n['max_drawdown'] is not None else None):>8}"
              f"{str(o[8]):>9}{str(n['volatility']):>9}"
              f"{str(o[4])+'/'+str(o[5]):>8}{str(n['profit_count'])+'/'+str(n['loss_count']):>8}")

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
