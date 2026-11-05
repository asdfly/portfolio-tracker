#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""补齐 2026-10-22/10-23/10-26 持仓快照中缺失的 23 只 ETF 行。

背景
----
采集器在 2026-10-22 起连续只写入场外基金行（35→12 行塌缩），ETF 收盘价子源
停更；2026-10-26 价格已回补到 etf_price_history，但 portfolio_snapshots 未重建，
导致 portfolio_summary.daily_return 失真为 −21.62%。

本脚本用【历史 etf_price_history 收盘价 × 10-21 持仓数量】补齐缺失 ETF 行，
不触碰已有场外行（INSERT OR REPLACE 按 (date,code) 命中不存在的行 = 纯 INSERT）。
数量基准取自 2026-10-21 全量快照（已核实 10-22→10-26 trades 表无成交，数量未变）。
880013（结算类科目，无价格）按面值 1.0 兜底，市值沿用 10-21。

用法
----
    venv313\\Scripts\\python.exe scripts\\rebuild_missing_etf_snapshots.py --dry
    venv313\\Scripts\\python.exe scripts\\rebuild_missing_etf_snapshots.py --apply
"""
import argparse
import os
import sqlite3
import sys

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from config.settings import DATABASE_PATH  # noqa: E402

BASE_DATE = "2026-10-21"          # 数量/成本/名称基线（最后全量日）
TARGET_DATES = ["2026-10-22", "2026-10-23", "2026-10-26"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="真正写库（默认 dry-run）")
    args = ap.parse_args()

    db = str(DATABASE_PATH)
    con = sqlite3.connect(db)
    con.execute("PRAGMA busy_timeout=15000")
    cur = con.cursor()

    # 基线：10-21 全部 35 行
    base = {}
    for r in cur.execute(
        "SELECT code,name,quantity,cost_price,current_price,market_value,pnl,pnl_rate,ytd_return,beta "
        "FROM portfolio_snapshots WHERE date=?", (BASE_DATE,)
    ):
        base[r[0]] = dict(name=r[1], quantity=r[2], cost_price=r[3], current_price=r[4],
                          market_value=r[5], pnl=r[6], pnl_rate=r[7], ytd_return=r[8], beta=r[9])
    print(f"[基线] 10-21 持仓 {len(base)} 行")

    total_insert = 0
    for dt in TARGET_DATES:
        existing = {r[0] for r in cur.execute(
            "SELECT code FROM portfolio_snapshots WHERE date=?", (dt,))}
        print(f"\n=== {dt}：已有 {len(existing)} 行，待补 {len(set(base)-existing)} 行 ===")
        for code, b in base.items():
            if code in existing:
                continue  # 已有（场外行），不触碰
            qty = b["quantity"]
            cost = b["cost_price"]
            if code == "880013":
                # 结算类科目无价格，按面值兜底
                price = 1.0
                mv = b["market_value"] if b["market_value"] else qty
                pnl = 0.0
                pnl_rate = 0.0
            else:
                row = cur.execute(
                    "SELECT close FROM etf_price_history WHERE code=? AND date=?", (code, dt)
                ).fetchone()
                if not row:
                    print(f"  [跳过] {code}：etf_price_history 无 {dt} 收盘价")
                    continue
                price = row[0]
                mv = price * qty
                pnl = (price - cost) * qty if cost is not None else 0.0
                pnl_rate = (pnl / (cost * qty) * 100) if (cost and qty and cost > 0) else 0.0
            vals = (dt, code, b["name"], qty, cost, price, mv, pnl, pnl_rate,
                    b["ytd_return"], b["beta"])
            if args.apply:
                cur.execute(
                    "INSERT OR REPLACE INTO portfolio_snapshots "
                    "(date,code,name,quantity,cost_price,current_price,market_value,pnl,pnl_rate,ytd_return,beta) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?)", vals)
            total_insert += 1
            print(f"  [{'WRITE' if args.apply else 'PRE'}] {code} price={price} mv={mv:.2f} pnl={pnl:.2f} rate={pnl_rate:.2f}")

    if args.apply:
        con.commit()
        print(f"\n[APPLY] 共写入 {total_insert} 行（仅缺失 ETF，未触碰场外行）")
    else:
        print(f"\n[DRY-RUN] 拟写入 {total_insert} 行；加 --apply 落地")
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
