#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""重建「持仓快照塌缩日」缺失的 ETF 行，并修正当日 portfolio_summary 失真汇总。

背景
----
采集器在部分交易日只写入场外基金行（35 -> 12 行塌缩），ETF 行缺失。
典型形态（2026-10-22~10-26 事故、2026-11-03~11-04 事故）：
  - portfolio_snapshots 当日仅剩 12 只 OTC；
  - etf_price_history 当日 ETF 收盘价**完好**（故不是行情断供）；
  - portfolio_summary.total_value / daily_return 按 12 行口径静默失真。

与 scripts/rebuild_missing_etf_snapshots.py 的区别
--------------------------------------------------
后者是 2026-10-22~10-26 事故的一次性脚本（BASE/TARGET 日期写死、不修 summary）。
本脚本参数化、可复用，且额外修正目标日的失真汇总行（UPDATE 而非 REPLACE，
保留 vs_hs300 / sharpe / max_drawdown / volatility 等无法真实重算的列）。

严格的数据纪律
--------------
1. 只用**当日已落库的真实收盘价** etf_price_history.close × 基线数量，不拉实时价、
   不复制上一日价格（避免违反「复制行守卫」与「run_analysis --date 改写历史」铁律）。
2. 数量前向填充前**硬断言** trades 表在 (base_date, max(target_dates)] 无成交。
3. 880013（结算类科目「天添利」，无行情）按面值 1.0 + 基线市值兜底，沿用既有范式。
4. 不触碰目标日已存在的 OTC 行（INSERT OR REPLACE 命中不存在的 (date,code) = 纯 INSERT）。
5. summary 仅 UPDATE 可由快照真实求和、相邻日市值比、或 index_quotes 真实涨幅计算的
   8 列（含 vs_hs300）；仅 sharpe_ratio / max_drawdown / volatility 三列原样保留，
   并在日志显式标注「未重算，口径不可比」——不允许静默沿用也不允许编造。
6. 默认 dry-run；--apply 才写库。写库前调用方须自行备份 data/backups/。

用法
----
    venv313\\Scripts\\python.exe scripts\\rebuild_collapsed_snapshots.py \
        --base-date 2026-11-02 --dates 2026-11-03,2026-11-04 [--apply]
"""
import argparse
import datetime
import math
import os
import sqlite3
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(ROOT, 'data', 'database', 'portfolio.db')


def fail(msg):
    print(f'[FATAL] {msg}')
    sys.exit(3)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--base-date', required=True, help='最后一个完整快照日（数量/成本基线）')
    ap.add_argument('--dates', required=True, help='待重建日期，逗号分隔，按升序')
    ap.add_argument('--summary-date', default=None,
                    help='需修正 portfolio_summary 的日期（默认取 --dates 最后一日）')
    ap.add_argument('--apply', action='store_true', help='真正写库（默认 dry-run）')
    args = ap.parse_args()

    dates = [d.strip() for d in args.dates.split(',') if d.strip()]
    if not dates:
        fail('--dates 为空')
    dates.sort()
    summary_date = args.summary_date or dates[-1]
    if summary_date not in dates:
        fail(f'--summary-date {summary_date} 不在 --dates 中')

    con = sqlite3.connect(DB)
    con.execute('PRAGMA busy_timeout=15000')
    cur = con.cursor()

    # ---- 基线 ----
    base_rows = cur.execute(
        "SELECT code,name,quantity,cost_price,current_price,market_value,pnl,pnl_rate,ytd_return,beta "
        "FROM portfolio_snapshots WHERE date=?", (args.base_date,)).fetchall()
    if len(base_rows) < 34:
        fail(f'基线日 {args.base_date} 仅 {len(base_rows)} 行（<34），拒绝作为数量基线')
    base = {r[0]: dict(name=r[1], quantity=r[2], cost=r[3], price=r[4], mv=r[5],
                       pnl=r[6], rate=r[7], ytd=r[8], beta=r[9]) for r in base_rows}
    print(f'[基线] {args.base_date} 完整快照 {len(base)} 行')

    # ---- 硬断言：数量未变 ----
    n_trades = cur.execute(
        "SELECT COUNT(*) FROM trades WHERE date > ? AND date <= ?",
        (args.base_date, dates[-1])).fetchone()[0]
    if n_trades:
        fail(f'{args.base_date} ~ {dates[-1]} 存在 {n_trades} 笔成交，数量不可前向填充，拒绝重建')
    print(f'[断言] trades 在 ({args.base_date}, {dates[-1]}] 无成交 -> 数量前向填充合法')

    # ---- 逐日补缺失 ETF 行 ----
    planned = []
    for dt in dates:
        existing = {r[0] for r in cur.execute(
            "SELECT code FROM portfolio_snapshots WHERE date=?", (dt,))}
        missing = sorted(set(base) - existing)
        print(f'\n=== {dt}：现有 {len(existing)} 行，待补 {len(missing)} 行 ===')
        if not missing:
            print('  [跳过] 无缺失行')
            continue
        if existing - set(base):
            fail(f'{dt} 存在不在基线中的 code：{sorted(existing - set(base))}')
        for code in missing:
            b = base[code]
            qty, cost = b['quantity'], b['cost']
            if code == '880013':
                price = 1.0
                mv = b['mv'] if b['mv'] else qty
                pnl, rate = 0.0, 0.0
                src = '面值兜底(结算类科目,沿用基线市值)'
            else:
                row = cur.execute(
                    "SELECT close FROM etf_price_history WHERE code=? AND date=?",
                    (code, dt)).fetchone()
                if not row or row[0] is None:
                    fail(f'{dt} {code} 在 etf_price_history 无收盘价，拒绝编造/跳过')
                price = float(row[0])
                mv = price * qty
                pnl = (price - cost) * qty if cost is not None else 0.0
                rate = (pnl / (cost * qty) * 100) if (cost and qty and cost > 0) else 0.0
                src = f'etf_price_history {dt} close'
            vals = (dt, code, b['name'], qty, cost, price, mv, pnl, rate, b['ytd'], b['beta'])
            planned.append((vals, src))
            print(f'  [{"WRITE" if args.apply else "PRE"}] {code} {b["name"][:12]:<12} '
                  f'qty={qty:>12.2f} close={price:>8.4f} mv={mv:>12.2f}  <- {src}')

    # ---- 预演 summary 修正（基于修后快照的预期 Σmv）----
    # 修后每日 Σmv = 当日现有 OTC Σmv + 待补 ETF Σmv
    projected = {}
    for dt in dates:
        otc_mv = cur.execute(
            "SELECT COALESCE(SUM(market_value),0) FROM portfolio_snapshots WHERE date=?",
            (dt,)).fetchone()[0]
        add_mv = sum(v[6] for v, _ in planned if v[0] == dt)
        projected[dt] = otc_mv + add_mv
    idx = dates.index(summary_date)
    if idx == 0:
        fail(f'{summary_date} 是首个目标日，其前一交易日不在重建范围内，'
             f'daily_return 分母口径不可核实，拒绝修正 summary')
    prev_dt = dates[idx - 1]
    v_cur, v_prev = projected[summary_date], projected[prev_dt]
    d_pnl = v_cur - v_prev
    d_ret = d_pnl / v_prev * 100 if v_prev > 0 else None

    # 由修后快照真实求和的其余汇总列
    extra_cost, extra_pnl, extra_profit, extra_loss = 0.0, 0.0, 0, 0
    for v, _ in planned:
        if v[0] == summary_date:
            extra_cost += (v[4] or 0) * (v[3] or 0)
            extra_pnl += v[7] or 0
            if (v[7] or 0) >= 0:
                extra_profit += 1
            else:
                extra_loss += 1
    srow = cur.execute(
        "SELECT total_cost,total_pnl,profit_count,loss_count,daily_return_settled "
        "FROM portfolio_summary WHERE date=?", (summary_date,)).fetchone()
    if not srow:
        fail(f'{summary_date} 无 portfolio_summary 行，拒绝新建（避免引入无来源字段）')
    old_cost, old_pnl, old_profit, old_loss, settled = srow
    new_cost = (old_cost or 0) + extra_cost
    new_pnl = (old_pnl or 0) + extra_pnl
    new_profit = (old_profit or 0) + extra_profit
    new_loss = (old_loss or 0) + extra_loss

    print(f'\n=== summary 修正预演 {summary_date}（UPDATE，仅 7 列）===')
    print(f'  total_value : {cur.execute("SELECT total_value FROM portfolio_summary WHERE date=?", (summary_date,)).fetchone()[0]:,.2f}'
          f'  ->  {v_cur:,.2f}   (= 修后 Σmarket_value)')
    print(f'  total_cost  : {old_cost:,.2f}  ->  {new_cost:,.2f}   (+ 补入行 cost*qty)')
    print(f'  total_pnl   : {old_pnl:,.2f}  ->  {new_pnl:,.2f}   (+ 补入行 pnl)')
    print(f'  daily_pnl   : ->  {d_pnl:,.2f}   ({summary_date} Σmv - {prev_dt} Σmv)')
    print(f'  daily_return: ->  {d_ret:+.4f}%  (市值口径；两日 OTC 口径一致，真实可算)')
    print(f'  profit/loss : {old_profit}/{old_loss} -> {new_profit}/{new_loss}')
    hs = cur.execute(
        "SELECT change_pct FROM index_quotes WHERE date=? AND code='sh000300'",
        (summary_date,)).fetchone()
    old_vs = cur.execute(
        "SELECT vs_hs300 FROM portfolio_summary WHERE date=?",
        (summary_date,)).fetchone()[0]
    if hs and hs[0] is not None:
        print(f'  vs_hs300    : {old_vs}  ->  {d_ret - float(hs[0]):+.4f}'
              f'   (= daily_return - index_quotes 沪深300 {float(hs[0]):+.4f}%)')
    else:
        print(f'  vs_hs300    : {old_vs}  ->  NULL'
              f'   (index_quotes 无沪深300当日涨幅，交由报告 fallback 计算)')
    print(f'  [保留未重算] sharpe_ratio / max_drawdown / volatility '
          f'= 采集器 12 行口径原值，口径不可比，已在日志与报告标注')
    print(f'  daily_return_settled 现值 = {settled}')

    if not args.apply:
        print(f'\n[DRY-RUN] 拟写入 {len(planned)} 行快照 + 1 行 summary UPDATE；加 --apply 落地')
        con.close()
        return 0

    # ---- 写库 ----
    try:
        con.execute('BEGIN')
        for v, _ in planned:
            cur.execute(
                "INSERT OR REPLACE INTO portfolio_snapshots "
                "(date,code,name,quantity,cost_price,current_price,market_value,pnl,pnl_rate,ytd_return,beta) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?)", v)
        # 汇总列一律由【修后快照】SQL 求和得出，不沿用任何 12 行口径的旧值，
        # 也不使用 dry-run 阶段的估算增量（避免 total_pnl 与 mv-cost 存在口径差）。
        # daily_* 用修后两日真实 Σmv 之比，同样在落库后现算。
        v_prev_real = cur.execute(
            "SELECT SUM(market_value) FROM portfolio_snapshots WHERE date=?",
            (prev_dt,)).fetchone()[0]
        v_cur_real = cur.execute(
            "SELECT SUM(market_value) FROM portfolio_snapshots WHERE date=?",
            (summary_date,)).fetchone()[0]
        d_pnl_real = v_cur_real - v_prev_real
        d_ret_real = d_pnl_real / v_prev_real * 100 if v_prev_real > 0 else None
        hs = cur.execute(
            "SELECT change_pct FROM index_quotes WHERE date=? AND code='sh000300'",
            (summary_date,)).fetchone()
        vs_real = ((d_ret_real - float(hs[0]))
                   if (hs and hs[0] is not None and d_ret_real is not None) else None)
        cur.execute("""
            UPDATE portfolio_summary SET
              total_value  = (SELECT SUM(market_value) FROM portfolio_snapshots WHERE date=?),
              total_cost   = (SELECT SUM(cost_price * quantity) FROM portfolio_snapshots WHERE date=?),
              total_pnl    = (SELECT SUM(pnl) FROM portfolio_snapshots WHERE date=?),
              profit_count = (SELECT SUM(CASE WHEN pnl >= 0 THEN 1 ELSE 0 END)
                              FROM portfolio_snapshots WHERE date=?),
              loss_count   = (SELECT SUM(CASE WHEN pnl < 0 THEN 1 ELSE 0 END)
                              FROM portfolio_snapshots WHERE date=?),
              daily_pnl = ?, daily_return = ?, vs_hs300 = ?, daily_return_settled = 1
            WHERE date = ?
        """, (summary_date, summary_date, summary_date, summary_date, summary_date,
              d_pnl_real, d_ret_real, vs_real, summary_date))
        con.commit()
        _vs_txt = (f'{vs_real:+.4f}' if vs_real is not None
                   else 'NULL(报告 fallback)')
        print(f'  [重算] {prev_dt} Σmv={v_prev_real:,.2f} -> {summary_date} Σmv={v_cur_real:,.2f}'
              f'  daily_return={d_ret_real:+.4f}%  vs_hs300={_vs_txt}')
    except Exception as e:
        con.rollback()
        fail(f'写库失败已回滚：{e}')

    # ---- 落库后硬校验 ----
    print('\n=== 落库后校验 ===')
    ok = True
    for dt in dates:
        n = cur.execute("SELECT COUNT(*) FROM portfolio_snapshots WHERE date=?", (dt,)).fetchone()[0]
        smv = cur.execute("SELECT SUM(market_value) FROM portfolio_snapshots WHERE date=?", (dt,)).fetchone()[0]
        null_n = cur.execute(
            "SELECT COUNT(*) FROM portfolio_snapshots WHERE date=? AND "
            "(market_value IS NULL OR current_price IS NULL OR quantity IS NULL)", (dt,)).fetchone()[0]
        nan_n = sum(1 for (x,) in cur.execute(
            "SELECT market_value FROM portfolio_snapshots WHERE date=?", (dt,))
            if x is None or (isinstance(x, float) and (math.isnan(x) or math.isinf(x))))
        print(f'  {dt}: rows={n}/{len(base)}  Σmv={smv:,.2f}  NULL字段行={null_n}  NaN/inf={nan_n}')
        ok &= (n == len(base) and null_n == 0 and nan_n == 0 and abs(smv - projected[dt]) < 1.0)
    tv = cur.execute("SELECT total_value,daily_return FROM portfolio_summary WHERE date=?",
                     (summary_date,)).fetchone()
    print(f'  summary {summary_date}: total_value={tv[0]:,.2f}  daily_return={tv[1]:+.4f}%')
    prev_real = cur.execute(
        "SELECT SUM(market_value) FROM portfolio_snapshots WHERE date=?",
        (prev_dt,)).fetchone()[0]
    snap_real = cur.execute(
        "SELECT SUM(market_value) FROM portfolio_snapshots WHERE date=?",
        (summary_date,)).fetchone()[0]
    expect_d_ret = (snap_real - prev_real) / prev_real * 100 if prev_real > 0 else None
    ok &= (abs(tv[0] - v_cur) < 1.0)
    ok &= (tv[1] is not None and expect_d_ret is not None
           and abs(tv[1] - expect_d_ret) < 1e-6)
    ident = abs(tv[0] - snap_real) < 1.0
    print(f'  Σmarket_value ≡ total_value : {ident}'
          f'  (Σmv={snap_real:,.2f}, summary={tv[0]:,.2f})')
    ok &= ident
    print(f'  VERDICT: {"OK" if ok else "FAILED"}')
    con.close()
    return 0 if ok else 4


if __name__ == '__main__':
    sys.exit(main())
