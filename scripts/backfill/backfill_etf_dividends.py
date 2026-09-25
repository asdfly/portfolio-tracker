#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
分红数据回填：将当前持仓标的的真实分红/收益分配事件写入 portfolio_events 表。

数据源（容错双通道，对齐项目「东财主 + 新浪备」惯例）：
  主通道 · 新浪：ak.fund_etf_dividend_sina(symbol="sh/sz"+code) -> 日期 / 累计分红
  回退 · 东财：ak.fund_announcement_dividend_em(symbol=code)      -> 公告日期 / 公告标题
  剔除：880013 天添利（货币基金，code[0]=='8' 直接跳过）

为什么需要：
  Tab4 收益日历事件模块此前为「通用 A 股日历 + 真实持仓上下文」，预留了
  data_loader.load_portfolio_events() 接口（返回 []）。本脚本把真实除息/收益分配事件落库，
  使事件日历按持仓个性化。脚本幂等（INSERT OR IGNORE），可重复安全运行。

用法：
  venv313/Scripts/python.exe scripts/backfill/backfill_etf_dividends.py [--dry-run] [--verify]
"""
import argparse
import sys
import os
import time
from datetime import datetime

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from data_loader import get_db_connection, ensure_portfolio_events_table  # noqa: E402


def prefix_of(code: str):
    """上交所 5/6/9 开头 -> sh；深交所 0/1/2/3 -> sz；其余（如 88 货币基金）跳过。"""
    if not code:
        return None
    c = code[0]
    if c in "569":
        return "sh"
    if c in "0123":
        return "sz"
    return None


def fetch_dividend_events(code: str):
    """返回 (events, source)。events: list[dict(event_date, per_unit, cumulative, detail)]。

    主通道 fund_etf_dividend_sina；为空时回退 fund_announcement_dividend_em。
    """
    import akshare as ak

    pref = prefix_of(code)
    if not pref:
        # 货币基金（880013 等）/ 非 ETF 代码：无除息事件，双通道均跳过
        return [], None
    source = None
    df = None
    sym = pref + code
    try:
        df = ak.fund_etf_dividend_sina(symbol=sym)
    except Exception:
        df = None
    if df is not None and not df.empty and "日期" in df.columns:
        source = "akshare:fund_etf_dividend_sina"
        events = []
        prev_cum = 0.0
        for _, r in df.iterrows():
            edate = str(r["日期"])[:10]
            cum = float(r["累计分红"]) if r["累计分红"] is not None else 0.0
            per = round(cum - prev_cum, 6)
            prev_cum = cum
            events.append(
                {"event_date": edate, "per_unit": per, "cumulative": cum, "detail": ""}
            )
        return events, source

    # 回退：东财收益分配公告
    try:
        df2 = ak.fund_announcement_dividend_em(symbol=code)
    except Exception:
        df2 = None
    if df2 is not None and not df2.empty and "公告日期" in df2.columns:
        source = "akshare:fund_announcement_dividend_em"
        events = []
        for _, r in df2.iterrows():
            edate = str(r["公告日期"])[:10]
            title = str(r.get("公告标题", "") or "")
            events.append(
                {"event_date": edate, "per_unit": None, "cumulative": None, "detail": title}
            )
        return events, source

    return [], None


def latest_snapshot_holdings():
    """返回 {code: canonical_name}（最新快照去重，取最长名称）。"""
    conn = get_db_connection()
    row = conn.execute(
        "SELECT date FROM portfolio_snapshots ORDER BY date DESC LIMIT 1"
    ).fetchone()
    if not row:
        conn.close()
        return {}
    latest = row[0]
    rows = conn.execute(
        "SELECT code, name FROM portfolio_snapshots WHERE date = ?", (latest,)
    ).fetchall()
    conn.close()
    by_code = {}
    for code, name in rows:
        if code not in by_code or (name and len(name) > len(by_code[code])):
            by_code[code] = name
    return by_code


def main():
    p = argparse.ArgumentParser(description="回填持仓分红事件到 portfolio_events")
    p.add_argument("--dry-run", action="store_true", help="只打印将要写入的事件，不落库")
    p.add_argument("--verify", action="store_true", help="写入后校验行数并抽样")
    args = p.parse_args()

    holdings = latest_snapshot_holdings()
    if not holdings:
        print("[WARN] 未读取到持仓快照，跳过")
        return

    print(f"[INFO] 持仓去重 {len(holdings)} 只，开始回填分红事件"
          f"{'（DRY-RUN）' if args.dry_run else ''}")

    conn = None
    if not args.dry_run:
        conn = get_db_connection()
        ensure_portfolio_events_table(conn)

    now = datetime.now().isoformat(timespec="seconds")
    total = 0
    n_ok = n_empty = n_err = 0
    t0 = time.time()
    for code, name in sorted(holdings.items()):
        try:
            events, source = fetch_dividend_events(code)
        except Exception as e:  # 单只异常隔离
            n_err += 1
            print(f"  [ERR ] {code} {name}: {type(e).__name__}: {str(e)[:80]}")
            continue
        if not events:
            n_empty += 1
            print(f"  [----] {code} {name}: 无分红数据（跳过）")
            continue
        n_ok += 1
        if args.dry_run:
            for ev in events:
                print(f"  [DRY ] {code} {name}: {ev['event_date']} "
                      f"per={ev['per_unit']} cum={ev['cumulative']} src={source}")
            total += len(events)
            continue
        inserted = 0
        for ev in events:
            cur = conn.execute(
                "INSERT OR IGNORE INTO portfolio_events"
                "(code,name,event_type,event_date,per_unit,cumulative,detail,source,created_at)"
                " VALUES(?,?,?,?,?,?,?,?,?)",
                (code, name, "dividend", ev["event_date"], ev["per_unit"],
                 ev["cumulative"], ev["detail"], source, now),
            )
            inserted += cur.rowcount
        total += inserted
        print(f"  [OK  ] {code} {name}: 事件 {len(events)} 条，新写入 {inserted} 条（src={source}）")

    if conn is not None:
        conn.commit()
        conn.close()

    print(f"[DONE] 耗时 {time.time()-t0:.1f}s | 有数据 {n_ok} / 无数据 {n_empty} / 异常 {n_err}"
          f" | 累计事件 {total} 条")

    if args.verify and not args.dry_run:
        c2 = get_db_connection()
        ensure_portfolio_events_table(c2)
        cnt = c2.execute("SELECT COUNT(*) FROM portfolio_events").fetchone()[0]
        by_code = c2.execute(
            "SELECT code, COUNT(*) FROM portfolio_events GROUP BY code ORDER BY COUNT(*) DESC"
        ).fetchall()
        print(f"[VERIFY] portfolio_events 总行数: {cnt}")
        print("[VERIFY] 按 code 分布 (top8):")
        for code, n in by_code[:8]:
            sample = c2.execute(
                "SELECT event_date, per_unit, cumulative FROM portfolio_events "
                "WHERE code=? ORDER BY event_date DESC LIMIT 1", (code,)
            ).fetchone()
            print(f"   {code}: {n} 条 | 最近: {sample}")
        c2.close()


if __name__ == "__main__":
    main()
