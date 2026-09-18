"""问题十一 · 一次性历史重算：用 void 感知口径重建 portfolio_summary。

仅用于把已落库的 portfolio_summary 按「复制行旁路表」口径重算（total_value 向后结转 void 行、
daily_return 共同持仓法排除 void 标的）。ASC 顺序逐日重算，使每个日期的 prev 取自刚算出的上一日，
避免 backfill_portfolio_history 的 DESC + summary 前驱链在批量重算时断裂而退化为 snapshot-sum 口径。

用法：
  python audit/apply_void_aware_recompute.py <db_path> [--dry-run]
默认对 data/database/portfolio.db 跑 dry-run（只读打印差异），加 --apply 才写库。
"""
from __future__ import annotations
import argparse, os, sqlite3, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from src.analysis.replica_void import (
    void_aware_total_value, void_codes_on, void_excluded_common_codes,
)


def recompute(conn: sqlite3.Connection, surgical: bool = True):
    dates = [r[0] for r in conn.execute(
        "SELECT DISTINCT date FROM portfolio_snapshots ORDER BY date ASC")]
    existing = {r[0]: (r[1], r[2]) for r in conn.execute(
        "SELECT date, total_value, daily_return FROM portfolio_summary")}
    # 仅「自身或前驱含 void」的日期被复制行污染，surgical 模式只重写这些日期，
    # 其余日期沿用已落库的口径 B / 折算闸门结果，避免大范围静默改口径。
    void_dates = set()
    for d, in conn.execute("SELECT DISTINCT date FROM portfolio_snapshots_replica_void"):
        void_dates.add(d)
    rows = []
    prev_total = None
    prev_date = None
    for dt in dates:
        if surgical and dt not in void_dates and prev_date not in void_dates:
            prev_date = dt
            continue  # 非污染日期：跳过，保留已落库值
        total_value = void_aware_total_value(conn, dt)
        row = conn.execute(
            "SELECT SUM(cost_price*quantity), SUM(pnl), "
            "SUM(CASE WHEN pnl>=0 THEN 1 ELSE 0 END), SUM(CASE WHEN pnl<0 THEN 1 ELSE 0 END) "
            "FROM portfolio_snapshots WHERE date=?", (dt,)).fetchone()
        total_cost = row[0] or 0
        total_pnl = row[1] or 0
        profit_count = row[2] or 0
        loss_count = row[3] or 0

        daily_return = 0.0
        daily_pnl = 0.0
        if prev_date is not None:
            prev_snaps = {r[0]: (r[1], r[2]) for r in conn.execute(
                "SELECT code, quantity, market_value FROM portfolio_snapshots WHERE date=?", (prev_date,))}
            curr_snaps = {r[0]: (r[1], r[2], r[3]) for r in conn.execute(
                "SELECT code, quantity, market_value, current_price FROM portfolio_snapshots WHERE date=?", (dt,))}
            vprev = void_codes_on(conn, prev_date)
            vcurr = void_codes_on(conn, dt)
            common = void_excluded_common_codes(
                set(prev_snaps) & set(curr_snaps), vprev, vcurr)
            price_adj_mv = sum(curr_snaps[c][2] * prev_snaps[c][0] for c in common)
            prev_common_mv = sum(prev_snaps[c][1] for c in common)
            if prev_common_mv > 0:
                daily_pnl = price_adj_mv - prev_common_mv
                daily_return = daily_pnl / prev_common_mv * 100
        rows.append((dt, total_value, total_cost, total_pnl, daily_pnl,
                     daily_return, profit_count, loss_count))
        prev_total = total_value
        prev_date = dt
    return rows, existing


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("db", nargs="?", default="data/database/portfolio.db")
    ap.add_argument("--apply", action="store_true", help="写库；默认 dry-run")
    ap.add_argument("--full", action="store_true", help="全量重算（默认 surgical：只重写被复制行污染的日期）")
    args = ap.parse_args()
    db = os.path.abspath(args.db)
    uri = "file:" + db + "?mode=ro" if not args.apply else db
    conn = sqlite3.connect(uri, uri=bool(not args.apply))
    rows, existing = recompute(conn, surgical=not args.full)
    if not args.apply:
        conn.close()
    print(f"{'DRY-RUN' if not args.apply else 'APPLY'} on {db}, {len(rows)} dates")
    print("date        | total_value(changed?) | daily_return(changed?)")
    changed = 0
    for dt, tv, tc, tp, dp, dr, pc, lc in rows:
        ex = existing.get(dt)
        tv_s = f"{tv:,.2f}"
        dr_s = f"{dr:.4f}"
        mark = ""
        if ex is not None:
            otv, odr = ex
            if otv is not None and abs(otv - tv) > 0.005:
                mark += " TV*"
            if odr is not None and abs(odr - dr) > 1e-6:
                mark += " DR*"
        if mark:
            changed += 1
        if mark or dt in ("2026-06-15","2026-06-29","2026-06-30","2026-07-01","2026-09-15","2026-09-16"):
            print(f"{dt} | {tv_s} | {dr_s}{mark}")
    print(f"\n{changed} dates differ from stored.")
    if args.apply:
        cur = conn.cursor()
        cur.executemany("""INSERT OR REPLACE INTO portfolio_summary
            (date,total_value,total_cost,total_pnl,daily_pnl,daily_return,profit_count,loss_count)
            VALUES (?,?,?,?,?,?,?,?)""", [tuple(r) for r in rows])
        conn.commit()
        conn.close()
        print("APPLIED.")


if __name__ == "__main__":
    main()
