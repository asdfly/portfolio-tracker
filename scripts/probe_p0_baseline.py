"""P0 诊断脚本（只读）：确认场内/场外划分 + 修复前基线数字。

运行：
  venv313/Scripts/python.exe scripts/probe_p0_baseline.py
"""
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

DB = ROOT / "data" / "database" / "portfolio.db"


def main():
    con = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    cur = con.cursor()

    cols = [r[1] for r in cur.execute("PRAGMA table_info(portfolio_snapshots)")]
    print("portfolio_snapshots 列:", cols)

    # 1) 场内 vs 场外：以 etf_technical 为场内权威集合
    snap = dict(cur.execute(
        "SELECT code, MAX(name) FROM portfolio_snapshots GROUP BY code").fetchall())
    tech = {r[0] for r in cur.execute("SELECT DISTINCT code FROM etf_technical")}
    otc = sorted(set(snap) - tech)
    print(f"\n全历史标的 {len(snap)} 只；场内(etf_technical) {len(tech)} 只；场外 {len(otc)} 只")
    print("场外清单:")
    for c in otc:
        print(f"  {c}  {snap[c]}")

    # 2) 全局最新快照日期 vs 每标的各自最新日期
    gmax = cur.execute("SELECT MAX(date) FROM portfolio_snapshots").fetchone()[0]
    print(f"\n全局最新快照日期: {gmax}")

    row_g = cur.execute(
        "SELECT COUNT(*), SUM(market_value) FROM portfolio_snapshots "
        "WHERE date=? AND market_value IS NOT NULL", [gmax]).fetchone()
    print(f"  [旧逻辑] 取全局最新日期 -> {row_g[0]} 只, 总市值 {row_g[1] or 0:,.2f}")

    rows = cur.execute(
        "SELECT code, MAX(date) FROM portfolio_snapshots "
        "WHERE date<=? AND market_value IS NOT NULL GROUP BY code", [gmax]).fetchall()
    per_code_last = dict(rows)
    print(f"  [新逻辑] 按 code 各取最新 -> {len(rows)} 只")

    # 新逻辑总市值
    q = ("SELECT ps.code, ps.market_value FROM portfolio_snapshots ps "
         "JOIN (SELECT code, MAX(date) AS md FROM portfolio_snapshots "
         "WHERE date<=? AND market_value IS NOT NULL GROUP BY code) t "
         "ON ps.code=t.code AND ps.date=t.md")
    tot = 0.0
    n = 0
    for code, mv in cur.execute(q, [gmax]):
        n += 1
        tot += mv or 0.0
    print(f"  [新逻辑] 总市值 {tot:,.2f}  ({n} 只)")

    # 3) 陈旧快照：各标的最新日期 vs 全局最新
    print("\n各标的最新快照日期（早于全局最新 7 天以上者标 *）:")
    from datetime import date as _d
    gy, gm, gd = (int(x) for x in gmax.split("-"))
    gdate = _d(gy, gm, gd)
    for code, d in sorted(per_code_last.items(), key=lambda kv: kv[1]):
        y, m, dd = (int(x) for x in d.split("-"))
        gap = (gdate - _d(y, m, dd)).days
        flag = " *" if gap > 7 else ""
        print(f"  {code}  {d}  落后 {gap} 天{flag}  {snap.get(code,'')}")

    con.close()


if __name__ == "__main__":
    main()
