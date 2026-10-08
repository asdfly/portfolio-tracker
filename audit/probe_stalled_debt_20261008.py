# -*- coding: utf-8 -*-
"""只读探针：核实两项停滞高危债务的**当轮真实状态**（2026-10-08）。

严格只读：以 file:...?mode=ro 连接生产库，全程不写、不改、不删。
目的：文档记录停留在 2026-09-15/09-16，必须先确定「代码是否已修 / 存量数据是否已刷」。

债务①  NAV 恒等式：portfolio_nav.total_units 究竟是 prev_v（旧错位 bug）
                    还是 v/unit_nav（07_ 建议的修法）？
债务②  报告库漂移：库内 portfolio_summary 现状（行数 / max date）。
"""

import sqlite3

DB = "file:data/database/portfolio.db?mode=ro"

conn = sqlite3.connect(DB, uri=True)
cur = conn.cursor()

print("=" * 78)
print("债务① portfolio_nav.total_units 恒等式（当轮实测）")
print("=" * 78)

total = cur.execute("SELECT COUNT(*) FROM portfolio_nav").fetchone()[0]
max_dt = cur.execute("SELECT MAX(date) FROM portfolio_nav").fetchone()[0]
print(f"行数 = {total}    max(date) = {max_dt}")

# --- 判定 A：旧 bug 形态 —— total_units 是否等于「前一交易日的 total_value」 ---
hit_bug = cur.execute(
    "SELECT COUNT(*) FROM ("
    "  SELECT total_units, LAG(total_value) OVER (ORDER BY date) AS pv FROM portfolio_nav"
    ") WHERE pv IS NOT NULL AND total_units IS NOT NULL AND ABS(total_units - pv) < 0.01"
).fetchone()[0]

# --- 判定 B：应为形态 —— total_units * unit_nav ≈ total_value ---
hit_ok = cur.execute(
    "SELECT COUNT(*) FROM ("
    "  SELECT total_units, unit_nav, total_value FROM portfolio_nav"
    ") WHERE total_units IS NOT NULL AND unit_nav IS NOT NULL AND unit_nav != 0 "
    "  AND ABS(total_units * unit_nav - total_value) < 1.0"
).fetchone()[0]

notnull = cur.execute(
    "SELECT COUNT(*) FROM portfolio_nav WHERE total_units IS NOT NULL"
).fetchone()[0]
comparable = total - 1  # 首行无前值，不可参与 A 判定

print(f"  total_units 非 NULL          = {notnull} / {total}")
print(f"  A) 与「前一日 total_value」相等 = {hit_bug} / {comparable}   <- 旧 bug 形态")
print(f"  B) 满足 units*unit_nav≈value  = {hit_ok} / {notnull}        <- 正确形态")

print("\n最近 8 行样本：")
print(f"  {'date':<12}{'unit_nav':>11}{'total_units':>15}{'total_value':>15}"
      f"{'前一日tv':>15}{'units*nav':>15}")
rows = cur.execute(
    "SELECT date, unit_nav, total_units, total_value,"
    "       LAG(total_value) OVER (ORDER BY date) "
    "FROM portfolio_nav ORDER BY date DESC LIMIT 8"
).fetchall()
for d, nav, tu, tv, pv in rows:
    prod = (tu * nav) if (tu is not None and nav is not None) else None
    print(f"  {d:<12}"
          f"{nav:>11.4f}"
          f"{(tu if tu is not None else float('nan')):>15,.2f}"
          f"{(tv if tv is not None else float('nan')):>15,.2f}"
          f"{(pv if pv is not None else float('nan')):>15,.2f}"
          f"{(prod if prod is not None else float('nan')):>15,.2f}")

print("\n" + "=" * 78)
print("债务② 报告库漂移基准：portfolio_summary 现状")
print("=" * 78)
s_total = cur.execute("SELECT COUNT(*) FROM portfolio_summary").fetchone()[0]
s_min, s_max = cur.execute(
    "SELECT MIN(date), MAX(date) FROM portfolio_summary").fetchone()
print(f"行数 = {s_total}    date 区间 = {s_min} ~ {s_max}")

print("\n最近 5 行：")
for d, tv in cur.execute(
        "SELECT date, total_value FROM portfolio_summary "
        "ORDER BY date DESC LIMIT 5").fetchall():
    print(f"  {d}  total_value = {tv:,.2f}")

conn.close()
print("\n[探针结束] 连接为只读，未执行任何写操作。")
