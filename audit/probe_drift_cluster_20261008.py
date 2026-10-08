# -*- coding: utf-8 -*-
"""只读探针③：定位 08_ 登记之后新发生的那次「未登记历史改写」。

背景：探针②发现 2026-06-17 ~ 06-26 等日期由 08_ 登记的 MATCH（-0.09~-0.41）
变成 DIFFER（HTML − 库 ≈ +212），且符号与旧窗口「恒负」相反 ⇒ 09-16 之后
历史又一次被改写。本探针回答：**改写了哪些日期、幅度多大、是否同一批**。

严格只读（SQLite mode=ro + HTML 只读）。
"""

import os
import re
import sqlite3

DB = "file:data/database/portfolio.db?mode=ro"
REPORTS = "data/reports"
VAL_RE = re.compile(
    r'总市值</div><div class="v"[^>]*>\s*[¥￥]?\s*([0-9][0-9,]*(?:\.[0-9]+)?)'
)
DATE_RE = re.compile(r"enhanced_report_(\d{8})\.html")

conn = sqlite3.connect(DB, uri=True)
base = dict(conn.execute("SELECT date, total_value FROM portfolio_summary").fetchall())
conn.close()

rows = []
for f in sorted(os.listdir(REPORTS)):
    m = DATE_RE.fullmatch(f)
    if not m or not (f.startswith("enhanced_report_") and f.endswith(".html")):
        continue
    raw = m.group(1)
    d = f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"
    with open(os.path.join(REPORTS, f), encoding="utf-8", errors="replace") as fh:
        head = fh.read(30000)
    vm = VAL_RE.search(head)
    if vm is None or base.get(d) is None:
        continue
    hv = float(vm.group(1).replace(",", ""))
    rows.append((d, hv, base[d], hv - base[d]))

print("=" * 84)
print("DIFFER 的 delta 按「同一批改写」聚类（delta = HTML − 库值）")
print("=" * 84)

# 聚类：|delta| 相近的归为一批（容差 30 元），只列 >=2 成员的簇
small = [r for r in rows if abs(r[3]) < 1000]
print(f"\n[小差值簇 |delta| < 1,000 —— 疑似「报告生成后又被微调」的同批改写]")
small.sort(key=lambda r: r[3])
if not small:
    print("  无")
else:
    print(f"  {'date':<12}{'HTML':>13}{'库值':>15}{'delta':>12}")
    for d, hv, dv, delta in small:
        print(f"  {d:<12}{hv:>13,.0f}{dv:>15,.2f}{delta:>12,.2f}")
    vals = [r[3] for r in small]
    print(f"\n  簇成员 {len(small)} 天；delta 区间 [{min(vals):,.2f}, {max(vals):,.2f}]；"
          f"日期范围 {small[0][0]} ~ {small[-1][0]}")

big = [r for r in rows if abs(r[3]) >= 1000]
print(f"\n[大差值簇 |delta| >= 1,000 —— 08_ 已登记的旧窗口/零散改写]  共 {len(big)} 天")
big.sort(key=lambda r: r[0])
pos = [r for r in big if r[3] > 0]
neg = [r for r in big if r[3] < 0]
print(f"  delta > 0（HTML 更大）: {len(pos)} 天")
print(f"  delta < 0（库值更大）: {len(neg)} 天，区间 "
      f"[{min(r[3] for r in neg):,.2f}, {max(r[3] for r in neg):,.2f}]")

print("\n" + "=" * 84)
print("结论提示：若小差值簇集中在一段连续交易日且 delta 近乎同值，")
print("即为「一次未被任何文档登记的历史改写」的窗口。")
print("=" * 84)
