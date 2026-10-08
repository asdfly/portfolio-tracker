# -*- coding: utf-8 -*-
"""只读探针②：按 08_ 文档「完全相同的判定口径」重算报告库漂移现状（2026-10-08）。

08_report_library_drift.md 于 2026-09-16 登记 97 份 → MATCH 57 / DIFFER 35 / NO_DB_ROW 5。
本脚本**不改**判定口径，仅把同一套判据重跑在今天，回答一个问题：
**漂移是收敛了，还是仍在扩大？**

严格只读：SQLite 以 file:...?mode=ro 打开；HTML 只读文本。
"""

import os
import re
import sqlite3

DB = "file:data/database/portfolio.db?mode=ro"
REPORTS = "data/reports"
DATE_MIN_TRADE = "2026-09-15"  # 08_ 登记的窗口上限，用于区分「已登记」与「新增」

# ⚠️ 必须用精确正则。08_ 明确警告：不可用「总市值 + 任意字符」去抓，
#    因为 <div class="v" style="color:#1a73e8"> 里的 #1a73e8 含数字会被误命中成 1.0。
VAL_RE = re.compile(
    r'总市值</div><div class="v"[^>]*>\s*[¥￥]?\s*([0-9][0-9,]*(?:\.[0-9]+)?)'
)
DATE_RE = re.compile(r"enhanced_report_(\d{8})\.html")
THRESHOLD = 0.51  # HTML 为整数千分位、库内 2 位小数 ⇒ 真实误差只来自四舍五入

conn = sqlite3.connect(DB, uri=True)
base = dict(conn.execute("SELECT date, total_value FROM portfolio_summary").fetchall())
conn.close()

files = sorted(
    f for f in os.listdir(REPORTS)
    if f.startswith("enhanced_report_") and f.endswith(".html")
)

rows = []
for f in files:
    m = DATE_RE.fullmatch(f)
    if not m:
        continue
    raw = m.group(1)
    d = f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"
    with open(os.path.join(REPORTS, f), encoding="utf-8", errors="replace") as fh:
        head = fh.read(30000)
    vm = VAL_RE.search(head)
    html_val = None if vm is None else float(vm.group(1).replace(",", ""))
    db_val = base.get(d)
    if html_val is None:
        verdict = "NO_HEADER"
    elif db_val is None:
        verdict = "NO_DB_ROW"
    elif abs(html_val - db_val) < THRESHOLD:
        verdict = "MATCH"
    else:
        verdict = "DIFFER"
    rows.append((f, d, html_val, db_val, verdict))

total = len(rows)
count = {}
for r in rows:
    count[r[4]] = count.get(r[4], 0) + 1

print("=" * 84)
print(f"报告库漂移 —— 当轮重算（判定口径完全沿用 08_ §一，阈值 {THRESHOLD}）")
print("=" * 84)
print(f"HTML 份数 = {total}   （08_ 于 2026-09-16 登记为 97 份）")
for k in ("MATCH", "DIFFER", "NO_DB_ROW", "NO_HEADER"):
    if k in count:
        print(f"  {k:<12} = {count[k]}")
print(f"  --- 恒等式: MATCH + DIFFER + NO_DB_ROW + NO_HEADER = "
      f"{sum(count.get(k, 0) for k in ('MATCH', 'DIFFER', 'NO_DB_ROW', 'NO_HEADER'))} / {total}")

new_rows = [r for r in rows if r[1] > DATE_MIN_TRADE]
print(f"\n新增报告（晚于 {DATE_MIN_TRADE}，即 08_ 登记之后产生的）= {len(new_rows)} 份")
for f, d, hv, dv, v in new_rows:
    hs = f"{hv:,.0f}" if hv is not None else "—"
    ds = f"{dv:,.2f}" if dv is not None else "—"
    print(f"  {f}  {d}  HTML={hs:>12}  库={ds:>15}  {v}")

diff_rows = [r for r in rows if r[4] == "DIFFER"]
print(f"\nDIFFER 清单（{len(diff_rows)} 份，前 12 行；08_ 原登记 35 份）：")
for f, d, hv, dv, v in diff_rows[:12]:
    print(f"  {f}  {d}  HTML={hv:,.0f}  库={dv:,.2f}  差={hv - dv:,.2f}")
if len(diff_rows) > 12:
    print(f"  ...（其余 {len(diff_rows) - 12} 份省略）")

print("\n[探针结束] SQLite 只读、HTML 只读，未执行任何写操作。")
