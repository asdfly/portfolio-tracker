# -*- coding: utf-8 -*-
"""只读探针④：给「2026-06-17~06-29 那次未登记改写」定时间窗。

方法：逐个只读打开 data/backups/portfolio_PRE_RECONCILE_*.db，
取窗口内 portfolio_summary.total_value，与当前生产库对照。
若某备份内已是下调后的值 ⇒ 改写发生在该备份时间点之前（二分定位）。

同时核对 reconcile 的作用域表：fund_flows 是否被改、portfolio_summary 是否被改。
严格只读：所有连接均为 mode=ro。
"""

import glob
import os
import sqlite3

CUR = "file:data/database/portfolio.db?mode=ro"
WINDOW = [
    "2026-06-17", "2026-06-18", "2026-06-19", "2026-06-22", "2026-06-23",
    "2026-06-24", "2026-06-25", "2026-06-26", "2026-06-29",
]
CHECKS = WINDOW + ["2026-06-15", "2026-06-30"]  # 06-15/06-30 作窗口边界对照

Q_SUM = ("SELECT date, total_value FROM portfolio_summary "
         "WHERE date BETWEEN '2026-06-15' AND '2026-06-30' ORDER BY date")


def snapshot_sum(path):
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        return dict(conn.execute(Q_SUM).fetchall())
    finally:
        conn.close()


def snapshot_ff(path):
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        n = conn.execute("SELECT COUNT(*) FROM fund_flows").fetchone()[0]
        m = conn.execute("SELECT MAX(date) FROM fund_flows").fetchone()[0]
        return n, m
    finally:
        conn.close()


cur = snapshot_sum("data/database/portfolio.db")
print("=" * 92)
print("当前生产库 vs 各 PRE_RECONCILE 备份 —— portfolio_summary.total_value")
print("=" * 92)
print(f"{'date':<12}{'当前库':>16}" + "".join(f"{'':>2}" for _ in []) )

files = sorted(glob.glob("data/backups/portfolio_PRE_RECONCILE_*.db"))
print(f"发现备份 {len(files)} 份：")
for f in files:
    print(f"  {os.path.basename(f)}")

snaps = {}
for f in files:
    try:
        snaps[f] = snapshot_sum(f)
    except Exception as e:
        print(f"  [跳过] {os.path.basename(f)}: {e}")

header_done = False
for d in CHECKS:
    if d not in cur:
        continue
    if not header_done:
        names = [os.path.basename(f)[-19:-3] for f in files]  # 时间戳部分
        print("\n" + f"{'date':<12}{'当前库':>16}" +
              "".join(f"{n:>18}" for n in names))
        header_done = True
    line = f"{d:<12}{cur[d]:>16,.2f}"
    for f in files:
        v = snaps.get(f, {}).get(d)
        line += f"{(v if v is not None else float('nan')):>18,.2f}"
    print(line)

print("\n" + "=" * 92)
print("reconcile 作用域旁证：fund_flows 行数 / max(date)")
print("=" * 92)
cn, cm = snapshot_ff("data/database/portfolio.db")
print(f"  当前生产库      rows={cn}  max={cm}")
for f in files:
    try:
        n, m = snapshot_ff(f)
        print(f"  {os.path.basename(f)}  rows={n}  max={m}")
    except Exception as e:
        print(f"  {os.path.basename(f)}  读取失败: {e}")

print("""
判读提示
--------
1) 若某备份的 06-17~06-29 值与「当前库」一致 ⇒ 改写早于该备份；
   仍≠HTML 且≈1,385,xxx ⇒ 那次改写发生在 08_ 登记(09-16)之后、该备份之前。
2) fund_flows 行数/最大日期若随备份递增 ⇒ reconcile 确实每天在改 fund_flows；
   但这**不等于**它改了 portfolio_summary ——两者必须分开证明，不可混为一谈。
""")
