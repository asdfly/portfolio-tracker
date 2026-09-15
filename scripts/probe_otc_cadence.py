"""只读：查看 13 只场外基金各自的最新快照日期（核对补采覆盖情况）。"""
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from config.settings import OTC_FUND_CODES, SNAPSHOT_STALE_DAYS_OVERRIDE  # noqa: E402

con = sqlite3.connect(f"file:{ROOT / 'data' / 'database' / 'portfolio.db'}?mode=ro", uri=True)
q = ("SELECT ps.code, ps.name, ps.date, ps.market_value FROM portfolio_snapshots ps "
     "JOIN (SELECT code, MAX(date) AS md FROM portfolio_snapshots GROUP BY code) t "
     "ON ps.code=t.code AND ps.date=t.md")
lines = []
for code, name, d, mv in con.execute(q):
    if code not in OTC_FUND_CODES:
        continue
    lines.append(f"{code} {name[:24]:<26} {d}  市值 {mv:>12,.2f}  "
                 f"{'[周更 override=%d天]' % SNAPSHOT_STALE_DAYS_OVERRIDE[code]
                    if code in SNAPSHOT_STALE_DAYS_OVERRIDE else ''}")
lines.append(f"\n场外合计 {len(lines)} 只")
con.close()
print("\n".join(lines))
