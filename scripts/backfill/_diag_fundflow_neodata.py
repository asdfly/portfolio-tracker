#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Diagnostic: 1) 25 ETF codes + RST gap from live DB; 2) neodata oversized-file coverage."""
import json, os, sqlite3, glob

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import sys
sys.path.insert(0, ROOT)
from config.settings import DATABASE_PATH

DB = str(DATABASE_PATH)
print("DB:", DB, "exists:", os.path.exists(DB))

conn = sqlite3.connect(DB)
# 25 ETF codes from fund_flows(category='etf')
rows = conn.execute(
    "SELECT code, name, MAX(date), COUNT(*) FROM fund_flows WHERE category='etf' GROUP BY code ORDER BY code"
).fetchall()
print(f"\n=== fund_flows etf category: {len(rows)} codes ===")
targets = {}
for code, name, mx, cnt in rows:
    targets[code] = name
    print(f"  {code:12s} {name:24s} max={mx} rows={cnt}")
conn.close()

# Parse oversized neodata files
TOOLDIR = "C:/Users/HUAWEI/.workbuddy/projects/c-Users-HUAWEI-WorkBuddy-2026-09-23-07-52-52/a85aeec5-beac-4d1c-b081-69e4d0170b87/tool-results"
files = [
    "mcp-neodata-fund_flow-1790265229025-e1271d.txt",
    "mcp-neodata-fund_flow-1790265229501-eb3bf7.txt",
    "chatcmpl-tool-9e700a8ad4296033.txt",
]
print("\n=== neodata oversized-file coverage ===")
covered = {}
for fn in files:
    p = os.path.join(TOOLDIR, fn)
    if not os.path.exists(p):
        print("  MISSING", fn); continue
    try:
        doc = json.load(open(p, encoding="utf-8"))
    except Exception as e:
        print("  PARSE-ERR", fn, e); continue
    res = doc.get("result", [])
    print(f"  {fn}: {len(res)} codes")
    for item in res:
        code = item.get("code")
        name = item.get("name")
        data = item.get("data") or []
        dates = [d.get("trading_date") for d in data]
        covered.setdefault(code, {"name": name, "dates": set(), "count": 0})
        covered[code]["dates"].update(dates)
        covered[code]["count"] += len(data)

# Cross-check: which of the 25 target ETFs are present in neodata files
print("\n=== target ETF coverage in neodata files ===")
present, missing = [], []
for code in sorted(targets):
    if code in covered:
        ds = sorted(covered[code]["dates"])
        present.append(code)
        print(f"  [OK]   {code:12s} {targets[code]:24s} days={len(ds)} {ds[0]}~{ds[-1]}")
    else:
        missing.append(code)
        print(f"  [GAP]  {code:12s} {targets[code]:24s} NOT in neodata files")
print(f"\nSummary: present={len(present)} / missing={len(missing)} (of {len(targets)} target ETFs)")
if missing:
    print("Missing codes:", missing)

# Also: what non-target codes are in neodata files (e.g. stocks)?
extra = [c for c in covered if c not in targets]
print(f"\nNon-target codes in neodata files: {len(extra)} -> {extra[:20]}")
