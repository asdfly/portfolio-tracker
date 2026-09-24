#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
从 westock MCP 导出的 JSON 文件加载 index_quotes（幂等 upsert）。

输入 JSON 格式（每行一个对象，或整体为一个数组）：
  [date, open, close, high, low, volume, amount]
或
  {"date":..., "open":..., "close":..., "high":..., "low":..., "volume":..., "amount":...}

用法：
  venv313/Scripts/python.exe scripts/backfill/load_index_quotes_json.py \
      --json <file> --db-code sh932000 --name 中证2000 [--verify]
"""
import argparse
import json
import os
import sqlite3
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from config.settings import DATABASE_PATH  # noqa: E402


def load(path):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    rows = []
    if isinstance(data, list):
        for item in data:
            if isinstance(item, list):
                date, o, c, h, l, v, a = item[0], item[1], item[2], item[3], item[4], item[5], item[6]
            else:
                date, o, c, h, l, v, a = (
                    item["date"], item["open"], item["close"], item["high"],
                    item["low"], item.get("volume", 0), item.get("amount", 0),
                )
            rows.append((str(date), float(o), float(c), float(h), float(l),
                         float(v) if v not in (None, "") else 0.0,
                         float(a) if a not in (None, "") else 0.0))
    return rows


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--json", required=True)
    p.add_argument("--db-code", required=True)
    p.add_argument("--name", required=True)
    p.add_argument("--verify", action="store_true", help="加载后交叉校验已知收盘价")
    args = p.parse_args()

    rows = load(args.json)
    if not rows:
        print("[WARN] 空数据，跳过")
        return

    conn = sqlite3.connect(str(DATABASE_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    prev_close = None
    inserted = 0
    for date, o, c, h, l, v, a in rows:
        change_pct = round((c - prev_close) / prev_close * 100.0, 4) if prev_close else None
        prev_close = c
        conn.execute(
            "INSERT OR REPLACE INTO index_quotes(date,code,name,close,change_pct,volume,amount) VALUES(?,?,?,?,?,?,?)",
            (date, args.db_code, args.name, c, change_pct, v, a),
        )
        inserted += 1
    conn.commit()

    # 统计
    cnt = conn.execute("SELECT COUNT(*) FROM index_quotes WHERE code=?", (args.db_code,)).fetchone()[0]
    dmin = conn.execute("SELECT MIN(date) FROM index_quotes WHERE code=?", (args.db_code,)).fetchone()[0]
    dmax = conn.execute("SELECT MAX(date) FROM index_quotes WHERE code=?", (args.db_code,)).fetchone()[0]
    conn.close()
    print(f"[OK] upsert {inserted} 行；{args.db_code} 现有 {cnt} 行，区间 {dmin} ~ {dmax}")

    if args.verify:
        checks = {"2026-09-24": 3209.63, "2025-09-18": 3139.76, "2024-09-30": 2166.17}
        conn = sqlite3.connect(str(DATABASE_PATH))
        ok = True
        for d, exp in checks.items():
            r = conn.execute("SELECT close FROM index_quotes WHERE code=? AND date=?", (args.db_code, d)).fetchone()
            got = r[0] if r else None
            flag = "OK" if got == exp else "MISMATCH"
            if got != exp:
                ok = False
            print(f"  verify {d}: expected {exp} got {got} -> {flag}")
        conn.close()
        print("[VERIFY]" + ("PASS" if ok else "FAIL"))


if __name__ == "__main__":
    main()
