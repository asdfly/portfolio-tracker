#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
从「westock data_kline 同构」的原始 JSON（{"ok":true,"data":{"nodes":[...]}} 形态，
收盘字段名 last）回填 index_quotes（幂等 upsert）。

数据源解耦说明（2026-10-08 起）：
  本脚本只认 JSON 形态，不绑定任何具体取数源。历史上由 westock-mcp data_kline 产出该
  JSON；但 westock-mcp 在 WB 自动化运行时长期不可连接，且官方中证指数公司源
  akshare.stock_zh_index_hist_csindex('932000') 更权威、本机直连可用、不依赖任何连接器。
  故自 2026-10-08 起，**本任务的取数源正式改为官方 CSIndex**，由同目录
  refresh_sh932000.py（一步到位：取数→落盘→回填→清毒→终验）调用本脚本完成落库。
  若仍用 westock 取数，JSON 形态完全一致，可继续喂给本脚本。

输入 JSON 形态：
  {"ok":true,"data":{"nodes":[{"date":"2026-09-24","open":...,"last":...,"high":...,"low":...,"volume":...,"amount":...}, ...]}}

用法：
  venv313/Scripts/python.exe scripts/backfill/backfill_index_quotes_westock.py \
      --raw scripts/backfill/data/cs932000_latest.json \
      --db-code sh932000 --name 中证2000 [--verify]

自动化（WB 每日维护）权威入口：
  venv313/Scripts/python.exe scripts/backfill/refresh_sh932000.py
  （内部取 CSIndex 真实数据 → 落盘 westock 同构 JSON → 调用本脚本 --verify → 清毒 → 终验）
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


def parse_raw(path):
    """解析 westock data_kline 原始 JSON → [(date, open, close, high, low, volume, amount), ...]"""
    with open(path, "r", encoding="utf-8") as f:
        doc = json.load(f)
    nodes = doc.get("data", {}).get("nodes", [])
    rows = []
    for n in nodes:
        date = str(n["date"])
        o = float(n.get("open", 0) or 0)
        c = float(n.get("last", 0) or 0)          # westock 收盘字段名是 last
        h = float(n.get("high", 0) or 0)
        l = float(n.get("low", 0) or 0)
        v = float(n.get("volume", 0) or 0)
        a = float(n.get("amount", 0) or 0)
        rows.append((date, o, c, h, l, v, a))
    # 按日期升序，保证 change_pct 递推正确
    rows.sort(key=lambda r: r[0])
    return rows


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--raw", required=True, help="westock data_kline 导出的原始 JSON 路径")
    p.add_argument("--db-code", required=True)
    p.add_argument("--name", required=True)
    p.add_argument("--verify", action="store_true", help="加载后交叉校验已知收盘价")
    args = p.parse_args()

    rows = parse_raw(args.raw)
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

    cnt = conn.execute("SELECT COUNT(*) FROM index_quotes WHERE code=?", (args.db_code,)).fetchone()[0]
    dmin = conn.execute("SELECT MIN(date) FROM index_quotes WHERE code=?", (args.db_code,)).fetchone()[0]
    dmax = conn.execute("SELECT MAX(date) FROM index_quotes WHERE code=?", (args.db_code,)).fetchone()[0]
    conn.close()
    print(f"[OK] upsert {inserted} 行；{args.db_code} 现有 {cnt} 行，区间 {dmin} ~ {dmax}")

    if args.verify:
        # 关键点来自 westock 与东财交叉验证（背景：东财 RST 阻断前的最后可用快照）
        checks = {"2026-09-24": 3209.63, "2025-09-18": 3139.76, "2024-09-30": 2166.17}
        conn = sqlite3.connect(str(DATABASE_PATH))
        ok = True
        for d, exp in checks.items():
            r = conn.execute("SELECT close FROM index_quotes WHERE code=? AND date=?",
                             (args.db_code, d)).fetchone()
            got = r[0] if r else None
            flag = "OK" if got == exp else "MISMATCH"
            if got != exp:
                ok = False
            print(f"  verify {d}: expected {exp} got {got} -> {flag}")
        conn.close()
        print("[VERIFY]" + ("PASS" if ok else "FAIL"))


if __name__ == "__main__":
    main()
