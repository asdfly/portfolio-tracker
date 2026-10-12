#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Merge a neodata fund_flow batch raw response into per-code seed JSON files.

Usage:
  venv313/Scripts/python.exe scripts/backfill/merge_neodata_pull.py <raw_batch_json> <data_dir>

- raw_batch_json: neodata fund_flow raw response {"result":[{"code":"510300.SH","name":..,"data":[...]}]}
  or normalized {"records":[...]} (single code). Supports multi-code batch (result list).
- For each code, unions records by date into fund_flow_neodata_<CODE>.json (new batch priority).
- NEVER deletes existing historical dates.
"""
import json
import os
import sys


def _iso(d):
    d = str(d)
    if len(d) == 8 and d.isdigit():
        return f"{d[0:4]}-{d[4:6]}-{d[6:8]}"
    return d


def _rec(d):
    return {
        "date": _iso(d.get("trading_date")),
        "main_net_inflow": d.get("main_net_inflow"),
        "main_inflow": d.get("main_inflow"),
        "main_outflow": d.get("main_outflow"),
        "super_large_net_inflow": d.get("super_large_net_inflow"),
        "large_net_inflow": d.get("large_net_inflow"),
    }


def load_items(raw):
    j = json.load(open(raw, encoding="utf-8"))
    if "result" in j:
        return j["result"] or []
    if "records" in j:
        return [{"code": j.get("code", ""), "name": j.get("name", ""), "data": j["records"]}]
    return []


def main():
    raw = sys.argv[1]
    data_dir = sys.argv[2]
    items = load_items(raw)
    total = 0
    for it in items:
        exc = it.get("code", "")
        bare = exc.split(".")[0]
        if not bare:
            continue
        recs = [_rec(d) for d in (it.get("data") or [])]
        # drop records with no date or no main_net_inflow (honest: no dirty write)
        recs = [r for r in recs if r["date"] and r["main_net_inflow"] is not None]
        if not recs:
            print(f"  [SKIP] {bare}: 无有效资金流记录（跳过，不写脏值）")
            continue
        seed_path = os.path.join(data_dir, f"fund_flow_neodata_{bare}.json")
        existing = {}
        if os.path.exists(seed_path):
            sj = json.load(open(seed_path, encoding="utf-8"))
            for r in sj.get("records", []):
                existing[r["date"]] = r
        for r in recs:
            existing[r["date"]] = r  # new batch priority
        out = {
            "code": bare,
            "name": it.get("name", "") or (json.load(open(seed_path, encoding="utf-8")).get("name", "") if os.path.exists(seed_path) else ""),
            "records": sorted(existing.values(), key=lambda x: x["date"]),
        }
        json.dump(out, open(seed_path, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
        total += len(recs)
        print(f"  [OK] {bare}: 合并 {len(recs)} 行 -> 种子共 {len(out['records'])} 行 ({out['records'][0]['date']}~{out['records'][-1]['date']})")
    print(f"\n[DONE] 合并 {total} 行（按日期并集，保留历史）")


if __name__ == "__main__":
    main()
