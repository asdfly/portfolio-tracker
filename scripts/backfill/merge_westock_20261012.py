#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Merge freshly pulled 2026-10-12 westock data_fund_flow into existing per-code
seed files (scripts/backfill/data/fund_flow_westock_<CODE>.json), preserving history.

Only the NEW 10-12 triple (MainNetFlow/JumboNetFlow/BlockNetFlow) is embedded for the
21 codes that returned data. 561910 / 563020 returned data:[] (no coverage) -> skipped.
"""
import json
import os

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA = os.path.join(ROOT, "scripts", "backfill", "data")

NEW_DATE = "2026-10-12"

# code -> (main_net_inflow, super_large_inflow, large_inflow)  for 2026-10-12
NEW = {
    "159220": (-267994, 552732, -820725),
    "159267": (-6244659, -6673818, 429159),
    "159300": (4436546, -7307776, 11744322),
    "159650": (1893006, -12687097, 14580103),
    "159732": (144889, -1519821, 1664711),
    "159770": (-8387135, -9523520, 1136385),
    "159796": (-2499431, -16251937, 13752506),
    "159819": (-41394718, -38489219, -2905499),
    "159949": (-142140706, -178661655, 36520949),
    "159992": (-46482765, -60376517, 13893752),
    "510300": (-166673497, -328898617, 162225120),
    "510500": (-51872869, -21458131, -30414738),
    "511380": (-140810865, -124580027, -16230838),
    "511520": (21924808, -50287538, 72212346),
    "512010": (2599409, 5714582, -3115173),
    "512100": (-80934076, -33746437, -47187640),
    "512810": (-4024426, -3220411, -804014),
    "515010": (978497, 1523008, -544512),
    "515120": (-37228304, -56668708, 19440404),
    "516160": (2551087, 5928819, -3377732),
    "588000": (-573173094, -538100922, -35072172),
}

def read_records(path):
    with open(path, encoding="utf-8") as fh:
        j = json.load(fh)
    if "records" in j:
        return j.get("records", [])
    if isinstance(j.get("data"), dict) and "data" in j["data"]:
        return [{"date": str(d.get("date")),
                 "main_net_inflow": d.get("MainNetFlow"),
                 "super_large_inflow": d.get("JumboNetFlow"),
                 "large_inflow": d.get("BlockNetFlow")} for d in j["data"]["data"]]
    return []

def main():
    updated = 0
    created = []
    skipped = []
    for code, (net, sup, large) in NEW.items():
        path = os.path.join(DATA, f"fund_flow_westock_{code}.json")
        rec = {"date": NEW_DATE, "main_net_inflow": net,
               "super_large_inflow": sup, "large_inflow": large}
        if os.path.exists(path):
            recs = {r["date"]: r for r in read_records(path) if r.get("date")}
            recs[NEW_DATE] = rec  # new wins
            out = {"code": code, "records": sorted(recs.values(), key=lambda r: r["date"])}
            json.dump(out, open(path, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
            updated += 1
            print(f"  [OK] {code}: merged 10-12 (now {len(out['records'])} recs, "
                  f"{out['records'][0]['date']}..{out['records'][-1]['date']})")
        else:
            created.append(code)
            out = {"code": code, "records": [rec]}
            json.dump(out, open(path, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
            print(f"  [NEW] {code}: created seed with 10-12 only")
    # codes with no data -> skip (keep existing seed untouched)
    for code in ("561910", "563020"):
        path = os.path.join(DATA, f"fund_flow_westock_{code}.json")
        has = os.path.exists(path)
        skipped.append(code)
        print(f"  [SKIP] {code}: westock data:[] (no coverage); "
              f"existing seed {'kept' if has else 'absent'}")
    print(f"\nMerged into {updated} existing seeds; created {len(created)}: {created}; "
          f"skipped (no coverage) {skipped}")

if __name__ == "__main__":
    main()
