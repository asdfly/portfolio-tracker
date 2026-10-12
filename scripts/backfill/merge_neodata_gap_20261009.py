#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Merge the freshly pulled neodata 2026-10-08 / 2026-10-09 ETF fund-flow records
into the existing per-code seed files (fund_flow_neodata_<CODE>.json).

- Union by date; new records win on conflict (idempotent, safe to rerun).
- Does NOT delete any existing rows (the 39 prior days 2026-08-06..09-30 stay).
- 561910 / 563020 are intentionally omitted: neodata returned only a single
  zero-filled record with no trading_date for them (data gap), so nothing to add.
"""
import json
import os

DATA = r"D:/HuaweiMoveData/Users/HUAWEI/Documents/lingxi-claw/portfolio_tracker/scripts/backfill/data"

# code -> [ {date, main_net_inflow, main_inflow, main_outflow, super_large_net_inflow, large_net_inflow} ]
NEW = {
    "159220": [
        {"date": "2026-10-09", "main_net_inflow": -256795.0, "main_inflow": 10000835, "main_outflow": 10257630, "super_large_net_inflow": 460439.0, "large_net_inflow": -717234.0},
        {"date": "2026-10-08", "main_net_inflow": 66257.0, "main_inflow": 18763211.7, "main_outflow": 18696954.7, "super_large_net_inflow": 1691462.0, "large_net_inflow": -1625205.0},
    ],
    "159267": [
        {"date": "2026-10-09", "main_net_inflow": -5432505.0, "main_inflow": 87662014, "main_outflow": 93094520, "super_large_net_inflow": -8773001.0, "large_net_inflow": 3340496.0},
        {"date": "2026-10-08", "main_net_inflow": -1117047.9, "main_inflow": 62862269.6, "main_outflow": 63979317.5, "super_large_net_inflow": 31941.30000000447, "large_net_inflow": -1148989.200000003},
    ],
    "159300": [
        {"date": "2026-10-09", "main_net_inflow": -9392400.0, "main_inflow": 25521229, "main_outflow": 34913629, "super_large_net_inflow": -6363747.0, "large_net_inflow": -3028653.0},
        {"date": "2026-10-08", "main_net_inflow": 6162893.2, "main_inflow": 166430859.3, "main_outflow": 160267966.1, "super_large_net_inflow": -21901206.700000003, "large_net_inflow": 28064099.9},
    ],
    "159650": [
        {"date": "2026-10-09", "main_net_inflow": -2520161.0, "main_inflow": 94799381, "main_outflow": 97319542, "super_large_net_inflow": -5829678.0, "large_net_inflow": 3309517.0},
        {"date": "2026-10-08", "main_net_inflow": -3396229.7, "main_inflow": 323420644.8, "main_outflow": 326816874.5, "super_large_net_inflow": 64569983.599999994, "large_net_inflow": -67966213.3},
    ],
    "159732": [
        {"date": "2026-10-09", "main_net_inflow": -33271082, "main_inflow": 239664252, "main_outflow": 272935334, "super_large_net_inflow": -28919131, "large_net_inflow": -4351950.0},
        {"date": "2026-10-08", "main_net_inflow": -11994885.4, "main_inflow": 151037460.5, "main_outflow": 163032345.9, "super_large_net_inflow": -13856202.200000003, "large_net_inflow": 1861316.799999997},
    ],
    "159770": [
        {"date": "2026-10-09", "main_net_inflow": -10025384, "main_inflow": 104064555, "main_outflow": 114089939, "super_large_net_inflow": -13547873, "large_net_inflow": 3522489.0},
        {"date": "2026-10-08", "main_net_inflow": -31113062.3, "main_inflow": 88589349.69999999, "main_outflow": 119702412, "super_large_net_inflow": -29209158.1, "large_net_inflow": -1903904.200000003},
    ],
    "159796": [
        {"date": "2026-10-09", "main_net_inflow": -3408359.0, "main_inflow": 496865122, "main_outflow": 500273482, "super_large_net_inflow": -28838480, "large_net_inflow": 25430121},
        {"date": "2026-10-08", "main_net_inflow": -19286312.937, "main_inflow": 147289730.2, "main_outflow": 166576043.137, "super_large_net_inflow": -15533038.794, "large_net_inflow": -3753274.1430000067},
    ],
    "159819": [
        {"date": "2026-10-09", "main_net_inflow": -24189277, "main_inflow": 213215150, "main_outflow": 237404428, "super_large_net_inflow": 1178618.0, "large_net_inflow": -25367896},
        {"date": "2026-10-08", "main_net_inflow": -64284616.5, "main_inflow": 162579167.2, "main_outflow": 226863783.7, "super_large_net_inflow": -62983849.2, "large_net_inflow": -1300767.299999997},
    ],
    "159949": [
        {"date": "2026-10-09", "main_net_inflow": -156668089, "main_inflow": 1575320491, "main_outflow": 1731988579, "super_large_net_inflow": -188723476, "large_net_inflow": 32055388},
        {"date": "2026-10-08", "main_net_inflow": -129644217.408, "main_inflow": 1267053137.306, "main_outflow": 1396697354.7140002, "super_large_net_inflow": -244321478.9080001, "large_net_inflow": 114677261.5},
    ],
    "159992": [
        {"date": "2026-10-09", "main_net_inflow": -33912555, "main_inflow": 493029454, "main_outflow": 526942009, "super_large_net_inflow": -30182341, "large_net_inflow": -3730214.0},
        {"date": "2026-10-08", "main_net_inflow": -155525520.921, "main_inflow": 634199020.6, "main_outflow": 789724541.5209999, "super_large_net_inflow": -225427446.973, "large_net_inflow": 69901926.05200002},
    ],
    "510300": [
        {"date": "2026-10-09", "main_net_inflow": -509108241, "main_inflow": 4256705034, "main_outflow": 4765813275, "super_large_net_inflow": -931174299, "large_net_inflow": 422066058},
        {"date": "2026-10-08", "main_net_inflow": -604182287.121, "main_inflow": 2143839282.8600001, "main_outflow": 2748021569.981, "super_large_net_inflow": -708635058.9209998, "large_net_inflow": 104452771.80000007},
    ],
    "510500": [
        {"date": "2026-10-09", "main_net_inflow": -150926403, "main_inflow": 3479316925, "main_outflow": 3630243328, "super_large_net_inflow": -224993752, "large_net_inflow": 74067349},
        {"date": "2026-10-08", "main_net_inflow": -288269277.157, "main_inflow": 1617234320.5229998, "main_outflow": 1905503597.6799998, "super_large_net_inflow": -226185130.16400003, "large_net_inflow": -62084146.99300003},
    ],
    "511380": [
        {"date": "2026-10-09", "main_net_inflow": 80495166, "main_inflow": 5955768669, "main_outflow": 5875273502, "super_large_net_inflow": 665630.0, "large_net_inflow": 79829536},
        {"date": "2026-10-08", "main_net_inflow": -111659928.641, "main_inflow": 4936938056.42, "main_outflow": 5048597985.061001, "super_large_net_inflow": -148365163.12000036, "large_net_inflow": 36705234.478999615},
    ],
    "511520": [
        {"date": "2026-10-09", "main_net_inflow": -16894485, "main_inflow": 1010140962, "main_outflow": 1027035448, "super_large_net_inflow": -16321197, "large_net_inflow": -573288.0},
        {"date": "2026-10-08", "main_net_inflow": 32012181.7, "main_inflow": 863171443.7, "main_outflow": 831159262, "super_large_net_inflow": 81461786.39999998, "large_net_inflow": -49449604.70000002},
    ],
    "512010": [
        {"date": "2026-10-09", "main_net_inflow": -18659356, "main_inflow": 335385807, "main_outflow": 354045163, "super_large_net_inflow": -35410032, "large_net_inflow": 16750676},
        {"date": "2026-10-08", "main_net_inflow": -15155342.82, "main_inflow": 347345107.20000005, "main_outflow": 362500450.02, "super_large_net_inflow": -23506887.072999954, "large_net_inflow": 8351544.252999999},
    ],
    "512100": [
        {"date": "2026-10-09", "main_net_inflow": -548798047, "main_inflow": 4995935786, "main_outflow": 5544733833, "super_large_net_inflow": -651131114, "large_net_inflow": 102333067},
        {"date": "2026-10-08", "main_net_inflow": -101041554.956, "main_inflow": 1989021942.059, "main_outflow": 2090063497.0149999, "super_large_net_inflow": 38350.57100009918, "large_net_inflow": -101079905.52700001},
    ],
    "512810": [
        {"date": "2026-10-09", "main_net_inflow": -4261474.0, "main_inflow": 27181821, "main_outflow": 31443296, "super_large_net_inflow": -2535056.0, "large_net_inflow": -1726419.0},
        {"date": "2026-10-08", "main_net_inflow": -1917396.2, "main_inflow": 20790791.5, "main_outflow": 22708187.7, "super_large_net_inflow": -1517432.3999999985, "large_net_inflow": -399963.80000000075},
    ],
    "515010": [
        {"date": "2026-10-09", "main_net_inflow": -893549.0, "main_inflow": 46621513, "main_outflow": 47515061, "super_large_net_inflow": -7043281.0, "large_net_inflow": 6149732.0},
        {"date": "2026-10-08", "main_net_inflow": -2942888.2, "main_inflow": 32486646, "main_outflow": 35429534.2, "super_large_net_inflow": -3476080.5, "large_net_inflow": 533192.3000000007},
    ],
    "515120": [
        {"date": "2026-10-09", "main_net_inflow": -23809500, "main_inflow": 349757714, "main_outflow": 373567213, "super_large_net_inflow": -32525755, "large_net_inflow": 8716256.0},
        {"date": "2026-10-08", "main_net_inflow": -112924841.027, "main_inflow": 406029828.00699997, "main_outflow": 518954669.03400004, "super_large_net_inflow": -193795221.569, "large_net_inflow": 80870380.542},
    ],
    "516160": [
        {"date": "2026-10-09", "main_net_inflow": 2375024.0, "main_inflow": 76390932, "main_outflow": 74015908, "super_large_net_inflow": -1351773.0, "large_net_inflow": 3726797.0},
        {"date": "2026-10-08", "main_net_inflow": -9307683.7, "main_inflow": 102089406, "main_outflow": 111397089.69999999, "super_large_net_inflow": -7030504.299999997, "large_net_inflow": -2277179.399999991},
    ],
    "588000": [
        {"date": "2026-10-09", "main_net_inflow": -1476614866, "main_inflow": 7651979642, "main_outflow": 9128594509, "super_large_net_inflow": -2298194925, "large_net_inflow": 821580058},
        {"date": "2026-10-08", "main_net_inflow": -2339003277.315, "main_inflow": 5098588653.952, "main_outflow": 7437591931.267, "super_large_net_inflow": -2912624533.6819997, "large_net_inflow": 573621256.3670001},
    ],
}


def main():
    ok = 0
    for code, recs in NEW.items():
        path = os.path.join(DATA, f"fund_flow_neodata_{code}.json")
        if os.path.exists(path):
            seed = json.load(open(path, encoding="utf-8"))
        else:
            seed = {"code": code, "name": "", "records": []}
        existing = {r.get("date"): r for r in seed.get("records", []) if r.get("date")}
        added = 0
        for r in recs:
            d = r["date"]
            if d not in existing:
                added += 1
            existing[d] = r  # new wins
        seed["records"] = sorted(existing.values(), key=lambda r: r["date"])
        seed["code"] = code
        json.dump(seed, open(path, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
        ok += 1
        print(f"  {code}: +{added} new, total {len(seed['records'])} rows, max={seed['records'][-1]['date']}")
    print(f"\nMerged neodata gap for {ok}/{len(NEW)} codes "
          f"(561910/563020 omitted: neodata returned no valid 10-08/10-09 record).")


if __name__ == "__main__":
    main()
