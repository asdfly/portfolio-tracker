#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Merge the freshly pulled 2026-10-12 neodata fund_flow records into the
existing per-code seed files (scripts/backfill/data/fund_flow_neodata_<CODE>.json).

Only the NEW trading day 2026-10-12 is embedded (21 codes). The seeds already
hold 41 days through 2026-10-09; union-by-date keeps history, adds 10-12.
561910 / 563020 are intentionally omitted (neodata returns zero-coverage junk).
"""
import json
import os

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA = os.path.join(ROOT, "scripts", "backfill", "data")

NEW_DATE = "2026-10-12"

# code -> (name, net, inflow, outflow, super_large, large)
DATA_1012 = {
    "159220": ("港股通红利低波ETF华宝", -267994.0, 1.7876966E7, 1.814496E7, 552732.0, -820725.0),
    "159267": ("航天ETF华安", -6244659.0, 5.9744258E7, 6.5988917E7, -6673818.0, 429159.0),
    "159300": ("沪深300ETF富国", 4436546.0, 3.9429734E7, 3.4993187E7, -7307776.0, 1.1744322E7),
    "159650": ("国开债ETF博时", 1893006.0, 6.44736715E8, 6.4284371E8, -1.2687097E7, 1.4580103E7),
    "159732": ("消费电子ETF华夏", 144889.0, 1.55593237E8, 1.55448348E8, -1519821.0, 1664711.0),
    "159770": ("机器人ETF天弘", -8387135.0, 4.3703261E7, 5.2090396E7, -9523520.0, 1136385.0),
    "159796": ("电池ETF汇添富", -2499431.0, 2.14859718E8, 2.17359149E8, -1.6251937E7, 1.3752506E7),
    "159819": ("人工智能ETF易方达", -4.1394718E7, 1.12348446E8, 1.53743164E8, -3.8489219E7, -2905499.0),
    "159949": ("创业板50ETF华安", -1.42140706E8, 1.062148481E9, 1.204289187E9, -1.78661655E8, 3.6520949E7),
    "159992": ("创新药ETF银华", -4.6482765E7, 4.94600851E8, 5.41083616E8, -6.0376517E7, 1.3893752E7),
    "510300": ("沪深300ETF华泰柏瑞", -1.66673497E8, 2.293920491E9, 2.460593988E9, -3.28898617E8, 1.6222512E8),
    "510500": ("中证500ETF南方", -5.1872869E7, 2.034943204E9, 2.086816073E9, -2.1458131E7, -3.0414738E7),
    "511380": ("可转债ETF博时", -1.40810865E8, 5.225637657E9, 5.366448522E9, -1.24580027E8, -1.6230838E7),
    "511520": ("政金债ETF富国", 2.1924808E7, 1.024520269E9, 1.002595461E9, -5.0287538E7, 7.2212346E7),
    "512010": ("医药ETF易方达", 2599409.0, 2.99800447E8, 2.97201038E8, 5714582.0, -3115173.0),
    "512100": ("中证1000ETF南方", -8.0934076E7, 2.127686288E9, 2.208620364E9, -3.3746437E7, -4.718764E7),
    "512810": ("军工ETF华宝", -4024426.0, 2.178795E7, 2.5812376E7, -3220411.0, -804014.0),
    "515010": ("证券ETF华夏", 978497.0, 3.2492298E7, 3.1513801E7, 1523008.0, -544512.0),
    "515120": ("创新药ETF广发", -3.7228304E7, 2.56963548E8, 2.94191852E8, -5.6668708E7, 1.9440404E7),
    "516160": ("新能源ETF南方", 2551087.0, 8.7916364E7, 8.5365278E7, 5928819.0, -3377732.0),
    "588000": ("科创50ETF华夏", -5.73173094E8, 4.394863458E9, 4.968036552E9, -5.38100922E8, -3.5072172E7),
}

def main():
    updated = 0
    skipped = []  # codes with no existing seed
    for code, (name, net, inflow, outflow, sup, large) in DATA_1012.items():
        path = os.path.join(DATA, f"fund_flow_neodata_{code}.json")
        rec = {
            "date": NEW_DATE,
            "main_net_inflow": net,
            "main_inflow": inflow,
            "main_outflow": outflow,
            "super_large_net_inflow": sup,
            "large_net_inflow": large,
        }
        if os.path.exists(path):
            with open(path, encoding="utf-8") as fh:
                seed = json.load(fh)
            recs = {r["date"]: r for r in seed.get("records", []) if r.get("date")}
            recs[NEW_DATE] = rec  # new batch wins
            # keep existing name if present
            name = seed.get("name") or name
            out = {"code": code, "name": name,
                   "records": sorted(recs.values(), key=lambda r: r["date"])}
            json.dump(out, open(path, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
            updated += 1
            print(f"  [OK] {code}: merged 10-12 (now {len(out['records'])} recs, "
                  f"{out['records'][0]['date']}..{out['records'][-1]['date']})")
        else:
            skipped.append(code)
            print(f"  [SKIP] {code}: no existing seed file")
    print(f"\nMerged 10-12 into {updated} seeds; {len(skipped)} skipped (no seed): {skipped}")

if __name__ == "__main__":
    main()
