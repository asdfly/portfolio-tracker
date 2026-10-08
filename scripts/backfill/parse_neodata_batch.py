#!/usr/bin/env python3
# Parse the 3 neodata fund_flow batch tool-result files, normalize per-ETF,
# merge with any existing seed (union by date, new batch wins on conflict),
# and write scripts/backfill/data/fund_flow_neodata_<CODE>.json
import json, os, glob

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA = os.path.join(ROOT, "scripts", "backfill", "data")
TR = "C:/Users/HUAWEI/.workbuddy/projects/d-HuaweiMoveData-Users-HUAWEI-Documents-lingxi-claw-portfolio_tracker/44653eac-86c5-4c52-8c59-e1080266a127/tool-results"

BATCH_FILES = [
    os.path.join(TR, "mcp-neodata-fund_flow-1791413566544-ca7963.txt"),  # batch1: 10 codes
    os.path.join(TR, "mcp-neodata-fund_flow-1791413566586-ff269f.txt"),  # batch2: 10 codes
    os.path.join(TR, "chatcmpl-tool-8925b05fba401ab2.txt"),              # batch3: 3 codes
]

FIELDS = ["main_net_inflow", "main_inflow", "main_outflow",
          "super_large_net_inflow", "large_net_inflow"]

def parse_file(path):
    raw = open(path, encoding="utf-8", errors="replace").read()
    try:
        return json.loads(raw)
    except Exception:
        s = raw.find("{")
        e = raw.rfind("}")
        return json.loads(raw[s:e+1])

def iso(d):
    if d is None:
        return None
    d = str(d).strip()
    if len(d) == 8 and d.isdigit():
        return f"{d[0:4]}-{d[4:6]}-{d[6:8]}"
    return d

def main():
    # code -> list of records (date -> record dict)
    merged = {}
    meta = {}   # code -> name
    per_file = []
    for fp in BATCH_FILES:
        if not os.path.exists(fp):
            per_file.append((os.path.basename(fp), "MISSING"))
            continue
        j = parse_file(fp)
        results = j.get("result") if isinstance(j, dict) else None
        if not results:
            per_file.append((os.path.basename(fp), f"no-result(status={j.get('status') if isinstance(j,dict) else '?'})"))
            continue
        cnt = 0
        for it in results:
            exc = it.get("code", "")
            bare = exc.split(".")[0]
            name = it.get("name", "")
            meta.setdefault(bare, name)
            recs = merged.setdefault(bare, {})
            for d in (it.get("data") or []):
                date = iso(d.get("trading_date"))
                if not date:
                    continue
                rec = {"date": date}
                for f in FIELDS:
                    rec[f] = d.get(f)
                # new batch wins
                recs[date] = rec
                cnt += 1
        per_file.append((os.path.basename(fp), f"codes={len(results)} recs={cnt}"))
    # write merged seeds
    written = 0
    for code, recs in merged.items():
        out = {
            "code": code,
            "name": meta.get(code, ""),
            "records": sorted(recs.values(), key=lambda r: r["date"]),
        }
        opath = os.path.join(DATA, f"fund_flow_neodata_{code}.json")
        # merge with existing seed if present
        if os.path.exists(opath):
            try:
                ej = json.load(open(opath, encoding="utf-8"))
                existing = {r.get("date"): r for r in ej.get("records", []) if r.get("date")}
                # existing kept unless new batch has same date
                for date, r in existing.items():
                    if date not in recs:
                        recs[date] = r
                out["records"] = sorted(recs.values(), key=lambda r: r["date"])
            except Exception as e:
                print(f"  [warn] merge existing {code} failed: {e}")
        json.dump(out, open(opath, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
        written += 1
    print("BATCH FILES:")
    for f, s in per_file:
        print(f"  {f}: {s}")
    print(f"\nWROTE {written} seed files to {DATA}")
    # summary: date ranges
    print("\nPER-ETF DATE RANGE (new seeds):")
    for code in sorted(merged.keys()):
        recs = merged[code]
        dates = sorted(recs.keys())
        print(f"  {code}: n={len(dates)} {dates[0]}..{dates[-1]}")

if __name__ == "__main__":
    main()
