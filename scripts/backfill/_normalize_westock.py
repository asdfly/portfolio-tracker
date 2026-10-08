#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""一次性 helper：把 westock data_fund_flow 原始响应归一化为 fund_flow_westock_<CODE>.json。
用法: python _normalize_westock.py <raw_json_path> <bare_code> [out_dir]
原始形态: {"ok":true,"data":{"code":"sz159267","data":[{"date","MainNetFlow","JumboNetFlow","BlockNetFlow",...}]}}
输出形态: {"code":"<裸码>","records":[{"date","main_net_inflow","super_large_inflow","large_inflow"}]}
"""
import sys, json, os


def _num(v):
    if v is None or v == "":
        return None
    try:
        f = float(v)
        return f
    except (TypeError, ValueError):
        return None


def main():
    raw = sys.argv[1]
    code = sys.argv[2]
    out_dir = sys.argv[3] if len(sys.argv) > 3 else \
        r"D:/HuaweiMoveData/Users/HUAWEI/Documents/lingxi-claw/portfolio_tracker/scripts/backfill/data"

    with open(raw, encoding="utf-8") as fh:
        j = json.load(fh)

    if not (j.get("ok") and isinstance(j.get("data"), dict) and isinstance(j["data"].get("data"), list)):
        if "records" in j:
            recs = j["records"]
        else:
            print(f"[SKIP] {code}: 原始响应格式异常，无 data.data")
            return
    else:
        recs = []
        for d in j["data"]["data"]:
            dt = d.get("date")
            if not dt:
                continue
            recs.append({
                "date": str(dt),
                "main_net_inflow": _num(d.get("MainNetFlow")),
                "super_large_inflow": _num(d.get("JumboNetFlow")),
                "large_inflow": _num(d.get("BlockNetFlow")),
            })

    if not recs:
        print(f"[SKIP] {code}: 0 条记录，不写文件")
        return

    out = {"code": code, "records": recs}
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"fund_flow_westock_{code}.json")
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(out, fh, ensure_ascii=False, indent=2)
    print(f"[OK] {code}: 归一化 {len(recs)} 行 -> {out_path}")


if __name__ == "__main__":
    main()
