#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
合并 2026-10-13 维护拉取的 neodata / westock 资金流原始响应到既有种子文件。

纪律：
  - 按日期并集（union by date），新批覆盖同日期旧值，不删历史。
  - neodata 仅取有 trading_date 的有效记录（跳过 561910/563020 的全 0 无日期占位）。
  - westock 仅取 MainNetFlow 非空的记录（data:[] 或空 -> 不动既有种子）。
  - 不触碰生产库；只写 scripts/backfill/data/fund_flow_*_<CODE>.json。

用法：
  venv313/Scripts/python.exe scripts/backfill/merge_fundflow_20261013.py
"""
import glob
import json
import math
import os

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA = os.path.join(ROOT, "scripts", "backfill", "data")
TMP = os.path.join(DATA, "_tmp_raw")


def _iso(d):
    d = str(d)
    if len(d) == 8 and d.isdigit():
        return f"{d[0:4]}-{d[4:6]}-{d[6:8]}"
    return d


def _f(v):
    try:
        fv = float(v)
        return fv if math.isfinite(fv) else None
    except (TypeError, ValueError):
        return None


def _load_norm(path):
    if not os.path.exists(path):
        return {}, ""
    with open(path, encoding="utf-8") as fh:
        j = json.load(fh)
    existing = {}
    for r in j.get("records", []):
        existing[r["date"]] = r
    return existing, j.get("name", "")


def merge_neodata():
    raws = sorted(glob.glob(os.path.join(TMP, "neo_raw_*.json")))
    if not raws:
        print("[NEO] 无原始响应文件，跳过")
        return
    for rp in raws:
        with open(rp, encoding="utf-8") as fh:
            j = json.load(fh)
        items = j.get("result") or []
        for it in items:
            exc = it.get("code", "")
            bare = exc.split(".")[0]
            name = it.get("name", "")
            recs = []
            for d in (it.get("data") or []):
                td = d.get("trading_date")
                if not td:
                    continue  # 跳过全 0 无日期占位（561910/563020 等结构性缺口）
                net = _f(d.get("main_net_inflow"))
                recs.append({
                    "date": _iso(td),
                    "main_net_inflow": net,
                    "main_inflow": _f(d.get("main_inflow")),
                    "main_outflow": _f(d.get("main_outflow")),
                    "super_large_net_inflow": _f(d.get("super_large_net_inflow")),
                    "large_net_inflow": _f(d.get("large_net_inflow")),
                })
            if not recs:
                print(f"[NEO] {bare}: 无有效记录，跳过")
                continue
            seed_path = os.path.join(DATA, f"fund_flow_neodata_{bare}.json")
            existing, old_name = _load_norm(seed_path)
            for r in recs:
                existing[r["date"]] = r
            merged = sorted(existing.values(), key=lambda r: r["date"])
            out = {"code": bare, "name": name or old_name, "records": merged}
            with open(seed_path, "w", encoding="utf-8") as fh:
                json.dump(out, fh, ensure_ascii=False, indent=2)
            print(f"[NEO] {bare}: 合并后 {len(merged)} 行（本次新增/刷新 {len(recs)} 条）")


def merge_westock():
    raws = sorted(glob.glob(os.path.join(TMP, "ws_raw_*.json")))
    if not raws:
        print("[WS] 无原始响应文件，跳过")
        return
    for rp in raws:
        bn = os.path.basename(rp)
        bare = bn[len("ws_raw_"):-len(".json")]
        with open(rp, encoding="utf-8") as fh:
            j = json.load(fh)
        d0 = j.get("data", {})
        inner = d0.get("data") if isinstance(d0, dict) else None
        if not inner:
            print(f"[WS] {bare}: 空数据（data:[]），不动既有种子")
            continue
        recs = []
        for d in inner:
            net = _f(d.get("MainNetFlow"))
            if net is None:
                continue
            recs.append({
                "date": str(d.get("date")),
                "main_net_inflow": net,
                "super_large_inflow": _f(d.get("JumboNetFlow")),
                "large_inflow": _f(d.get("BlockNetFlow")),
            })
        if not recs:
            print(f"[WS] {bare}: 无有效记录，跳过")
            continue
        seed_path = os.path.join(DATA, f"fund_flow_westock_{bare}.json")
        existing, _ = _load_norm(seed_path)
        for r in recs:
            existing[r["date"]] = r
        merged = sorted(existing.values(), key=lambda r: r["date"])
        out = {"code": bare, "records": merged}
        with open(seed_path, "w", encoding="utf-8") as fh:
            json.dump(out, fh, ensure_ascii=False, indent=2)
        print(f"[WS] {bare}: 合并后 {len(merged)} 行（本次新增/刷新 {len(recs)} 条）")


if __name__ == "__main__":
    merge_neodata()
    merge_westock()
    print("[DONE] 合并完成")
