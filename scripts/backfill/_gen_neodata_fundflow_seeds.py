#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
一次性生成脚本：把 neodata fund_flow 落盘的 3 个超限文件拆分为
per-ETF 种子 JSON（scripts/backfill/data/fund_flow_neodata_<CODE6>.json）。

设计要点：
- neodata 返回的代码带 .SH/.SZ 后缀，DB 中 fund_flows 用裸 6 位代码，需剥离后缀。
- trading_date 形如 "20260924"，统一转 ISO "2026-09-24"。
- 001323 / 002152 在 DB 中被误标为 category='etf'，但实为「混合基金」；
  neodata 按股票代码返回的是个股资金流（慕思股份 / 广电运通），与国家队标的无关，
  故显式排除，避免污染 fund_flows。
- 仅保留回填/交叉验证所需的字段：主力净流入及流入/流出额、超大单、大单净额。
  中单/小单 neodata 未单列（其 retail = 中+小），故不填，保持诚实。
"""
import json, os, sqlite3, sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT)
from config.settings import DATABASE_PATH

TOOLDIR = "C:/Users/HUAWEI/.workbuddy/projects/c-Users-HUAWEI-WorkBuddy-2026-09-23-07-52-52/a85aeec5-beac-4d1c-b081-69e4d0170b87/tool-results"
FILES = [
    "mcp-neodata-fund_flow-1790265229025-e1271d.txt",
    "mcp-neodata-fund_flow-1790265229501-eb3bf7.txt",
    "chatcmpl-tool-9e700a8ad4296033.txt",
]
OUTDIR = os.path.join(ROOT, "scripts", "backfill", "data")

# DB 中 category='etf' 的 25 个代码（来自 fund_flows）；排除实为混合基金的 2 个
EXCLUDE = {"001323", "002152"}  # 误标 ETF 的基金，neodata 返回个股资金流，不匹配

# 字段映射：neodata 原始键 -> 提取
KEEP = ["main_net_inflow", "main_inflow", "main_outflow",
        "super_large_net_inflow", "large_net_inflow"]


def iso(d):
    d = str(d)
    if len(d) == 8 and d.isdigit():
        return f"{d[0:4]}-{d[4:6]}-{d[6:8]}"
    return d


def main():
    os.makedirs(OUTDIR, exist_ok=True)
    # 收集 DB 中 etf 代码集合（用于对齐落盘）
    conn = sqlite3.connect(str(DATABASE_PATH))
    db_codes = {r[0] for r in conn.execute(
        "SELECT DISTINCT code FROM fund_flows WHERE category='etf'").fetchall()}
    conn.close()

    # 聚合 neodata 数据：code6 -> {name, exc, records by date}
    agg = {}
    for fn in FILES:
        p = os.path.join(TOOLDIR, fn)
        if not os.path.exists(p):
            print("[WARN] 缺失文件", fn); continue
        doc = json.load(open(p, encoding="utf-8"))
        for it in doc.get("result", []):
            exc = it.get("code", "")
            bare = exc.split(".")[0]
            if bare in EXCLUDE:
                print(f"[EXCLUDE] {bare} ({exc} -> {it.get('name')}) 混合基金，跳过")
                continue
            if bare not in db_codes:
                # 非目标 ETF（可能 neodata 里混了别的），仅记录不落盘
                continue
            recs = agg.setdefault(bare, {"name": it.get("name"), "exc": exc, "bydate": {}})
            for d in (it.get("data") or []):
                dt = iso(d.get("trading_date"))
                row = {k: d.get(k) for k in KEEP}
                recs["bydate"][dt] = row

    written = 0
    for bare in sorted(agg):
        info = agg[bare]
        records = [{"date": dt, **info["bydate"][dt]} for dt in sorted(info["bydate"])]
        if not records:
            continue
        payload = {
            "code": bare,
            "name": info["name"],
            "exchange_code": info["exc"],
            "source": "neodata_mcp",
            "records": records,
        }
        out = os.path.join(OUTDIR, f"fund_flow_neodata_{bare}.json")
        with open(out, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        written += 1
        ds = [r["date"] for r in records]
        print(f"[OK] {bare:12s} {info['name']:22s} {len(records):3d}行 {ds[0]}~{ds[-1]}")

    # 检查是否所有 db etf 代码（除 EXCLUDE）都拿到种子
    missing = [c for c in sorted(db_codes - EXCLUDE) if c not in agg]
    print(f"\n落盘 {written} 个种子；缺失(neodata未返回): {missing}")
    print("完成。")


if __name__ == "__main__":
    main()
