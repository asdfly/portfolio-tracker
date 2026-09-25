#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
etf_fundamental 的「连接器 + MCP 兜底」回填 / 维护脚本。

背景：
  etf_fundamental 由东财 fund_etf_spot_em 主源写入，受 host 级 RST 阻断，
  23 只行业 ETF 的每日快照长期卡在 2026-09-21（见手over 17_ §7.6/§7.7）。
  本脚本用 neodata-mcp fund_quote 日K 作为兜底主源，补 09-22/23/24 等缺口，
  复用 fund_flows(neodata_mcp) 已回填的资金流字段，并以 westock-mcp data_etf
  交叉验证价格/成交额一致性。

与 backfill_fund_flows_neodata.py 同构：
  - 只读 neodata 导出的组合种子 JSON (scripts/backfill/data/etf_fundamental_neodata.json)
  - 幂等 INSERT OR IGNORE 补齐缺口日期（不覆盖既有 EM 行）
  - 加 source / is_estimated / confidence 列以审计来源
  - --verify 产出 neodata×westock×EM 三源价格交叉验证报告

用法：
  venv313/Scripts/python.exe scripts/backfill/backfill_etf_fundamental_neodata.py \
      --dir scripts/backfill/data [--verify] [--dry-run]
"""
from __future__ import annotations
import argparse
import json
import os
import sqlite3
import sys
from datetime import datetime

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from config.settings import DATABASE_PATH  # noqa: E402

SOURCE = "neodata_mcp"
SEED_FILE = "etf_fundamental_neodata.json"

# 资金流字段：从 fund_flows(neodata_mcp) 按 date+code JOIN 复用，保持与资金流表一致
FLOW_MAP = {
    "net_inflow": "main_net_inflow",
    "net_inflow_pct": "main_net_inflow_pct",
    "super_large_inflow": "super_large_net_inflow",
    "super_large_pct": "super_large_net_pct",
    "large_inflow": "large_net_inflow",
    "large_pct": "large_net_pct",
}


def ensure_columns(conn):
    cur = conn.cursor()
    for col, ctype in (("source", "TEXT"), ("is_estimated", "INTEGER"), ("confidence", "REAL")):
        cur.execute(f"SELECT COUNT(*) FROM pragma_table_info('etf_fundamental') WHERE name='{col}'")
        if cur.fetchone()[0] == 0:
            cur.execute(f"ALTER TABLE etf_fundamental ADD COLUMN {col} {ctype}")
    conn.commit()


def load_seed(seed_path):
    with open(seed_path, encoding="utf-8") as f:
        return json.load(f)


def load_westock_seed(seed_dir):
    p = os.path.join(seed_dir, "etf_fundamental_westock.json")
    if os.path.exists(p):
        with open(p, encoding="utf-8") as f:
            return json.load(f)
    return None


def fetch_flows(conn, code, date):
    """从 fund_flows(neodata_mcp) 取资金流字段，复用已回填数据。"""
    row = conn.execute(
        "SELECT net_inflow, net_inflow_pct, super_large_inflow, super_large_pct, "
        "large_inflow, large_pct FROM fund_flows "
        "WHERE code=? AND date=? AND source='neodata_mcp'",
        (code, date),
    ).fetchone()
    if not row:
        return {}
    return {
        "main_net_inflow": row[0], "main_net_inflow_pct": row[1],
        "super_large_net_inflow": row[2], "super_large_net_pct": row[3],
        "large_net_inflow": row[4], "large_net_pct": row[5],
    }


def upsert_all(seed_dir, verify, dry_run):
    seed_path = os.path.join(seed_dir, SEED_FILE)
    if not os.path.exists(seed_path):
        print(f"[fundamental] 种子缺失: {seed_path}")
        return None
    seed = load_seed(seed_path)
    westock = load_westock_seed(seed_dir)

    conn = sqlite3.connect(str(DATABASE_PATH))
    try:
        ensure_columns(conn)
        written = 0
        skipped = 0
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for code, info in seed["etfs"].items():
            name = info.get("name", "")
            sector = conn.execute(
                "SELECT sector FROM etf_fundamental WHERE code=? LIMIT 1", (code,)
            ).fetchone()
            sector = sector[0] if sector and sector[0] else ""
            for rec in info["records"]:
                dt = rec["date"]
                # 幂等：缺口日期本就不存在；用 INSERT OR IGNORE 跳过既有行
                exists = conn.execute(
                    "SELECT 1 FROM etf_fundamental WHERE code=? AND date=?", (code, dt)
                ).fetchone()
                if exists:
                    skipped += 1
                    continue
                flows = fetch_flows(conn, code, dt)
                row = {
                    "date": dt, "code": code, "name": name, "sector": sector,
                    "price": rec.get("price"), "iopv": None, "discount_rate": None,
                    "change_pct": None,
                    "volume": rec.get("volume"), "amount": rec.get("amount"),
                    "turnover_rate": rec.get("turnover_rate"), "volume_ratio": None,
                    "shares": None, "float_mv": None, "total_mv": None,
                    "source": SOURCE, "is_estimated": 0, "confidence": 1.0,
                    "created_at": now,
                }
                row.update(flows)
                cols = list(row.keys())
                ph = ", ".join(["?"] * len(cols))
                if dry_run:
                    written += 1
                    continue
                conn.execute(
                    f"INSERT OR IGNORE INTO etf_fundamental ({', '.join(cols)}) VALUES ({ph})",
                    [row[c] for c in cols],
                )
                written += 1
        if not dry_run:
            conn.commit()
    finally:
        conn.close()

    # 缺口校验
    conn = sqlite3.connect(str(DATABASE_PATH))
    try:
        maxd = conn.execute(
            "SELECT MAX(date) FROM etf_fundamental WHERE source='neodata_mcp'"
        ).fetchone()[0]
    finally:
        conn.close()
    print(f"[fundamental] 待写入/已写入={written} 跳过既有={skipped} "
          f"neodata 最新日期={maxd} (期望>=2026-09-24)")
    if verify:
        run_verify(seed_dir)
    return {"written": written, "skipped": skipped, "max_date": maxd}


def run_verify(seed_dir):
    """neodata × westock × EM 三源价格交叉验证。"""
    seed_path = os.path.join(seed_dir, SEED_FILE)
    seed = load_seed(seed_path)
    westock = load_westock_seed(seed_dir)
    conn = sqlite3.connect(str(DATABASE_PATH))
    try:
        report = {"generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                  "price_cross_check": [], "em_continuity": []}
        for code, info in seed["etfs"].items():
            for rec in info["records"]:
                dt = rec["date"]
                neo_price = rec.get("price")
                # westock 交叉（仅 09-24 有概览样本）
                ws_price = None
                if westock and code in westock.get("etfs", {}):
                    for wr in westock["etfs"][code]["records"]:
                        if wr["date"] == dt:
                            ws_price = wr.get("price")
                            break
                # EM 连续性：与最近一条 EM 行比
                em = conn.execute(
                    "SELECT price FROM etf_fundamental WHERE code=? AND source IS NULL "
                    "ORDER BY date DESC LIMIT 1", (code,)
                ).fetchone()
                em_price = em[0] if em else None
                item = {"code": code, "date": dt, "neodata": neo_price,
                        "westock": ws_price, "em_last": em_price}
                if ws_price:
                    item["neo_ws_abs_diff_pct"] = round(
                        abs(neo_price - ws_price) / ws_price * 100, 3) if ws_price else None
                if em_price:
                    item["em_to_neo_chg_pct"] = round(
                        (neo_price - em_price) / em_price * 100, 3)
                report["price_cross_check"].append(item)
        # 汇总
        ws_matches = [i for i in report["price_cross_check"] if i.get("westock")]
        ws_ok = [i for i in ws_matches if i.get("neo_ws_abs_diff_pct", 99) < 0.5]
        report["summary"] = {
            "neodata_rows": len(report["price_cross_check"]),
            "westock_cross_rows": len(ws_matches),
            "westock_match_lt_0.5pct": len(ws_ok),
        }
    finally:
        conn.close()
    out = os.path.join(seed_dir, "etf_fundamental_multisource_report.json")
    with open(out, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print(f"[fundamental] 三源验证报告: {out}")
    print(f"  neodata 行={report['summary']['neodata_rows']} "
          f"westock 交叉={report['summary']['westock_cross_rows']} "
          f"一致(<0.5%)={report['summary']['westock_match_lt_0.5pct']}")
    return report


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "data"))
    ap.add_argument("--verify", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    upsert_all(args.dir, args.verify, args.dry_run)


if __name__ == "__main__":
    main()
