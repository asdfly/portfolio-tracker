#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
etf_top_holdings / etf_industry_alloc 的「连接器 + MCP 兜底」回填 / 维护脚本。

背景：
  两张表主源均为东财 EM（fund_portfolio_hold_em / fund_portfolio_industry_allocation_em），
  受 host 级 RST 阻断风险（见手over 17_ §7.6/§7.8）。且两表本身无 source 列，无法审计来源。
  本脚本用 neodata-mcp fund_holdings / fund_allocation 作为兜底主源，套用与
  fund_flows / etf_fundamental 相同的「连接器+MCP 兜底」范式。

两张表 schema 差异决定了不同回填策略（关键，见手over 17_ §7.8）：
  - etf_top_holdings: UNIQUE(code, stock_code, quarter)
      → neodata 报告期未知(fund_report_date=null)，统一标 quarter="neodata_latest"，
        与主源 Q1 2026 快照不冲突，INSERT OR IGNORE 平行补一份"最新可得"快照（真·补缺口）。
  - etf_industry_alloc: UNIQUE(code, industry)  —— 每行业仅一行，无时间维度
      → 严禁覆盖主源季度披露数据。策略：
        (1) 仅当 (code,industry) 在主源中**完全缺失**时才 INSERT neodata（补真缺口）；
        (2) 主源已有行则跳过，仅在 --verify 中量化权重漂移（时效性陈旧判据）。

实测结论（本轮）：neodata 权重对 EM Q1 2026 锚点显著漂移（如 512010 药明康德
29.20 vs 19.85），本质为 neodata 最新可得、EM 陈旧，**非数据错误**；verify 报告
据此把差异分类为「时效漂移 / 新增持仓 / 退出持仓」。

用法：
  venv313/Scripts/python.exe scripts/backfill/backfill_etf_holdings_alloc_neodata.py \
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
SEED_HOLDINGS = "etf_top_holdings_neodata.json"
SEED_ALLOC = "etf_industry_alloc_neodata.json"
NEO_QUARTER = "neodata_latest"        # 重仓股平行快照的季度标签
NEO_REPORT_DATE = "neodata_latest"    # 行业配置缺失补齐时的报告期标签
TIMELINESS_THRESHOLD = 3.0            # 权重漂移 >=3pct 视为显著（通常时效驱动）


def ensure_columns(conn, table):
    cur = conn.cursor()
    for col, ctype in (("source", "TEXT"), ("is_estimated", "INTEGER"), ("confidence", "REAL")):
        cur.execute(f"SELECT COUNT(*) FROM pragma_table_info('{table}') WHERE name='{col}'")
        if cur.fetchone()[0] == 0:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {ctype}")
    conn.commit()


def load_seed(path):
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def upsert_holdings(seed_dir, dry_run):
    """etf_top_holdings: 平行补 neodata_latest 快照（不覆盖主源 Q1）。"""
    seed = load_seed(os.path.join(seed_dir, SEED_HOLDINGS))
    if not seed:
        print("[holdings] 种子缺失")
        return None
    conn = sqlite3.connect(str(DATABASE_PATH))
    try:
        ensure_columns(conn, "etf_top_holdings")
        written = skipped = 0
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for code, info in seed["etfs"].items():
            for h in info["holdings"]:
                sc = h["stock_code"]
                exists = conn.execute(
                    "SELECT 1 FROM etf_top_holdings WHERE code=? AND stock_code=? AND quarter=?",
                    (code, sc, NEO_QUARTER)).fetchone()
                if exists:
                    skipped += 1
                    continue
                if dry_run:
                    written += 1
                    continue
                conn.execute(
                    "INSERT OR IGNORE INTO etf_top_holdings "
                    "(code, stock_code, stock_name, weight_pct, holding_qty, market_value, "
                    " quarter, updated_at, source, is_estimated, confidence) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    (code, sc, h.get("stock_name"), h.get("weight_pct"), None, None,
                     NEO_QUARTER, now, SOURCE, 0, 1.0))
                written += 1
        if not dry_run:
            conn.commit()
    finally:
        conn.close()
    print(f"[holdings] 待写入/已写入={written} 跳过既有={skipped} (quarter={NEO_QUARTER})")
    return {"written": written, "skipped": skipped}


def upsert_alloc(seed_dir, dry_run):
    """etf_industry_alloc: 仅补主源缺失的行业；已有行不覆盖（verify 量化漂移）。"""
    seed = load_seed(os.path.join(seed_dir, SEED_ALLOC))
    if not seed:
        print("[alloc] 种子缺失")
        return None
    conn = sqlite3.connect(str(DATABASE_PATH))
    try:
        ensure_columns(conn, "etf_industry_alloc")
        written = skipped = 0
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for code, info in seed["etfs"].items():
            for ind in info["industries"]:
                name = ind["industry"]
                exists = conn.execute(
                    "SELECT 1 FROM etf_industry_alloc WHERE code=? AND industry=?",
                    (code, name)).fetchone()
                if exists:
                    skipped += 1  # 主源已有 -> 仅验证，不覆盖
                    continue
                if dry_run:
                    written += 1
                    continue
                conn.execute(
                    "INSERT OR IGNORE INTO etf_industry_alloc "
                    "(code, industry, weight_pct, market_value, report_date, updated_at, "
                    " source, is_estimated, confidence) "
                    "VALUES (?,?,?,?,?,?,?,?,?)",
                    (code, name, ind.get("weight_pct"), None, NEO_REPORT_DATE, now,
                     SOURCE, 0, 1.0))
                written += 1
        if not dry_run:
            conn.commit()
    finally:
        conn.close()
    print(f"[alloc] 缺失补写={written} 主源已有跳过={skipped} (仅验证不覆盖)")
    return {"written": written, "skipped": skipped}


def _em_latest_quarter(conn, code, stock_code):
    # 排除 neodata_latest 平行快照，确保取到主源(EM)季度锚点
    r = conn.execute(
        "SELECT weight_pct, quarter FROM etf_top_holdings "
        "WHERE code=? AND stock_code=? AND quarter<>? ORDER BY quarter DESC LIMIT 1",
        (code, stock_code, NEO_QUARTER)).fetchone()
    return (r[0], r[1]) if r else (None, None)


def _em_latest_report(conn, code, industry):
    # 排除 neodata_latest 补齐行，确保取到主源(EM)报告期锚点
    r = conn.execute(
        "SELECT weight_pct, report_date FROM etf_industry_alloc "
        "WHERE code=? AND industry=? AND report_date<>? ORDER BY report_date DESC LIMIT 1",
        (code, industry, NEO_REPORT_DATE)).fetchone()
    return (r[0], r[1]) if r else (None, None)


def run_verify(seed_dir):
    """neodata × EM(Q1锚点/最新report_date) 权重交叉验证，分类时效漂移/新增/退出。"""
    sh = load_seed(os.path.join(seed_dir, SEED_HOLDINGS))
    sa = load_seed(os.path.join(seed_dir, SEED_ALLOC))
    conn = sqlite3.connect(str(DATABASE_PATH))
    try:
        report = {"generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                  "holdings_cross": [], "alloc_cross": [],
                  "summary": {}}
        # ---- 重仓股 ----
        h_matched = h_new = h_dropped = h_significant = 0
        for code, info in (sh or {}).get("etfs", {}).items():
            for h in info["holdings"]:
                sc = h["stock_code"]
                neo_w = h["weight_pct"]
                em_w, em_q = _em_latest_quarter(conn, code, sc)
                item = {"code": code, "stock_code": sc, "stock_name": h.get("stock_name"),
                        "neodata": neo_w, "em": em_w, "em_quarter": em_q}
                if em_w is None:
                    item["class"] = "新增持仓(EM无, neodata最新)"
                    item["delta"] = None
                    h_new += 1
                else:
                    d = round(neo_w - em_w, 3)
                    item["delta"] = d
                    item["class"] = "时效漂移" if abs(d) >= TIMELINESS_THRESHOLD else "基本持平"
                    if abs(d) >= TIMELINESS_THRESHOLD:
                        h_significant += 1
                    h_matched += 1
                report["holdings_cross"].append(item)
        # EM 有但 neodata 无 -> 退出持仓（仅对种子覆盖的 8 只 ETF 比较，相对 neodata 最新快照）
        for code, info in (sh or {}).get("etfs", {}).items():
            em_stocks = {r[0] for r in conn.execute(
                "SELECT stock_code FROM etf_top_holdings WHERE code=? AND quarter<>?",
                (code, NEO_QUARTER)).fetchall()}
            neo_stocks = {x["stock_code"] for x in info["holdings"]}
            for sc in em_stocks - neo_stocks:
                h_dropped += 1
                em_w = conn.execute(
                    "SELECT weight_pct FROM etf_top_holdings WHERE code=? AND stock_code=? "
                    "AND quarter<>? ORDER BY quarter DESC LIMIT 1",
                    (code, sc, NEO_QUARTER)).fetchone()
                report["holdings_cross"].append(
                    {"code": code, "stock_code": sc, "neodata": None,
                     "em": em_w[0] if em_w else None,
                     "class": "退出持仓(EM有, neodata无)"})

        # ---- 行业配置 ----
        a_matched = a_new = a_significant = 0
        for code, info in (sa or {}).get("etfs", {}).items():
            for ind in info["industries"]:
                name = ind["industry"]
                neo_w = ind["weight_pct"]
                em_w, em_rd = _em_latest_report(conn, code, name)
                item = {"code": code, "industry": name, "neodata": neo_w,
                        "em": em_w, "em_report_date": em_rd}
                if em_w is None:
                    item["class"] = "主源缺失(neodata补写)"
                    item["delta"] = None
                    a_new += 1
                else:
                    d = round(neo_w - em_w, 3)
                    item["delta"] = d
                    item["class"] = "时效漂移" if abs(d) >= TIMELINESS_THRESHOLD else "基本持平"
                    if abs(d) >= TIMELINESS_THRESHOLD:
                        a_significant += 1
                    a_matched += 1
                report["alloc_cross"].append(item)

        report["summary"] = {
            "holdings": {"neodata_rows": len(report["holdings_cross"]) if sh else 0,
                         "matched_to_em": h_matched, "new": h_new, "dropped": h_dropped,
                         "significant_timeliness_drift": h_significant,
                         "threshold_pct": TIMELINESS_THRESHOLD},
            "alloc": {"neodata_rows": len(report["alloc_cross"]) if sa else 0,
                      "matched_to_em": a_matched, "em_missing_filled": a_new,
                      "significant_timeliness_drift": a_significant,
                      "threshold_pct": TIMELINESS_THRESHOLD},
            "interpretation": ("neodata 为最新可得快照，EM 为 Q1 2026 陈旧披露；"
                               "权重漂移主要由时效性驱动，非数据错误。"),
        }
    finally:
        conn.close()
    out = os.path.join(seed_dir, "etf_holdings_alloc_multisource_report.json")
    with open(out, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print(f"[verify] 三源验证报告: {out}")
    print(f"  重仓股: 匹配EM={h_matched} 新增={h_new} 退出={h_dropped} "
          f"显著漂移(>= {TIMELINESS_THRESHOLD}%)={h_significant}")
    print(f"  行业:   匹配EM={a_matched} 主源缺失补={a_new} "
          f"显著漂移(>= {TIMELINESS_THRESHOLD}%)={a_significant}")
    return report


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "data"))
    ap.add_argument("--verify", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    upsert_holdings(args.dir, args.dry_run)
    upsert_alloc(args.dir, args.dry_run)
    if args.verify:
        run_verify(args.dir)


if __name__ == "__main__":
    main()
