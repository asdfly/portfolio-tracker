#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
经 neodata MCP fund_flow 回填 / 维护 fund_flows（ETF 资金流）。

为什么需要这个脚本：
  portfolio 主取数链（东财 push2his / fund_etf_spot_em）在本机被 host 级 RST 阻断，
  导致 23 只行业 ETF 资金流长期卡在最后成功日（2026-09-21），实时增量补数也因同一
  RST 失败、缺口跨日累积。
  实测 neodata-mcp 的 fund_flow 能稳定返回 ETF 完整资金流（主力净流入/超大单/大单，单位「元」），
  且与东财口径在主力净流入上量级一致，故作为「连接器 + MCP 兜底」数据源，绕开东财 RST。

本脚本与 backfill_etf_price_history_westock.py / backfill_index_quotes_westock.py 同构：
只读 neodata 导出的种子 JSON（scripts/backfill/data/fund_flow_neodata_<CODE6>.json），
幂等 upsert 入 fund_flows；不负责取数（取数由 WB 每日自动化调 neodata-mcp 完成，落 JSON 后本脚本消费）。

字段映射（neodata 原始 -> fund_flows 列）：
  trading_date               -> date      (ISO "2026-09-24")
  main_net_inflow           -> net_inflow
  main_inflow               -> buy_amount        (主力流入额)
  main_outflow              -> sell_amount       (主力流出额)
  super_large_net_inflow    -> super_large_inflow
  large_net_inflow          -> large_inflow
  （中单/小单 neodata 未单列 retail=中+小，故留 NULL，保持诚实；net_inflow_pct 亦不填）

落库纪律（P1-A + #130/#135）：
  - source='neodata_mcp', is_estimated=0, confidence=1.0
  - 仅补齐「缺口」：已存在的真实源行(em_spot/em_push2his/neodata_mcp) 不覆盖；
    仅当某日现有行是 kline_est(estimate) 时才用真实值升级覆盖。
  - 复用 src.data_sources.fund_flow.save_fund_flows，自动继承其 #130/#135 守卫
    （指标全空跳过 / 不以 NULL 覆盖真值 / net_inflow 为空拒绝 INSERT）。

多源交叉验证（--verify）：
  源A(主写) neodata_mcp vs
  源B(交叉) westock-mcp data_fund_flow（种子 fund_flow_westock_<CODE6>.json，如果存在）vs
  锚点(历史真值) fund_flows 现有 em_spot / em_push2his 行（同 code+date）。
  产出三方一致率报告 -> data/fund_flow_multisource_report.json。

用法：
  venv313/Scripts/python.exe scripts/backfill/backfill_fund_flows_neodata.py --dir scripts/backfill/data [--verify] [--dry-run]
"""
import argparse
import glob
import json
import math
import os
import sqlite3
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from config.settings import DATABASE_PATH  # noqa: E402
from src.data_sources.fund_flow import save_fund_flows  # noqa: E402

SOURCE = "neodata_mcp"
CATEGORY = "etf"
# 真实可信源（不覆盖）；其余（如 kline_est）视为可升级
TRUSTED_SOURCES = {"em_spot", "em_push2his", "neodata_mcp", "westock_mcp"}

# 交叉验证相对容差
REL_TOL = 0.05        # 5% 视为「接近」，超过视为分歧
ABS_TOL = 1000.0      # 1000 元以内视为一致（舍入）


def _f(v):
    try:
        fv = float(v)
        return fv if math.isfinite(fv) else None
    except (TypeError, ValueError):
        return None


def _iso(d):
    d = str(d)
    if len(d) == 8 and d.isdigit():
        return f"{d[0:4]}-{d[4:6]}-{d[6:8]}"
    return d


def load_seed(path):
    with open(path, encoding="utf-8") as fh:
        j = json.load(fh)
    # 兼容两种形态：
    #  (A) 归一化种子：{"code","name","records":[{"date", "main_net_inflow", ...}]}
    #  (B) neodata 原始响应：{"result":[{"code":"510300.SH","name":...,"data":[
    #        {"trading_date":"20260924","main_net_inflow":...,"main_inflow":...,
    #         "main_outflow":...,"super_large_net_inflow":...,"large_net_inflow":...}]}]}
    if "records" in j:
        return j
    if "result" in j:
        items = j["result"] or []
        if not items:
            return {"code": "", "name": "", "records": []}
        it = items[0]
        exc = it.get("code", "")
        bare = exc.split(".")[0]
        recs = []
        for d in (it.get("data") or []):
            recs.append({
                "date": _iso(d.get("trading_date")),
                "main_net_inflow": d.get("main_net_inflow"),
                "main_inflow": d.get("main_inflow"),
                "main_outflow": d.get("main_outflow"),
                "super_large_net_inflow": d.get("super_large_net_inflow"),
                "large_net_inflow": d.get("large_net_inflow"),
            })
        return {"code": bare, "name": it.get("name", ""), "exchange_code": exc, "records": recs}
    return j


def load_westock_seed(seed_dir, code):
    p = os.path.join(seed_dir, f"fund_flow_westock_{code}.json")
    if not os.path.exists(p):
        return None
    with open(p, encoding="utf-8") as fh:
        j = json.load(fh)
    # 归一化种子：{"records":[{"date","main_net_inflow","super_large_inflow","large_inflow"}]}
    if "records" in j:
        return j
    # 原始 westock 响应：{"ok":true,"data":{"code":"sh510300","data":[
    #    {"date":"2026-08-25","MainNetFlow":...,"JumboNetFlow":...,"BlockNetFlow":...}]}}
    if "data" in j and isinstance(j["data"], dict) and "data" in j["data"]:
        recs = []
        for d in (j["data"].get("data") or []):
            recs.append({
                "date": str(d.get("date")),
                "main_net_inflow": d.get("MainNetFlow"),
                "super_large_inflow": d.get("JumboNetFlow"),
                "large_inflow": d.get("BlockNetFlow"),
            })
        return {"code": code, "records": recs}
    return j


def decide_actions(conn, code, records):
    """返回 (to_insert, skipped, upgraded)：基于现有 fund_flows 决定写入策略。"""
    existing = {}
    for date, src, est in conn.execute(
        "SELECT date, source, is_estimated FROM fund_flows WHERE code=? AND category=?",
        (code, CATEGORY),
    ).fetchall():
        existing[date] = (src, int(est or 0))

    to_insert = []
    skipped = 0
    upgraded = 0
    for rec in records:
        dt = rec["date"]
        if dt not in existing:
            to_insert.append(rec)
        else:
            src, est = existing[dt]
            if est == 1 or src not in TRUSTED_SOURCES:
                # 现有是估算/未知源 -> 用真实 neodata 升级覆盖
                to_insert.append(rec)
                upgraded += 1
            else:
                skipped += 1  # 已是可信真实源，保留
    return to_insert, skipped, upgraded


def build_df_rows(seed):
    code = seed["code"]
    name = seed.get("name", "")
    rows = []
    for rec in seed["records"]:
        net = _f(rec.get("main_net_inflow"))
        if net is None:
            continue  # 主指标缺失不落库（守卫 3）
        rows.append({
            "date": rec["date"],
            "code": code,
            "name": name,
            "net_inflow": net,
            "buy_amount": _f(rec.get("main_inflow")),
            "sell_amount": _f(rec.get("main_outflow")),
            "super_large_inflow": _f(rec.get("super_large_net_inflow")),
            "large_inflow": _f(rec.get("large_net_inflow")),
            "category": CATEGORY,
            "source": SOURCE,
            "is_estimated": 0,
            "confidence": 1.0,
        })
    return rows


def upsert_all(seed_dir, verify, dry_run):
    files = sorted(glob.glob(os.path.join(seed_dir, "fund_flow_neodata_*.json")))
    if not files:
        print(f"[ERROR] 在 {seed_dir} 未找到 fund_flow_neodata_*.json")
        return None

    conn = sqlite3.connect(str(DATABASE_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    try:
        total_insert = 0
        total_skip = 0
        total_upgrade = 0
        per_code = []
        for fp in files:
            seed = load_seed(fp)
            code = seed["code"]
            records = seed.get("records", [])
            to_insert, skipped, upgraded = decide_actions(conn, code, records)
            rows = build_df_rows({"code": code, "name": seed.get("name", ""),
                                  "records": to_insert})
            if not rows:
                per_code.append((code, 0, skipped, upgraded))
                continue
            if dry_run:
                print(f"[DRY] {code}: 将写入 {len(rows)} 行（跳过 {skipped} / 升级 {upgraded}）"
                      f" {rows[0]['date']}~{rows[-1]['date']}")
                total_skip += skipped
                total_upgrade += upgraded
                per_code.append((code, len(rows), skipped, upgraded))
                continue
            import pandas as pd
            df = pd.DataFrame(rows)
            n = save_fund_flows(conn, df)
            total_insert += n
            total_skip += skipped
            total_upgrade += upgraded
            per_code.append((code, n, skipped, upgraded))
            print(f"[OK] {code}: 写入 {n} 行（跳过 {skipped} 既有可信 / 升级 {upgraded} 估算）"
                  f" {rows[0]['date']}~{rows[-1]['date']}")

        # 缺口校验
        asof = "2026-09-24"
        print(f"\n=== 缺口回填校验（期望 max(date) >= {asof}）===")
        all_ok = True
        for code, n, sk, up in per_code:
            r = conn.execute(
                "SELECT MAX(date), COUNT(*) FROM fund_flows WHERE code=? AND category=?",
                (code, CATEGORY)).fetchone()
            maxd = r[0] or ""
            ok = maxd >= asof
            if not ok:
                all_ok = False
            print(f"  {code}: max={maxd} rows={r[1]} [{'OK' if ok else 'GAP'}]")
        print(f"\n[DONE] 写入 {total_insert} 行；跳过既有可信 {total_skip}；"
              f"升级估算 {total_upgrade}；"
              + ("全部缺口已补齐 ✅" if all_ok else "仍有缺口 ⚠️"))

        if verify:
            report = run_verify(conn, seed_dir)
            return {"insert": total_insert, "skip": total_skip, "upgrade": total_upgrade,
                    "gap_ok": all_ok, "verify": report}
        return {"insert": total_insert, "skip": total_skip, "upgrade": total_upgrade,
                "gap_ok": all_ok}
    finally:
        conn.close()


# ------------------------- 多源交叉验证 -------------------------
def _rel_diff(a, b):
    if a is None or b is None:
        return None
    denom = max(abs(a), abs(b), 1.0)
    return abs(a - b) / denom


def run_verify(conn, seed_dir):
    """neodata(主写) vs westock(交叉) vs em_spot/em_push2his(锚点)。返回报告 dict。"""
    files = sorted(glob.glob(os.path.join(seed_dir, "fund_flow_neodata_*.json")))
    anchor_src = {"em_spot", "em_push2his"}
    report = {"rel_tol": REL_TOL, "abs_tol": ABS_TOL, "codes": {}, "summary": {}}

    tot_anchor = tot_anchor_match = 0
    tot_ws = tot_ws_match = 0
    codes_with_ws = 0

    for fp in files:
        seed = load_seed(fp)
        code = seed["code"]
        # 锚点：DB 现有真实源行
        anchor_map = {}
        for date, net, src in conn.execute(
            "SELECT date, net_inflow, source FROM fund_flows "
            "WHERE code=? AND category=? AND source IN ('em_spot','em_push2his')",
            (code, CATEGORY)).fetchall():
            anchor_map[date] = (net, src)
        # westock 种子
        ws = load_westock_seed(seed_dir, code)
        ws_map = {r["date"]: r for r in ws.get("records", [])} if ws else None
        if ws_map:
            codes_with_ws += 1

        a_overlap = a_match = a_close = a_mismatch = 0
        a_examples = []
        w_overlap = w_match = w_close = w_mismatch = 0
        w_examples = []

        neo_map = {r["date"]: r for r in seed["records"]}
        for dt, rec in neo_map.items():
            neo_net = _f(rec.get("main_net_inflow"))
            # 锚点
            if dt in anchor_map:
                anc_net, anc_src = anchor_map[dt]
                anc_net = _f(anc_net)
                a_overlap += 1
                tot_anchor += 1
                rd = _rel_diff(neo_net, anc_net)
                if rd is not None and (abs(neo_net - anc_net) <= ABS_TOL or rd <= REL_TOL):
                    a_match += 1
                    tot_anchor_match += 1
                elif rd is not None and rd <= 0.15:
                    a_close += 1
                else:
                    a_mismatch += 1
                    if len(a_examples) < 3:
                        a_examples.append({"date": dt, "neodata": neo_net,
                                           "anchor": anc_net, "rel_diff": round(rd, 4),
                                           "anchor_src": anc_src})
            # westock
            if ws_map and dt in ws_map:
                ws_net = _f(ws_map[dt].get("main_net_inflow"))
                w_overlap += 1
                tot_ws += 1
                rd = _rel_diff(neo_net, ws_net)
                if rd is not None and (abs(neo_net - ws_net) <= ABS_TOL or rd <= REL_TOL):
                    w_match += 1
                    tot_ws_match += 1
                elif rd is not None and rd <= 0.15:
                    w_close += 1
                else:
                    w_mismatch += 1
                    if len(w_examples) < 3:
                        w_examples.append({"date": dt, "neodata": neo_net,
                                           "westock": ws_net, "rel_diff": round(rd, 4)})

        code_rep = {
            "anchor_overlap": a_overlap, "anchor_match": a_match,
            "anchor_close": a_close, "anchor_mismatch": a_mismatch,
            "anchor_match_rate": round(a_match / a_overlap, 4) if a_overlap else None,
            "anchor_examples": a_examples,
        }
        if ws_map:
            code_rep.update({
                "westock_overlap": w_overlap, "westock_match": w_match,
                "westock_close": w_close, "westock_mismatch": w_mismatch,
                "westock_match_rate": round(w_match / w_overlap, 4) if w_overlap else None,
                "westock_examples": w_examples,
            })
        report["codes"][code] = code_rep

    report["summary"] = {
        "codes_total": len(files),
        "codes_with_westock": codes_with_ws,
        "anchor_overlap": tot_anchor, "anchor_match": tot_anchor_match,
        "anchor_match_rate": round(tot_anchor_match / tot_anchor, 4) if tot_anchor else None,
        "westock_overlap": tot_ws, "westock_match": tot_ws_match,
        "westock_match_rate": round(tot_ws_match / tot_ws, 4) if tot_ws else None,
    }
    # 落盘报告
    out = os.path.join(seed_dir, "fund_flow_multisource_report.json")
    with open(out, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print(f"\n=== 多源交叉验证报告 ===")
    s = report["summary"]
    print(f"  锚点(em_spot/em_push2his)重叠 {s['anchor_overlap']} 点，"
          f"一致率 {s['anchor_match_rate']}（容差±{int(REL_TOL*100)}%）")
    if s["codes_with_westock"]:
        print(f"  westock 重叠 {s['westock_overlap']} 点（{s['codes_with_westock']} 只），"
              f"一致率 {s['westock_match_rate']}")
    else:
        print("  westock 种子缺失：运行每日自动化拉取 westock 后可补交叉验证")
    print(f"  报告已写入 {out}")
    return report


# ------------------------- 历史锚点对账（维护动作） -------------------------
def reconcile_anchor(conn, seed_dir, dry_run):
    """将「neodata 与 westock 双源一致、但与 em_spot/em_push2his 锚点背离」的历史行，
    用 neodata 真实值upgrade覆盖（仅当双源确认，避免单源误改）。

    背景（数据质量根因）：多源交叉验证发现，em_spot(em via akshare fund_etf_spot_em)
    对中小盘 ETF 的主力净流入与 neodata / westock 双源背离达 2~5 倍甚至反向；而 neodata
    与 westock（两家独立厂商）在全部抽样标的上逐元一致。故 em_spot 历史值不可信，
    应以双源一致的 neodata 为准回填历史。

    安全约束：
    - 必须存在 westock 种子（双源确认）才对账，杜绝单源误改；
    - 仅覆盖「现有锚点行确实背离(>5%)」的日期，已一致的日期不动；
    - 执行前自动备份 DB；dry-run 仅报告不动库。
    """
    import shutil, time as _t
    if not dry_run:
        _bdir = os.path.join(ROOT, "data", "backups")
        os.makedirs(_bdir, exist_ok=True)
        _bak = os.path.join(_bdir, f"portfolio_PRE_RECONCILE_{_t.strftime('%Y%m%d_%H%M%S')}.db")
        shutil.copy2(str(DATABASE_PATH), _bak)
        print(f"[BACKUP] 已备份 DB -> {_bak}")
    files = sorted(glob.glob(os.path.join(seed_dir, "fund_flow_neodata_*.json")))
    total = 0
    per_code = []
    for fp in files:
        seed = load_seed(fp)
        code = seed["code"]
        ws = load_westock_seed(seed_dir, code)
        if not ws:
            per_code.append((code, 0, "no_westock"))
            continue
        ws_map = {r["date"]: r for r in ws.get("records", [])}
        neo_map = {r["date"]: r for r in seed["records"]}
        # 双源一致日期
        reconciled = 0
        for dt, nrec in neo_map.items():
            if dt not in ws_map:
                continue
            wrec = ws_map[dt]
            neo_net = _f(nrec.get("main_net_inflow"))
            ws_net = _f(wrec.get("main_net_inflow"))
            if neo_net is None or ws_net is None:
                continue
            rd = _rel_diff(neo_net, ws_net)
            if not (rd is not None and (abs(neo_net - ws_net) <= ABS_TOL or rd <= REL_TOL)):
                continue  # 双源本身不一致，跳过（不冒险）
            # 现有锚点行（em_spot / em_push2his）
            row = conn.execute(
                "SELECT id, net_inflow, source FROM fund_flows "
                "WHERE code=? AND date=? AND category=? AND source IN ('em_spot','em_push2his')",
                (code, dt, CATEGORY)).fetchone()
            if not row:
                continue
            anchor_net = _f(row[1])
            if anchor_net is not None:
                ard = _rel_diff(neo_net, anchor_net)
                if not (ard is None or abs(neo_net - anchor_net) <= ABS_TOL or ard <= REL_TOL):
                    # 背离 -> 需要覆盖
                    if dry_run:
                        reconciled += 1
                        continue
                    conn.execute(
                        """UPDATE fund_flows SET net_inflow=?, super_large_inflow=?, large_inflow=?,
                           buy_amount=?, sell_amount=?, source=?, is_estimated=0, confidence=1.0
                           WHERE id=?""",
                        (_f(nrec.get("main_net_inflow")), _f(nrec.get("super_large_net_inflow")),
                         _f(nrec.get("large_net_inflow")), _f(nrec.get("main_inflow")),
                         _f(nrec.get("main_outflow")), SOURCE, row[0]))
                    reconciled += 1
        per_code.append((code, reconciled, "ok" if reconciled else "-"))
        total += reconciled
    conn.commit()
    if not dry_run:
        print(f"\n[DONE] 对账覆盖 {total} 行（双源确认 + 锚点背离的历史 em_spot 行 -> neodata_mcp）")
    else:
        print(f"\n[DRY] 将对账覆盖 {total} 行（双源确认 + 锚点背离）")
    for code, n, st in per_code:
        print(f"  {code}: {n} 行 [{st}]")
    return total


def main():
    p = argparse.ArgumentParser(description="经 neodata-mcp 回填 fund_flows（ETF 资金流）")
    p.add_argument("--dir", default=os.path.join(ROOT, "scripts", "backfill", "data"),
                   help="含 fund_flow_neodata_*.json 的目录")
    p.add_argument("--verify", action="store_true", help="多源交叉验证（neodata vs westock vs 锚点）")
    p.add_argument("--reconcile", action="store_true",
                   help="双源确认下，将背离的 em_spot 历史行对账为 neodata（需 westock 种子，自动备份DB）")
    p.add_argument("--dry-run", action="store_true", help="只预览，不写库")
    args = p.parse_args()

    if args.reconcile:
        # 维护路径：先 upsert 补齐每日新数据（不重复验证），再双源对账修正历史 em_spot 脏行，
        # 最后统一跑三源交叉验证刷新报告。reconcile 以「neodata+westock 双源确认」为前提，
        # 仅覆盖确实背离的历史锚点行，执行前自动备份 DB，可安全重入。
        upsert_all(args.dir, False, args.dry_run)
        if args.dry_run:
            n = reconcile_anchor(sqlite3.connect(str(DATABASE_PATH)), args.dir, True)
            return
        conn = sqlite3.connect(str(DATABASE_PATH))
        try:
            reconcile_anchor(conn, args.dir, args.dry_run)
        finally:
            conn.close()
        conn2 = sqlite3.connect(str(DATABASE_PATH))
        try:
            run_verify(conn2, args.dir)
        finally:
            conn2.close()
        return

    res = upsert_all(args.dir, args.verify, args.dry_run)
    if res is None:
        sys.exit(1)


if __name__ == "__main__":
    main()
