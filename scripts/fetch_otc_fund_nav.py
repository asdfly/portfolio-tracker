#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""场外基金（OTC）净值采集器 —— 补齐 portfolio_snapshots 中 13 只场外基金的每日估值行。

背景
----
场外基金不产生交易所行情，现有采集链路（run_analysis.py / data_sources）只覆盖场内 ETF，
导致 13 只场外基金的 portfolio_snapshots 长期停留在 2026-07-31（约 45 天），
合计 571,642.83 元（占组合 37.8%）在用陈旧估值参与再平衡权重计算。

设计约束（不可违反）
--------------------
1. **只 INSERT，不 UPDATE / DELETE 任何已存在行**。nav_engine 用本表算 TWR，
   重写历史会改写净值曲线。
2. **幂等**：(date, code) 唯一约束 + INSERT OR IGNORE + 内存预判重，重复执行不产生新行。
3. **范围受控**：默认只做 2026-08-01 起，可用 --start-date 放开；不做全历史回填。
4. **字段自洽**：quantity / cost_price / name / beta 沿用该 code 最后已知值；
   current_price = 当日单位净值；market_value = quantity × current_price；
   pnl = market_value − quantity × cost_price；pnl_rate 同口径重算（百分数）。
5. 880013（天添利）无公开净值源（券商资管现金管理产品，akshare 报
   Data_netWorthTrend is not defined），跳过并 logger.warning。

用法
----
    # 只打印将要插入什么，不写库（默认）
    venv313\\Scripts\\python.exe scripts\\fetch_otc_fund_nav.py

    # 确认后实写
    venv313\\Scripts\\python.exe scripts\\fetch_otc_fund_nav.py --apply

    # 放开起始日期（需单独评估 TWR 影响）
    venv313\\Scripts\\python.exe scripts\\fetch_otc_fund_nav.py --start-date 2026-01-01
"""

import argparse
import logging
import os
import sqlite3
import sys
from datetime import datetime

# 保证即使从任意 cwd 调用都能 import 到 config / src
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from config.settings import DATABASE_PATH, OTC_FUND_CODES  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("fetch_otc_fund_nav")

# 无公开净值源的场外标的：券商资管现金管理产品，akshare 取不到净值走势
NO_NAV_SOURCE = {
    "880013": "天添利（券商资管现金管理产品，非公募基金，akshare 无净值源）",
}

# 单位净值与上一已知净值偏离超过该阈值时告警（可能为份额折算 / 分红除权，
# 此时份额会变，沿用旧 quantity 会失真，需人工核对）
JUMP_WARN_THRESHOLD = 0.30

SNAPSHOT_COLUMNS = (
    "date", "code", "name", "quantity", "cost_price", "current_price",
    "market_value", "pnl", "pnl_rate", "ytd_return", "beta",
)


# --------------------------------------------------------------------------
# 数据获取
# --------------------------------------------------------------------------
def fetch_nav_history(code):
    """返回 [(date_str, nav_float), ...]，按日期升序，已去重。失败返回 None。"""
    import akshare as ak  # 延迟导入，避免无网络环境下 import 成本

    df = ak.fund_open_fund_info_em(symbol=code)
    if df is None or df.empty:
        return None

    date_col = next((c for c in df.columns if "净值日期" in str(c)), None)
    nav_col = next((c for c in df.columns if "单位净值" in str(c)), None)
    if date_col is None or nav_col is None:
        logger.warning("%s 净值表缺少『净值日期/单位净值』列，实际列=%s", code, list(df.columns))
        return None

    import pandas as pd

    out = {}
    for raw_d, raw_v in zip(df[date_col], df[nav_col]):
        try:
            d = pd.to_datetime(raw_d).strftime("%Y-%m-%d")
            v = float(raw_v)
        except Exception:
            continue
        if v > 0:
            out[d] = v  # 同日重复取最后一条

    if not out:
        return None
    return sorted(out.items())


# --------------------------------------------------------------------------
# 读库
# --------------------------------------------------------------------------
def load_baseline(conn, code):
    """该 code 最后一条快照（携带份额/成本/名称/beta 基线）。"""
    cur = conn.cursor()
    cur.execute(
        "SELECT date, name, quantity, cost_price, current_price, ytd_return, beta "
        "FROM portfolio_snapshots WHERE code=? ORDER BY date DESC, id DESC LIMIT 1",
        (code,),
    )
    row = cur.fetchone()
    if not row:
        return None
    return dict(zip(
        ("date", "name", "quantity", "cost_price", "current_price", "ytd_return", "beta"),
        row,
    ))


def load_existing_dates(conn, code):
    cur = conn.cursor()
    cur.execute("SELECT date FROM portfolio_snapshots WHERE code=?", (code,))
    return {r[0] for r in cur.fetchall()}


def latest_per_code(conn):
    """每个 code 的最新一条快照（用于组合口径前后对比）。"""
    cur = conn.cursor()
    cur.execute(
        "SELECT s.code, s.date, s.name, s.market_value, s.current_price "
        "FROM portfolio_snapshots s "
        "WHERE s.date = (SELECT MAX(x.date) FROM portfolio_snapshots x WHERE x.code = s.code) "
        "ORDER BY s.code"
    )
    return [
        dict(zip(("code", "date", "name", "market_value", "current_price"), r))
        for r in cur.fetchall()
    ]


# --------------------------------------------------------------------------
# 构造待插入行
# --------------------------------------------------------------------------
def build_rows(code, nav_hist, baseline, existing_dates, start_date):
    """按规则推导待插入行。返回 (rows, meta)。"""
    qty = baseline["quantity"]
    cost = baseline["cost_price"]
    if not qty or qty <= 0:
        logger.warning("%s 基线 quantity=%s 非正数，跳过（疑似已清仓）", code, qty)
        return [], {"skipped": "quantity<=0"}
    if not cost or cost <= 0:
        logger.warning("%s 基线 cost_price=%s 非正数，pnl/pnl_rate 按 0 处理", code, cost)
        cost = None

    name = baseline["name"]
    ytd = baseline["ytd_return"]
    beta = baseline["beta"]
    prev_nav = baseline["current_price"]

    rows = []
    jumps = []
    for d, nav in nav_hist:
        if d < start_date:
            continue
        if d <= baseline["date"]:
            continue
        if d in existing_dates:
            continue  # 幂等：已存在则跳过，绝不改写

        if prev_nav and prev_nav > 0:
            chg = nav / prev_nav - 1.0
            if abs(chg) > JUMP_WARN_THRESHOLD:
                jumps.append((d, prev_nav, nav, chg))
                logger.warning(
                    "%s %s 单位净值 %.4f -> %.4f（%+.1f%%）超阈值，疑似份额折算/分红除权，"
                    "沿用旧份额 %.2f 会失真，请人工核对 trade_records",
                    code, d, prev_nav, nav, chg * 100, qty,
                )

        mv = round(qty * nav, 2)
        cost_total = (qty * cost) if cost else 0.0
        pnl = round(mv - cost_total, 2)
        pnl_rate = round(pnl / cost_total * 100, 2) if cost_total else 0.0

        rows.append((
            d, code, name, qty, cost, nav, mv, pnl, pnl_rate, ytd, beta,
        ))
        prev_nav = nav

    meta = {
        "last_snapshot_date": baseline["date"],
        "nav_latest": nav_hist[-1] if nav_hist else None,
        "nav_count": len(nav_hist),
        "jumps": jumps,
    }
    return rows, meta


# --------------------------------------------------------------------------
# 写库
# --------------------------------------------------------------------------
def insert_rows(conn, rows):
    """只 INSERT，已存在 (date, code) 的行由 UNIQUE + OR IGNORE 保护，绝不覆盖。"""
    sql = "INSERT OR IGNORE INTO portfolio_snapshots ({}) VALUES ({})".format(
        ", ".join(SNAPSHOT_COLUMNS), ", ".join(["?"] * len(SNAPSHOT_COLUMNS))
    )
    cur = conn.cursor()
    cur.executemany(sql, rows)
    return cur.rowcount


# --------------------------------------------------------------------------
# 组合口径前后对比
# --------------------------------------------------------------------------
def portfolio_view(latest_rows, otc_new_latest, exclude_codes):
    """返回 (total, n_positions, stale_amount, stale_items)。"""
    total = 0.0
    n = 0
    stale = 0.0
    stale_items = []
    if not latest_rows:
        return total, n, stale, stale_items

    max_date = max(r["date"] for r in latest_rows)
    cutoff = max_date  # 陈旧阈值在下方按天数计算
    from datetime import timedelta
    cutoff = (datetime.strptime(max_date, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")

    for r in latest_rows:
        if r["code"] in exclude_codes:
            continue
        d = r["date"]
        mv = r["market_value"] or 0.0
        # 场外标的若本次有新净值，用新值
        if r["code"] in otc_new_latest:
            d, mv = otc_new_latest[r["code"]]
        total += mv
        n += 1
        if d < cutoff:
            stale += mv
            stale_items.append((r["code"], r["name"], d, mv))
    return total, n, stale, stale_items


# --------------------------------------------------------------------------
# 编程入口（供 run_analysis.py 日常管线调用）
# --------------------------------------------------------------------------
def run_otc_nav(start_date=None, codes=None, apply=True, log=print):
    """采集场外基金净值并写入 portfolio_snapshots（只 INSERT，幂等）。

    与 CLI main() 的区别：不打印、不读 argv、返回结构化结果供调用方上报。
    单只标的失败只记 warning 并继续，不中断整批；调用方据 ok/failed 决定阶段状态。

    Parameters
    ----------
    start_date : str | None  起始日期(含)，None 则用 CLI 默认 2026-08-01
    codes      : list | None 指定 code 子集，None 则用 OTC_FUND_CODES 全集
    apply      : bool        False 则只采集不写库（dry-run）
    log        : callable    日志函数，默认 print

    Returns
    -------
    dict: {"ok": int, "failed": int, "skipped": int, "inserted": int,
           "error": str|None, "per_code": list[dict]}
    """
    start_date = start_date or "2026-08-01"
    target = sorted(OTC_FUND_CODES)
    if codes:
        only = {str(c).strip() for c in codes if str(c).strip()}
        target = [c for c in target if c in only]

    result = {"ok": 0, "failed": 0, "skipped": 0, "inserted": 0,
              "error": None, "per_code": []}

    con = sqlite3.connect(str(DATABASE_PATH))
    con.execute("PRAGMA busy_timeout=15000")
    all_rows = []
    try:
        for code in target:
            if code in NO_NAV_SOURCE:
                log(f"  [{code}] 跳过：{NO_NAV_SOURCE[code]}")
                result["skipped"] += 1
                result["per_code"].append({"code": code, "status": "无净值源-跳过", "rows": 0})
                continue

            baseline = load_baseline(con, code)
            if not baseline:
                log(f"  [{code}] 跳过：无历史快照基线")
                result["skipped"] += 1
                result["per_code"].append({"code": code, "status": "无基线-跳过", "rows": 0})
                continue

            try:
                nav_hist = fetch_nav_history(code)
            except Exception as e:
                log(f"  [{code}] 净值获取失败：{type(e).__name__}: {e}")
                result["failed"] += 1
                result["per_code"].append(
                    {"code": code, "status": f"抓取失败-{type(e).__name__}", "rows": 0})
                continue

            if not nav_hist:
                log(f"  [{code}] 净值序列为空")
                result["failed"] += 1
                result["per_code"].append({"code": code, "status": "空序列", "rows": 0})
                continue

            existing = load_existing_dates(con, code)
            rows, meta = build_rows(code, nav_hist, baseline, existing, start_date)
            all_rows.extend(rows)
            result["ok"] += 1
            result["per_code"].append({
                "code": code, "status": "OK", "rows": len(rows),
                "nav_latest_date": meta["nav_latest"][0],
                "row_range": (rows[0][0], rows[-1][0]) if rows else None,
            })
            log(f"  [{code}] 最新净值 {meta['nav_latest'][0]} | 待插入 {len(rows)} 行")

        if not apply:
            log(f"  [dry-run] 共 {len(all_rows)} 行待插入，未写库")
            return result

        if all_rows:
            try:
                con.execute("BEGIN")
                result["inserted"] = insert_rows(con, all_rows)
                con.commit()
            except Exception as e:
                con.rollback()
                result["error"] = f"写库失败已回滚: {type(e).__name__}: {e}"
                log(f"  {result['error']}")
    finally:
        con.close()
    return result


# --------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="场外基金净值采集器（只插入新行，幂等）")
    ap.add_argument("--start-date", default="2026-08-01",
                    help="回填起始日期（含），默认 2026-08-01；放开前请先评估 TWR 影响")
    ap.add_argument("--apply", action="store_true",
                    help="真正写库；不加则只 dry-run 打印")
    ap.add_argument("--codes", default="",
                    help="逗号分隔，只处理指定 code（调试用，默认处理 OTC_FUND_CODES 全集）")
    args = ap.parse_args()

    db_path = str(DATABASE_PATH)
    mode = "APPLY(实写)" if args.apply else "DRY-RUN(不写库)"
    print("=" * 96)
    print(f"场外基金净值采集器  |  模式={mode}  |  起始日期={args.start_date}")
    print(f"数据库: {db_path}")
    print("=" * 96)

    codes = sorted(OTC_FUND_CODES)
    if args.codes:
        only = {c.strip() for c in args.codes.split(",") if c.strip()}
        codes = [c for c in codes if c in only]

    con = sqlite3.connect(db_path)
    con.execute("PRAGMA busy_timeout=15000")

    all_rows = []
    per_code = []
    otc_new_latest = {}
    before_latest = latest_per_code(con)

    for code in codes:
        if code in NO_NAV_SOURCE:
            logger.warning("跳过 %s：%s（报告中标注『无净值源』）", code, NO_NAV_SOURCE[code])
            per_code.append({"code": code, "status": "无净值源-跳过", "rows": 0})
            continue

        baseline = load_baseline(con, code)
        if not baseline:
            logger.warning("跳过 %s：portfolio_snapshots 无任何历史快照，无份额/成本基线", code)
            per_code.append({"code": code, "status": "无基线-跳过", "rows": 0})
            continue

        try:
            nav_hist = fetch_nav_history(code)
        except Exception as e:
            logger.warning("跳过 %s：净值获取失败 %s: %s", code, type(e).__name__, e)
            per_code.append({"code": code, "status": f"抓取失败-{type(e).__name__}", "rows": 0})
            continue

        if not nav_hist:
            logger.warning("跳过 %s：净值序列为空", code)
            per_code.append({"code": code, "status": "空序列-跳过", "rows": 0})
            continue

        existing = load_existing_dates(con, code)
        rows, meta = build_rows(code, nav_hist, baseline, existing, args.start_date)
        all_rows.extend(rows)

        latest_nav_d, latest_nav = meta["nav_latest"]
        new_mv = round((baseline["quantity"] or 0) * latest_nav, 2)
        otc_new_latest[code] = (latest_nav_d, new_mv)

        per_code.append({
            "code": code,
            "status": "OK",
            "rows": len(rows),
            "last_snap": meta["last_snapshot_date"],
            "last_snap_price": baseline["current_price"],
            "nav_latest_date": latest_nav_d,
            "nav_latest": latest_nav,
            "qty": baseline["quantity"],
            "cost": baseline["cost_price"],
            "new_mv": new_mv,
            "row_range": (rows[0][0], rows[-1][0]) if rows else None,
            "jumps": meta["jumps"],
        })
        print(f"  [{code}] 基线快照 {meta['last_snapshot_date']} 价 {baseline['current_price']} "
              f"| 净值最新 {latest_nav_d} = {latest_nav} | 待插入 {len(rows)} 行 "
              f"| 重算市值 {new_mv:,.2f}")

    # ---------------- 汇总 ----------------
    print("-" * 96)
    if all_rows:
        dates = sorted(r[0] for r in all_rows)
        print(f"待插入总行数: {len(all_rows)}")
        print(f"日期范围: {dates[0]} ~ {dates[-1]}  (共 {len(set(dates))} 个不同日期)")
    else:
        print("待插入总行数: 0（无新增）")
    print(f"涉及 code 数: {sum(1 for p in per_code if p['status'] == 'OK' and p['rows'] > 0)}")
    print("-" * 96)

    # 组合口径：旧 vs 新
    print("\n【组合口径对比】(按 code 取最新一条快照求和)")
    total_old, n_old, stale_old, stale_items_old = portfolio_view(
        before_latest, {}, exclude_codes={"159732"})
    total_new, n_new, stale_new, stale_items_new = portfolio_view(
        before_latest, otc_new_latest, exclude_codes={"159732"})
    print(f"  组合总市值(旧) = {total_old:,.2f}  ({n_old} 只)")
    print(f"  组合总市值(新) = {total_new:,.2f}  ({n_new} 只)")
    print(f"  变化           = {total_new - total_old:+,.2f}")
    print(f"  陈旧金额(>7天) 旧 = {stale_old:,.2f}  → 新 = {stale_new:,.2f}")
    print("  新口径下仍陈旧的标的:")
    for c, nm, d, mv in stale_items_new:
        print(f"      {c}  {nm}  {d}  {mv:,.2f}")

    # ---------------- 写库 ----------------
    if not args.apply:
        print("\n[DRY-RUN] 未写库。确认无误后加 --apply 实写。")
        print(f"          建议先备份:  copy \"{db_path}\" \"{db_path}.bak_otcnav\"")
        con.close()
        return 0

    if not all_rows:
        print("\n[APPLY] 无新行需要写入，未改动数据库。")
        con.close()
        return 0

    try:
        con.execute("BEGIN")
        affected = insert_rows(con, all_rows)
        con.commit()
    except Exception as e:
        con.rollback()
        logger.error("写库失败，已回滚: %s: %s", type(e).__name__, e)
        con.close()
        return 1
    con.close()

    print(f"\n[APPLY] 已提交 {affected} 行（只 INSERT，未修改任何已存在行）。")

    # 复核
    con2 = sqlite3.connect(db_path)
    after_latest = latest_per_code(con2)
    con2.close()
    print("\n【写入后复核】场外标的最新快照日期")
    after_map = {r["code"]: r["date"] for r in after_latest}
    for p in per_code:
        if p["status"] == "OK":
            print(f"      {p['code']}  {after_map.get(p['code'], 'N/A')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
