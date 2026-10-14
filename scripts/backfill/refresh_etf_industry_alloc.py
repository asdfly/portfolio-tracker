#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
etf_industry_alloc 结构修复 + 行业配置刷新脚本。

============================ 根因说明（2026-10-14 诊断） ============================
原表结构：UNIQUE(code, industry) —— 每只 ETF 的每个行业**仅一行，无时间维度**。
而东财 EM 源 ak.fund_portfolio_industry_allocation_em(symbol=code, date=year) 一次
返回**多个报告期**（如 2026 年返回 06-30 与 03-31 两期，06-30 在前）。

写入函数 src/data_sources/etf_fundamental.py::save_to_db 用 `INSERT OR REPLACE`，
冲突键由表的 UNIQUE 约束决定（传入的 unique_cols 只用于文档，不参与冲突判定），
于是逐行覆盖：先写遍 06-30，再写遍 03-31，**旧报告期反向覆盖新报告期**。

实证：512100 EM 返回 36 行（06-30 18 行 + 03-31 18 行），落库后只剩 03-31，
06-30 仅残留 7 行（即 03-31 中不存在的行业）—— 与 DB 实测完全吻合。

后果：门类级归因（scripts/research/attribute_sector_exposure.py）只能用滞后的
03-31 口径，时效性损失约半年。

============================ 修复方案 ============================
1) --rebuild   : 重建表，UNIQUE 改为 (code, industry, report_date)，支持多报告期并存。
                 旧数据整表迁移，report_date 为空的补 '1970-01-01'。
2) --refresh   : 逐只 ETF 拉 EM 源，按 report_date 分组取**最新报告期**，
                 先 DELETE (code, report_date) 再 INSERT，保证幂等。
3) --verify    : 校验报告期分布、各 ETF 权重和、组合覆盖率。

============================ 用法 ============================
    venv313/Scripts/python.exe scripts/backfill/refresh_etf_industry_alloc.py --rebuild
    venv313/Scripts/python.exe scripts/backfill/refresh_etf_industry_alloc.py --refresh
    venv313/Scripts/python.exe scripts/backfill/refresh_etf_industry_alloc.py --verify
    # 一次性全跑（推荐）
    venv313/Scripts/python.exe scripts/backfill/refresh_etf_industry_alloc.py --rebuild --refresh --verify

注意：
  - --rebuild 前请确认已备份 DB（脚本会自检 portfolio.db.bak_* 存在性并打印提示）。
  - 本脚本只写 etf_industry_alloc 一张表，不碰其它任何表。
  - 非日期格式的 report_date（如 'neodata_latest'）属 neodata 兜底产物，
    本脚本**不删除**（保留可审计），但 verify 会单独列出，消费端须自行过滤。
"""
from __future__ import annotations

import argparse
import os
import re
import sqlite3
import sys
import time
from datetime import datetime

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from config.settings import DATABASE_PATH  # noqa: E402

TABLE = "etf_industry_alloc"
SOURCE_TAG = "em_akshare"
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

NEW_SCHEMA = """
CREATE TABLE etf_industry_alloc (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL,
    industry TEXT NOT NULL,
    weight_pct REAL,
    market_value REAL,
    report_date TEXT NOT NULL DEFAULT '1970-01-01',
    updated_at TEXT, source TEXT, is_estimated INTEGER, confidence REAL,
    UNIQUE(code, industry, report_date)
)
"""


def _log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def has_date_dimension(conn) -> bool:
    """判断当前表 UNIQUE 是否已含 report_date。"""
    cur = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (TABLE,))
    row = cur.fetchone()
    if not row or not row[0]:
        raise RuntimeError(f"表 {TABLE} 不存在")
    return "report_date" in row[0].split("UNIQUE")[-1]


# ---------------------------------------------------------------- rebuild
def rebuild(conn) -> int:
    """重建表使 UNIQUE 含 report_date，并迁移旧数据。"""
    if has_date_dimension(conn):
        _log("表已是新结构（UNIQUE 含 report_date），跳过 rebuild")
        return 0

    # 备份自检
    db_dir = os.path.dirname(os.path.abspath(DATABASE_PATH))
    baks = [f for f in os.listdir(db_dir)
            if f.startswith("portfolio.db.bak")] if os.path.isdir(db_dir) else []
    if not baks:
        _log("⚠️ 未发现 portfolio.db.bak_* 备份，建议先备份再 rebuild（5 秒后继续）")
        time.sleep(5)
    else:
        _log(f"检测到备份 {len(baks)} 份（最新 {sorted(baks)[-1]}）")

    before = conn.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()[0]
    _log(f"rebuild 开始，迁移 {before} 行…")

    conn.execute("PRAGMA foreign_keys=off")
    cur = conn.cursor()
    try:
        cur.execute("BEGIN")
        cur.execute(f"ALTER TABLE {TABLE} RENAME TO {TABLE}_old")
        cur.execute(NEW_SCHEMA)
        cur.execute(f"""
            INSERT INTO {TABLE}
                (code, industry, weight_pct, market_value, report_date,
                 updated_at, source, is_estimated, confidence)
            SELECT code, industry, weight_pct, market_value,
                   COALESCE(report_date, '1970-01-01'),
                   updated_at, source, is_estimated, confidence
            FROM {TABLE}_old
        """)
        cur.execute(f"DROP TABLE {TABLE}_old")
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    after = conn.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()[0]
    _log(f"rebuild 完成：{before} → {after} 行")
    if before != after:
        raise RuntimeError(f"行数不一致 {before} != {after}，已回滚前请人工核查")
    return after


# ---------------------------------------------------------------- refresh
def target_codes(conn) -> list:
    """取最新快照的全部持仓代码（含场外基金），ETF 优先。"""
    snap_date = conn.execute("SELECT MAX(date) FROM portfolio_snapshots").fetchone()[0]
    rows = conn.execute(
        "SELECT DISTINCT code FROM portfolio_snapshots WHERE date=? ORDER BY code",
        (snap_date,)).fetchall()
    codes = [r[0] for r in rows]
    _log(f"目标代码 {len(codes)} 个（快照日期 {snap_date}）")
    return codes


def fetch_one(code: str, year: str):
    """返回 (最新报告期, [(industry, weight_pct, market_value), ...]) 或 (None, [])。"""
    import akshare as ak
    try:
        df = ak.fund_portfolio_industry_allocation_em(symbol=code, date=year)
    except Exception as e:
        return None, [], f"{type(e).__name__}: {e}"
    if df is None or df.empty or "截止时间" not in df.columns:
        return None, [], "空返回"

    df = df.copy()
    df["rd"] = df["截止时间"].astype(str)
    # 仅保留标准日期格式的报告期
    df = df[df["rd"].map(lambda x: bool(DATE_RE.match(x)))]
    if df.empty:
        return None, [], "无有效报告期"

    latest = sorted(df["rd"].unique())[-1]
    sub = df[df["rd"] == latest]
    rows = []
    for _, r in sub.iterrows():
        ind = str(r.get("行业类别", "")).strip()
        if not ind:
            continue
        try:
            w = float(r.get("占净值比例"))
        except (TypeError, ValueError):
            continue
        try:
            mv = float(r.get("市值"))
        except (TypeError, ValueError):
            mv = None
        rows.append((ind, w, mv))
    return latest, rows, "OK"


def refresh(conn, codes, year: str, sleep: float = 0.4):
    """逐只刷新最新报告期的行业配置。"""
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ok, skipped, failed = 0, 0, []
    inserted = 0
    for i, code in enumerate(codes, 1):
        latest, rows, msg = fetch_one(code, year)
        if not rows:
            failed.append((code, msg))
            _log(f"  [{i}/{len(codes)}] {code} ✗ {msg}")
            time.sleep(sleep)
            continue

        # 幂等：先清该 (code, report_date) 的旧行
        conn.execute(
            f"DELETE FROM {TABLE} WHERE code=? AND report_date=?", (code, latest))
        for ind, w, mv in rows:
            conn.execute(f"""
                INSERT OR REPLACE INTO {TABLE}
                    (code, industry, weight_pct, market_value, report_date,
                     updated_at, source, is_estimated, confidence)
                VALUES (?,?,?,?,?,?,?,0,NULL)
            """, (code, ind, w, mv, latest, now_str, SOURCE_TAG))
            inserted += 1
        conn.commit()
        ok += 1
        wsum = sum(w for _, w, _ in rows)
        _log(f"  [{i}/{len(codes)}] {code} ✓ {latest} {len(rows)}行 权重和={wsum:.2f}%")
        time.sleep(sleep)

    _log(f"refresh 完成：成功 {ok} / 失败 {len(failed)} / 写入 {inserted} 行")
    if failed:
        _log("失败明细（多为债券ETF/无行业配置，属预期）：")
        for c, m in failed:
            _log(f"    {c}: {m}")
    return ok, len(failed), inserted


# ---------------------------------------------------------------- verify
def verify(conn):
    print("\n" + "=" * 68)
    print("verify：etf_industry_alloc 校验")
    print("=" * 68)

    print("\n[1] report_date 分布")
    for rd, n, nc in conn.execute(
            f"SELECT report_date, COUNT(*), COUNT(DISTINCT code) FROM {TABLE}"
            " GROUP BY report_date ORDER BY report_date"):
        flag = "" if DATE_RE.match(str(rd)) else "  ← 非日期格式，消费端须过滤"
        print(f"    {rd:<16} 行={n:<5} ETF数={nc}{flag}")

    print("\n[2] 权重和异常检测（各 ETF 各报告期，正常区间 90~105%）")
    bad = conn.execute(f"""
        SELECT report_date, code, ROUND(SUM(weight_pct),2), COUNT(*)
        FROM {TABLE} GROUP BY report_date, code
        HAVING SUM(weight_pct) > 105 OR SUM(weight_pct) < 90
        ORDER BY report_date, code
    """).fetchall()
    if bad:
        for rd, code, w, n in bad:
            print(f"    ⚠️ {rd} {code} 权重和={w}% 行数={n}")
    else:
        print("    ✓ 无异常（全部落在 90~105%）")

    # 最新有效报告期
    valid = conn.execute(
        f"SELECT DISTINCT report_date FROM {TABLE} ORDER BY report_date").fetchall()
    valid = [r[0] for r in valid if DATE_RE.match(str(r[0]))]
    latest_rd = valid[-1] if valid else None
    print(f"\n[3] 最新有效报告期 = {latest_rd}")

    # 组合覆盖率
    snap_date = conn.execute("SELECT MAX(date) FROM portfolio_snapshots").fetchone()[0]
    snap = conn.execute(
        "SELECT code, SUM(market_value) FROM portfolio_snapshots WHERE date=? GROUP BY code",
        (snap_date,)).fetchall()
    total_mv = sum(mv for _, mv in snap if mv)
    have = set()
    if latest_rd:
        have = set(r[0] for r in conn.execute(
            f"SELECT DISTINCT code FROM {TABLE} WHERE report_date=?", (latest_rd,)))
    cov_mv = sum(mv for c, mv in snap if c in have and mv)
    miss = sorted([(c, mv) for c, mv in snap if c not in have], key=lambda x: -(x[1] or 0))

    print(f"\n[4] 组合覆盖率（快照 {snap_date}，口径=最新报告期 {latest_rd}）")
    print(f"    有行业配置市值 {cov_mv:,.0f} / 总市值 {total_mv:,.0f} "
          f"= {cov_mv / total_mv * 100:.2f}%")
    print(f"    覆盖 {len(have)} 只 / 快照 {len(snap)} 只")
    if miss:
        print("    未覆盖明细（降序）：")
        for c, mv in miss[:20]:
            print(f"      {c}  {(mv or 0):>14,.0f}  "
                  f"({(mv or 0) / total_mv * 100:>5.2f}%)")

    # 新旧口径对比
    print("\n[5] 新旧口径可用性对比（用于门类归因）")
    for rd in ["2026-03-31", "2026-06-30"]:
        codes = set(r[0] for r in conn.execute(
            f"SELECT DISTINCT code FROM {TABLE} WHERE report_date=?", (rd,)))
        mv = sum(m for c, m in snap if c in codes and m)
        print(f"    {rd}: 覆盖 {len(codes):>2} 只，市值占比 {mv / total_mv * 100:>6.2f}%")
    print("=" * 68)


# ---------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser(description="etf_industry_alloc 结构修复与刷新")
    ap.add_argument("--rebuild", action="store_true", help="重建表使 UNIQUE 含 report_date")
    ap.add_argument("--refresh", action="store_true", help="刷新各 ETF 最新报告期行业配置")
    ap.add_argument("--verify", action="store_true", help="校验报告期分布/权重和/覆盖率")
    ap.add_argument("--year", default=str(datetime.now().year), help="EM 源年份参数")
    ap.add_argument("--codes", nargs="*", help="限定代码（默认取最新快照全部）")
    ap.add_argument("--sleep", type=float, default=0.4, help="请求间隔秒")
    args = ap.parse_args()

    if not (args.rebuild or args.refresh or args.verify):
        ap.error("至少指定 --rebuild / --refresh / --verify 之一")
    if args.refresh and not args.rebuild:
        if not has_date_dimension(sqlite3.connect(DATABASE_PATH)):
            ap.error("表仍是旧结构（UNIQUE 不含 report_date），请先 --rebuild")

    conn = sqlite3.connect(DATABASE_PATH)
    try:
        if args.rebuild:
            rebuild(conn)
        if args.refresh:
            codes = args.codes or target_codes(conn)
            refresh(conn, codes, args.year, args.sleep)
        if args.verify:
            verify(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
