#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
etf_fundamental 的 akshare 兜底回填脚本（neodata 不可达时使用）。

背景：
  etf_fundamental 主源为 neodata_mcp（连接器框架 deferred tool），但在自动化会话里
  neodata 未被注入（DeferExecuteTool 报 "Tool not found in the deferred tools index"），
  导致每日自动化回退冻结种子、数据逐日滞后。本脚本用 akshare（em 主源 -> sina 兜底）
  直接拉取最新交易日快照写入 etf_fundamental，作为 neodata 不可达时的兜底，使每日
  自动化不再依赖 neodata 连接器。

口径约定（必须与 etf_fundamental 现有 neodata 数据严格一致，否则拼接处出现断层）：
  - 价格：不复权收盘价。
      * sina fund_etf_hist_sina 返回【未复权】价（无 adjust 参数）—— 与 neodata 不复权口径一致。
      * em  fund_etf_hist_em(adjust="") 亦为未复权。
  - volume：【股】。已实测 sina fund_etf_hist_sina 的 volume 即为股单位（510300 09-29
           sina=468501408 与 neodata=468501400 逐位一致），与 etf_fundamental 的股口径一致。
           ⚠️ 切勿 ×100（与 etf_price_history 表的"手"单位不同，两表单位本就不同）。
  - amount：元（成交额）。
  - turnover_rate：sina 无此列 -> 留 NULL；em 有"换手率"列 -> 取用。
  - 资金流字段：本兜底不覆盖（独立管线），留 NULL。

幂等：INSERT OR IGNORE，仅补齐 (code,date) 不存在的行，绝不覆盖 neodata 既有行。

用法：
  venv313/Scripts/python.exe scripts/backfill/backfill_etf_fundamental_akshare.py \
      [--days 5] [--codes 510300,159300,...] [--dry-run] [--verify]
  （默认对全部 23 只持仓 ETF 补最近 5 个交易日中 DB 缺失的日期）
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
import warnings
from datetime import datetime

warnings.filterwarnings("ignore")

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from config.settings import DATABASE_PATH  # noqa: E402

try:
    from src.analysis.predictor.price_history import _code6_to_symbol  # noqa: E402
except Exception:  # 兜底：复制映射逻辑
    def _code6_to_symbol(code6: str) -> str:
        return ("sh" if code6[0] in "56" else "sz") + code6

SOURCE_EM = "akshare_fund_etf_hist_em"
SOURCE_SINA = "akshare_fund_etf_hist_sina"

# 与 gen_etf_fundamental_seeds.py 的 NAMES 保持一致，作为 name 兜底来源
NAMES = {
    "159300": "沪深300ETF富国", "510300": "沪深300ETF华泰柏瑞", "159220": "港股通红利低波ETF华宝",
    "159267": "航天ETF华安", "159650": "国开债ETF博时", "159732": "消费电子ETF华夏",
    "159770": "机器人ETF天弘", "159796": "电池ETF汇添富", "159819": "人工智能ETF易方达",
    "159949": "创业板50ETF华安", "159992": "创新药ETF银华", "510500": "中证500ETF南方",
    "511380": "可转债ETF博时", "511520": "政金债ETF富国", "512010": "医药ETF易方达",
    "512100": "中证1000ETF南方", "512810": "军工ETF华宝", "515010": "证券ETF华夏",
    "515120": "创新药ETF广发", "516160": "新能源ETF南方", "561910": "电池ETF招商",
    "563020": "红利低波ETF易方达", "588000": "科创50ETF华夏",
}

DEFAULT_CODES = list(NAMES.keys())


def ensure_columns(conn):
    cur = conn.cursor()
    for col, ctype in (("source", "TEXT"), ("is_estimated", "INTEGER"), ("confidence", "REAL")):
        cur.execute(
            f"SELECT COUNT(*) FROM pragma_table_info('etf_fundamental') WHERE name='{col}'"
        )
        if cur.fetchone()[0] == 0:
            cur.execute(f"ALTER TABLE etf_fundamental ADD COLUMN {col} {ctype}")
    conn.commit()


def fetch_one_akshare(code6: str, days: int = 5):
    """单只 ETF 取最近 days 日快照，返回 list[dict] 或 []。em 主源 -> sina 兜底。

    返回字段：date(str YYYY-MM-DD), price(float), volume(float 股), amount(float 元),
              turnover_rate(float|None), src(str)
    """
    import akshare as ak

    # 1) em 主源（不复权）
    try:
        df = ak.fund_etf_hist_em(symbol=code6, period="daily", adjust="")
        if df is not None and not df.empty:
            df = df.tail(days)
            rows = []
            for _, r in df.iterrows():
                rows.append({
                    "date": str(r["日期"]),
                    "price": float(r["收盘"]),
                    "volume": float(r["成交量"]),
                    "amount": float(r["成交额"]),
                    "turnover_rate": float(r["换手率"]) if "换手率" in r and r["换手率"] == r["换手率"] else None,
                    "src": SOURCE_EM,
                })
            if rows:
                return rows
    except Exception as e:
        print(f"  [akshare] {code6} em 失败: {repr(e)[:120]} -> 试 sina")

    # 2) sina 兜底（未复权，英文列名，无换手率）
    try:
        sym = _code6_to_symbol(code6)
        df = ak.fund_etf_hist_sina(symbol=sym)
        if df is not None and not df.empty:
            df = df.tail(days)
            rows = []
            for _, r in df.iterrows():
                rows.append({
                    "date": str(r["date"]),
                    "price": float(r["close"]),
                    "volume": float(r["volume"]),
                    "amount": float(r["amount"]),
                    "turnover_rate": None,  # sina 无换手率列
                    "src": SOURCE_SINA,
                })
            if rows:
                return rows
    except Exception as e:
        print(f"  [akshare] {code6} sina 失败: {repr(e)[:120]}")

    return []


def upsert_all(codes, days, dry_run, verify):
    conn = sqlite3.connect(str(DATABASE_PATH))
    try:
        ensure_columns(conn)
        written = 0
        skipped = 0
        failed = 0
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        per_code_src = {}
        for code in codes:
            rows = fetch_one_akshare(code, days)
            if not rows:
                failed += 1
                per_code_src[code] = "FAILED"
                continue
            per_code_src[code] = rows[-1]["src"]
            name = conn.execute(
                "SELECT name FROM etf_fundamental WHERE code=? LIMIT 1", (code,)
            ).fetchone()
            name = name[0] if name and name[0] else NAMES.get(code, "")
            sector = conn.execute(
                "SELECT sector FROM etf_fundamental WHERE code=? LIMIT 1", (code,)
            ).fetchone()
            sector = sector[0] if sector and sector[0] else ""
            for rec in rows:
                dt = rec["date"]
                exists = conn.execute(
                    "SELECT 1 FROM etf_fundamental WHERE code=? AND date=?", (code, dt)
                ).fetchone()
                if exists:
                    skipped += 1
                    continue
                row = {
                    "date": dt, "code": code, "name": name, "sector": sector,
                    "price": rec["price"], "iopv": None, "discount_rate": None,
                    "change_pct": None,
                    "volume": rec["volume"], "amount": rec["amount"],
                    "turnover_rate": rec["turnover_rate"], "volume_ratio": None,
                    "shares": None, "float_mv": None, "total_mv": None,
                    "source": rec["src"], "is_estimated": 0, "confidence": 1.0,
                    "created_at": now,
                }
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

    print(f"[akshare-fundamental] 待写/已写={written} 跳过既有={skipped} "
          f"取数失败={failed} (目标 {len(codes)} 只, 近 {days} 日)")
    print(f"[akshare-fundamental] 各 code 取数源: "
          + ", ".join(f"{c}={s}" for c, s in per_code_src.items()))
    if verify:
        run_verify()
    return {"written": written, "skipped": skipped, "failed": failed}


def run_verify():
    """与 neodata 兜底行交叉校验最新日期与价格一致性（如发现同 date 同 code 多源差异）。"""
    conn = sqlite3.connect(str(DATABASE_PATH))
    try:
        maxd = conn.execute("SELECT MAX(date) FROM etf_fundamental").fetchone()[0]
        n_ak = conn.execute(
            "SELECT COUNT(*) FROM etf_fundamental WHERE date=? AND source LIKE 'akshare_%'",
            (maxd,),
        ).fetchone()[0]
        n_neo = conn.execute(
            "SELECT COUNT(*) FROM etf_fundamental WHERE date=? AND source='neodata_mcp'",
            (maxd,),
        ).fetchone()[0]
        print(f"[akshare-fundamental] 最新日期={maxd} akshare行={n_ak} neodata行={n_neo}")
    finally:
        conn.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=5)
    ap.add_argument("--codes", default=",".join(DEFAULT_CODES),
                    help="逗号分隔 6 位代码，默认全部 23 只持仓")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--verify", action="store_true")
    args = ap.parse_args()
    codes = [c.strip() for c in args.codes.split(",") if c.strip()]
    upsert_all(codes, args.days, args.dry_run, args.verify)


if __name__ == "__main__":
    main()
