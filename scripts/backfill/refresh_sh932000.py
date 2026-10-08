#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""中证2000(932000) index_quotes 缓存刷新 —— 官方 CSIndex 源（westock 解耦）。

本脚本是「每日维护 932000 缓存」任务的**唯一权威入口**，取代了原先依赖
westock-mcp data_kline 的取数链路（westock 在 WB 自动化运行时长期不可连接，且
CSIndex 比 westock 更权威、本机 akshare 直连可用、不依赖任何连接器）。

流程（全链路，幂等、可重跑）：
  1. 取数：akshare 官方中证指数公司源 stock_zh_index_hist_csindex('932000')，
     显式窗口 start=20240101 / end=今日动态，取得真实完整 OHLCV。
  2. 落盘：写成 westock data_kline 同构 JSON（{"ok":true,"data":{"nodes":[...]}}），
     收盘字段名 last，供 backfill_index_quotes_westock.py 直接消费。
  3. 回填：调用 backfill_index_quotes_westock.py --verify（幂等 upsert + 三项锚点校验）。
  4. 清毒：删除主管线可能写入的「假期占位行」（date > 末真实交易日 且 change_pct IS NULL）。
  5. 终验：行数 >=500、PRAGMA integrity_check=ok、poison=0。

注：主管线「占位单点写日期行未填真实价」的 bug 已于 2026-10-08 在
src/analysis/portfolio.py / src/utils/database.py 根治（缓存兜底行不再落库），
本脚本的清毒步骤仅作为历史残留与防御性兜底。
"""
import os
import sys
import json
import shutil
import sqlite3
import subprocess
from datetime import date, datetime

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from config.settings import DATABASE_PATH  # noqa: E402

BACKFILL = os.path.join(ROOT, "scripts/backfill/backfill_index_quotes_westock.py")
RAW = os.path.join(ROOT, "scripts/backfill/data/cs932000_latest.json")
DB_CODE = "sh932000"
NAME = "中证2000"
START = "20240101"


def fetch_csindex():
    """取官方 CSIndex 真实 OHLCV，返回 nodes 列表（升序）。"""
    import akshare as ak
    end = date.today().strftime("%Y%m%d")
    df = ak.stock_zh_index_hist_csindex(symbol="932000", start_date=START, end_date=end)
    nodes = []
    for _, r in df.iterrows():
        d = str(r["日期"])
        try:
            d = d[:10]
        except Exception:
            pass
        nodes.append({
            "date": d,
            "open": float(r["开盘"]),
            "last": float(r["收盘"]),
            "high": float(r["最高"]),
            "low": float(r["最低"]),
            "volume": float(r["成交量"]),
            "amount": float(r["成交金额"]),
        })
    nodes.sort(key=lambda n: n["date"])
    doc = {"ok": True, "data": {"nodes": nodes}}
    os.makedirs(os.path.dirname(RAW), exist_ok=True)
    with open(RAW, "w", encoding="utf-8") as f:
        json.dump(doc, f, ensure_ascii=False)
    return nodes


def run_backfill():
    """调用 backfill_index_quotes_westock.py --verify，返回 (rc, stdout)。"""
    proc = subprocess.run(
        [sys.executable, BACKFILL, "--raw", RAW, "--db-code", DB_CODE,
         "--name", NAME, "--verify"],
        capture_output=True, text=True,
    )
    print(proc.stdout)
    if proc.returncode != 0:
        print(proc.stderr)
    return proc.returncode, proc.stdout


def cleanup_poison(latest_real):
    """删除 932000 中 date>latest_real 且 change_pct IS NULL 的占位行（精确按 code+date）。"""
    db = str(DATABASE_PATH)
    conn = sqlite3.connect(db)
    conn.execute("PRAGMA journal_mode=WAL")
    poison = conn.execute(
        "SELECT date, close, change_pct FROM index_quotes "
        "WHERE code=? AND date>? AND change_pct IS NULL",
        (DB_CODE, latest_real),
    ).fetchall()
    removed = 0
    for d, c, cp in poison:
        conn.execute("DELETE FROM index_quotes WHERE code=? AND date=?", (DB_CODE, d))
        print(f"[CLEAN] 删除占位行 {d} (close={c}, change_pct={cp})")
        removed += 1
    conn.commit()
    conn.close()
    return removed


def final_verify(latest_real):
    db = str(DATABASE_PATH)
    conn = sqlite3.connect(db)
    cnt = conn.execute("SELECT COUNT(*) FROM index_quotes WHERE code=?", (DB_CODE,)).fetchone()[0]
    dmin = conn.execute("SELECT MIN(date) FROM index_quotes WHERE code=?", (DB_CODE,)).fetchone()[0]
    dmax = conn.execute("SELECT MAX(date) FROM index_quotes WHERE code=?", (DB_CODE,)).fetchone()[0]
    integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
    poison = conn.execute(
        "SELECT COUNT(*) FROM index_quotes WHERE code=? AND date>? AND change_pct IS NULL",
        (DB_CODE, latest_real),
    ).fetchone()[0]
    conn.close()
    print(f"[FINAL] count={cnt} range={dmin}~{dmax} integrity={integrity} poison={poison}")
    ok = (cnt >= 500) and (integrity == "ok") and (poison == 0)
    print("[VERIFY]PASS" if ok else "[VERIFY]FAIL")
    return ok


def backup_db():
    """写库前按项目铁律备份生产库（干净单文件副本）。"""
    from config.settings import DATABASE_PATH
    db = str(DATABASE_PATH)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    bk_dir = os.path.join(ROOT, "data", "backups")
    os.makedirs(bk_dir, exist_ok=True)
    bk = os.path.join(bk_dir, f"portfolio_db_backup_{ts}_sh932000_refresh.db")
    shutil.copy2(db, bk)
    print(f"[BACKUP] {bk}")
    return bk


def main():
    backup_db()  # 写库前备份（项目铁律）
    nodes = fetch_csindex()
    print(f"[FETCH] CSIndex nodes={len(nodes)} range={nodes[0]['date']}~{nodes[-1]['date']}")
    rc, _ = run_backfill()
    if rc != 0:
        print("[FAIL] backfill 非零退出")
        sys.exit(1)
    cleanup_poison(nodes[-1]["date"])
    ok = final_verify(nodes[-1]["date"])
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
