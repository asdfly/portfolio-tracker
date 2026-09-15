#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""一次性修复: index_pe_history 来源/日期口径污染 (P0)。

缺陷根因
--------
1. 日期列混存两种格式: 中证官方(csindex) 为 'YYYY-MM-DD', neodata 为 'YYYYMMDD'。
   SQLite 按字符串排序, '-'(0x2D) < '0'(0x30), 故 '2026-09-03' < '20260911',
   `ORDER BY date` 把所有 csindex 行排到前面, hist[-1] 永远取到 neodata 的值。
2. neodata 的 PE 口径与中证官方不一致 (实测同日最大偏离 ~2.7 倍),
   用 neodata 的当前 PE 去比 csindex 的历史分布 -> 估值分位严重失真
   (实测 399959 军工: 89.6 vs 官方口径 16.1)。

本脚本做的四件事
----------------
A. 增加 source 列 (TEXT, DEFAULT 'unknown'), 按日期格式回填 'csindex' / 'neodata';
B. 把 8 位紧凑日期统一成 'YYYY-MM-DD';
C. (index_code, date) 去重, 冲突时保留 csindex;
D. 全程单事务, 失败回滚; 可重复执行 (幂等)。

用法
----
    venv313/Scripts/python.exe scripts/migrate_index_pe_history_20260915.py [--apply]

默认 dry-run (只打印将要发生什么), 必须显式 --apply 才落库。
"""
from __future__ import annotations

import argparse
import os
import shutil
import sqlite3
import sys
from datetime import datetime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(ROOT, "data", "database", "portfolio.db")
BACKUP_DIR = os.path.join(ROOT, "data", "backups")
KNOWN_BACKUP = os.path.join(BACKUP_DIR, "portfolio_PRE_P0FIX_20260915.db")

TABLE = "index_pe_history"
COMPACT8 = "[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]"
DASH10 = "____-__-__"
# 归一化表达式: 'YYYYMMDD' -> 'YYYY-MM-DD'
NORM = "substr(date,1,4)||'-'||substr(date,5,2)||'-'||substr(date,7,2)"
# 子查询里必须限定表名, 否则未限定的 date 会被解析成内层表 b 的列
NORM_A = NORM.replace("date", "a.date")
NORM_T = NORM.replace("date", f"{TABLE}.date")


def _norm_date(d: str) -> str:
    d = (d or "").strip()
    if len(d) == 8 and d.isdigit():
        return f"{d[:4]}-{d[4:6]}-{d[6:]}"
    return d


def stats(conn: sqlite3.Connection) -> dict:
    c = conn.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()[0]
    codes = conn.execute(f"SELECT COUNT(DISTINCT index_code) FROM {TABLE}").fetchone()[0]
    compact = conn.execute(
        f"SELECT COUNT(*) FROM {TABLE} WHERE date GLOB ?", (COMPACT8,)).fetchone()[0]
    dash = conn.execute(
        f"SELECT COUNT(*) FROM {TABLE} WHERE date LIKE ?", (DASH10,)).fetchone()[0]
    other = c - compact - dash
    src = {}
    if _has_source(conn):
        for s, n in conn.execute(f"SELECT source, COUNT(*) FROM {TABLE} GROUP BY source"):
            src[s or "NULL"] = n
    return {"rows": c, "codes": codes, "compact8": compact, "dash10": dash,
            "other": other, "by_source": src}


def _has_source(conn: sqlite3.Connection) -> bool:
    cols = {r[1] for r in conn.execute(f"PRAGMA table_info({TABLE})").fetchall()}
    return "source" in cols


def _backup_or_abort(apply: bool) -> str:
    """确认备份存在; 不存在则自建一份 (shutil, Git Bash 的 cp 不可用)。"""
    if os.path.exists(KNOWN_BACKUP):
        print(f"[backup] 已存在: {KNOWN_BACKUP} "
              f"({os.path.getsize(KNOWN_BACKUP) / 1048576:.1f} MB)")
        return KNOWN_BACKUP
    if not apply:
        print(f"[backup] 警告: 未找到 {KNOWN_BACKUP}")
        return ""
    os.makedirs(BACKUP_DIR, exist_ok=True)
    dst = os.path.join(
        BACKUP_DIR, f"portfolio_PRE_MIGRATE_{datetime.now():%Y%m%d_%H%M%S}.db")
    print(f"[backup] 未找到既有备份, 自建: {dst}")
    shutil.copy2(DB_PATH, dst)
    return dst


def migrate(conn: sqlite3.Connection, apply: bool) -> dict:
    before = stats(conn)
    print("\n=== 迁移前 ===")
    for k, v in before.items():
        print(f"  {k}: {v}")

    # 预检: 是否有第三种无法判定的日期格式
    others = conn.execute(
        f"SELECT DISTINCT date FROM {TABLE} WHERE date NOT GLOB ? AND date NOT LIKE ? "
        f"LIMIT 10", (COMPACT8, DASH10)).fetchall()
    if others:
        print(f"\n[ABORT] 发现无法判定的第三种日期格式: {[r[0] for r in others]}")
        return {"aborted": True, "unknown_formats": [r[0] for r in others]}

    log = {"aborted": False}
    cur = conn.cursor()
    try:
        # --- A. source 列 --------------------------------------------------
        if not _has_source(conn):
            print("\n[A] 增加 source 列")
            if apply:
                cur.execute(
                    f"ALTER TABLE {TABLE} ADD COLUMN source TEXT DEFAULT 'unknown'")
        else:
            print("\n[A] source 列已存在 (幂等跳过)")

        # 回填: 只有仍是 unknown/NULL 的行才回填, 已标注的不动 -> 幂等
        if apply:
            cur.execute(
                f"UPDATE {TABLE} SET source = CASE "
                f"  WHEN date LIKE ? THEN 'csindex' "
                f"  WHEN date GLOB ? THEN 'neodata' "
                f"  ELSE 'unknown' END "
                f"WHERE source IS NULL OR source = 'unknown'", (DASH10, COMPACT8))
            print(f"[A] 回填 source: {cur.rowcount} 行")

        # --- C-1. 先删冲突的 neodata 行 (必须在归一化之前, 否则撞 UNIQUE) ---
        conflict = conn.execute(
            f"SELECT COUNT(*) FROM {TABLE} a WHERE a.date GLOB ? AND EXISTS ("
            f"  SELECT 1 FROM {TABLE} b WHERE b.index_code=a.index_code "
            f"  AND b.date={NORM_A} AND b.date LIKE ?)",
            (COMPACT8, DASH10)).fetchone()[0]
        print(f"\n[C1] 与 csindex 同日冲突的 neodata 行: {conflict}")
        log["conflict_dropped"] = conflict
        if apply:
            cur.execute(
                f"DELETE FROM {TABLE} WHERE source='neodata' AND EXISTS ("
                f"  SELECT 1 FROM {TABLE} b WHERE b.index_code={TABLE}.index_code "
                f"  AND b.date={NORM_T} AND b.source='csindex')")

        # --- B. 日期归一化 --------------------------------------------------
        to_norm = conn.execute(
            f"SELECT COUNT(*) FROM {TABLE} WHERE date GLOB ?", (COMPACT8,)).fetchone()[0]
        print(f"[B] 待归一化 8 位日期: {to_norm}")
        log["normalized"] = to_norm
        if apply:
            cur.execute(
                f"UPDATE {TABLE} SET date = {NORM} WHERE date GLOB ?", (COMPACT8,))

        # --- C-2. 兜底去重 (保留最小 id) ------------------------------------
        dup = conn.execute(
            f"SELECT COUNT(*) FROM (SELECT index_code, date FROM {TABLE} "
            f"GROUP BY 1,2 HAVING COUNT(*)>1)").fetchone()[0]
        print(f"[C2] 剩余重复 (index_code,date) 组: {dup}")
        log["dup_groups"] = dup
        if apply:
            cur.execute(
                f"DELETE FROM {TABLE} WHERE id NOT IN ("
                f"  SELECT MIN(id) FROM {TABLE} GROUP BY index_code, date)")
            print(f"[C2] 删除重复行: {cur.rowcount}")

        if apply:
            conn.commit()
            print("\n[commit] 事务已提交")
        else:
            conn.rollback()
            print("\n[rollback] dry-run, 未落库")
    except Exception as e:
        conn.rollback()
        print(f"\n[ERROR] 已回滚: {type(e).__name__}: {e}")
        raise

    after = stats(conn) if apply else before
    if apply:
        print("\n=== 迁移后 ===")
        for k, v in after.items():
            print(f"  {k}: {v}")
        print(f"\n行数变化: {before['rows']} -> {after['rows']} "
              f"(删除 {before['rows'] - after['rows']})")
        log.update({"before": before, "after": after})
    return log


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="真正落库 (默认 dry-run)")
    ap.add_argument("--db", default=DB_PATH)
    args = ap.parse_args()

    print(f"DB: {args.db}")
    print(f"MODE: {'APPLY' if args.apply else 'DRY-RUN'}")
    _backup_or_abort(args.apply)

    conn = sqlite3.connect(args.db)
    conn.isolation_level = None  # 关闭 python 隐式事务, 全程手动 BEGIN/COMMIT/ROLLBACK
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        conn.execute("BEGIN")
        migrate(conn, args.apply)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
