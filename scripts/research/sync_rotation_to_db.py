#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
sync_rotation_to_db.py — 将 A股投研线 history.json 同步入库到 portfolio.db

整合方案 Tier 1。数据源 = research/sector_rotation/history.json（来自投研线
自动化每日落盘的快照库），**不重新联网抓取**，避免引入口径漂移。

产出 4 张表（幂等 upsert，可重复执行）：
  sector_fund_flow   申万一级 31 行业日度涨跌幅 + 主力净流入
  northbound_daily   北向成交总额日度序列（由快照内滚动序列去重重建）
  style_ladder       规模谱系（上证50→中证2000）日度涨跌幅
  style_growth_value 成长/价值指数对日度涨跌幅与剪刀差 gap

设计要点：
  1. 全部 UNIQUE 约束 + INSERT OR REPLACE，重复执行不产生重复行。
  2. 北向序列的日期只有 "MM-DD"，需结合快照日期推断年份
     （若 MM-DD 的月份 > 快照月份，判定为上一年）。
  3. 缺值一律写 NULL，绝不填 0 或编造（北向 v=None 表示未披露）。
  4. 风格坐标（罗盘 x/y）不在 history.json 中，仅存在于仪表盘 HTML 的
     SNAPS，且经核实该字段在 HTML 中无法稳定提取，故本脚本不产出
     style_compass 表；风格维度改由 style_ladder + style_growth_value
     的原始数据表达（可靠、覆盖全量）。

用法：
  python scripts/research/sync_rotation_to_db.py                    # 全量同步
  python scripts/research/sync_rotation_to_db.py --date 2026-10-13  # 单日增量
  python scripts/research/sync_rotation_to_db.py --verify           # 同步后核验

退出码：0 成功；1 失败（源缺失/写库异常）
"""

import argparse
import json
import os
import sqlite3
import sys
from datetime import datetime

# 保证可导入 config.settings（脚本位于 scripts/research/ 下）
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from config.settings import DATABASE_PATH  # noqa: E402

DEFAULT_HISTORY = os.path.join(ROOT, "research", "sector_rotation", "history.json")

DDL = [
    """
    CREATE TABLE IF NOT EXISTS sector_fund_flow (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        date            TEXT NOT NULL,
        sector_name     TEXT NOT NULL,
        change_pct      REAL,
        main_net_inflow REAL,
        is_core         INTEGER,
        source          TEXT,
        created_at      TEXT,
        UNIQUE(date, sector_name)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS northbound_daily (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        nb_date       TEXT NOT NULL UNIQUE,
        amount        REAL,
        snapshot_date TEXT,
        created_at    TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS style_ladder (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        date        TEXT NOT NULL,
        index_name  TEXT NOT NULL,
        change_pct  REAL,
        scale_tag   TEXT,
        created_at  TEXT,
        UNIQUE(date, index_name)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS style_growth_value (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        date        TEXT NOT NULL,
        growth_idx  TEXT NOT NULL,
        growth_chg  REAL,
        value_idx   TEXT,
        value_chg   REAL,
        gap         REAL,
        created_at  TEXT,
        UNIQUE(date, growth_idx)
    )
    """,
]


def ensure_tables(conn):
    for ddl in DDL:
        conn.execute(ddl)
    conn.commit()


def infer_year(mmdd, snapshot_date):
    """北向序列日期形如 '09-01'，结合快照日期推断完整 YYYY-MM-DD。

    规则：若 MM-DD 的月份大于快照月份，判定为上一年度（跨年滚动窗口）。
    """
    try:
        mm, dd = mmdd.split("-")
        snap_year, snap_month, _ = snapshot_date.split("-")
        year = int(snap_year)
        if int(mm) > int(snap_month):
            year -= 1
        return f"{year:04d}-{mm}-{dd}"
    except Exception:
        return None


def load_history(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"history.json 不存在: {path}")
    with open(path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    if isinstance(data, dict):
        snaps = data.get("snapshots", [])
    elif isinstance(data, list):
        snaps = data
    else:
        raise ValueError(f"无法识别的 history.json 结构: {type(data)}")
    return snaps


def sync(conn, snaps, only_date=None, verbose=True):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    stats = {"sector": 0, "northbound": 0, "ladder": 0, "gv": 0}
    skipped = []

    for snap in snaps:
        date = snap.get("date")
        if not date:
            skipped.append("<no-date>")
            continue
        if only_date and date != only_date:
            continue

        # ---- 1) 行业资金流 ----
        for s in snap.get("sectors") or []:
            name = s.get("n")
            if not name:
                continue
            conn.execute(
                """INSERT OR REPLACE INTO sector_fund_flow
                   (date, sector_name, change_pct, main_net_inflow, is_core, source, created_at)
                   VALUES (?,?,?,?,?,?,?)""",
                (
                    date,
                    name,
                    s.get("p"),
                    s.get("f"),
                    1 if s.get("core") else 0,
                    "history.json",
                    now,
                ),
            )
            stats["sector"] += 1

        # ---- 2) 北向成交序列（滚动窗口，跨快照去重）----
        for nb in snap.get("northbound") or []:
            mmdd = nb.get("d")
            if not mmdd:
                continue
            nb_date = infer_year(mmdd, date)
            if not nb_date:
                continue
            conn.execute(
                """INSERT OR REPLACE INTO northbound_daily
                   (nb_date, amount, snapshot_date, created_at)
                   VALUES (?,?,?,?)""",
                (nb_date, nb.get("v"), date, now),
            )
            stats["northbound"] += 1

        # ---- 3) 规模谱系 ----
        for l in snap.get("ladder") or []:
            idx = l.get("n")
            if not idx:
                continue
            conn.execute(
                """INSERT OR REPLACE INTO style_ladder
                   (date, index_name, change_pct, scale_tag, created_at)
                   VALUES (?,?,?,?,?)""",
                (date, idx, l.get("v"), l.get("t"), now),
            )
            stats["ladder"] += 1

        # ---- 4) 成长/价值 ----
        for g in snap.get("gv") or []:
            gi = g.get("g")
            if not gi:
                continue
            conn.execute(
                """INSERT OR REPLACE INTO style_growth_value
                   (date, growth_idx, growth_chg, value_idx, value_chg, gap, created_at)
                   VALUES (?,?,?,?,?,?,?)""",
                (date, gi, g.get("gv"), g.get("v"), g.get("vv"), g.get("gap"), now),
            )
            stats["gv"] += 1

    conn.commit()
    if verbose:
        print(f"[SYNC] 行业={stats['sector']} 北向={stats['northbound']} "
              f"规模谱系={stats['ladder']} 成长价值={stats['gv']}")
        if skipped:
            print(f"[WARN] 跳过无日期快照 {len(skipped)} 条")
    return stats


def verify(conn):
    print("\n[VERIFY] 入库核验")
    checks = [
        ("sector_fund_flow", "SELECT COUNT(*), MIN(date), MAX(date) FROM sector_fund_flow"),
        ("northbound_daily", "SELECT COUNT(*), MIN(nb_date), MAX(nb_date) FROM northbound_daily"),
        ("style_ladder", "SELECT COUNT(*), MIN(date), MAX(date) FROM style_ladder"),
        ("style_growth_value", "SELECT COUNT(*), MIN(date), MAX(date) FROM style_growth_value"),
    ]
    ok = True
    for name, sql in checks:
        row = conn.execute(sql).fetchone()
        print(f"  {name:20s} rows={row[0]:<6} range={row[1]} ~ {row[2]}")
        if not row[0]:
            ok = False

    # 行业覆盖度：每日应 31 行业
    bad = conn.execute(
        """SELECT date, COUNT(*) c FROM sector_fund_flow
           GROUP BY date HAVING c <> 31 ORDER BY date"""
    ).fetchall()
    print(f"  行业数≠31 的日期: {len(bad)} 个" + (f" {bad[:5]}" if bad else ""))

    # 自洽校验：抽最近一日，f 求和（对照投研线"≈全市场主力净流入"口径，此处仅打印）
    latest = conn.execute("SELECT MAX(date) FROM sector_fund_flow").fetchone()[0]
    if latest:
        s = conn.execute(
            "SELECT SUM(main_net_inflow), COUNT(*) FROM sector_fund_flow WHERE date=?",
            (latest,),
        ).fetchone()
        print(f"  {latest} 主力净流入合计={s[0]:.2f}亿（{s[1]} 行业）")
    return ok


def main():
    ap = argparse.ArgumentParser(description="同步投研线 history.json 到 portfolio.db")
    ap.add_argument("--history", default=DEFAULT_HISTORY, help="history.json 路径")
    ap.add_argument("--date", default=None, help="仅同步指定交易日（增量模式）")
    ap.add_argument("--verify", action="store_true", help="同步后打印核验信息")
    args = ap.parse_args()

    try:
        snaps = load_history(args.history)
    except Exception as e:
        print(f"[ERROR] 读取 history.json 失败: {e}", file=sys.stderr)
        return 1

    mode = f"增量 {args.date}" if args.date else f"全量 {len(snaps)} 快照"
    print(f"[INFO] 源: {args.history}")
    print(f"[INFO] 模式: {mode}")
    print(f"[INFO] 库: {DATABASE_PATH}")

    conn = sqlite3.connect(str(DATABASE_PATH))
    try:
        ensure_tables(conn)
        sync(conn, snaps, only_date=args.date)
        if args.verify:
            verify(conn)
        print("\n[DONE] 同步完成")
        return 0
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] 写库失败: {e}", file=sys.stderr)
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
