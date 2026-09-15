#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""把 portfolio_nav 中越界的 mwr_return 清成 NULL。

背景
----
`_solve_period_irr()` 此前没有合理性护栏，2026-09-15 重建时把 10.444292（+1044%）
这个发散解广播到了全表 3477 行。该字段有下游读取（nav_engine.get_nav_series 的 SELECT），
留着坏值迟早会漏进日报/看板，所以数据层直接清掉，不只靠展示层隐藏。

判据与 nav_engine._MWR_MIN / _MWR_MAX 保持一致（默认 -0.99 ~ 5.0）。

用法
----
    venv313\\Scripts\\python.exe scripts\\purge_bad_mwr.py            # dry-run
    venv313\\Scripts\\python.exe scripts\\purge_bad_mwr.py --apply    # 落库
"""

import argparse
import os
import sqlite3
import sys

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from config.settings import DATABASE_PATH  # noqa: E402
from src.analysis.nav_engine import _MWR_MAX, _MWR_MIN  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="真正写库（默认 dry-run 只打印）")
    args = ap.parse_args()

    db_path = str(DATABASE_PATH)
    print(f"数据库: {db_path}")
    print(f"护栏区间: [{_MWR_MIN}, {_MWR_MAX}]  （与 nav_engine._MWR_MIN/_MWR_MAX 一致）")

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA busy_timeout=15000")
    cur = conn.cursor()

    cur.execute("SELECT mwr_return, COUNT(*) FROM portfolio_nav GROUP BY mwr_return ORDER BY 2 DESC")
    print("\n当前 mwr_return 取值分布:")
    for v, c in cur.fetchall():
        print(f"   {str(v):>20}  {c:>6} 行")

    cur.execute(
        "SELECT COUNT(*) FROM portfolio_nav WHERE mwr_return IS NOT NULL "
        "AND (mwr_return < ? OR mwr_return > ? OR mwr_return != mwr_return)",
        (_MWR_MIN, _MWR_MAX),
    )
    bad = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM portfolio_nav")
    total = cur.fetchone()[0]
    print(f"\n越界（含 NaN）需清成 NULL 的行数 = {bad} / {total}")

    if bad == 0:
        print("无需清理。")
        conn.close()
        return 0

    if not args.apply:
        print("\n[DRY-RUN] 未写库。加 --apply 执行清理。")
        conn.close()
        return 0

    cur.execute("BEGIN")
    cur.execute(
        "UPDATE portfolio_nav SET mwr_return = NULL WHERE mwr_return IS NOT NULL "
        "AND (mwr_return < ? OR mwr_return > ? OR mwr_return != mwr_return)",
        (_MWR_MIN, _MWR_MAX),
    )
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM portfolio_nav WHERE mwr_return IS NULL")
    nulls = cur.fetchone()[0]
    cur.execute("SELECT mwr_return, COUNT(*) FROM portfolio_nav GROUP BY mwr_return")
    print(f"\n[APPLY] 已清理 {cur.rowcount if cur.rowcount > 0 else bad} 行")
    print(f"清理后 mwr_return IS NULL 的行数 = {nulls} / {total}")
    print("清理后取值分布:")
    for row in cur.fetchall():
        print(f"   {str(row[0]):>20}  {row[1]:>6} 行")

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
