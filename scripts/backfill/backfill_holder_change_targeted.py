#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""股东增减持(stock_holder_change) 一次性定向回填 (止损, 盲区#12).

对指定真实缺口日(默认 2026-09-29 / 2026-09-30)尝试取回数据:
- 默认 --dry-run: 仅探针(fetch 不写库), 打印各日返回行数与"若落库将写入 N 条"。
- --write: 先备份生产库, 再仅对返回行数>0 的日期 save_market_events;
  空结果判定为源端披露窗口已超出(永久缺口), 跳过且**不 enqueue、不造幻影队列项**。

不触碰重试队列(holder_change 不在 enqueue_retry 机制内, 仅 margin 使用)。
遵循项目铁律: 写库前备份 + INSERT OR REPLACE 幂等 + 仅对非空结果落库(盲区#4/#5)。
"""
import os
import sys
import argparse
import shutil
from datetime import datetime

import warnings
warnings.filterwarnings("ignore")

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from config.settings import DATABASE_PATH  # noqa: E402
from src.utils.database import get_db_connection  # noqa: E402
from src.data_sources.market_events import (  # noqa: E402
    fetch_holder_change_data, save_market_events)

TABLE = "stock_holder_change"
DEFAULT_DATES = ["2026-09-29", "2026-09-30"]


def current_latest(conn):
    cur = conn.cursor()
    row = cur.execute(f"SELECT MAX(date), COUNT(*) FROM {TABLE}").fetchone()
    return (row[0], row[1] if row else (None, 0))


def main():
    ap = argparse.ArgumentParser(description="holder_change 定向回填(止血, #12)")
    ap.add_argument("--dates", nargs="+", default=DEFAULT_DATES,
                    help="目标缺口日 YYYY-MM-DD (默认 2026-09-29 2026-09-30)")
    ap.add_argument("--write", action="store_true",
                    help="落库(默认仅 dry-run 探针, 不触碰生产库)")
    args = ap.parse_args()

    conn = get_db_connection()
    try:
        latest, cnt = current_latest(conn)
        print(f"[当前] {TABLE}: latest={latest}, rows={cnt}")

        # ---- 探针(不写库) ----
        probes = {}
        for d in args.dates:
            n = 0
            df = None
            try:
                df = fetch_holder_change_data(d.replace("-", ""))
                n = 0 if df is None else len(df)
            except Exception as e:  # 网络/接口异常: 视为该日暂不可取
                print(f"[探针] {d}: 取数异常({e}), 视为 0 条")
            probes[d] = (df, n)
            print(f"[探针] {d}: 返回 {n} 条")

        total = sum(n for _, n in probes.values())
        print(f"[汇总] 若落库将写入 {total} 条 (dry-run={'否' if args.write else '是'})")

        if not args.write:
            print("[dry-run] 未触碰生产库。确认有数据后加 --write 落库。")
            return

        # ---- 落库分支 ----
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup = os.path.abspath(os.path.join(
            os.path.dirname(str(DATABASE_PATH)), "..", "backups",
            f"portfolio_PRE_HC_TARGETED_{ts}.db"))
        os.makedirs(os.path.dirname(backup), exist_ok=True)
        shutil.copy(str(DATABASE_PATH), backup)
        print(f"[备份] 生产库已备份至 {backup}")

        written = 0
        for d, (df, n) in probes.items():
            if n == 0:
                print(f"[跳过] {d}: 源端披露窗口已超出, 永久缺口, 不落库/不入队")
                continue
            k = save_market_events(conn, df, TABLE, ["date", "holder_name"],
                                   target_date=d)
            written += k
            print(f"[落库] {d}: 写入 {k} 条")
        print(f"[落库] 合计写入 {written} 条")

        latest2, cnt2 = current_latest(conn)
        print(f"[更新] {TABLE}: latest={latest2} (原 {latest}), "
              f"rows={cnt2} (原 {cnt})")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
