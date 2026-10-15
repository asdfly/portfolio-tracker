#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""ETF 日线 OHLCV 缺口巡检判据（market_last + tail_gaps + mid_holes）。

取代原「逐码 MAX(date) < portfolio_snapshots.MAX(date)」内联判据（2026-10-15）。
原判据两个缺陷：
  1) **节假日误报**：`portfolio_snapshots` 在节假日也写行（冻结价、35 行/日，
     实测 2026-10-01/10-05 均有行且 code 集合与前一交易日相同）⇒ 快照日≠交易日。
     若巡检在节假日当晚运行，23 只全部 max < snap_max ⇒ 全批假缺口 + fetch NODATA + 邮件误报。
  2) **中间断日漏报**：仅比 MAX ⇒「max 已齐平但窗口内某交易日缺行」永久隐身
     （如夜链补了 D 但漏 D-1，次日 D+1 全量补齐后 MAX 齐平）。

新判据（三路并用）：
  - `market_last`：价格表自身多数覆盖定义的「最近交易日」——按日聚合
    `COUNT(DISTINCT code) >= min_codes`（默认 2）的最大日期。节假日两源都不写行
    （补数函数跳过非交易日）⇒ 免疫节假日；单码独行（artifact）不构成交易日。
  - `tail_gaps`：逐码 MAX(date) < market_last（原判据修正版，基准换成 market_last）。
  - `mid_holes`：回溯窗口内「交易日集合 T」中、晚于该码上市日（其 MIN(date)）的缺行日期。
  - `calendar_note`：快照中存在但晚于 market_last 的日期（信息性：节假日或全源停摆），
    不计入缺口；由调用方（自动化 prompt）决定是否触发取数——取数本身是交易日仲裁者。

输出（stdout 机器可读，供自动化分支）：
  MARKET_LAST=<YYYY-MM-DD|NONE>
  SNAPSHOTS_MAX=<YYYY-MM-DD|NONE>
  TAIL_GAPS=<code,code,...|NONE>
  TAIL_GAP_DETAIL=<code:maxdate;code:maxdate|NONE>
  MID_HOLES=<code:date,date;code:date|NONE>
  CALENDAR_NOTE=<text>
  SUMMARY=tail=<n> mid=<n>
exit：0=无缺口；1=有缺口（tail 或 mid 非空）；2=库/表异常（快照表无数据或价格表无数据）。

用法：
  venv313/Scripts/python.exe scripts/backfill/check_etf_price_gaps.py
  venv313/Scripts/python.exe scripts/backfill/check_etf_price_gaps.py --self-test
"""
import argparse
import os
import sqlite3
import sys
from datetime import datetime, timedelta

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from config.settings import DATABASE_PATH  # noqa: E402

CODES = [
    "159220", "159267", "159300", "159650", "159732", "159770", "159796",
    "159819", "159949", "159992", "510300", "510500", "511380", "511520",
    "512010", "512100", "512810", "515010", "515120", "516160", "561910",
    "563020", "588000",
]


def market_last(conn, min_codes=2):
    """价格表自身多数覆盖定义的最近交易日；无数据返回 None。"""
    row = conn.execute(
        """SELECT date FROM etf_price_history
           GROUP BY date HAVING COUNT(DISTINCT code) >= ?
           ORDER BY date DESC LIMIT 1""",
        (min_codes,),
    ).fetchone()
    return row[0] if row else None


def run_check(conn, codes, min_codes=2, lookback_days=60):
    """执行三路判据，返回结果 dict。"""
    snap_max = conn.execute("SELECT MAX(date) FROM portfolio_snapshots").fetchone()[0]
    mlast = market_last(conn, min_codes)

    res = {
        "snapshots_max": snap_max,
        "market_last": mlast,
        "tail_gaps": [],        # [(code, maxdate)]
        "mid_holes": {},        # code -> [dates]
        "calendar_note": "",
    }
    if not mlast:
        return res

    # 交易日集合 T：回溯窗口内 >= min_codes 个不同 code 有行的日期
    lo = (datetime.strptime(mlast, "%Y-%m-%d") - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    t_days = [
        r[0]
        for r in conn.execute(
            """SELECT date FROM etf_price_history
               WHERE date >= ? AND date <= ?
               GROUP BY date HAVING COUNT(DISTINCT code) >= ?
               ORDER BY date""",
            (lo, mlast, min_codes),
        )
    ]
    t_set = set(t_days)

    for code in codes:
        rows = conn.execute(
            "SELECT MAX(date), MIN(date) FROM etf_price_history WHERE code=?", (code,)
        ).fetchone()
        cmax, cmin = rows[0], rows[1]
        if not cmax:
            res["tail_gaps"].append((code, None))  # 全无数据
            continue
        if cmax < mlast:
            res["tail_gaps"].append((code, cmax))
        # 中间断日：仅统计晚于该码上市日（MIN(date)）的交易日
        if cmin:
            have = set(
                r[0]
                for r in conn.execute(
                    "SELECT date FROM etf_price_history WHERE code=? AND date>=? AND date<=?",
                    (code, lo, mlast),
                )
            )
            miss = sorted(d for d in t_set if d > cmin and d not in have)
            if miss:
                res["mid_holes"][code] = miss

    if snap_max and snap_max > mlast:
        beyond = [
            r[0]
            for r in conn.execute(
                "SELECT DISTINCT date FROM portfolio_snapshots WHERE date > ? ORDER BY date",
                (mlast,),
            )
        ]
        res["calendar_note"] = (
            f"快照日期 {','.join(beyond)} 晚于 market_last={mlast}"
            "（节假日则正常；若为交易日且价格缺失将由取数仲裁）"
        )
    return res


def print_report(res):
    print("MARKET_LAST=", res["market_last"] or "NONE")
    print("SNAPSHOTS_MAX=", res["snapshots_max"] or "NONE")
    tail = res["tail_gaps"]
    mid = res["mid_holes"]
    print("TAIL_GAPS=", ",".join(c for c, _ in tail) if tail else "NONE")
    print(
        "TAIL_GAP_DETAIL=",
        ";".join(f"{c}:{m or 'NO_DATA'}" for c, m in tail) if tail else "NONE",
    )
    print(
        "MID_HOLES=",
        ";".join(f"{c}:{','.join(ds)}" for c, ds in sorted(mid.items())) if mid else "NONE",
    )
    print("CALENDAR_NOTE=", res["calendar_note"] or "(none)")
    print(f"SUMMARY=tail={len(tail)} mid={len(mid)}")


# ---------------------------------------------------------------- self-test
def _mkdb():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """CREATE TABLE etf_price_history(
               date TEXT, code TEXT, open REAL, high REAL, low REAL, close REAL,
               volume REAL, amount REAL, adj_close REAL, source TEXT)"""
    )
    conn.execute("CREATE TABLE portfolio_snapshots(date TEXT, code TEXT)")
    return conn


def _ins_price(conn, code, dates):
    conn.executemany(
        "INSERT INTO etf_price_history(date,code,source) VALUES(?,?,'t')",
        [(d, code) for d in dates],
    )


def _ins_snap(conn, dates, codes=None):
    cs = codes or CODES
    conn.executemany(
        "INSERT INTO portfolio_snapshots(date,code) VALUES(?,?)",
        [(d, c) for d in dates for c in cs],
    )


def _self_test():
    ok_all = True

    def check(name, cond, detail=""):
        nonlocal ok_all
        print(f"  [{'PASS' if cond else 'FAIL'}] {name} {detail}")
        if not cond:
            ok_all = False

    # 用例 1：节假日免疫——快照有 10-01（冻结价 35 行），价格只到 09-30
    c1 = _mkdb()
    for x in CODES:
        _ins_price(c1, x, ["2026-09-29", "2026-09-30"])
    _ins_snap(c1, ["2026-09-29", "2026-09-30", "2026-10-01"])
    r1 = run_check(c1, CODES)
    check("holiday_immunity", r1["market_last"] == "2026-09-30"
          and not r1["tail_gaps"] and not r1["mid_holes"],
          f"market_last={r1['market_last']}")

    # 用例 2：9/14 分缝覆盖——9 只有 10-14，14 只没有 ⇒ tail_gaps=14
    c2 = _mkdb()
    nine, rest = CODES[:9], CODES[9:]
    for x in nine:
        _ins_price(c2, x, ["2026-10-12", "2026-10-13", "2026-10-14"])
    for x in rest:
        _ins_price(c2, x, ["2026-10-12", "2026-10-13"])
    _ins_snap(c2, ["2026-10-12", "2026-10-13", "2026-10-14"])
    r2 = run_check(c2, CODES)
    check("split_coverage", r2["market_last"] == "2026-10-14"
          and {c for c, _ in r2["tail_gaps"]} == set(rest), f"tail={len(r2['tail_gaps'])}")

    # 用例 3：中间断日——全员有 10-12/10-13/10-14，独一只缺 10-13；MAX 齐平
    c3 = _mkdb()
    for x in CODES:
        _ins_price(c3, x, ["2026-10-12", "2026-10-13", "2026-10-14"])
    # 删掉 CODES[5] 的 10-13 行模拟断日
    c3.execute("DELETE FROM etf_price_history WHERE code=? AND date='2026-10-13'", (CODES[5],))
    _ins_snap(c3, ["2026-10-12", "2026-10-13", "2026-10-14"])
    r3 = run_check(c3, CODES)
    check("mid_hole", not r3["tail_gaps"]
          and r3["mid_holes"].get(CODES[5]) == ["2026-10-13"],
          f"mid={r3['mid_holes']}")

    # 用例 4：单码 artifact——独一只有 10-15，其余到 10-14 ⇒ 10-15 非交易日，无缺口
    c4 = _mkdb()
    for x in CODES:
        _ins_price(c4, x, ["2026-10-13", "2026-10-14"])
    _ins_price(c4, CODES[3], ["2026-10-13", "2026-10-14", "2026-10-15"])
    _ins_snap(c4, ["2026-10-13", "2026-10-14", "2026-10-15"])
    r4 = run_check(c4, CODES)
    check("single_code_artifact", r4["market_last"] == "2026-10-14"
          and not r4["tail_gaps"] and not r4["mid_holes"])

    # 用例 5：新码上市前不误报——CODES[22] 自 10-13 起才有行，10-12 是交易日但不是它的洞
    c5 = _mkdb()
    for x in CODES[:-1]:
        _ins_price(c5, x, ["2026-10-12", "2026-10-13", "2026-10-14"])
    _ins_price(c5, CODES[22], ["2026-10-13", "2026-10-14"])
    _ins_snap(c5, ["2026-10-12", "2026-10-13", "2026-10-14"])
    r5 = run_check(c5, CODES)
    check("new_code_no_false_hole", not r5["tail_gaps"]
          and CODES[22] not in r5["mid_holes"], f"mid={r5['mid_holes']}")

    # 用例 6：价格表全空 ⇒ 异常 exit 2 语义（market_last None）
    c6 = _mkdb()
    _ins_snap(c6, ["2026-10-14"])
    r6 = run_check(c6, CODES)
    check("empty_price_table", r6["market_last"] is None)

    print("SELF_TEST:", "ALL PASS" if ok_all else "HAS FAILURES")
    return 0 if ok_all else 1


def main():
    ap = argparse.ArgumentParser(description="ETF OHLCV 缺口巡检判据（market_last/tail/mid）")
    ap.add_argument("--codes", default=",".join(CODES), help="逗号分隔 6 位码，默认 canonical 23 只")
    ap.add_argument("--min-codes", type=int, default=2, help="判定交易日所需最少不同 code 数，默认 2")
    ap.add_argument("--lookback-days", type=int, default=60, help="mid_holes 回溯日历天数，默认 60")
    ap.add_argument("--self-test", action="store_true", help="在内置内存库上运行判据用例")
    args = ap.parse_args()

    if args.self_test:
        sys.exit(_self_test())

    codes = [s.strip() for s in args.codes.split(",") if s.strip()]
    conn = sqlite3.connect(f"file:{DATABASE_PATH}?mode=ro", uri=True)
    try:
        res = run_check(conn, codes, args.min_codes, args.lookback_days)
    except sqlite3.Error as e:
        print(f"ERROR=sqlite: {e}")
        sys.exit(2)
    finally:
        conn.close()

    print_report(res)
    if not res["market_last"] or not res["snapshots_max"]:
        # 快照表无数据或价格表无数据：调用方按异常分支处理，不执行兜底
        sys.exit(2)
    sys.exit(1 if (res["tail_gaps"] or res["mid_holes"]) else 0)


if __name__ == "__main__":
    main()
