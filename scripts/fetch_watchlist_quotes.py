#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""观察名单（已清仓但保持关注）行情 / 技术面采集器。

背景
----
159732 消费电子ETF华夏 已清仓，但用户要求**保持关注**。它被 `is_delisted()`
全域排除（data_loader / rebalance_engine / build_base 三处），后果是
`etf_price_history` 0 行、`etf_technical` 停在 2026-07-31 不再更新——
想看它走势都没数据。

本脚本给这类标的开一条**只采数据、不给决策权**的通道：

    ✅ 写 etf_price_history（OHLCV）       —— 供观察区画 K 线
    ✅ 写 etf_technical（技术指标）        —— 供观察区看技术面
    ❌ 不写 portfolio_snapshots / portfolio_summary  —— 不进持仓统计
    ❌ 不进再平衡                                    —— 不生成任何买卖建议
    ❌ 不写 etf_features / etf_forward_returns       —— 不进预测底座

第三条的**安全性依据**：`build_prediction_base` 的标的域来自
`resolve_target_codes()`（src/analysis/predictor/build_base.py），
该函数已从「最新快照持仓」+ `is_delisted()` 双重过滤，159732 不在其中，
所以只写行情表**不会**污染预测域。每次改动本脚本后请复验一次
（见文件末尾的自查 SQL）。

设计约束
--------
1. **复用既有取数逻辑**，不自己拉数：OHLCV 走
   `predictor/price_history.py::backfill_etf_price_history`（内含 EM 主源 →
   腾讯 qfq 兜底，两者均为前复权、按 MAX(date) 增量、末日重取、INSERT OR REPLACE 幂等）。
2. **幂等**：连跑两次，第二次新增行数必须为 0（只会重算末日 1 行）。
3. **只补增量**：etf_technical 只算 MAX(date) 之后的新交易日；若末日恰好是
   最新交易日则重算该日（末日重取），不会重写更早的历史行。
4. **单只失败只 warning**，不阻断整轮。

⚠️ 口径提醒（详见 docs/handover/07_known_data_issues.md 问题八）
--------------------------------------------------------------
`etf_technical` 是三套口径混在一张表里：历史行由 `backfill_full_history.py:498`
用 `portfolio_snapshots.current_price` + 另一套算法生成；日频行由 `portfolio.py:234`
用 `ds_manager.get_kline` + `TechnicalAnalyzer` 生成。本脚本走后者（算法一致），
但有一个**刻意的差异**：

- 生产日频行：行日期 = `self.today`，而 K 线是当时能拿到的最近 40 根 ——
  跑批早于行情源更新时，**行日期比 K 线末日晚一个交易日**（间歇性错位）。
- 本脚本：K 线末日 == 行日期（写 D 就算到 D），语义正确。

所以 159732 的序列在 2026-08-03 处会从「滞后一天」切成「同日」。
这是迁就存量 bug 与保持正确语义之间的取舍，选了后者；若要严格连续，
需按问题八的修法把三套口径全量重刷，不是本脚本能单独解决的。

已验证 `etf_price_history` 与 `ds_manager.get_kline` 的价格**逐字段完全一致**
（2026-07-21 ~ 09-14 共 40 天，0 处不一致），所以用库内行情替代取数是安全的。

用法
----
    # 预览（不写库）
    venv313\\Scripts\\python.exe scripts\\fetch_watchlist_quotes.py

    # 实写
    venv313\\Scripts\\python.exe scripts\\fetch_watchlist_quotes.py --apply

    # 指定标的
    venv313\\Scripts\\python.exe scripts\\fetch_watchlist_quotes.py --apply --codes 159732

自查 SQL（每次改动后必跑，第三条最关键）
----------------------------------------
    SELECT COUNT(*) FROM etf_price_history  WHERE code='159732';  -- 应 > 0
    SELECT COUNT(*), MAX(date) FROM etf_technical WHERE code='159732';  -- 应追到最近交易日
    SELECT COUNT(*) FROM etf_features        WHERE code='159732';  -- 必须 = 0
    SELECT COUNT(*) FROM etf_forward_returns WHERE code='159732';  -- 必须 = 0
"""

import argparse
import logging
import os
import sqlite3
import sys

# 保证即使从任意 cwd 调用都能 import 到 config / src
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from config.settings import DATABASE_PATH, WATCHLIST_CODES  # noqa: E402
from src.analysis.predictor.price_history import (  # noqa: E402
    backfill_etf_price_history,
)
from src.analysis.technical import TechnicalAnalyzer  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("fetch_watchlist_quotes")

# 技术指标所需 K 线根数，与生产链路 portfolio.py:345 的 count=40 保持一致
KLINE_BARS = 40


class _NoCommitConn:
    """dry-run 包装：转发 sqlite3 连接的所有操作，但把 commit() 变成空操作。

    `backfill_etf_price_history` 内部会 `conn.commit()`，没有参数可以关掉。
    包一层后，dry-run 能走**与实写完全相同**的代码路径，只是不落盘——
    避免出现「dry-run 一套分支、apply 另一套分支」的半残实现。
    """

    def __init__(self, conn):
        self._conn = conn

    def commit(self):
        pass

    def __getattr__(self, name):
        return getattr(self._conn, name)


# --------------------------------------------------------------------------
# 读库
# --------------------------------------------------------------------------
def _last_tech_date(conn, code: str):
    """etf_technical 中该标的最新日期，无为 None。"""
    cur = conn.execute("SELECT MAX(date) FROM etf_technical WHERE code=?", (code,))
    row = cur.fetchone()
    return row[0] if row and row[0] else None


def _price_dates(conn, code: str):
    """etf_price_history 中该标的全部日期（升序）。"""
    cur = conn.execute(
        "SELECT date FROM etf_price_history WHERE code=? ORDER BY date", (code,))
    return [r[0] for r in cur.fetchall()]


def _load_kline(conn, code: str, end_date: str, bars: int = KLINE_BARS):
    """取 <= end_date 的最近 bars 根 K 线，返回升序 list[dict]（TechnicalAnalyzer 期望格式）。"""
    cur = conn.execute(
        "SELECT date, open, high, low, close, volume FROM etf_price_history "
        "WHERE code=? AND date<=? AND close IS NOT NULL ORDER BY date DESC LIMIT ?",
        (code, end_date, bars))
    rows = cur.fetchall()
    rows.reverse()  # 升序
    return [{"date": r[0], "open": r[1], "high": r[2], "low": r[3],
             "close": r[4], "volume": r[5]} for r in rows]


# --------------------------------------------------------------------------
# 主流程
# --------------------------------------------------------------------------
def run_watchlist(codes=None, apply_db=True, log=print) -> dict:
    """为观察名单标的补采 OHLCV + 技术指标。返回结构化结果。

    Args:
        codes: 标的列表，默认 WATCHLIST_CODES（排序后）。
        apply_db: False = dry-run，只算不写。
        log: 日志函数。

    Returns:
        {"ok", "failed", "price_rows", "tech_new", "tech_refresh",
         "per_code", "error"}
    """
    from config.settings import TECH_INDICATORS

    codes = list(codes) if codes else sorted(WATCHLIST_CODES)
    result = {
        "ok": 0, "failed": 0, "price_rows": 0,
        "price_new": 0, "price_refresh": 0,
        "tech_new": 0, "tech_refresh": 0,
        "per_code": [], "error": None,
    }
    if not codes:
        log("  观察名单为空，跳过")
        return result

    analyzer = TechnicalAnalyzer(TECH_INDICATORS)
    real_conn = sqlite3.connect(str(DATABASE_PATH))
    # dry-run：转发所有操作但吞掉 commit，退出时 rollback
    conn = real_conn if apply_db else _NoCommitConn(real_conn)

    try:
        for code in codes:
            entry = {"code": code, "price_rows": 0, "price_new": 0, "price_refresh": 0,
                     "tech_new": 0, "tech_refresh": 0, "status": "OK"}
            try:
                # ---- 1) OHLCV：复用既有增量补采（EM 主源 → 新浪兜底） ----
                before_dates = set(_price_dates(conn, code))
                n = backfill_etf_price_history(conn, [code], log=log)
                entry["price_rows"] = n
                result["price_rows"] += n
                # 区分「真·新交易日」与「末日重取」：新增是判断幂等的唯一指标
                after_dates = _price_dates(conn, code)
                new_dates = [d for d in after_dates if d not in before_dates]
                entry["price_new"] = len(new_dates)
                # 重取 = 本次写入行数里「原本已存在」的那部分（末日重取会重写最后一天）
                entry["price_refresh"] = max(0, n - len(new_dates))
                result["price_new"] += entry["price_new"]
                result["price_refresh"] += entry["price_refresh"]

                # ---- 2) 技术指标：只算新交易日 + 末日重取 ----
                dates = after_dates
                if not dates:
                    entry["status"] = "无行情-跳过技术指标"
                    logger.warning("%s etf_price_history 无数据，跳过技术指标", code)
                    result["failed"] += 1
                    result["per_code"].append(entry)
                    continue

                last_tech = _last_tech_date(conn, code)
                if last_tech is None:
                    targets = dates
                else:
                    targets = [d for d in dates if d > last_tech]
                    # 末日重取：末日若已是最新交易日，重算一次（修正半日行）
                    if dates[-1] == last_tech:
                        targets = [last_tech] + targets

                logger.info("%s 技术指标待算 %d 天（库内末日 %s）",
                            code, len(targets), last_tech)

                pending = []   # [(date, indicators)]
                for d in targets:
                    kline = _load_kline(conn, code, d)
                    if len(kline) < KLINE_BARS:
                        logger.warning("%s %s K线仅 %d 根（需 %d），跳过",
                                       code, d, len(kline), KLINE_BARS)
                        continue
                    ind = analyzer.calculate_all(kline)
                    if not ind:
                        logger.warning("%s %s 指标计算结果为空，跳过", code, d)
                        continue
                    pending.append((d, ind))

                # 释放本连接的读事务再写库：sqlite 默认 journal 模式下，
                # 读者持有的 SHARED 锁会挡住 DatabaseManager 那个连接的写入
                # （实测报 "database is locked"）。
                # 实写时 backfill 已 commit，rollback 丢不掉东西；
                # dry-run 时正好用它丢弃未落库的预览数据。
                conn.rollback()

                if pending and apply_db:
                    from src.utils.database import DatabaseManager
                    dm = DatabaseManager()      # 仅真正有东西要写时才建
                    for d, ind in pending:
                        dm.save_technical_indicators(d, code, ind)
                        if last_tech is not None and d <= last_tech:
                            entry["tech_refresh"] += 1
                            result["tech_refresh"] += 1
                        else:
                            entry["tech_new"] += 1
                            result["tech_new"] += 1
                elif pending:
                    # dry-run：只统计，不落库
                    for d, _ in pending:
                        if last_tech is not None and d <= last_tech:
                            entry["tech_refresh"] += 1
                            result["tech_refresh"] += 1
                        else:
                            entry["tech_new"] += 1
                            result["tech_new"] += 1

                entry["tech_latest"] = pending[-1][0] if pending else last_tech
                result["ok"] += 1
                log(f"  [{code}] OHLCV 新增 {entry['price_new']} 重取 "
                    f"{entry['price_refresh']}（写入 {entry['price_rows']} 行）| "
                    f"技术指标 新增 {entry['tech_new']} 重算 {entry['tech_refresh']} | "
                    f"最新 {entry['tech_latest']}")

            except Exception as e:  # 单只失败不阻断
                logger.warning("%s 采集失败：%s: %s", code, type(e).__name__, e)
                entry["status"] = f"失败-{type(e).__name__}"
                result["failed"] += 1
            result["per_code"].append(entry)
    finally:
        if not apply_db:
            conn.rollback()
        real_conn.close()

    if result["ok"] == 0 and result["failed"] > 0:
        result["error"] = "观察名单采集全部失败"
    return result


def main():
    ap = argparse.ArgumentParser(description="观察名单行情/技术面采集（默认 dry-run）")
    ap.add_argument("--apply", action="store_true", help="实写数据库（默认只预览）")
    ap.add_argument("--codes", nargs="*", default=None, help="指定标的，默认 WATCHLIST_CODES")
    args = ap.parse_args()

    codes = args.codes or sorted(WATCHLIST_CODES)
    print("=" * 88)
    print(f"观察名单采集  {'(实写)' if args.apply else '(dry-run，不写库)'}")
    print(f"标的: {', '.join(codes)}")
    print("=" * 88)

    res = run_watchlist(codes=codes, apply_db=args.apply, log=print)

    print("-" * 88)
    for p in res["per_code"]:
        print(f"  {p['code']}  {p['status']:12s} OHLCV 新增{p['price_new']:5d} "
              f"重取{p['price_refresh']:3d}  技术 新增{p['tech_new']:4d} "
              f"重算{p['tech_refresh']:3d}  最新 {p.get('tech_latest')}")
    print("-" * 88)
    print(f"成功 {res['ok']} 只 / 失败 {res['failed']} 只")
    print(f"OHLCV 写入 {res['price_rows']} 行（新增 {res['price_new']} / "
          f"末日重取 {res['price_refresh']}）")
    print(f"技术指标 新增 {res['tech_new']} 重算 {res['tech_refresh']}")
    print(f"==> 幂等判据：新增合计 = {res['price_new'] + res['tech_new']}（第二次跑应为 0）")
    if res["error"]:
        print(f"错误: {res['error']}")
    print("=" * 88)


if __name__ == "__main__":
    main()
