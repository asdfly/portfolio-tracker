#!/usr/bin/env python3
"""P4 次日补采窗口(早盘) — 开盘前采集前一交易日数据 + 消费补采队列。

与 16:30 同日晚窗口互补，构成完整闭环:
  - 16:30 当晚: 重试当日两融等(多数情况下源尚未发布 -> 保留 pending)
  - 09:00 次日: 前一交易日数据补采(两融明细通常次日清晨发布) + 队列消费 -> done

职责(参考早盘采集任务描述, 语义改为"补前一交易日"):
  1. 计算前一交易日(周末回溯; 节假日与主分析一致, 不额外处理)
  2. fetch_all_macro_daily()                        —— 宏观(最新可得)
  3. run_market_events_collection(target_date=前一日) —— 龙虎榜/两融/股东增减持/机构调研/大宗交易
     (走 P2 真实性闸门落库; 两融仍空自动 enqueue_retry 入队)
  4. run_etf_fundamental_collection(..., target_date=前一日)
     —— 注意: spot 为实时快照无法回溯, 会被 P2 闸门整体拒绝(诚实行为, 记 spot_historical);
        估值/持仓等非实时部分仍采集; 真实前一日净值走 15:30 主分析 + 新浪兜底。
  5. run_pending_margin_retries(date_display=前一日) —— 消费两融补采队列
  6. 摘要写 data/reports/morning_<date>.json

用法:
  venv313\\Scripts\\python.exe run_morning.py [--date 2026-08-05] [--max-attempts 3]
"""
import argparse
import json
import logging
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

PROJECT_DIR = Path(__file__).parent
sys.path.insert(0, str(PROJECT_DIR))

from data_loader import get_db_connection
from src.data_sources.collect_core import (
    install_requests_timeout,
    list_pending_retries,
)
from src.data_sources.market_events import run_pending_margin_retries

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("run_morning")


def previous_trading_day(d=None):
    """向前回溯最近一个工作日(周末回溯; 节假日与主分析 is_trading_day 一致, 不额外处理)。"""
    d = d or date.today()
    for _ in range(7):
        d = d - timedelta(days=1)
        if d.weekday() < 5:   # 0-4 周一至周五
            return d
    return d - timedelta(days=1)


def summarize_queue(conn):
    counts = {}
    for s in ("pending", "done", "exhausted"):
        counts[s] = conn.execute(
            "SELECT COUNT(*) FROM collection_retry_queue WHERE status=?",
            (s,)).fetchone()[0]
    return counts


def main(argv=None):
    parser = argparse.ArgumentParser(description="投资组合次日补采窗口(早盘)")
    parser.add_argument("--date", dest="date_", default=None,
                        help="目标补采日 YYYY-MM-DD (默认自动取前一交易日)")
    parser.add_argument("--max-attempts", type=int, default=3,
                        help="单项目最大尝试次数(默认3)")
    opts = parser.parse_args(argv)

    # P1 超时补丁: 内部 ak.* 调用也受请求级超时保护, 杜绝挂死
    install_requests_timeout(connect=10, read=30)

    target = opts.date_ or previous_trading_day().strftime("%Y-%m-%d")
    logger.info(f"[次日补采] 目标日期(前一交易日): {target}")

    # ---- 1. 宏观(最新可得) ----
    stats1 = {}
    try:
        from src.data_sources.macro_daily import fetch_all_macro_daily
        stats1 = fetch_all_macro_daily()
        logger.info(f"[次日补采] 宏观: {stats1}")
    except Exception as e:
        logger.warning(f"[次日补采] 宏观采集失败: {e}")
        stats1["errors"] = [str(e)[:120]]

    # ---- 2. 市场事件(前一交易日, 走 P2 闸门) ----
    stats2 = {}
    try:
        from src.data_sources.market_events import run_market_events_collection
        stats2 = run_market_events_collection(target_date=target)
        logger.info(f"[次日补采] 市场事件: {stats2}")
    except Exception as e:
        logger.warning(f"[次日补采] 市场事件采集失败: {e}")
        stats2["errors"] = [str(e)[:120]]

    # ---- 3. ETF 基本面(前一交易日; spot 被 P2 闸门拒绝属预期) ----
    stats3 = {}
    try:
        from src.data_sources.etf_fundamental import run_etf_fundamental_collection
        from config.settings import ETF_CATEGORIES
        stats3 = run_etf_fundamental_collection(
            list(ETF_CATEGORIES.keys()), ETF_CATEGORIES, target_date=target)
        logger.info(f"[次日补采] ETF基本面: {stats3}")
    except Exception as e:
        logger.warning(f"[次日补采] ETF基本面采集失败: {e}")
        stats3["errors"] = [str(e)[:120]]

    # ---- 4. 消费两融补采队列(前一交易日 pending; 昨晨明细通常已发布) ----
    conn = get_db_connection()
    pending_before = list_pending_retries(conn, source="stock_margin",
                                          target_date=target)
    retry = {}
    if pending_before:
        retry = run_pending_margin_retries(conn, date_display=target,
                                           max_attempts=opts.max_attempts)
        logger.info(f"[次日补采] 两融队列消费: {retry}")
    else:
        logger.info("[次日补采] 队列无该日待重试项")
    queue = summarize_queue(conn)
    pending_after = list_pending_retries(conn, source="stock_margin")
    conn.close()

    # ---- 5. 摘要落盘 ----
    summary = {
        "date": target,
        "run_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "macro": stats1,
        "market_events": stats2,
        "etf_fundamental": stats3,
        "margin_retry": retry,
        "queue": queue,
        "pending_after": [list(p) for p in pending_after],
    }
    out = PROJECT_DIR / "data" / "reports" / f"morning_{target}.json"
    try:
        out.parent.mkdir(parents=True, exist_ok=True)
        with open(out, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        logger.info(f"[次日补采] 摘要已写入: {out}")
    except Exception as e:
        logger.warning(f"[次日补采] 摘要写入失败: {e}")

    # 失败项统计(对齐早盘任务描述: 报告是否有失败项)
    fails = {k: v for k, v in (("宏观", stats1.get("errors")),
                               ("市场事件", stats2.get("errors")),
                               ("ETF基本面", stats3.get("errors")))
             if v}
    logger.info(f"[次日补采] 失败项: {fails if fails else '无'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
