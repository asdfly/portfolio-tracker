#!/usr/bin/env python3
"""P4 补采巡检脚本 — 供 16:30 补采窗口 / 次日重试自动化调用。

职责（与主分析 run_analysis.py 完全解耦，绝不重跑主管道）：
  1. 消费补采重试队列中的两融(stock_margin)待重试项 -> run_pending_margin_retries
     （内部走 P2 真实性闸门落库；空数据保留 pending 待下次窗口，达上限置 exhausted）
  2. 汇总队列状态（pending / done / exhausted）
  3. 读取当日 run_report_<date>.json（主分析 15:30 已产出）打印关键事实
  4. 全程启用 P1 请求超时补丁，杜绝挂死

用法:
  venv313\\Scripts\\python.exe run_supplemental.py [--date 2026-08-05] [--max-attempts 3]
"""
import argparse
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

PROJECT_DIR = Path(__file__).parent
sys.path.insert(0, str(PROJECT_DIR))

from config.settings import DATABASE_PATH
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
logger = logging.getLogger("run_supplemental")


def summarize_queue(conn):
    """汇总补采队列状态。"""
    ensure_q = "SELECT COUNT(*) FROM collection_retry_queue WHERE status=?"
    counts = {
        s: conn.execute(ensure_q, (s,)).fetchone()[0]
        for s in ("pending", "done", "exhausted")
    }
    rows = conn.execute(
        "SELECT target_date, source, status, attempts FROM collection_retry_queue "
        "ORDER BY id DESC LIMIT 10").fetchall()
    recent = [{"target_date": r[0], "source": r[1],
               "status": r[2], "attempts": r[3]} for r in rows]
    return counts, recent


def read_run_report(date_str):
    """读取主分析产出的 run_report_<date>.json（可能不存在，需容错）。"""
    path = PROJECT_DIR / "data" / "reports" / f"run_report_{date_str}.json"
    if not path.exists():
        return None
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"读取运行报告失败: {e}")
        return None


def main(argv=None):
    args = argparse.ArgumentParser(description="投资组合补采巡检(16:30/次日重试)")
    args.add_argument("--date", dest="date_", default=None,
                      help="仅处理该目标日期的 pending 项 (YYYY-MM-DD); 缺省处理全部")
    args.add_argument("--max-attempts", type=int, default=3,
                      help="单项目最大尝试次数(默认3)")
    opts = args.parse_args(argv)

    # P1 超时补丁: 两融 fetch 内部 ak.* 调用也受请求级超时保护, 杜绝挂死
    install_requests_timeout(connect=10, read=30)
    date_display = opts.date_ or datetime.now().strftime("%Y-%m-%d")

    logger.info(f"[补采巡检] 目标窗口日期: {date_display}")
    conn = get_db_connection()

    # 1) 消费两融补采队列
    pending_before = list_pending_retries(conn, source="stock_margin",
                                          target_date=opts.date_)
    logger.info(f"[补采巡检] 两融 pending 项: {len(pending_before)} {pending_before}")
    result = {}
    if pending_before:
        result = run_pending_margin_retries(
            conn, date_display=opts.date_, max_attempts=opts.max_attempts)
        logger.info(f"[补采巡检] 两融补采结果: {result}")
    else:
        logger.info("[补采巡检] 队列无待重试项, 无需补采")

    # 2) 汇总队列状态
    counts, recent = summarize_queue(conn)
    pending_after = list_pending_retries(conn)
    logger.info(f"[补采巡检] 队列状态: {counts}")

    # 3) 读取当日主分析报告摘要
    report = read_run_report(date_display)
    if report:
        logger.info(
            f"[补采巡检] 主分析报告({date_display}): dq_score={report.get('dq_score')}, "
            f"alerts={len(report.get('alerts', []))}, "
            f"pending={report.get('retry_queue_pending')}")
    else:
        logger.info(f"[补采巡检] 未找到 run_report_{date_display}.json "
                    "(主分析尚未运行或未启用P5)")

    # 4) 输出 JSON 摘要(供自动化解析)
    summary = {
        "date": date_display,
        "margin_retry": result,
        "queue": counts,
        "pending_after": [list(p) for p in pending_after],
        "main_report": (report.get("date"), report.get("dq_score"),
                        len(report.get("alerts", []))) if report else None,
        "run_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    out = PROJECT_DIR / "data" / "reports" / f"supplemental_{date_display}.json"
    try:
        out.parent.mkdir(parents=True, exist_ok=True)
        with open(out, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        logger.info(f"[补采巡检] 摘要已写入: {out}")
    except Exception as e:
        logger.warning(f"[补采巡检] 摘要写入失败: {e}")

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
