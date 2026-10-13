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

from config.settings import DATABASE_PATH, OTC_FUND_CODES
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


def probe_otc_nav_freshness(conn, today):
    """A4(2026-10-13): 主报告缺失时的 otc_nav 新鲜度 DB 直验替代路径。

    场外基金净值写入 portfolio_snapshots, 取每只 OTC 代码最新 date 与今日比较,
    输出 per-code 滞后天数与状态。主报告存在时该信息已在 otc_nav_missing 告警中,
    此处补"报告缺失"场景下原缺失的观测路径(盲区 2026-10-10)。
    """
    codes = sorted(OTC_FUND_CODES or [])
    if not codes:
        return None
    placeholders = ",".join("?" * len(codes))
    try:
        rows = conn.execute(
            f"SELECT code, MAX(date) FROM portfolio_snapshots "
            f"WHERE code IN ({placeholders}) GROUP BY code", codes).fetchall()
    except Exception as e:
        logger.warning(f"[A4] otc_nav 直验查询失败: {e}")
        return None
    latest = {r[0]: r[1] for r in rows}
    out = []
    for code in codes:
        d = latest.get(code)
        if d is None:
            out.append({"code": code, "latest": None, "lag_days": None,
                        "status": "无数据"})
            continue
        try:
            lag = (datetime.strptime(today, "%Y-%m-%d")
                   - datetime.strptime(d, "%Y-%m-%d")).days
        except Exception:
            lag = None
        out.append({"code": code, "latest": d, "lag_days": lag,
                    "status": "OK" if (lag is not None and lag <= 1) else "滞后"})
    return out


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
    otc_probe = None  # A4: 仅当主报告缺失时做 DB 直验, 否则置 None 不重复
    if report:
        # run_status 是主分析"本次是否跑完整"的唯一判据(失败时 dq_score 为 null)。
        # 不打印它, 16:30 的巡检就会看着一份 run_report 说"一切正常", 而同一时刻
        # monitor 里可能正记着 failed —— 同一信息要在两处才能看到即为观测缺口。
        # 旧格式报告无该字段, 显式标为"未记录(旧格式)"而非静默显示 None。
        run_status = report.get("run_status")
        logger.info(
            f"[补采巡检] 主分析报告({date_display}): "
            f"run_status={run_status or '未记录(旧格式)'}, "
            f"dq_score={report.get('dq_score')}, "
            f"alerts={len(report.get('alerts', []))}, "
            f"pending={report.get('retry_queue_pending')}")
        if run_status and run_status != "ok":
            logger.warning(
                f"[补采巡检] 主分析本次运行未完整(run_status={run_status}): "
                f"{report.get('dq_score_reason') or '原因未记录'}; "
                f"当日报告与库数据可信度需人工确认")
    else:
        logger.info(f"[补采巡检] 未找到 run_report_{date_display}.json "
                    "(主分析尚未运行或未启用P5)")
        # A4: 主报告缺失时, 用 DB 直验路径补 otc_nav 新鲜度观测
        try:
            otc_probe = probe_otc_nav_freshness(conn, date_display)
        except Exception as e:
            logger.warning(f"[补采巡检·A4] otc_nav 直验异常: {e}")
            otc_probe = None
        if otc_probe:
            lagged = [o for o in otc_probe if o["status"] != "OK"]
            logger.info(f"[补采巡检·A4] otc_nav DB直验: {len(otc_probe)}只, "
                        f"滞后 {len(lagged)}只; 最新示例 {otc_probe[:3]}")

    # 4) 输出 JSON 摘要(供自动化解析)
    summary = {
        "date": date_display,
        "margin_retry": result,
        "queue": counts,
        "pending_after": [list(p) for p in pending_after],
        "main_report": (report.get("date"), report.get("dq_score"),
                        len(report.get("alerts", []))) if report else None,
        # 新增键(不动 main_report 三元组的元数, 避免破坏既有消费方);
        # 旧格式报告无该字段时为 None
        "main_run_status": report.get("run_status") if report else None,
        "otc_nav_probe": otc_probe,  # A4: 仅主报告缺失时有值, 否则 None
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
