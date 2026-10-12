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
    list_exhausted_retries,
    is_trading_date,
)
from src.data_sources.market_events import run_pending_margin_retries

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("run_morning")


def previous_trading_day(d=None):
    """向前回溯最近一个交易日(周末 + 法定节假日均跳过, 治盲区#11)。

    节假日识别走 akshare 交易日历(见 collect_core._load_trade_calendar);
    日历不可用(网络/接口)时退化为仅跳过周末, 不影响既有流程。
    回溯上限扩至 30 天, 覆盖春节等长假期。
    """
    d = d or date.today()
    for _ in range(30):
        d = d - timedelta(days=1)
        if is_trading_date(d.strftime("%Y-%m-%d")):
            return d
    return d - timedelta(days=1)


def summarize_queue(conn):
    counts = {}
    for s in ("pending", "done", "exhausted", "skipped_non_trading"):
        counts[s] = conn.execute(
            "SELECT COUNT(*) FROM collection_retry_queue WHERE status=?",
            (s,)).fetchone()[0]
    return counts


# 报告核对的表 -> 日期列。
# 注意: etf_fundamental 仅含 spot 快照(price/iopv/mv), 其 date 滞后 ≠
# "ETF基本面整体陈旧"; 行业配置(etf_industry_alloc.report_date)与重仓股
# (etf_top_holdings.quarter) 是独立表、独立刷新, 须分开呈现(治盲区#9)。
DB_LATEST_TABLES = {
    "stock_lhb": "date",
    "stock_margin": "date",
    "stock_holder_change": "date",
    "stock_institution_research": "date",
    "stock_block_trade": "date",
    "etf_fundamental": "date",            # 仅 spot 快照
    "etf_industry_alloc": "report_date",  # 行业配置刷新日期(独立表)
    "etf_top_holdings": "quarter",        # 季度, 无日历滞后
}


def collect_db_latest(conn, target):
    """核对各表**实际落库最新日期**与滞后天数。

    治报告口径不一致: 采集统计按"目标日期"计数, 但落库按事件真实日期
    (如机构调研的披露窗口会把 09-01/09-02 的事件归到 08-31 的查询批次)。
    只看采集数量无法判断数据是否真的追上了目标日期, 故显式给出落库最新日期。
    """
    out = {}
    for table, date_col in DB_LATEST_TABLES.items():
        try:
            row = conn.execute(
                f"SELECT MAX({date_col}), COUNT(*) FROM {table}").fetchone()
        except Exception as e:                      # 表不存在/列缺失
            out[table] = {"error": str(e)[:80]}
            continue
        latest, total = (row[0] if row else None), (row[1] if row else 0)
        item = {"latest": latest, "rows": total}
        # 日历类列(date / report_date)做滞后计算; 季度列(如重仓股)不做。
        if latest and date_col in ("date", "report_date"):
            try:
                sd = (datetime.strptime(target, "%Y-%m-%d")
                      - datetime.strptime(latest, "%Y-%m-%d")).days
                if sd < 0:
                    # 最新日期晚于目标日(如机构调研披露窗口归并): 非"落后/损坏"。
                    item["stale_days"] = 0
                    item["coverage"] = "ahead_or_covered"   # 盲区#6
                else:
                    item["stale_days"] = sd
                    item["coverage"] = "stale" if sd > 0 else "fresh"
            except (ValueError, TypeError):
                pass
        out[table] = item
    return out


def collect_event_coverage(conn, target):
    """核对事件类表近5交易日(目标日往前4天)的覆盖率与缺失工作日。

    治盲区#2: db_latest 的 stale_days 只看'最新一天', 无法暴露中间断日。
    本函数显式列出窗口内缺失的工作日, 让断档一眼可见。

    注: 仅把周一~周五视为应有数据(未排除法定节假日), 故节假日会临时误报为
    缺失属预期(节假日日历待盲区#11 落地后细化)。
    """
    tables = ["stock_lhb", "stock_margin", "stock_holder_change",
              "stock_institution_research", "stock_block_trade"]
    try:
        t = datetime.strptime(target, "%Y-%m-%d")
    except (ValueError, TypeError):
        return {}
    window_start = (t - timedelta(days=4)).strftime("%Y-%m-%d")
    out = {}
    for tbl in tables:
        try:
            rows = conn.execute(
                f"SELECT DISTINCT date FROM {tbl} "
                f"WHERE date BETWEEN ? AND ?",
                (window_start, target)).fetchall()
        except Exception:
            out[tbl] = {"error": "query_failed"}
            continue
        present = {r[0] for r in rows if r[0]}
        expected, missing = [], []
        for i in range(4, -1, -1):
            d = t - timedelta(days=i)
            if d.weekday() < 5:   # 仅工作日视为应有数据
                ds = d.strftime("%Y-%m-%d")
                expected.append(ds)
                if ds not in present:
                    missing.append(ds)
        out[tbl] = {
            "window": f"{window_start}~{target}",
            "expected_trading_days": len(expected),
            "present_days": len(present & set(expected)),
            "missing_days": missing,
        }
    return out


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
    # 盲区#10: 列出已 exhausted 的补采缺口(具体哪日/哪源), 而非仅计数
    exhausted_items = list_exhausted_retries(conn)
    # 盲区#2: 事件表近5交易日覆盖率/缺失工作日
    event_coverage = collect_event_coverage(conn, target)
    db_latest = collect_db_latest(conn, target)

    def _fmt_db(t, v):
        base = f"{t}={v.get('latest')}"
        cov = v.get("coverage")
        if cov == "ahead_or_covered":
            return base + "(已覆盖/超前)"
        if cov == "stale" and v.get("stale_days"):
            return base + f"(滞后{v['stale_days']}天)"
        if cov == "fresh":
            return base + "(已追平)"
        return base

    logger.info(f"[次日补采] 落库最新日期核对: "
                + ", ".join(_fmt_db(t, v) for t, v in db_latest.items()))
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
        # 各表实际落库最新日期 + 滞后天数(区别于上方按目标日期的采集计数)
        "db_latest": db_latest,
        # 盲区#10: 已 exhausted 的补采缺口清单(具体哪日/哪源, 而非仅计数)
        "queue_exhausted_gaps": [
            {"id": r[0], "target_date": r[1], "source": r[2],
             "attempts": r[3], "last_tried": r[4]}
            for r in exhausted_items
        ],
        # 盲区#2: 事件类表近5交易日覆盖率/缺失工作日(中间断日可见)
        "event_coverage": event_coverage,
        # 字段口径说明, 避免把"闸门按预期拒绝"误读为采集失败
        "_notes": {
            "etf_fundamental.spot": (
                "P2_GATE_REJECTED_HISTORICAL_SNAPSHOT: spot 为实时快照, 无法回溯"
                "历史日期。请求前一交易日时被真实性闸门整体拒绝属**预期行为**, "
                "非采集失败; 真实前一日净值由 15:30 主分析 + 新浪兜底负责。"),
            "etf_fundamental.split": (
                "etf_fundamental.latest 仅表示 spot 快照(price/iopv/mv)刷新日期;"
                "行业配置(etf_industry_alloc.report_date)与重仓股"
                "(etf_top_holdings.quarter)是独立表、独立刷新, 分开列于 db_latest。"
                "报告'etf_fundamental 滞后'≠ ETF基本面整体陈旧, 仅 spot 快照缺失"
                "(被 P2 闸门预期拒绝时尤甚)。盲区#9。"),
            "market_events.counts": (
                "各源计数为本次**尝试写入**行数, 按目标日期归集; 数据实际落库"
                "按事件真实日期(披露窗口类源可能晚于目标日)。是否追上目标日期"
                "请看 db_latest 的 latest / stale_days。"),
            "market_events.institution_research": (
                "机构调研走披露窗口语义, 已列入 DATE_GATE_EXEMPT_TABLES 豁免日期"
                "闸门, 按实际调研日期落库; 主采集当日返回 0 条不等于无数据, "
                "可能由健康检查回填以其他查询日批次补入。"),
            "queue.exhausted_gaps": (
                "queue_exhausted_gaps 列出已 exhausted 的补采项(target_date/source), "
                "即'已放弃重试的真实/幻影缺口'——仅看 queue.exhausted 计数无法察觉"
                "具体哪日数据永久缺失(盲区#10)。"),
            "event_coverage": (
                "event_coverage 给出各事件表近5交易日(目标日往前4天)的覆盖率与"
                "缺失工作日, 让中间断日可见(盲区#2)。仅周一~周五视为应有数据,"
                "未排除法定节假日, 节假日可能临时误报缺失(节假日日历待盲区#11 落地)。"),
        },
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
    # 源级告警(非致命, 但此前被静默吞掉, 盲区#7): 如实呈现, 不阻断
    warns = {k: v for k, v in (("ETF基本面", stats3.get("warnings")),) if v}
    if warns:
        logger.warning(f"[次日补采] 源级告警(已记入报告): {warns}")
    else:
        logger.info("[次日补采] 源级告警: 无")
    return 0


if __name__ == "__main__":
    sys.exit(main())
