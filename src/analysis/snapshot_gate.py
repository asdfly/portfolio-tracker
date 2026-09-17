#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""快照基线闸门 + 场外净值缺失告警（#116 契约 1 / 契约 2）。

事故（2026-09-16 15:30 日频管线，日志 logs/portfolio_20260916.log:10592-10638）::

    15:30:17   [001194] 最新净值 2026-09-15 | 待插入 0 行     ← 13 只逐一同形
    15:30:25   场外净值: 成功 13 只, 失败 0 只, 跳过(无净值源) 0 只, 新增 0 行
    15:30:28   保存持仓快照: 2026-09-16, 22条记录
    15:30:41   持仓数量: 22 / 总市值: 933,195.10 / 当日收益: -0.07%

场外源当日只到 D-1（T+1 披露），库内 D-1 行已存在 ⇒ 采集器判「待插入 0 行」
⇒ 整篮子静默跳过 ⇒ `portfolio_snapshots` 当日只有 22 行（场外 0 行），
`portfolio_summary.total_value` 只剩场内 ETF 的 933,195.10，
而当日快照真实合计 1,527,928.85 —— 少 38.1%，run_status 却仍是 "ok"、日报照发。

修复（2026-09-17 10:49，7ee4cf5）: `src/analysis/portfolio.py` 落地「场内台账 +
库内场外」并集合并 —— 对当日无净值的场外标的，以上一可用净值补位**并落当日快照行**。
故 09-16 那种「整篮子缺行」的形态在合并路径上线后已不复存在；剩下的「源只到 D-1」
只是 T+1 披露的**结构性常态**（周一是 lag=3，节后更长）。

09-17 15:30 实测（logs/scheduled_run.log，18 个阶段全部 status=ok）::

    16886-16898  13 只场外逐一行「最新净值 2026-09-16 | 待插入 0 行」
    16900        [otc_nav_missing] 场外当日净值缺失 12 只(目标日 2026-09-17)
                 …这些标的本日**不落快照行**(禁止静默跳过)   ← 此句与事实不符
    16924        [持仓合并] 这 12 只取自 2026-09-16（价格沿用该日净值）
    16944        保存持仓快照: 2026-09-17, 34条记录     ← code 集合与 09-16 完全相同
    16967        [口径B] daily_return 排除 12 只价格非当日的标的，共 22/34 只，61.08%
    17289        [EMAIL] [CRITICAL] … run_status=degraded
    17291        [WARN] report email send FAILED, rc=1
    最终         总市值 1,524,395.24 / 当日收益 -0.38%，portfolio_summary 已正常落库
    未生成       data/reports/email_report_20260917_light.html

即：这 12 只**确实落了当日快照行**（16944 的 code 集合与 09-16 相同），却被 16900 那句
「不落快照行 ⇒ error」打进 degraded ⇒ `scripts/send_report_email.py:68` 的
`_RUN_STATUS_BLOCKING` 会**每一个交易日**都拒发日报。真正代表 09-16 事故形态的
契约 2（check_snapshot_baseline）当天反而是**通过**的（告警里没有
summary_refused_snapshot_incomplete）。

本模块提供两道**互相独立**的闸门，判据都不依赖"阶段是否执行"这类间接信号：

  1. check_otc_nav_coverage —— 「场外源当日无净值」按**目标日快照是否已覆盖这些
     code** 分成两种形态（这是唯一能区分"T+1 常态"与"09-16 整篮子缺行事故"的信号）：
       * filled    —— 该 code 已存在于 `portfolio_snapshots WHERE date=D`
                      ⇒ 合并路径已用上一可用净值补位并落行 ⇒ T+1 常态
                      ⇒ 告警级别 warning、ok=True（**不写 alerts 表、不降级 run_status**）；
       * uncovered —— 该 code 不存在于 `portfolio_snapshots WHERE date=D`
                      ⇒ 该篮子本日未落库 ⇒ 09-16 事故形态
                      ⇒ 告警级别 error、ok=False。
  2. check_snapshot_baseline —— 生成 portfolio_summary **之前**校验当日
     portfolio_snapshots 是否覆盖基线标的域，不覆盖则拒绝生成 summary。

基线口径（数据驱动，**不硬编码 34/35**）::

    base         = 最近 N 个快照日 code 集合的**交集**（天天都在的标的域）
    friday_extra = 只在（多数）周五出现的标的
    期望行数     = |base|（+ |friday_extra| 当目标日是周五）

实测（近 30 个快照日 2026-08-06~2026-09-16）: 行数分布 {34: 24, 35: 6}，
35 行日全部是周五且多出的恒为 027293 ⇒ **周五 35 行属正常，不得判为异常**。

为什么不能只看行数（这是本闸门的关键设计）:
    只要当日的场外 12 行曾被**别的路径**补过（人工修复、backfill、
    scripts/recompute_summary_window.py），`portfolio_snapshots` 行数就等于 34，
    单看行数会放行；而本次运行真正参与汇总的 `positions` 仍只有 22 只
    ⇒ 残缺的 933,195 会被再写一次。2026-09-17 08:59 的「回填模式」无人值守运行
    正是如此：它把 08:5x 刚修好的 09-16 summary（1,527,928.85 / +1.3346%）
    打回 933,195.10 / +0.65%，并二次发出日报。
    故判据 = 「当日快照覆盖基线标的域」**且**「本次 positions 覆盖当日全部快照行」。

纪律：本模块只读快照数据；唯一的写操作是契约 2 拒绝时往 alerts 表落一条 error 告警。
契约 1 的 filled(warning) 形态**不写库、不降级** run_status —— 它只是把 T+1 披露的
常态留在报告里可见；只有 uncovered(error) 形态才由调用方落 alerts 表。
"""
from __future__ import annotations

import datetime as _dt
import logging
import sqlite3

logger = logging.getLogger(__name__)

# --- 基线参数 ---------------------------------------------------------------
# 窗口取 30 个快照日（约 6 周）：足以覆盖 6 个周五（周五效应样本下限），
# 又不会长到把"标的池已经变了"的陈旧日期算进交集里。
BASELINE_LOOKBACK_DATES = 30
# 少于该数量的历史快照日 ⇒ 基线不可判定（新库/首次部署），此时不拦，只告警。
BASELINE_MIN_DATES = 5
# 窗口内周五里出现比例达到该阈值的"非交集"标的，视为周五固定增量。
FRIDAY_EXTRA_MIN_RATIO = 0.6
FRIDAY_WEEKDAY = 4  # datetime.weekday(): 周一=0 … 周五=4

# --- 告警事件名（须同步登记到 config/notification.json 的 events 白名单）----
OTC_NAV_MISSING_KIND = "otc_nav_missing"
SUMMARY_REFUSED_KIND = "summary_refused_snapshot_incomplete"

# 与 src/utils/db_schema.py:174 的 alerts 表同构，仅为"独立可测"而内联。
_ALERTS_DDL = """
CREATE TABLE IF NOT EXISTS alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_name TEXT NOT NULL,
    level TEXT NOT NULL,
    message TEXT,
    created_at TEXT,
    acknowledged INTEGER DEFAULT 0
)
"""


def _connect(db_path):
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA busy_timeout=15000")
    return conn


def _weekday(date_str):
    return _dt.date.fromisoformat(str(date_str)).weekday()


def _lag_days(latest, target):
    """最新净值日到目标日的自然日差（无法解析返回 None）。"""
    try:
        return (_dt.date.fromisoformat(str(target))
                - _dt.date.fromisoformat(str(latest))).days
    except (TypeError, ValueError):
        return None


def expected_universe(conn, date_str, lookback=BASELINE_LOOKBACK_DATES):
    """推导目标日的基线标的域。

    Returns:
        dict(base, friday_extra, expected_codes, n_dates, lookback_dates) 或
        None（历史快照日不足 BASELINE_MIN_DATES，基线不可判定）。
    """
    cur = conn.execute(
        "SELECT DISTINCT date FROM portfolio_snapshots WHERE date < ? "
        "ORDER BY date DESC LIMIT ?", (str(date_str), int(lookback)))
    dates = [r[0] for r in cur.fetchall()]
    if len(dates) < BASELINE_MIN_DATES:
        return None

    sets = {}
    for d in dates:
        sets[d] = {r[0] for r in conn.execute(
            "SELECT DISTINCT code FROM portfolio_snapshots WHERE date = ?", (d,))}
    base = set.intersection(*sets.values())
    union = set.union(*sets.values())

    fridays = [d for d in dates if _weekday(d) == FRIDAY_WEEKDAY]
    friday_extra = set()
    if fridays:
        need = max(1, int(len(fridays) * FRIDAY_EXTRA_MIN_RATIO + 0.999))
        for code in (union - base):
            if sum(1 for d in fridays if code in sets[d]) >= need:
                friday_extra.add(code)

    expected = set(base)
    if _weekday(date_str) == FRIDAY_WEEKDAY:
        expected |= friday_extra
    return {"base": base, "friday_extra": friday_extra,
            "expected_codes": expected, "n_dates": len(dates),
            "lookback_dates": dates}


def record_error_alert(db_path, rule_name, message):
    """往 alerts 表落一条 error 级告警。

    写入失败**不得**改变"拒绝"结论（调用方的拒绝判断先于本函数成立），
    故这里只记 error 日志并返回 False，绝不向上抛。
    """
    try:
        conn = _connect(db_path)
        try:
            conn.execute(_ALERTS_DDL)
            conn.execute(
                "INSERT INTO alerts (rule_name, level, message, created_at, acknowledged) "
                "VALUES (?,?,?,?,0)",
                (str(rule_name), "error", str(message), _dt.datetime.now().isoformat()))
            conn.commit()
            return True
        finally:
            conn.close()
    except sqlite3.Error as e:
        logger.error("[快照闸门] error 级告警写入失败(%s): %s —— 拒绝结论不变",
                     rule_name, e)
        return False


def check_otc_nav_coverage(db_path, date_str, per_code, otc_codes=None):
    """契约 1：场外当日无来源净值 ⇒ 按**当日快照是否已覆盖**区分常态/事故形态。

    判据（唯一能区分两者的信号，不是"阶段是否执行"）:
      * filled    —— 该 code 已存在于 `portfolio_snapshots WHERE date=date_str`
                     ⇒ 「场内台账 + 库内场外」并集合并路径已用上一可用净值补位
                     并落当日快照行 ⇒ 场外源 T+1 披露的**结构性常态**
                     ⇒ alert_level="warning"、ok=True（不写 alerts 表、不降级 run_status）；
      * uncovered —— 该 code 不存在于当日快照 ⇒ 该篮子本日未落库
                     ⇒ 2026-09-16 事故形态 ⇒ alert_level="error"、ok=False。

    Args:
        db_path: 库路径。
        date_str: 目标分析日（须与 portfolio_snapshots 的日期口径一致）。
        per_code: scripts/fetch_otc_fund_nav.py:run_otc_nav() 返回的 per_code 列表，
                  每项含 code / status / nav_latest_date。
        otc_codes: 可选的场外 code 全集，用于过滤非场外记录。

    Returns:
        dict(ok, missing, deferred, expected, filled, uncovered, alert_level, message)
        - ok=False 仅当**基线标的**里有标的当日无净值**且当日快照缺该行**；
          deferred 只报不拦（它本就不属于当日应有标的域，如 027293 只在周五有行）；
        - alert_level ∈ {"error", "warning", ""}，调用方据此分流告警级别。

    读快照失败时按**保守方向**回退：视为全部 uncovered(error)，绝不因为读不到
    覆盖信息就把事故形态降级成 warning。
    """
    conn = None
    covered = set()
    try:
        conn = _connect(db_path)
        uni = expected_universe(conn, date_str)
        covered = {r[0] for r in conn.execute(
            "SELECT DISTINCT code FROM portfolio_snapshots WHERE date = ?",
            (str(date_str),))}
    except sqlite3.Error as e:
        logger.warning("[场外净值] 基线/当日快照读取失败(%s)，按保守方向判为未覆盖", e)
        uni = None
        covered = set()
    finally:
        if conn is not None:
            conn.close()

    expected = set(uni["expected_codes"]) if uni else set()
    allowed = None
    if otc_codes is not None:
        allowed = {str(c).strip() for c in otc_codes}

    missing, deferred = [], []
    for pc in (per_code or []):
        code = str(pc.get("code") or "").strip()
        if not code or (allowed is not None and code not in allowed):
            continue
        latest = pc.get("nav_latest_date")
        if latest is not None:
            latest = str(latest)
        if pc.get("status", "").startswith("无净值源-跳过"):
            latest = None
        if latest is not None and latest >= str(date_str):
            continue
        rec = {"code": code, "latest": latest,
               "lag_days": _lag_days(latest, date_str) if latest else None,
               "status": pc.get("status")}
        if expected and code not in expected:
            deferred.append(rec)
        else:
            missing.append(rec)

    if not missing:
        return {"ok": True, "missing": [], "deferred": deferred,
                "expected": sorted(expected), "filled": [], "uncovered": [],
                "alert_level": "", "message": ""}

    filled = [r for r in missing if r["code"] in covered]
    uncovered = [r for r in missing if r["code"] not in covered]

    detail = ", ".join(
        "%s(最新 %s%s)" % (r["code"], r["latest"] or "无",
                          "" if r["lag_days"] is None else " 滞后%d自然日" % r["lag_days"])
        for r in missing[:20])
    if len(missing) > 20:
        detail += " …(共 %d 只)" % len(missing)

    # lag 一律现算（周一 lag=3、节后更长），**不得**硬编码成 1。
    _latest_dates = sorted({r["latest"] for r in missing if r["latest"]})
    latest_max = _latest_dates[-1] if _latest_dates else None
    lag = _lag_days(latest_max, date_str) if latest_max else None

    if uncovered:
        message = (
            "[%s] 场外当日净值缺失 %d 只(目标日 %s)，其中 %d 只当日快照**缺行**"
            "⇒ 整篮子未落库(2026-09-16 事故形态): %s；"
            "原因=场外源当日只到前一交易日(T+1 披露)、库内该行已存在，采集器判"
            "「待插入 0 行」，且「场内台账+库内场外」合并路径也未为这些标的落当日快照行。"
            % (OTC_NAV_MISSING_KIND, len(missing), date_str, len(uncovered),
               ", ".join(r["code"] for r in uncovered[:20]))
        )
    else:
        message = (
            "[%s] 场外源 T+1 披露：%d 只标的当日(%s)无来源净值(最新 %s%s)；"
            "这 %d 只已由「场内台账+库内场外」并集合并路径以上一可用净值(%s)补位"
            "并落当日快照行；已按 daily_return 口径B 从分子/分母排除 ⇒ "
            "不构成 2026-09-16 事故形态(整篮子未落库)。"
            % (OTC_NAV_MISSING_KIND, len(missing), date_str, latest_max or "无",
               "" if lag is None else "，滞后 %d 自然日" % lag,
               len(filled), latest_max or "无")
        )
    message += " 明细: " + detail
    if deferred:
        message += " 非当日基线标的(延后披露，不拦): %s" % ", ".join(
            r["code"] for r in deferred)
    return {"ok": not uncovered, "missing": missing, "deferred": deferred,
            "expected": sorted(expected), "filled": filled, "uncovered": uncovered,
            "alert_level": "error" if uncovered else "warning", "message": message}


def check_snapshot_baseline(db_path, date_str, positions,
                            computed_total_value=None):
    """契约 2：生成 portfolio_summary 之前的基线闸门。

    判据（两个都过才算 ok）：
      a) 当日 portfolio_snapshots 的 code 集合 ⊇ 基线标的域（缺一个即拒绝）；
      b) 本次 positions 的 code 集合 ⊇ 当日全部快照行（漏一个即拒绝）。

    Args:
        db_path: 库路径。
        date_str: 目标分析日。
        positions: 本次参与汇总的持仓列表（PortfolioAnalyzer 的 positions）。
        computed_total_value: 可选，本次算出的 total_value，仅写进告警便于对账。

    Returns:
        dict(ok, expected_n, actual_n, missing, extra, not_summarized,
             mv_db, computed_total_value, reason, alert_written, baseline_available)
    """
    conn = _connect(db_path)
    try:
        uni = expected_universe(conn, date_str)
        actual_codes = {r[0] for r in conn.execute(
            "SELECT DISTINCT code FROM portfolio_snapshots WHERE date = ?",
            (str(date_str),))}
        row = conn.execute(
            "SELECT SUM(market_value), COUNT(*) FROM portfolio_snapshots WHERE date = ?",
            (str(date_str),)).fetchone()
    finally:
        conn.close()

    mv_db = float(row[0] or 0.0)
    actual_n = int(row[1] or 0)
    pos_codes = {str(p.get("code") or "").strip() for p in (positions or [])}
    pos_codes.discard("")

    if uni is None:
        logger.warning(
            "[快照闸门] 历史快照日不足 %d 天，基线不可判定 ⇒ 本次不拦（%s）",
            BASELINE_MIN_DATES, date_str)
        return {"ok": True, "baseline_available": False, "expected_n": None,
                "actual_n": actual_n, "missing": [], "extra": sorted(actual_codes),
                "not_summarized": [], "mv_db": mv_db,
                "computed_total_value": computed_total_value,
                "reason": "baseline_unavailable(历史快照日不足)", "alert_written": False}

    expected = uni["expected_codes"]
    missing = sorted(expected - actual_codes)
    extra = sorted(actual_codes - expected)
    not_summarized = sorted(actual_codes - pos_codes)

    ok = not missing and not not_summarized
    if ok:
        return {"ok": True, "baseline_available": True, "expected_n": len(expected),
                "actual_n": actual_n, "missing": [], "extra": extra,
                "not_summarized": [], "mv_db": mv_db,
                "computed_total_value": computed_total_value,
                "reason": "", "alert_written": False}

    parts = ["[%s] 当日快照不覆盖基线标的域，已拒绝生成 portfolio_summary(%s)"
             % (SUMMARY_REFUSED_KIND, date_str)]
    if actual_n != len(expected):
        parts.append("行数 %d != 基线 %d" % (actual_n, len(expected)))
    else:
        # 行数够但 positions 不够 —— 必须点明，否则读者会以为行数判据失效
        parts.append("行数 %d == 基线 %d（行数判据通过，但本次汇总并未覆盖全部行）"
                     % (actual_n, len(expected)))
    if missing:
        parts.append("缺失 %d 只 %s" % (len(missing), ",".join(missing[:20])))
    if not_summarized:
        parts.append("本次 positions 未覆盖当日快照行 %d 只 %s"
                     % (len(not_summarized), ",".join(not_summarized[:20])))
    if extra:
        parts.append("基线外新增 %d 只(不拦) %s" % (len(extra), ",".join(extra[:10])))
    parts.append("Σmarket_value(DB)=%.2f" % mv_db)
    if computed_total_value is not None:
        parts.append("本次算出 total_value=%.2f" % float(computed_total_value))
    if missing:
        parts.append("原因=场外当日无净值未落库（该篮子未写当日快照行）")
    if not_summarized:
        parts.append("原因=本次汇总未并入场外持仓（其当日快照行由别的路径写入）")
    parts.append("已拒绝写入，而不是写一个残缺值")
    message = "；".join(parts)

    written = record_error_alert(db_path, SUMMARY_REFUSED_KIND, message)
    return {"ok": False, "baseline_available": True, "expected_n": len(expected),
            "actual_n": actual_n, "missing": missing, "extra": extra,
            "not_summarized": not_summarized, "mv_db": mv_db,
            "computed_total_value": computed_total_value,
            "reason": message, "alert_written": written}
