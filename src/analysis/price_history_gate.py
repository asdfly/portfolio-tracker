#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""etf_price_history 连续性闸门（#140 收口 / 报告15 §3 数据完整性）。

根因（2026-09-18 实测）：etf_price_history 曾**静默**断崖——当日整表 0 行、9 只标的
最新价停在 09-15、其余 14 只停在 09-17，且既无告警也无重试队列，直到一次并发进程巧合自愈
才被发现。这正是「判数据新鲜只查 MAX(date)、不查每日只数」会漏掉的典型形态。

本闸门在每日管线阶段 3.25（预测底座增量维护、etf_price_history 刷新之后）运行，做三件事：
  1. 只读快照，推导「参考交易日历」= 活跃标的中覆盖最完整的那只的日期集合；
     （系统级断崖 / A 鲜度闸门：用 portfolio_snapshots 的最新日作对照，若 etf_price_history
      全局最新日**落后 ≥2 个交易日** ⇒ 视为当日全量回填失败/全市场源中断，单独 error 告警。
      🔴 2026-10-27 修正：原为「严格落后哪怕 1 天即 error」，前提是「正常 post-close 跑批后
      两者应相等」——但本管线 15:30 跑批，当日日线要到收盘后才有、21:45 才兜底回填，
      故 15:34 时「当日尚未回填」是常态而非断崖。实测 2026-10-13~10-26 每个被判 error 的
      日期事后全部补齐、无一天真缺，却累计 14 条 error 并拦掉 15/18 次日报。
      现按交易日计宽限：滞后 1 个交易日 ⇒ warning「待回填」；≥2 ⇒ error。
      当日真缺由 21:45 兜底巡检（check_etf_price_gaps.py）判定。见 SYSTEMIC_GRACE_TD。）
  2. 对每只活跃标的，统计窗口内缺失的交易日 = 参考日历 − 该标的有数据的日期
     （只计该标的最早日期之后的参考日，避免把「新纳入标的」的早期缺失误判为断崖）；
  3. 缺失 ≥ 1 天 ⇒ warning 级（不写 alerts 表、不降级 run_status，避免每日拦死日报）；
     缺失 ≥ GAP_ERROR_THRESHOLD 天 ⇒ error 级（写 alerts 表 + 经 _reporter.alert 降级 run_status，
     即「不发布基于陈旧价的报告」）；
     设环境变量 ETF_GAP_AUTOREPAIR=1 时，对缺口标的显式增量回补（不静默、有日志）。

与 snapshot_gate 的一致性（2026-09-17 事故后的已裁定口径）：
  - 只读快照，唯一的写是 error 级时往 alerts 表落一条（与 check_snapshot_baseline 同构）；
  - warning 不写库、不降级，仅留在日志/run_report 可见；
  - error 级告警经 _reporter.alert("error", …) 进入 run_status 判据，使 degraded ⇒ 邮件闸门
    在「真实断崖」时拒发——这与 09-17 的教训不冲突：09-17 是被 T+1 常态**误判**成 error
    才每日拦死；本闸门 warning/error 分流正确，常态（1~2 天滞后）只报 warning，不会触发拦死。
"""
from __future__ import annotations

import argparse
import datetime as _dt
import logging
import os
import sqlite3

logger = logging.getLogger(__name__)

# 参考窗口：取最近 N 个自然日内的交易日历（足以覆盖几次停牌/数据源中断）。
GAP_LOOKBACK_DAYS = 15
# 判定"活跃标的"：窗口起点之前 30 天内 etf_price_history 有数据 ⇒ 视为在跟踪范围内。
ACTIVE_TAIL_DAYS = 30
# error 级阈值：单标的最近窗口缺失 ≥ 该天数（交易日）⇒ error。
GAP_ERROR_THRESHOLD = 3
# 活跃标的判定所需的"窗口起点前已有数据"的最小日期数（避免把新纳入标的误判为缺口）。
MIN_HISTORY_DATES = 2
# 系统级断崖阈值（历史常量，保留作参考）：旧逻辑用「落后 ≥ 该自然日数」判定全市场源中断。
# 2026-09-24 A 落地后改为严格 `snap_max > global_max`（见 detect_etf_price_gaps 顶部），
# 因正常 post-close 跑批后两者必相等，1 天滞后即代表当日全量回填失败。
SYSTEMIC_LAG_THRESHOLD = 4

# 🔴 系统级「待回填」宽限（2026-10-27 新增，交易日口径）。
# 上面「正常 post-close 跑批后两者必相等」这个前提在 15:30 的调度下**不成立**：
#   portfolio_snapshots 由本管线 15:30 写入（当日），而 etf_price_history 的当日日线
#   由预测底座阶段（3.25）+ 21:45 兜底巡检回填，源端（em/tx）收盘后才发布日线。
#   于是 15:34 跑闸门时「当日 OHLCV 尚未产出」是**常态**，实测：
#     2026-10-13~10-26 每个被判 error 的日期，事后全部补齐为 23 只（无一天真缺）；
#     alerts 表累计 14 条 etf_price_gap error；15/18 次日报拒发由它触发。
#   即该判据在 15:30 携带的信息量接近 0，却持续拦死日报 —— 典型告警疲劳制造机。
# 取舍：滞后 **1 个交易日** ⇒ warning「待回填」（不写 alerts、不降级 run_status）；
#       滞后 **≥2 个交易日** ⇒ error（09-23 形态整表 0 行实测滞后 4 个交易日，仍必抓）。
#   当日缺口是否真回填失败，由 21:45 的 ETF OHLCV 兜底巡检（check_etf_price_gaps.py，
#   以「当日 ≥2 码有行」定义 market_last）负责 —— 那是为当日缺口设计的判据，本闸门不是。
# 另：报告侧 gen_combo_report.EPH_CAVEAT_884 本就把该滞后显式标注为
#   「禁止用其估算当日逐 ETF 损益」，即报告设计上允许带着该滞后发出；
#   采集侧在此处硬阻断，属跨层口径矛盾。
# 逃生阀：ETF_GAP_STRICT=1 ⇒ 恢复旧行为（滞后 1 天即 error）。
SYSTEMIC_GRACE_TD = 1

KIND = "etf_price_gap"

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
    s = str(db_path)
    # 与 snapshot_gate 同样立场：URI 会让 sqlite3.connect 静默返回一个打不开的连接，
    # 下游再保守回退成 error 级误报。故显式报错，让误用立即暴露。
    if s.startswith("file:"):
        raise ValueError(
            "etf_price_history_gate._connect 只接受普通 db 路径，不应传 URI"
            f"（收到 {s!r}）。需要只读请改用 sqlite3.connect(path, uri=True)。")
    conn = sqlite3.connect(s)
    conn.execute("PRAGMA busy_timeout=15000")
    return conn


def _reference_calendar(conn, lookback_days):
    """以覆盖最完整的活跃标的的日期集合作为"参考交易日历"。

    不依赖外部交易日历：ETF 中至少有一只流动性标的每日刷新，其日期集合即近似真实交易日历。
    仅取窗口内数据，避免把多年历史全拉进来。
    """
    start = (_dt.date.today() - _dt.timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    rows = conn.execute(
        "SELECT code, date FROM etf_price_history WHERE date >= ?", (start,)).fetchall()
    by_code = {}
    for code, d in rows:
        by_code.setdefault(code, set()).add(d)
    best = set()
    for s in by_code.values():
        if len(s) > len(best):
            best = s
    return best


def _trading_day_lag(older, newer):
    """(older, newer] 区间内的交易日数，用于系统级宽限判定。

    🔴 为什么**不用** index_quotes 的 DISTINCT date 当交易日轴（gen_combo_report.EPH_LAG_TD
    用的是那套）：实测 2026-01-01 起 index_quotes 在休市日也有行（国庆 10-01~10-07 逐日有行，
    系 2026-10-27 之前的采集器缺陷，现已由 holiday_write_guard 拦住新增）。用它会算出
    「09-30 → 10-08 = 6 个交易日」这种荒谬值，把节后首日的常态待回填判成断崖。
    故改用本地离线休市表 src.utils.trading_calendar（2024/2025/2026 官方表）。
    该年无官方表时它退化为「仅周末」口径，对本判据的影响是可能多计（节假日算进来），
    与改动前行为同向，不会放松到漏拦。

    退化路径：连导入都失败时（按文件路径直跑本模块）退回「自然日剔除周末」计数。
    """
    try:
        try:
            from ..utils.trading_calendar import is_trading_day
        except ImportError:
            from src.utils.trading_calendar import is_trading_day
    except Exception:                                  # noqa: BLE001
        is_trading_day = None

    try:
        d0 = _dt.date.fromisoformat(str(older)[:10])
        d1 = _dt.date.fromisoformat(str(newer)[:10])
    except (TypeError, ValueError):
        return None, "unparsed"
    if d1 <= d0:
        return 0, "empty"

    cnt, cur = 0, d0 + _dt.timedelta(days=1)
    while cur <= d1:
        if is_trading_day is None:
            ok = cur.weekday() < 5
        else:
            ok = bool(is_trading_day(cur))
        if ok:
            cnt += 1
        cur += _dt.timedelta(days=1)
    return cnt, ("trading_calendar" if is_trading_day is not None else "weekday")


def detect_etf_price_gaps(conn, lookback_days=GAP_LOOKBACK_DAYS,
                          active_tail_days=ACTIVE_TAIL_DAYS,
                          min_history_dates=MIN_HISTORY_DATES):
    """只读检测 etf_price_history 缺口。返回 list[dict]。

    每项为 {code, latest, first_in_window, missing_count, missing_dates, severity}。
    severity ∈ {"warning", "error"}；系统级断崖记为 code="(systemic)"。
    """
    row = conn.execute("SELECT MAX(date) FROM etf_price_history").fetchone()
    global_max = row[0] if row and row[0] else None
    if not global_max:
        return []

    snap_row = conn.execute("SELECT MAX(date) FROM portfolio_snapshots").fetchone()
    snap_max = snap_row[0] if snap_row and snap_row[0] else None
    reference_latest = max(d for d in (global_max, snap_max) if d)

    gaps = []

    # --- A: 鲜度闸门（修复 09-23「success 假象」盲区）---
    # etf_price_history 全局最新日 严格落后 portfolio_snapshots 最新日 ⇒ 当日全量回填失败
    # （如 09-23 全源中断→整日 0 行）。原判据「snapshot 领先即 error」已按交易日加宽限，
    # 见 SYSTEMIC_GRACE_TD：15:30 跑批时领先 1 个交易日是常态，领先 ≥2 才是断崖。
    # 此判定独立于参考日历，能抓到「全表 0 行」——旧逻辑只看参考日历，
    # 而参考日历由数据本身推导，0 行时日历冻结 ⇒ 漏检（这正是 09-23 漏报的根因）。
    # 注：周末/节假日 snapshot 不会前进，故 snap_max == ohlcv_max，不会误报。
    #
    # ⚠ 顺序约束（2026-10-14 修复，勿回退）：A 必须排在参考日历计算**之前**。
    #   原实现先 `cal = _reference_calendar(...)`、`if not cal: return []`，
    #   于是"参考日历为空"时整个函数提前返回，A 一次都没机会执行 —— 而 A 唯一要抓的
    #   恰恰就是"参考日历退化 / 全表 0 行"这个场景。属注释承诺独立、实现却依赖的
    #   #130 式陷阱（测试 A 用 lookback 窗口外的日期即可复现：原实现 gaps=[]）。
    #   A 只依赖两张表的 MAX(date)，本身不需要 cal。
    if global_max and snap_max and snap_max > global_max:
        try:
            _lag_nd = (_dt.date.fromisoformat(snap_max)
                       - _dt.date.fromisoformat(global_max)).days
        except (TypeError, ValueError):
            _lag_nd = 0
        _lag_td, _axis = _trading_day_lag(global_max, snap_max)
        _strict = os.environ.get("ETF_GAP_STRICT") == "1"
        # 交易日口径优先；轴不可解析时才退回自然日（不会比交易日更小）
        _lag = _lag_td if _lag_td is not None else _lag_nd
        _pending = (not _strict) and (_lag is not None) and (_lag <= SYSTEMIC_GRACE_TD)
        if _pending and _lag == 0:
            # snap_max 前进了但区间内无新增交易日（休市日快照行进；2026-10-27 前
            # 采集器会在休市日写快照，现已由 holiday_write_guard 拦住新增）。
            _note = (f"etf_price_history 最新日({global_max}) 与 portfolio_snapshots "
                     f"最新日({snap_max}) 之间无新增交易日（跨休市）⇒ 非缺口（交易日轴={_axis}）")
        elif _pending:
            _note = (f"etf_price_history 全局最新日({global_max})落后 portfolio_snapshots "
                     f"最新日({snap_max}) {_lag} 个交易日（{_lag_nd} 自然日）：属**当日 OHLCV "
                     f"尚未回填**（收盘后日线发布 + 21:45 兜底回填窗口均晚于本次跑批）⇒ "
                     f"待回填，不据此拒发日报；当日是否真缺由 21:45 兜底巡检判定"
                     f"（交易日轴={_axis}）")
        else:
            _note = (f"etf_price_history 全局最新日({global_max})落后 portfolio_snapshots "
                     f"最新日({snap_max}) {_lag} 个交易日（{_lag_nd} 自然日），"
                     f"疑似当日全量回填失败/源中断"
                     + ("；ETF_GAP_STRICT=1 强制按断崖处理" if _strict else ""))
        gaps.append({
            "code": "(systemic)", "latest": global_max,
            "first_in_window": None, "missing_count": _lag,
            "missing_dates": [],
            "severity": "warning" if _pending else "error",
            "note": _note,
        })

    # --- 参考日历：仅用于下方「单标的缺口」判定，故允许为空并在此提前返回 ---
    #    （A 的结论必须在此之前已经产出并留在 gaps 里）
    cal = _reference_calendar(conn, lookback_days)
    if not cal:
        return gaps

    # --- 单标的缺口 ---
    tail_start = (_dt.date.fromisoformat(global_max)
                  - _dt.timedelta(days=active_tail_days)).strftime("%Y-%m-%d")
    active = conn.execute(
        "SELECT DISTINCT code FROM etf_price_history WHERE date >= ?",
        (tail_start,)).fetchall()
    # 当前持仓口径（最新快照日的 distinct code）：仅对"在册持仓"判缺口。
    # 已清仓标的（如 159732）即便近 30 天有数据，也不应判 error——
    # 否则会永久误报，导致每次日报 run_status=degraded 被闸门拦死（09-17 式灾难）。
    try:
        _held_rows = conn.execute(
            "SELECT DISTINCT code FROM portfolio_snapshots "
            "WHERE date=(SELECT MAX(date) FROM portfolio_snapshots)").fetchall()
        held = {r[0] for r in _held_rows}
    except sqlite3.Error:
        held = None
    for (code,) in active:
        if held is not None and code not in held:
            continue  # 已清仓 / 不在当前持仓：不判缺口，避免永久误报 error
        r = conn.execute(
            "SELECT MIN(date), MAX(date), COUNT(DISTINCT date) "
            "FROM etf_price_history WHERE code=? AND date >= ?",
            (code, tail_start)).fetchone()
        first, latest, cnt = r[0], r[1], (r[2] or 0)
        if cnt < min_history_dates:
            # 窗口起点之后才纳入的标的：其早期缺失是纳入前，非断崖，不判。
            continue
        actual = {d for (d,) in conn.execute(
            "SELECT date FROM etf_price_history WHERE code=? AND date >= ?",
            (code, tail_start)).fetchall()}
        expected = {d for d in cal if d >= (first or d)}
        missing = sorted(expected - actual)
        if not missing:
            continue
        # 单标的缺失天数超过系统级阈值也按 error（与系统级同口径，避免漏拦真实断崖）。
        sev = "error" if len(missing) >= GAP_ERROR_THRESHOLD else "warning"
        # D: 不依赖参考日历完整性，以 portfolio_snapshots 最新日为锚点判单标的缺口。
        #    若某持仓标的全局最新日落后 snap_max ≥ GAP_ERROR_THRESHOLD 个自然日 ⇒ error，
        #    使"缺口跨日累积"在下一轮即触发（即便其余标的也停更、参考日历退化，旧逻辑会漏检）。
        if sev != "error" and snap_max and latest:
            try:
                _cd = (_dt.date.fromisoformat(snap_max)
                       - _dt.date.fromisoformat(latest)).days
            except (TypeError, ValueError):
                _cd = 0
            if _cd >= GAP_ERROR_THRESHOLD:
                sev = "error"
        gaps.append({"code": code, "latest": latest, "first_in_window": first,
                     "missing_count": len(missing), "missing_dates": missing,
                     "severity": sev})
    gaps.sort(key=lambda g: (0 if g["code"] == "(systemic)" else 1,
                             -g["missing_count"], g["code"]))
    return gaps


def _record_alert(db_path, message):
    """往 alerts 表落一条 error 级告警（与 snapshot_gate.record_error_alert 同构）。"""
    try:
        conn = _connect(db_path)
        try:
            conn.execute(_ALERTS_DDL)
            conn.execute(
                "INSERT INTO alerts (rule_name, level, message, created_at, acknowledged) "
                "VALUES (?,?,?,?,0)",
                (KIND, "error", str(message), _dt.datetime.now().isoformat()))
            conn.commit()
            return True
        finally:
            conn.close()
    except sqlite3.Error as e:
        logger.error("[ETF缺口闸门] error 级告警写入失败(%s)，结论不变", e)
        return False


def _repair(conn_str, gaps, log=logger.info):
    """对缺口标的显式增量回补（etf_price_history 仅 qfq 源，INSERT OR REPLACE 幂等）。

    仅当 ETF_GAP_AUTOREPAIR=1（调用方传入 auto_repair=True）时调用，绝不静默触发。
    """
    try:
        from src.analysis.predictor.price_history import backfill_etf_price_history
    except Exception as e:  # noqa: BLE001
        log(f"[ETF缺口闸门] 无法导入 backfill_etf_price_history: {e} —— 跳过自动回补")
        return 0
    codes = [g["code"] for g in gaps if g["code"] != "(systemic)"]
    if not codes:
        return 0
    conn = _connect(conn_str)
    try:
        _bres = backfill_etf_price_history(conn, codes, sources=("em", "tx"), log=log)
        log(f"[ETF缺口闸门] 自动回补 {len(codes)} 只标的，写入 {_bres.rows} 行"
            + (f"；隔离 {len(_bres.quarantined)} 只" if _bres.quarantined else ""))
        return _bres.rows
    except Exception as e:  # noqa: BLE001
        log(f"[ETF缺口闸门] 自动回补失败: {e}")
        return 0
    finally:
        conn.close()


def check_etf_price_history_gaps(db_path, auto_repair=False, log=logger.info):
    """闸门主入口。只读检测；error 级写 alerts 表；可选自动回补。

    Returns dict(ok, gaps, alert_level, message, kind, repaired)。
    """
    conn = None
    try:
        conn = _connect(db_path)
        gaps = detect_etf_price_gaps(conn)
    except sqlite3.Error as e:
        logger.warning("[ETF缺口闸门] etf_price_history 读取失败，按保守方向判为无缺口: %s", e)
        return {"ok": True, "gaps": [], "alert_level": "",
                "message": "", "kind": KIND, "repaired": 0}
    finally:
        if conn is not None:
            conn.close()

    if not gaps:
        return {"ok": True, "gaps": [], "alert_level": "",
                "message": f"[{KIND}] etf_price_history 连续性校验通过"
                           f"（参考窗口内无缺失交易日）",
                "kind": KIND, "repaired": 0}

    errs = [g for g in gaps if g["severity"] == "error"]
    warns = [g for g in gaps if g["severity"] == "warning"]
    # 「待回填」与「缺口」必须分开表述：前者是 15:30 跑批的正常形态，
    # 混写成「缺口」会让下一轮排查把它当断崖（也正是旧文案造成的误读来源）。
    _pending_only = bool(warns) and not errs and any(
        g["code"] == "(systemic)" for g in warns)
    detail = "; ".join(
        (f"{g['code']} 缺{g['missing_count']}天"
         f"({g['missing_dates'][0]}~{g['missing_dates'][-1]})"
         if g["missing_dates"] else f"{g['code']}: {g.get('note','')}")
        for g in gaps[:20])
    if _pending_only:
        message = (f"[{KIND}] etf_price_history 待回填（非缺口）: warning {len(warns)} 只，"
                   f"不降级、不拒发。明细: {detail}")
    else:
        message = (f"[{KIND}] etf_price_history 缺口: error {len(errs)} 只 / "
                   f"warning {len(warns)} 只。明细: {detail}")

    repaired = 0
    if auto_repair and errs:
        repaired = _repair(conn_str=str(db_path), gaps=errs, log=log)

    alert_level = "error" if errs else "warning"
    # error 级写 alerts 表（与 snapshot_gate 契约2 同构）；warning 不写、不降级。
    if alert_level == "error":
        _record_alert(str(db_path), message)
    return {"ok": not errs, "gaps": gaps, "alert_level": alert_level,
            "message": message, "kind": KIND, "repaired": repaired}


def main(argv=None):
    p = argparse.ArgumentParser(description="etf_price_history 缺口防护")
    p.add_argument("--db", default=None, help="库路径（默认 config.DATABASE_PATH）")
    p.add_argument("--repair", action="store_true",
                   help="检测并对缺口标的增量回补（etf_price_history 仅 qfq 源）")
    args = p.parse_args(argv)
    db = args.db
    if not db:
        try:
            from config.settings import DATABASE_PATH
            db = str(DATABASE_PATH)
        except Exception:
            db = "data/database/portfolio.db"
    res = check_etf_price_history_gaps(db, auto_repair=args.repair)
    print("alert_level:", res["alert_level"])
    print("ok:", res["ok"])
    print("gaps:", len(res["gaps"]))
    for g in res["gaps"]:
        span = f"{g['missing_dates'][0]}~{g['missing_dates'][-1]}" if g["missing_dates"] \
            else g.get("note", "")
        print(f"  {g['code']} [{g['severity']}] missing={g['missing_count']} {span}")
    print("message:", res["message"])
    return 0 if res["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
