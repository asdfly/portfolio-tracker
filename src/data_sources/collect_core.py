"""采集底座（P1 止血）：硬超时 + 全局超时补丁 + 看门狗。

设计要点:
  - AKShare 底层 `akshare/request.py` 的 `requests.get(...)` 不传 timeout，
    一旦连接/读取卡死会无限挂起（已发生 5h 挂死）。本模块用两道防线止血:
      1) install_requests_timeout(): 给所有 `requests` 调用注入默认 (connect, read)
         超时。AKShare 自带重试退避，命中超时后由其自身重试逻辑接管，行为自然。
      2) run_with_hard_timeout(): 子进程 + 队列硬超时，超时即 kill 子进程，
         父进程绝不卡死。用于最显眼的少数调用与兜底。
  - start_watchdog(): 全局墙钟看门狗，作为最后兜底，到点强制退出，避免无限挂。

本模块刻意不 import akshare / 项目代码，保持独立、可被子进程安全 pickle。
"""
import logging
import multiprocessing as mp
import os
import random
import sqlite3
import sys
import threading
import time
from datetime import datetime

logger = logging.getLogger(__name__)


class CollectTimeout(Exception):
    """硬超时：子进程在限定时间内未返回，已被 kill。"""


class CollectError(Exception):
    """采集子进程返回了异常（非超时）。"""


# ---------------------------------------------------------------------------
# 1) 全局 requests 超时补丁（覆盖所有 ak.* 调用，零侵入）
# ---------------------------------------------------------------------------
_REQUESTS_PATCHED = False


def install_requests_timeout(connect: float = 10, read: float = 30):
    """给 requests 注入默认 (connect, read) 超时。

    仅当调用方未显式传 timeout 时生效（不覆盖新浪等已自带 timeout 的路径）。
    幂等：重复调用不会叠加补丁。
    """
    global _REQUESTS_PATCHED
    if _REQUESTS_PATCHED:
        return

    import requests  # 延迟导入，避免模块级副作用

    _orig = requests.Session.request

    def _patched(self, method, url, *args, **kwargs):
        if kwargs.get("timeout") is None:
            kwargs["timeout"] = (connect, read)
        return _orig(self, method, url, *args, **kwargs)

    requests.Session.request = _patched
    _REQUESTS_PATCHED = True


# ---------------------------------------------------------------------------
# 2) 硬超时原语（子进程 + 队列，超时 kill）
# ---------------------------------------------------------------------------
def _worker(q: "mp.Queue", func, args, kwargs):
    try:
        # 子进程内同样启用超时补丁，使 ak 调用在子进程内也有界
        install_requests_timeout()
        q.put((True, func(*args, **kwargs), None))
    except Exception as e:  # 捕获 AKShare 内部一切异常
        q.put((False, None, repr(e)))


def run_with_hard_timeout(func, *args, timeout: float = 30, **kwargs):
    """跨平台硬超时包裹。

    func 必须是可被 pickle 按引用定位的（模块级函数，如 `ak.stock_zh_a_spot_em`）。
    超时则杀掉子进程并抛 CollectTimeout；成功后返回原值。
    """
    ctx = mp.get_context("spawn")
    q = ctx.Queue(maxsize=1)
    p = ctx.Process(target=_worker, args=(q, func, args, kwargs), daemon=True)
    p.start()
    p.join(timeout)
    if p.is_alive():
        p.kill()
        p.join()
        raise CollectTimeout(
            f"{getattr(func, '__name__', '?')} 硬超时 {timeout}s，已杀子进程"
        )
    if not q.empty():
        ok, val, err = q.get()
        if ok:
            return val
        raise CollectError(str(err))
    raise CollectError(f"{getattr(func, '__name__', '?')} 子进程无返回")


def _is_network(err_text: str) -> bool:
    return any(
        k in err_text
        for k in ("ProxyError", "ConnectionError", "ConnectTimeout",
                 "ReadTimeout", "Timeout", "Max retries", "RemoteDisconnected",
                 "ConnectionReset")
    )


def retry_call(func, *args, timeout: float = 30, retries: int = 3,
               backoff: float = 2.0, jitter: float = 0.3,
               network_only: bool = True, **kwargs):
    """超时 + 退避重试（网络类异常才重试）。

    注: AKShare 自带重试，本函数主要面向非 ak 路径或需要显式收紧的场景。
    """
    last = None
    for attempt in range(retries):
        try:
            return run_with_hard_timeout(func, *args, timeout=timeout, **kwargs)
        except CollectTimeout:
            last = "timeout"
            if attempt < retries - 1:
                time.sleep(backoff * (attempt + 1) + random.uniform(0, jitter))
                continue
            raise
        except CollectError as e:
            if network_only and not _is_network(str(e)):
                raise
            last = e
            if attempt < retries - 1:
                time.sleep(backoff * (attempt + 1) + random.uniform(0, jitter))
                continue
            raise
    raise last or CollectError("retry exhausted")


# ---------------------------------------------------------------------------
# 3) 全局墙钟看门狗（最后兜底）
# ---------------------------------------------------------------------------
_watchdog = None


def start_watchdog(minutes: float = 50, logger_=None):
    """启动全局看门狗：超过 minutes 主流程仍未结束则强制退出。

    返回 Timer 对象，正常结束时务必调用 cancel_watchdog() 取消。
    """
    global _watchdog

    limit = max(1, int(minutes * 60))

    def _fire():
        msg = f"[WATCHDOG] 全局墙钟超时 {minutes}min，强制退出以避免无限挂死"
        if logger_:
            logger_.error(msg)
        try:
            sys.stderr.write(msg + "\n")
        except Exception:
            pass
        os._exit(1)  # 硬退出：即便主线程卡死在网络调用中也能终止

    _watchdog = threading.Timer(limit, _fire)
    _watchdog.daemon = True
    _watchdog.start()
    return _watchdog


def cancel_watchdog():
    global _watchdog
    if _watchdog is not None:
        _watchdog.cancel()
        _watchdog = None


# ---------------------------------------------------------------------------
# 4) P2 真实性闸门：落库前断言"返回日期 == 目标日期"
# ---------------------------------------------------------------------------

def ensure_quality_issues_table(conn):
    """创建 data_quality_issues 质量事件表（如不存在）。

    幂等；闸门在落库前按需调用，保证表存在。
    """
    conn.execute(
        "CREATE TABLE IF NOT EXISTS data_quality_issues ("
        " id INTEGER PRIMARY KEY AUTOINCREMENT,"
        " run_date TEXT,"          # 闸门运行的日期(本日)
        " target_date TEXT,"       # 我们试图采集的目标日期
        " source TEXT,"            # 数据源/表名, 如 'stock_margin'
        " issue_type TEXT,"        # date_mismatch | source_date_null | spot_historical | spot_stale
        " detail TEXT,"            # 人读说明
        " n_affected INTEGER,"     # 受影响行数
        " action TEXT,"            # rejected | stored_as_target | flagged
        " sample TEXT,"            # 违规样本值
        " created_at TEXT)"
    )
    conn.commit()


def _record_issues(conn, run_date, target_date, source_name, issues):
    ensure_quality_issues_table(conn)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for it in issues:
        conn.execute(
            "INSERT INTO data_quality_issues "
            "(run_date, target_date, source, issue_type, detail, "
            " n_affected, action, sample, created_at) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (run_date, target_date, source_name, it["issue_type"],
             it["detail"], it["n_affected"], it["action"],
             it.get("sample", ""), now),
        )
    conn.commit()


def gate_source_date_matches(df, date_col, target_date, source_name,
                             conn=None, run_date=None):
    """真实性闸门（带源日期列的数据：margin/lhb/institution/block_trade）。

    仅保留 `date_col == target_date` 的行；其余记为质量事件并**排除**，
    绝不把错误日期的数据落库（治 8/4 重标成 8/5 那类失真）。

    规则:
      - 日期列空值(null/NaN/空串) -> 无法校验，信任 target，保留(轻量提示)。
      - 日期列有值且 != target_date -> 'date_mismatch'，排除。
      - 日期列有值且 == target_date -> 通过。

    Returns:
        (passed_df, n_rejected, issues)
        issues: 供后续记录/展示的列表
    """
    import pandas as pd

    if df is None or df.empty or date_col not in df.columns:
        return df, 0, []

    run_date = run_date or datetime.now().strftime("%Y-%m-%d")
    norm_target = str(target_date).strip()

    raw = df[date_col]
    mask_pass = []
    mismatch = []
    n_null = 0
    for val in raw:
        if val is None or (isinstance(val, float) and pd.isna(val)) \
                or str(val).strip() == "":
            n_null += 1
            mask_pass.append(True)          # 无法校验 -> 信任 target
            continue
        if str(val).strip() == norm_target:
            mask_pass.append(True)
        else:
            mismatch.append(str(val).strip())
            mask_pass.append(False)

    passed_mask = pd.Series(mask_pass, index=df.index)
    passed_df = df[passed_mask].copy()
    n_rejected = int((~passed_mask).sum())

    issues = []
    if n_rejected:
        sample = ", ".join(sorted(set(mismatch))[:5])
        issues.append({
            "issue_type": "date_mismatch",
            "detail": (f"源返回 {n_rejected} 行日期≠目标({norm_target})，"
                       f"已排除，未以错误日期落库"),
            "n_affected": n_rejected,
            "action": "rejected",
            "sample": sample,
        })
        logger.warning(
            f"[真实性闸门] {source_name}: 排除 {n_rejected} 行日期失真 "
            f"(目标 {norm_target}, 实得 {sample})")
    if n_null:
        issues.append({
            "issue_type": "source_date_null",
            "detail": (f"源返回 {n_null} 行日期为空，已信任目标日期"
                       f"({norm_target})保留"),
            "n_affected": n_null,
            "action": "stored_as_target",
            "sample": "",
        })

    if conn is not None and issues:
        _record_issues(conn, run_date, target_date, source_name, issues)
    return passed_df, n_rejected, issues


def gate_spot_for_today(df, target_date, source_name, conn=None, run_date=None):
    """真实性闸门（实时快照类：spot/最新行情）。

    实时快照永远反映"最新"，无法回溯历史日期。仅当 target_date == 今天
    才允许落库；若请求历史日期却拿"最新"填充，整体排除，避免重标失真
    （即 8/4 重标 8/5 那类问题）。

    Returns:
        (passed_df, n_rejected, issues)
        passed_df 在拒绝时为空(保留列结构)，成功时为原 df。
    """
    if df is None or df.empty:
        return df, 0, []

    today = datetime.now().strftime("%Y-%m-%d")
    run_date = run_date or today
    if target_date == today:
        return df, 0, []

    n = len(df)
    issues = [{
        "issue_type": "spot_historical",
        "detail": (f"实时快照类源无法回溯历史日期；请求 {target_date} "
                   f"但数据实为最新({today})，已整体排除，避免重标失真"),
        "n_affected": n,
        "action": "rejected",
        "sample": f"requested={target_date}, actual_latest={today}",
    }]
    logger.warning(
        f"[真实性闸门] {source_name}: 拒绝 {n} 行历史快照 "
        f"(请求 {target_date}, 最新 {today})")
    if conn is not None:
        _record_issues(conn, run_date, target_date, source_name, issues)
    return df.iloc[0:0].copy(), n, issues


def flag_spot_stale(df, target_date, source_name, date_col="data_date",
                    conn=None, run_date=None):
    """软校验：实时快照的源数据日期(如 data_date)若与目标日期不一致，
    记录质量事件。仅"提示"，**不拒绝**落库（避免误杀正常数据）。

    用途: 捕捉"源未刷新，快照实为前一交易日"这类滞后失真。
    """
    import pandas as pd

    if df is None or df.empty or date_col not in df.columns:
        return
    raw = df[date_col]
    bad = []
    for val in raw:
        if val is None or (isinstance(val, float) and pd.isna(val)) \
                or str(val).strip() == "":
            continue
        if str(val).strip() != str(target_date).strip():
            bad.append(str(val).strip())
    if not bad:
        return

    run_date = run_date or datetime.now().strftime("%Y-%m-%d")
    issues = [{
        "issue_type": "spot_stale",
        "detail": (f"实时快照源数据日期({date_col})与目标({target_date})不一致，"
                   f"可能源未刷新；已落库但标记待核查"),
        "n_affected": len(bad),
        "action": "flagged",
        "sample": ", ".join(sorted(set(bad))[:5]),
    }]
    logger.warning(
        f"[真实性闸门] {source_name}: 源数据日期滞后 {sorted(set(bad))[:3]} "
        f"(目标 {target_date})")
    if conn is not None:
        _record_issues(conn, run_date, target_date, source_name, issues)


def record_quality_issue(conn, run_date, target_date, source_name, issues):
    """公共包装: 记录一组数据质量事件(供各采集模块在落库后补充标注)。

    例如 P3 的 'sina_iopv_proxy' —— 债券ETF经新浪兜底, iopv 实为收盘价代理,
    需诚实标注以便 P5 报告呈现。
    """
    _record_issues(conn, run_date, target_date, source_name, issues)


# ---------------------------------------------------------------------------
# 5) P3 补采重试队列（治 D6: 两融等易空数据无重试闭环）
# ---------------------------------------------------------------------------

def ensure_retry_queue_table(conn):
    """创建补采重试队列表(如不存在)。幂等。"""
    conn.execute(
        "CREATE TABLE IF NOT EXISTS collection_retry_queue ("
        " id INTEGER PRIMARY KEY AUTOINCREMENT,"
        " target_date TEXT NOT NULL,"
        " source TEXT NOT NULL,"
        " reason TEXT,"
        " created_at TEXT,"
        " attempts INTEGER DEFAULT 0,"
        " status TEXT DEFAULT 'pending',"   # pending | done | exhausted
        " last_tried TEXT,"
        " UNIQUE(target_date, source))"
    )
    conn.commit()


def is_confirmed_non_trading_day(conn, target_date):
    """判断 target_date 是否**确定**为非交易日(用于阻止假缺口入队)。

    治 2026-08-08(周六)被登记为两融待补采、retry 3 次耗尽后永久残留
    exhausted 的僵尸项问题 —— 非交易日本就无数据, 补采永远不会成功。

    判定策略(保守, 宁可放过不可误杀):
      - 周六/周日 -> 确定非交易日(无需查库)。
      - 工作日    -> 仅当 index_quotes 交易日历已覆盖到「晚于 target_date」
                    的日期(说明日历在该日之后仍有数据), 而 target_date
                    自身无行时, 才判定为节假日。日历尚未覆盖(如当日数据
                    还没落库)则返回 False, 正常入队, 避免漏掉真实缺口。

    Returns:
        True 表示确定非交易日, 应跳过入队。
    """
    try:
        dt = datetime.strptime(str(target_date).strip(), "%Y-%m-%d")
    except (ValueError, TypeError):
        return False  # 日期格式异常, 不拦截

    if dt.weekday() >= 5:
        return True

    try:
        row = conn.execute(
            "SELECT (SELECT COUNT(*) FROM index_quotes WHERE date = ?),"
            "       (SELECT COUNT(*) FROM index_quotes WHERE date > ?)",
            (target_date, target_date)).fetchone()
    except sqlite3.Error:
        return False  # 无 index_quotes 表(如单测内存库), 不拦截

    if not row:
        return False
    have_self, have_after = row[0] or 0, row[1] or 0
    return have_self == 0 and have_after > 0


def enqueue_retry(conn, target_date, source, reason="", max_attempts=3):
    """将一项待补采任务加入队列。

    幂等: 同 (target_date, source) 已存在 pending 时不重复插入。
    max_attempts 仅作占位, 实际次数上限由消费函数控制。
    非交易日(周末/已确认节假日)直接跳过, 不产生永远补不到的假缺口。
    """
    ensure_retry_queue_table(conn)
    if is_confirmed_non_trading_day(conn, target_date):
        logger.info(
            f"[补采队列] 跳过入队 {source}@{target_date}: 非交易日, 本无数据")
        return
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn.execute(
        "INSERT OR IGNORE INTO collection_retry_queue "
        "(target_date, source, reason, created_at, attempts, status) "
        "VALUES (?,?,?,?,0,'pending')",
        (target_date, source, reason, now))
    conn.commit()


def list_pending_retries(conn, source=None, target_date=None):
    """返回 pending 补采任务 [(id, target_date, source), ...]。"""
    ensure_retry_queue_table(conn)
    if source and target_date:
        rows = conn.execute(
            "SELECT id, target_date, source FROM collection_retry_queue "
            "WHERE status='pending' AND source=? AND target_date=?",
            (source, target_date)).fetchall()
    elif source:
        rows = conn.execute(
            "SELECT id, target_date, source FROM collection_retry_queue "
            "WHERE status='pending' AND source=?", (source,)).fetchall()
    elif target_date:
        rows = conn.execute(
            "SELECT id, target_date, source FROM collection_retry_queue "
            "WHERE status='pending' AND target_date=?", (target_date,)).fetchall()
    else:
        rows = conn.execute(
            "SELECT id, target_date, source FROM collection_retry_queue "
            "WHERE status='pending'").fetchall()
    return [(r[0], r[1], r[2]) for r in rows]


# ---------------------------------------------------------------------------
# 6) P5 可观测性: 运行报告 + 三类事件告警推送
# ---------------------------------------------------------------------------
import json as _json

DEFAULT_STALE_ALERT_THRESHOLD = 5
DEFAULT_DQ_SCORE_WARN = 80


class RunReporter:
    """采集运行报告器: 增量记录各源/阶段/告警, 结束时产出 run_report_<date>.json。

    设计: 在 run_analysis.main() 中创建并贯穿全程增量 record_*；最后在 finally
    中 finalize_and_write(), 无论成功或异常都产出报告(可观测兜底)。
    """

    def __init__(self, date_str, mode="daily", reports_dir=None, run_date=None):
        self.date = date_str                       # 业务日期(目标日/回填日)
        self.mode = mode
        self.run_date = run_date or datetime.now().strftime("%Y-%m-%d")
        self.start_ts = time.time()
        self.sources = {}        # name -> {ok, fail, timeout, source_used, detail}
        self.stages = {}         # name -> {status, duration_s, note}
        self.alerts = []         # [{level, kind, message}]
        self._dq_score = None
        self._hang_recovered = False
        self.reports_dir = reports_dir

    # --- 增量记录 ---
    def record_source(self, name, ok=0, fail=0, timeout=0, source_used="", detail=""):
        self.sources[name] = dict(ok=int(ok), fail=int(fail),
                                  timeout=int(timeout),
                                  source_used=source_used, detail=detail)

    def stage(self, name, status="ok", duration_s=None, note=""):
        self.stages[name] = dict(
            status=status,
            duration_s=round(duration_s, 2) if duration_s is not None else None,
            note=note)

    def alert(self, level, kind, message):
        self.alerts.append(dict(level=level, kind=kind, message=message))

    def set_dq_score(self, score):
        if score is not None:
            self._dq_score = score

    def mark_hang_recovered(self):
        self._hang_recovered = True

    # --- 终态 ---
    def finalize_and_write(self, dq_issues=None, queue_pending=0,
                           stale_threshold=DEFAULT_STALE_ALERT_THRESHOLD,
                           reports_dir=None, dispatch_config=None):
        """汇总并写 run_report_<date>.json; 计算 dq_score 与三类告警; 触发推送。

        Args:
            dq_issues: list of dict(issue_type, source, n_affected, action, detail)
                       来自 fetch_run_quality_issues() (本运行产生的质量事件)。
            queue_pending: 补采重试队列 pending 数。
            dispatch_config: notification.json 路径, 非空则按配置推送告警。
        Returns:
            (report_dict, path_or_None)
        """
        duration_s = round(time.time() - self.start_ts, 2)

        # data_quality_issues 汇总(供报告与告警)
        dq_issue_summary = []
        mismatch_n = stale_n = 0
        for it in (dq_issues or []):
            dq_issue_summary.append(dict(
                issue_type=it.get("issue_type"), source=it.get("source"),
                n_affected=it.get("n_affected"), action=it.get("action"),
                detail=it.get("detail")))
            if it.get("issue_type") == "date_mismatch":
                mismatch_n += int(it.get("n_affected") or 0)
            if it.get("issue_type") == "spot_stale":
                stale_n += int(it.get("n_affected") or 0)

        # dq_score: 优先外部传入(DataQualityChecker), 否则由 issue 推导
        dq_score = self._dq_score
        if dq_score is None:
            dq_score = max(0, 100 - mismatch_n * 5 - stale_n * 1)

        # --- 三类(及扩展)告警 ---
        if self._hang_recovered:
            self.alert("warning", "hang_recovered",
                       "某数据源发生硬超时(已被杀进程恢复), 主流程继续; 建议排查该源")
        core = ["market_events", "etf_fundamental", "fund_flow", "macro"]
        if core and all(self.stages.get(s, {}).get("status") == "error" for s in core):
            self.alert("critical", "source_all_down",
                       "核心采集源(市场事件/ETF基本面/资金流/宏观)全部失败")
        if mismatch_n > 0:
            self.alert("critical", "data_distortion_blocked",
                       f"数据失真拦截 {mismatch_n} 行(日期≠目标), 已阻止落库")
        if stale_n >= stale_threshold:
            self.alert("warning", "stale_over_threshold",
                       f"源数据滞后 {stale_n} 行(≥阈值{stale_threshold}), 需核查源刷新")
        if dq_score is not None and dq_score < DEFAULT_DQ_SCORE_WARN:
            self.alert("warning", "dq_low_score",
                       f"数据质量评分 {dq_score} 低于阈值 {DEFAULT_DQ_SCORE_WARN}")

        report = {
            "date": self.date,
            "run_date": self.run_date,
            "mode": self.mode,
            "duration_s": duration_s,
            "sources": [dict(name=k, **v) for k, v in self.sources.items()],
            "stages": self.stages,
            "data_quality_issues": dq_issue_summary,
            "dq_score": dq_score,
            "retry_queue_pending": queue_pending,
            "alerts": self.alerts,
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

        # 写文件
        out_dir = reports_dir or self.reports_dir
        if out_dir is None:
            out_dir = os.path.join(os.getcwd(), "data", "reports")
        try:
            os.makedirs(out_dir, exist_ok=True)
            path = os.path.join(out_dir, f"run_report_{self.date}.json")
            with open(path, "w", encoding="utf-8") as f:
                _json.dump(report, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"[P5] 运行报告写入失败: {e}")
            path = None

        # 推送(配置驱动)
        if dispatch_config and path:
            try:
                dispatch_alerts(report, dispatch_config)
            except Exception as e:
                logger.warning(f"[P5] 告警推送失败(不影响主流程): {e}")

        return report, path


def fetch_run_quality_issues(conn, run_date):
    """读取本运行(run_date)产生的数据质量事件, 供报告呈现。"""
    ensure_quality_issues_table(conn)
    rows = conn.execute(
        "SELECT issue_type, source, n_affected, action, detail "
        "FROM data_quality_issues WHERE run_date=?", (run_date,)).fetchall()
    return [dict(issue_type=r[0], source=r[1], n_affected=r[2],
                 action=r[3], detail=r[4]) for r in rows]


def count_pending_retries(conn):
    """返回补采重试队列 pending 条数。"""
    ensure_retry_queue_table(conn)
    return conn.execute(
        "SELECT COUNT(*) FROM collection_retry_queue WHERE status='pending'"
    ).fetchone()[0]


def dispatch_alerts(report, config_path):
    """按 config/notification.json 推送告警(挂死恢复/源全挂/stale超阈等)。

    配置缺省/未启用/无 webhook 时仅记日志, 不抛错。
    推送内容仅含 report['alerts'](为空则不推送)。log_file 通道始终可用,
    作为可观测兜底(即便 webhook 未配也能在本地留存告警流水)。
    """
    if not report.get("alerts"):
        return
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            cfg = _json.load(f)
    except Exception:
        logger.info("[P5] 未找到通知配置, 跳过告警推送")
        return
    if not cfg.get("enabled", False):
        return
    events = set(cfg.get("events", []))
    to_send = [a for a in report["alerts"] if a["kind"] in events] \
        if events else report["alerts"]
    if not to_send:
        return
    payload = {"report_date": report.get("date"), "alerts": to_send}

    ch = cfg.get("channels", {})
    # 1) webhook (若配置)
    wh = ch.get("webhook")
    if wh and wh.get("url"):
        import requests as _req
        try:
            _req.post(wh["url"], json=payload, timeout=10)
            logger.info(f"[P5] 告警已推送到 webhook ({len(to_send)} 条)")
        except Exception as e:
            logger.warning(f"[P5] webhook 推送失败: {e}")
    # 2) 本地日志文件(可观测兜底)
    lf = ch.get("log_file")
    if lf:
        try:
            os.makedirs(os.path.dirname(lf) or ".", exist_ok=True)
            with open(lf, "a", encoding="utf-8") as f:
                f.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " " +
                        _json.dumps(payload, ensure_ascii=False) + "\n")
        except Exception as e:
            logger.warning(f"[P5] 告警日志写入失败: {e}")
