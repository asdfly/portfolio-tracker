#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
运行后哨兵（post-run sentinel）
================================

每晚 15:30 夜线收尾后，独立检测当日
    data/reports/run_report_<YYYY-MM-DD>.json
是否缺失 / 损坏，并在【缺失】时通过【现有】邮件通道
（scripts/send_generic_email.py，复用 .env 的 SMTP 凭据）发出告警，
同时把事件写入 execution_logs 监控账本。

为什么需要（根因，详见 2026-10-13 排查）：
  2026-10-09 夜线在风险分析阶段被外部静默终止（进程硬死），既无 ERROR 也无
  run_report 写出；次日只能靠翻日志才发现。send_report_email.py 的"日报未送达"
  账本（daily_report_not_sent）只在 bat 链仍存活时才会写，硬死时整条链一起死，
  故留白。本哨兵作为【独立触发】的运行后检测，专门补这个无痕窗口。

设计铁律（与现有体系一致，勿破）：
  - 邮件通道共用 send_generic_email.py，绝不新造 SMTP 逻辑。
  - 账本写入只走 execution_logs（运维账本），业务库只读。
  - 非交易日（周末 / 法定假日）直接跳过，绝不误报。
  - 仅当报告【缺失 / 损坏】才发邮件告警；报告存在但
    run_status ∈ {partial, failed, degraded} 已由 send_report_email.py 的闸门拒发
    + daily_report_not_sent 账本覆盖，本哨兵只记账、不重复发信，避免告警疲劳。
  - 报告存在且 run_status = ok / 未知 → 一切正常，记账即退出。
  - 去重：同一业务日 24h 内已告警过则跳过重复发送（防 in-chain 与独立触发双发）。

退出码：
  0  正常（报告存在 / 非交易日跳过 / 过早跳过 / 夜线仍在运行跳过）
  2  检测到报告缺失 / 损坏 / **快照行数塌缩**（已发或尝试发告警 + 已记账）
  3  参数 / 内部前置错误
"""
from __future__ import annotations

import argparse
import datetime
import json
import os
import sqlite3
import statistics
import subprocess
import sys
import tempfile

# 项目根目录（脚本位于 portfolio_tracker/scripts/）
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

# 夜线触发时刻（与 scheduled_run.bat 的 15:30 对齐）
NIGHTLY_TRIGGER_HOUR = 15
NIGHTLY_TRIGGER_MIN = 30
# 最早可检查时刻 = 夜线触发时刻本身（grace=0）。
# 用途：仅拦截「夜线尚未触发的手动过早运行」（如 14:00 手动跑哨兵），
# 防其因报告尚未生成而误报。in-chain 哨兵（夜线返回后才跑，必然 >=15:30）
# 与独立 16:30 任务都不会被此守卫拦下。
# 「夜线仍在运行」由 _nightly_still_running() 进程守卫单独处理，不靠时间守卫。
DEFAULT_GRACE_MIN = 0
# 告警去重标记文件（落 logs/，不进 git）
FLAG_FILE = os.path.join(ROOT, "logs", "sentinel_alerted.json")

# ============ 快照完整性探针阈值（P0#2，2026-10-27 落地）============
# 🔴 事故：2026-10-22~10-26 组合快照由 35 行塌缩至 12 行（ETF 行全缺、仅剩场外基金），
#    而 run_report 仍 run_status=ok ⇒ 原哨兵「报告存在即正常」对这类**数据失真完全失明**，
#    全靠人工核验才发现。daily_return 因此被算成 -21.62%（真值 +0.157%）。
# 🔴 阈值必须与 scripts/gen_combo_report.py 的生成前熔断**同源同值**：
#    两处判据不一致会出现「哨兵报异常但生成器放行」或反之，比不检更危险。
#    改动任何一侧，必须同步另一侧（grep 关键字：SNAP_MEDIAN_RATIO）。
SNAP_MIN_ROWS = 20          # 绝对下限
SNAP_MEDIAN_RATIO = 0.70    # 相对近 N 日中位数的最小比例
SNAP_BASELINE_N = 5         # 基线窗口（个快照日）

# run_status 取值（与 src/data_sources/collect_core.py 的 RUN_STATUS_* 同义）。
_BLOCKING_STATUS = ("partial", "failed", "degraded")


def _log(msg: str) -> None:
    """统一日志行格式（stdout 会被 bat 重定向到 logs/sentinel.log）。"""
    print(f"[{datetime.datetime.now():%Y-%m-%d %H:%M:%S}] [SENTINEL] {msg}")


def load_env_simple(env_path: str) -> dict:
    """读 .env 的 EMAIL_*（与 scripts/send_generic_email.py 同口径，不 import 项目业务模块）。"""
    data: dict = {}
    try:
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, _, v = line.partition("=")
                data[k.strip()] = v.strip().strip("'\"")
    except OSError:
        pass
    return data


def is_trading_day_safe(d: datetime.date) -> bool:
    """交易日判定；日历不可用时退化为仅周末规则并告警。"""
    try:
        from src.utils.trading_calendar import is_trading_day
        return bool(is_trading_day(d))
    except Exception as exc:  # 日历模块缺失/异常 → 退化为仅周末，避免哨兵自身崩
        _log(f"交易日历不可用({exc})，退化为仅周末规则")
        return d.weekday() < 5


def _ledger(task: str, status: str, detail: str) -> None:
    """写 execution_logs 监控账本（运维账本，只读打开业务库之外唯一写入目标）。"""
    try:
        from config.settings import DATABASE_PATH
        from src.utils.monitor import Monitor
        Monitor(str(DATABASE_PATH), {}).log_execution(task, status, detail)
        _log(f"账本已记录: {task} {status}")
    except Exception as exc:  # 账本不可用不得改变判定结论
        _log(f"账本记录失败(不影响判定): {exc}")


def _load_flag() -> dict:
    try:
        if os.path.exists(FLAG_FILE):
            with open(FLAG_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return {}


def _save_flag(flag: dict) -> None:
    try:
        os.makedirs(os.path.dirname(FLAG_FILE), exist_ok=True)
        with open(FLAG_FILE, "w", encoding="utf-8") as f:
            json.dump(flag, f, ensure_ascii=False, indent=2)
    except Exception as exc:
        _log(f"告警去重标记写入失败: {exc}")


def _already_alerted(flag: dict, date_str: str) -> bool:
    """本日 24h 内是否因"缺失"已告警过（用于去重）。"""
    e = flag.get(date_str)
    if not e or e.get("status") != "missing":
        return False
    try:
        ts = datetime.datetime.strptime(e.get("ts", ""), "%Y-%m-%d %H:%M:%S")
        return (datetime.datetime.now() - ts).total_seconds() < 24 * 3600
    except Exception:
        return False


def _nightly_still_running() -> bool:
    """best-effort：检测 run_analysis.py 当前是否仍在运行（防夜线未结束误报）。

    优先用 wmic 读取 python 进程命令行；不可用/异常则退化为 False。
    """
    try:
        out = subprocess.run(
            ["wmic", "process", "where", "name='python.exe'", "get", "commandline", "/VALUE"],
            capture_output=True, text=True, timeout=15,
        ).stdout or ""
        return "run_analysis.py" in out
    except Exception:
        return False


def _build_text(date_str: str, reason: str) -> str:
    return (
        f"投资组合夜线运行账本缺失告警\n"
        f"业务日期：{date_str}\n"
        f"触发时间：{datetime.datetime.now():%Y-%m-%d %H:%M:%S}\n\n"
        f"现象：当日运行账本 data/reports/run_report_{date_str}.json 缺失。\n\n"
        f"根因推断：夜线在写出运行账本前被外部静默终止（进程硬死 / 被强杀 / 断电 / "
        f"机器休眠），未留下任何失败告警（与 2026-10-09 事故同形态）。\n\n"
        f"影响：当日收盘日报也不会送达（send_report_email 会因数据未就绪拒发）。\n\n"
        f"排查清单：\n"
        f"  1. 查 logs/scheduled_run.log 中 {date_str} 15:30 的运行起始与中断点；\n"
        f"  2. 查 logs/portfolio_{date_str.replace('-', '')}.log 的死亡边界；\n"
        f"  3. 确认主机 15:30 时段是否被休眠 / 断电 / 合盖 / 任务管理器强杀；\n"
        f"  4. 若确认硬死，次日 15:30 重新运行夜线即可；"
        f"切勿用 --date 改写历史日实时价（项目铁律）。\n\n"
        f"本告警由运行后哨兵（scripts/run_sentinel.py）自动发出。"
    )


def _build_html(date_str: str, reason: str) -> str:
    return f"""<html>
<body style="font-family: -apple-system, 'Segoe UI', Arial, sans-serif; color:#222; margin:20px;">
  <div style="background:#c0392b; color:#fff; padding:14px 18px; border-radius:8px;">
    <h2 style="margin:0;">⚠ 投资组合夜线运行账本缺失告警</h2>
  </div>
  <div style="margin-top:16px; line-height:1.7;">
    <p><strong>业务日期：</strong>{date_str}</p>
    <p><strong>触发时间：</strong>{datetime.datetime.now():%Y-%m-%d %H:%M:%S}</p>
    <p><strong>现象：</strong>当日运行账本 <code>data/reports/run_report_{date_str}.json</code> 缺失。</p>
    <p><strong>根因推断：</strong>{reason}</p>
    <p><strong>影响：</strong>当日收盘日报也不会送达（日报闸门会因数据未就绪拒发）。</p>
    <h3 style="margin-top:18px;">排查清单</h3>
    <ol style="line-height:1.9;">
      <li>查 <code>logs/scheduled_run.log</code> 中 {date_str} 15:30 的运行起始与中断点；</li>
      <li>查 <code>logs/portfolio_{date_str.replace('-', '')}.log</code> 的死亡边界；</li>
      <li>确认主机 15:30 时段是否被休眠 / 断电 / 合盖 / 任务管理器强杀；</li>
      <li>若确认硬死，次日 15:30 重新运行夜线即可；<strong>切勿用 --date 改写历史日实时价</strong>（项目铁律）。</li>
    </ol>
    <p style="color:#888; font-size:12px; margin-top:18px;">
      本告警由运行后哨兵（scripts/run_sentinel.py）自动发出。
    </p>
  </div>
</body>
</html>"""


def _snapshot_probe(date_str: str, db_path: str = None):
    """只读探测当日组合快照行数完整性。返回 dict 或 None（库/表不可用）。

    db_path 仅供离线验证（默认生产库），生产调用不传。

    🔴 业务库**只读**（URI mode=ro）：哨兵绝不写业务库，与既有铁律一致。
    返回键：date / rows / median / baseline_dates / verdict / detail
    verdict ∈ {'ok','missing','collapsed','no_baseline'}
    """
    db = db_path or os.path.join(ROOT, "data", "database", "portfolio.db")
    if not os.path.exists(db):
        _log(f"快照探针跳过：业务库不存在 {db}")
        return None
    try:
        con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
        try:
            cur = con.cursor()
            rows = cur.execute(
                "SELECT COUNT(*) FROM portfolio_snapshots WHERE date=?", (date_str,)
            ).fetchone()[0] or 0
            base = cur.execute(
                "SELECT COUNT(*) FROM portfolio_snapshots WHERE date < ? "
                "GROUP BY date ORDER BY date DESC LIMIT ?", (date_str, SNAP_BASELINE_N)
            ).fetchall()
        finally:
            con.close()
    except Exception as exc:
        _log(f"快照探针失败（不影响主判定）: {exc}")
        return None

    med = int(statistics.median([r[0] for r in base])) if base else 0
    if rows == 0:
        verdict = "missing"
    elif med == 0:
        verdict = "no_baseline"
    elif rows < med * SNAP_MEDIAN_RATIO:
        verdict = "collapsed"
    elif med >= SNAP_MIN_ROWS and rows < SNAP_MIN_ROWS:
        verdict = "collapsed"
    else:
        verdict = "ok"
    return {"date": date_str, "rows": rows, "median": med,
            "baseline_dates": len(base), "verdict": verdict,
            "detail": f"SNAP_ROWS={rows}/{med}"}


def _build_snap_text(p: dict) -> str:
    """快照塌缩告警正文（与缺失告警区分，附修复路径）。"""
    why = ("当日快照 0 行（数据完全未写入）" if p["verdict"] == "missing"
           else f"当日快照仅 {p['rows']} 行，远低于近 {p['baseline_dates']} 个快照日中位数 "
                f"{p['median']} 行")
    return (
        f"投资组合快照完整性告警（SNAP_ROWS 探针）\n"
        f"业务日期：{p['date']}\n"
        f"触发时间：{datetime.datetime.now():%Y-%m-%d %H:%M:%S}\n\n"
        f"现象：{why}。\n"
        f"SNAP_ROWS：{p['rows']}/{p['median']}（当日 / 近 {SNAP_BASELINE_N} 日中位数）\n"
        f"判据：< 中位数×{SNAP_MEDIAN_RATIO:.0%} 或 < 绝对下限 {SNAP_MIN_ROWS} 行\n\n"
        f"根因推断：与 2026-10-22~10-26 事故同形态 —— ETF 行情子源断供后，采集器只写入\n"
        f"场外基金行（约 12 行），ETF 行缺失。此时组合合计市值与 daily_return 会同步失真\n"
        f"（该次 -21.62% vs 真值 +0.157%），而 run_report 的 run_status 仍可能为 ok，\n"
        f"故本探针不依赖 run_status。\n\n"
        f"影响：\n"
        f"  1. 当日「组合+大盘综合视角」日报已被生成前熔断拦下（SNAP_ROWS 同源判据）；\n"
        f"  2. 知识库推送应 withholding，不得入库失真内容；\n"
        f"  3. 累计净值/收益曲线若已写入，需回填后重算（recompute_summary_window.py）。\n\n"
        f"修复路径：\n"
        f"  1. 补采 ETF 收盘价（backfill_etf_price_history / 兜底巡检脚本）；\n"
        f"  2. 用历史收盘价补齐当日缺失的 ETF 快照行（切勿改动既有场外行）；\n"
        f"  3. recompute_summary_window.py --start-date 当日 --end-date 当日 --apply --rebuild-nav；\n"
        f"  4. 重跑 gen_combo_report.py 还原日报。\n\n"
        f"本告警由运行后哨兵（scripts/run_sentinel.py）自动发出。"
    )


def _send_alert(date_str: str, reason: str, dry_run: bool,
                subject: str = None, text: str = None,
                with_html: bool = True) -> tuple:
    """通过现有通道 send_generic_email.py 发告警；返回 (sent_bool, detail)。

    subject/text 可覆盖默认「运行账本缺失」文案（供快照探针复用同一通道，
    不新造 SMTP 逻辑——哨兵铁律）。
    """
    env = load_env_simple(os.path.join(ROOT, ".env"))
    enabled = (env.get("EMAIL_ENABLED", "false").lower() == "true")
    recipients = [r.strip() for r in env.get("EMAIL_RECIPIENTS", "").split(",") if r.strip()]
    username = env.get("EMAIL_USERNAME", "").strip()
    if not enabled or not recipients or not username:
        return False, ("邮件未启用/收件人未配置"
                       "（EMAIL_ENABLED/EMAIL_RECIPIENTS/EMAIL_USERNAME）")
    subject = subject or f"[CRITICAL] 投资组合夜线运行账本缺失 {date_str}（疑似硬死）"
    text = text or _build_text(date_str, reason)
    html = _build_html(date_str, reason) if with_html else None
    html_path = None
    if with_html:
        try:
            fd, html_path = tempfile.mkstemp(suffix=".html",
                                             prefix=f"sentinel_alert_{date_str}_")
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(html)
        except Exception as exc:
            _log(f"临时 HTML 生成失败({exc})，仅发文本")
            html_path = None
    cmd = [sys.executable, os.path.join(ROOT, "scripts", "send_generic_email.py"),
           "--to", ",".join(recipients),
           "--subject", subject,
           "--body", text,
           "--from-name", "投资组合运行哨兵"]
    if html_path:
        cmd += ["--html-file", html_path]
    if dry_run:
        cmd.append("--dry-run")
    try:
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        out = (res.stdout or "") + (res.stderr or "")
        for line in out.splitlines():
            if line.strip():
                _log(f"[send_generic] {line}")
        if res.returncode == 0:
            return True, ("邮件已发送" if not dry_run else "dry-run 已模拟发送")
        return False, f"send_generic_email rc={res.returncode}: {out[-500:]}"
    except Exception as exc:
        return False, f"调用 send_generic_email 失败: {exc}"
    finally:
        if html_path and os.path.exists(html_path):
            try:
                os.remove(html_path)
            except OSError:
                pass


def main() -> int:
    parser = argparse.ArgumentParser(description="运行后哨兵：检测当日 run_report 是否缺失")
    parser.add_argument("--date", default=None,
                        help="业务日期 YYYY-MM-DD（默认今天；夜线当日运行用今天）")
    parser.add_argument("--dry-run", action="store_true",
                        help="检测 + 记账 + 日志，但不实际发邮件（转交 send_generic_email --dry-run）")
    parser.add_argument("--no-email", action="store_true",
                        help="完全跳过发信（仅记账 + 日志），用于本地联调")
    parser.add_argument("--no-calendar-skip", action="store_true",
                        help="即使非交易日也强制检测（调试用）")
    parser.add_argument("--grace-minutes", type=int, default=DEFAULT_GRACE_MIN,
                        help=f"最早可检查时刻 = 夜线触发时刻 + N 分钟（默认 {DEFAULT_GRACE_MIN}，"
                             f"即 15:30；仅拦截夜线未触发的手动过早运行）")
    parser.add_argument("--reports-dir", default=None,
                        help="覆盖 reports 目录（默认 <root>/data/reports）")
    args = parser.parse_args()

    date_str = args.date or datetime.date.today().strftime("%Y-%m-%d")
    _log(f"运行后哨兵启动: business_date={date_str} dry_run={args.dry_run} no_email={args.no_email}")

    try:
        bdate = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        _log(f"日期格式错误: {date_str}（应为 YYYY-MM-DD）")
        return 3

    reports_dir = args.reports_dir or os.path.join(ROOT, "data", "reports")
    report_path = os.path.join(reports_dir, f"run_report_{date_str}.json")

    # 1) 日历守卫：非交易日直接跳过，绝不误报
    if not args.no_calendar_skip and not is_trading_day_safe(bdate):
        _log(f"{date_str} 非交易日，跳过检测（不误报）")
        _ledger("sentinel_skip_nontrading", "ok", f"{date_str} 非交易日，跳过")
        return 0

    # 2) 过早守卫：早于"夜线预期完成时刻"则跳过，防手动过早触发误报
    expected = (datetime.datetime.combine(bdate, datetime.time(NIGHTLY_TRIGGER_HOUR, NIGHTLY_TRIGGER_MIN))
                + datetime.timedelta(minutes=args.grace_minutes))
    now = datetime.datetime.now()
    if now < expected:
        _log(f"当前 {now:%H:%M:%S} 早于预期完成时间 {expected:%H:%M:%S}，"
             f"跳过（防夜线仍运行误报）")
        _ledger("sentinel_skip_too_early", "ok", f"{date_str} 触发过早(早于{expected:%H:%M:%S})")
        return 0

    # 3) 进程守卫（best-effort）：夜线仍在运行则跳过
    if _nightly_still_running():
        _log("检测到 run_analysis.py 仍在运行，跳过（夜线未结束，稍后重试）")
        _ledger("sentinel_skip_running", "ok", f"{date_str} 夜线仍在运行")
        return 0

    flag = _load_flag()

    # 4) 报告检测
    if not os.path.exists(report_path):
        _log(f"[ALERT] 缺失: {report_path}")
        reason = (f"当日运行账本 run_report_{date_str}.json 缺失。"
                  f"根因推断：夜线在写出运行账本前被外部静默终止"
                  f"（进程硬死/被强杀/断电/机器休眠），未留任何失败告警"
                  f"（与 2026-10-09 事故同形态）。")
        if _already_alerted(flag, date_str):
            _log("本日已告警过（24h 内），跳过重复发送；仅补充账本")
            _ledger("sentinel_run_report_missing", "failed", reason)
            return 2
        sent, detail = (False, "dry-run") if args.no_email else _send_alert(date_str, reason, args.dry_run)
        _log(f"告警发送: sent={sent} {detail}")
        _ledger("sentinel_run_report_missing", "failed", reason + f" | 邮件:{detail}")
        flag[date_str] = {"ts": now.strftime("%Y-%m-%d %H:%M:%S"),
                          "status": "missing", "detail": detail}
        _save_flag(flag)
        return 2

    # 存在：校验可解析性与 run_status
    try:
        with open(report_path, "r", encoding="utf-8") as f:
            rep = json.load(f)
        if not isinstance(rep, dict):
            raise ValueError("非 JSON 对象")
        run_status = rep.get("run_status")
    except Exception as exc:
        _log(f"[ALERT] 报告存在但无法解析: {exc}")
        reason = f"run_report_{date_str}.json 存在但损坏/无法解析（{exc}）。"
        if not _already_alerted(flag, date_str):
            sent, detail = (False, "dry-run") if args.no_email else _send_alert(date_str, reason, args.dry_run)
            _log(f"告警发送: sent={sent} {detail}")
            flag[date_str] = {"ts": now.strftime("%Y-%m-%d %H:%M:%S"),
                              "status": "missing", "detail": detail}
            _save_flag(flag)
        _ledger("sentinel_run_report_corrupt", "failed", reason)
        return 2

    # 3.5) 快照完整性探针（P0#2，2026-10-27 落地）
    # 位置刻意放在「报告存在」之后：报告缺失已单独告警（更根本），此处专抓
    # 「run_report 正常但快照塌缩」这一哨兵历史盲区，且避免同夜双封告警。
    # 探针自身任何异常都不改变原判定（原哨兵职责不可被新功能削弱）。
    snap_bad = False
    try:
        # db_path 覆盖仅供离线验证（造塌缩库）；生产不设该环境变量 ⇒ 走生产库。
        probe = _snapshot_probe(date_str, os.environ.get("SENTINEL_DB_PATH") or None)
    except Exception as exc:
        _log(f"快照探针异常（不影响主判定）: {exc}")
        probe = None
    if probe:
        _log(f"快照完整性 {probe['detail']} verdict={probe['verdict']} "
             f"(floor={SNAP_MIN_ROWS}, ratio={SNAP_MEDIAN_RATIO:.0%}, "
             f"baseline_n={probe['baseline_dates']})")
        # 🔴 非交易日豁免：周末/节假日**本就不写快照**（实测生产库 2026-10-25 周日 = 0 行），
        #   此时 rows=0 是正常状态而非故障。不加此豁免，任何非交易日的手动/调试运行
        #   （--no-calendar-skip）都会误报 CRITICAL，几次之后这条告警就会被无视——
        #   告警疲劳是守卫失效最常见的方式（承接 2026-10 审阅结论）。
        _nontrading_zero = (probe["verdict"] == "missing"
                            and not is_trading_day_safe(bdate))
        if _nontrading_zero:
            _log(f"{date_str} 非交易日且当日无快照行 ⇒ 属正常（休市不写快照），不告警")
        if probe["verdict"] in ("missing", "collapsed") and not _nontrading_zero:
            snap_bad = True
            _snap_reason = (f"当日快照 {probe['rows']} 行 / 近 {probe['baseline_dates']} 个快照日"
                            f"中位数 {probe['median']} 行 ⇒ 组合合计市值与当日回报率必然失真。")
            _snap_key = date_str + ":snap"
            if _already_alerted(flag, _snap_key):
                _log("本日快照告警 24h 内已发过，跳过重复发送；仅补账本")
            else:
                sent, detail = ((False, "dry-run") if args.no_email
                                else _send_alert(
                                    date_str, _snap_reason, args.dry_run,
                                    subject=(f"[CRITICAL] 组合快照不完整 {date_str}"
                                             f"（SNAP_ROWS={probe['rows']}/{probe['median']}）"),
                                    text=_build_snap_text(probe), with_html=False))
                _log(f"快照告警发送: sent={sent} {detail}")
                flag[_snap_key] = {"ts": now.strftime("%Y-%m-%d %H:%M:%S"),
                                   "status": "missing", "detail": detail}
                _save_flag(flag)
            _ledger("sentinel_snapshot_degraded", "degraded",
                    f"SNAP_ROWS={probe['rows']}/{probe['median']} "
                    f"(baseline_n={probe['baseline_dates']}) | {_snap_reason}")
        elif probe["verdict"] == "no_baseline":
            _log("快照完整性：无中位数基线可比，放行（首个可对照日）")
        else:
            _log("快照完整性正常")
            # 该日曾因快照塌缩告警过、本次已恢复 ⇒ 清标记，便于未来真异常能再报
            _snap_key = date_str + ":snap"
            if _snap_key in flag:
                del flag[_snap_key]
                _save_flag(flag)

    # 存在且可解析
    if run_status in _BLOCKING_STATUS:
        _log(f"报告存在但 run_status={run_status}（日报闸门已拒发 + daily_report_not_sent "
             f"账本已覆盖，本哨兵不重复告警）")
        _ledger("sentinel_run_report_blocked", "ok", f"run_status={run_status}")
        if date_str in flag:  # 若该日曾误报，重置以便未来真缺失能再报
            del flag[date_str]
            _save_flag(flag)
        return 2 if snap_bad else 0

    # ok / 未知（缺字段）
    if snap_bad:
        # 快照塌缩时不得再记 sentinel_ok：账本同时出现 degraded 与 ok 会让「本夜是否正常」
        # 这个问题在事后审计时无解（两条记录互相矛盾，谁后写谁覆盖判断）。
        _log(f"报告存在且状态正常（run_status={run_status or '未知/缺字段'}），"
             f"但快照行数塌缩 ⇒ 本夜判定为异常（见 sentinel_snapshot_degraded）")
    else:
        _log(f"报告存在且状态正常（run_status={run_status or '未知/缺字段'}），夜线已正常收尾")
        _ledger("sentinel_ok", "ok", f"run_status={run_status}")
    if date_str in flag:
        del flag[date_str]
        _save_flag(flag)
    return 2 if snap_bad else 0


if __name__ == "__main__":
    sys.exit(main())
