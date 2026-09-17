#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""收盘日报邮件推送（挂接到定时链，复用现有 15:30 触发）。

读取 .env 的 SMTP 配置（config.NOTIFICATION_CONFIG['email']），
找到最新 enhanced_report HTML + smart_report MD，发送带 HTML 正文的邮件，
并把两份报告作为附件。未启用 (EMAIL_ENABLED != true) 时静默跳过。

数据就绪闸门（2026-09-16 P0 修复）:
  2026-09-15 15:30 管线在阶段一崩溃(rc=1)，portfolio_summary 未能写入 09-15
  （停在 09-14），但本脚本的 stale 守卫仍然"现场重生"了一份今日报告并推送成功。
  那份报告的页头指标逐项等于 09-14、持仓表却是 09-15 快照，两者相差 6,735 元，
  而页头日期用的是 datetime.now() —— 用户肉眼完全看不出这是拼接报告。
  因此现在在【重生之前】先过数据就绪闸门：
    a) portfolio_summary 的数据日期必须 == 今天，否则不重生、不发送、rc≠0；
    b) data/reports/run_report_<今天>.json 的 run_status 若为 partial/failed，
       同样拒绝（旧文件没有该字段时视为"未知"，不因此拒绝）；
    c) 正文若被 EnhancedReportBuilder 标记为降级(DEGRADED_MARKER)，一律拒绝发送。
  原则：**宁可当天不发，也不要发错日期的报告。**
  注意：管线失败时的 [ERROR] 投资组合告警邮件由 run_analysis.py 经
  src/utils/notification.py:send_alert 独立发出，与本脚本无耦合，不受上述改动影响。

监控账本（2026-09-16 裁定 3 取证后补）:
  上面那条 [ERROR] 告警覆盖不到"进程被硬杀"：src/data_sources/collect_core.py:167 的
  看门狗用 os._exit(1) 强退，except 与 finally 都不执行 ⇒ 既没有失败告警、
  portfolio_summary 又停在昨日 ⇒ 本脚本拒绝发送 ⇒ 用户当天收不到任何信号。
  这是唯一残留的静默窗口。按裁定"不新增邮件路径"，改为在每次"日报没送达"时
  往 execution_logs 监控账本写一条 daily_report_not_sent，让静默留下痕迹。

时效守卫(P1 修复): 若取到的最新 enhanced_report 不是"今日"，且数据就绪闸门已放行，
则现场调用 EnhancedReportBuilder 重生今日报告再发，杜绝"旧 HTML + 新摘要"的日期错配推送。

免确认：走项目内 SMTP（config.NOTIFICATION_CONFIG），不经过任何需要
人工二次确认的外部连接器。
"""
from __future__ import annotations

import datetime
import glob
import json
import os
import smtplib
import sys

from email.header import Header
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr

# 项目根目录（脚本位于 portfolio_tracker/scripts/）
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from config.settings import NOTIFICATION_CONFIG  # noqa: E402

# 报告若被降级标记（数据源日期不一致），邮件一律不发送。
from src.utils.enhanced_report import DEGRADED_MARKER  # noqa: E402

# 收盘时刻：smart_report 生成时间早于该时刻的视为盘中稿，不得当作收盘稿发送。
CLOSE_HOUR = 15

# run_status 取值（与 src/data_sources/collect_core.py 的 RUN_STATUS_* 常量同义）。
# "degraded"（#116 契约3）：阶段齐全但本次运行产出过 error 级告警
# （如快照基线闸门拒绝写 portfolio_summary、场外当日无净值）。
# 这种运行"跑完了但结果不可信"，日报必须和 partial/failed 一样拒发 ——
# 09-16 的 933,195.10（少 38.1%）正是以 run_status="ok" 发出去的。
_RUN_STATUS_BLOCKING = ("partial", "failed", "degraded")

# 监控账本任务名：日报"没送达用户"（被闸门拒绝 / 正文降级 / 与数据日期不符 / SMTP 失败）
# 时写入 execution_logs。与 portfolio_daily_analysis 同表，便于按时间对齐排查。
_NOT_SENT_LEDGER_TASK = "daily_report_not_sent"


def _log(msg: str) -> None:
    """统一日志行格式（stdout 会被 scheduled_run.bat 重定向到 logs/scheduled_run.log）。"""
    print(f"[{datetime.datetime.now():%Y-%m-%d %H:%M:%S}] [EMAIL] {msg}")


def _record_not_sent(reason: str) -> None:
    """把"今日日报没有送达用户"记进监控账本(execution_logs)。

    为什么需要（裁定 3）：管线失败时的 [ERROR] 告警邮件由 run_analysis.py 的顶层
    except 发出，而 os._exit 强退（看门狗超时 / 断电 / 任务计划程序终止）不会执行
    except 与 finally ⇒ 无告警 + summary 停更 ⇒ 本脚本静默拒发。本函数让这种"完全
    静默"在账本里至少留下一条记录，供次日巡检发现。

    裁定 3 明确要求**不新增邮件路径**：这里只记账，绝不发信。
    只读承诺的例外说明：_ro_connect 保证不写业务表；execution_logs 是运维账本，
    是本脚本唯一的写入目标（且 best-effort，失败只告警，绝不影响拒绝判定）。
    """
    try:
        from config.settings import DATABASE_PATH
        from src.utils.monitor import Monitor

        Monitor(str(DATABASE_PATH), {}).log_execution(
            _NOT_SENT_LEDGER_TASK, "failed", reason)
    except Exception as exc:  # 账本不可用不得改变"拒发"这个结论
        _log(f"监控账本记录失败(不影响拒绝判定): {exc}")


def _reports_dir() -> str:
    return os.path.join(ROOT, "data", "reports")


def read_run_status(today_str: str, reports_dir=None):
    """读取 data/reports/run_report_<today>.json 的 run_status。

    兼容性（硬要求）：旧报告在 2026-09-15 及以前没有 run_status 字段，此时
    返回 None 表示"未知"，调用方**不得**因此拒绝发送；文件缺失/损坏同样返回 None。
    """
    path = os.path.join(reports_dir or _reports_dir(), f"run_report_{today_str}.json")
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            report = json.load(f)
    except (OSError, ValueError) as exc:
        _log(f"run_report 读取失败({exc})，run_status 视为未知")
        return None
    if not isinstance(report, dict):
        return None
    value = report.get("run_status")
    if value is None:
        return None
    return str(value).strip().lower()


def _ro_connect(db_path):
    """以只读模式打开 SQLite（file: URI + mode=ro）—— 业务表一律只读。

    注：唯一的写入目标是运维账本 execution_logs（见 _record_not_sent），
    它不经本函数、也不改任何业务表。
    """
    import sqlite3

    from pathlib import Path as _Path
    uri = _Path(os.path.abspath(str(db_path))).as_uri() + "?mode=ro"
    return sqlite3.connect(uri, uri=True)


def data_readiness_gate(db_path, today_str: str, reports_dir=None):
    """数据就绪闸门：判断今天的数据是否足以生成并推送收盘日报。

    Returns:
        (ok: bool, reason: str, summary_date: str | None)

    判定项：
      1) portfolio_summary 的 MAX(date) 必须等于 today_str；
      2) run_report_<today>.json 的 run_status 不得为 partial/failed
         （缺字段/缺文件 = 未知，放行）。
    """
    import sqlite3

    if not os.path.exists(str(db_path)):
        return False, f"数据库文件不存在: {db_path}", None

    try:
        conn = _ro_connect(db_path)
    except (sqlite3.Error, ValueError) as exc:
        return False, f"只读打开数据库失败({exc})，无法确认数据就绪", None

    summary_date = None
    try:
        cur = conn.cursor()
        cur.execute("SELECT MAX(date) FROM portfolio_summary")
        row = cur.fetchone()
        summary_date = row[0] if row else None
    except sqlite3.Error as exc:
        return False, f"读取 portfolio_summary 失败({exc})，无法确认数据就绪", None
    finally:
        conn.close()

    if summary_date is None:
        return False, "portfolio_summary 无任何数据行", None
    if str(summary_date)[:10] != today_str:
        return False, (f"portfolio_summary 最新数据日期 {summary_date} != 今天 {today_str}"
                       f"（本次管线未成功写入今日汇总）"), summary_date

    run_status = read_run_status(today_str, reports_dir)
    if run_status in _RUN_STATUS_BLOCKING:
        return False, f"run_report_{today_str}.json 的 run_status={run_status}（本次运行不完整）", summary_date

    return True, f"数据就绪（summary={summary_date}, run_status={run_status or '未知/缺字段'}）", summary_date


def validate_summary_md(md_path, report_date: str):
    """校验 smart_report md 能否作为 report_date 的报告摘要/附件。

    Returns:
        (ok: bool, reason: str)

    校验项：
      1) 文件名日期 == 报告日期（原实现只比对文件名日期，但仅用于"是否附带"，
         且没有覆盖到"同名不同日的盘中稿"场景）；
      2) 正文 `**生成时间**` 的日期 == 报告日期（防改名/串档）；
      3) 正文生成时间不得早于收盘时刻 —— 否则是盘中稿，不能当收盘稿附上。
         无法解析生成时间时跳过第 3 项（不因解析失败而拒绝）。
    """
    if not md_path:
        return False, "无 smart_report 摘要文件"

    mbase = os.path.basename(md_path).replace("smart_report_", "").replace(".md", "")
    try:
        mdate = datetime.datetime.strptime(mbase, "%Y%m%d").strftime("%Y-%m-%d")
    except ValueError:
        mdate = mbase
    if mdate != report_date:
        return False, f"摘要日期 {mdate} 与报告日期 {report_date} 不一致"

    try:
        with open(md_path, "r", encoding="utf-8") as f:
            head = f.read(2000)
    except OSError as exc:
        return False, f"摘要读取失败({exc})"

    import re
    m = re.search(r"\*\*生成时间\*\*:\s*(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})", head)
    if not m:
        return True, "生成时间不可解析，仅按文件名日期校验通过"
    gen_date = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    gen_hour = int(m.group(4))
    if gen_date != report_date:
        return False, f"摘要正文生成时间 {gen_date} 与报告日期 {report_date} 不一致"
    if gen_hour < CLOSE_HOUR:
        return False, (f"摘要为盘中稿（生成时间 {gen_date} {m.group(4)}:{m.group(5)}"
                       f" 早于 {CLOSE_HOUR}:00 收盘时刻）")
    return True, "摘要日期与生成时段校验通过"


def find_latest(pattern: str):
    """在 data/reports 下按文件名排序取最新一份。"""
    files = glob.glob(os.path.join(_reports_dir(), pattern))
    if not files:
        return None
    files.sort(key=lambda p: os.path.basename(p))
    return files[-1]


def build_summary(md_path: str) -> str:
    """取 smart_report 全文作为纯文本正文（客户端不支持 HTML 时的兜底）。

    原仅取前 40 行，导致纯文本客户端只看到「执行摘要 + 第 1 条建议」，观感"内容太少"。
    改为全文，确保任何客户端都能看到完整建议、策略表现与市场分析。
    """
    if not md_path or not os.path.exists(md_path):
        return "（无文字摘要）"
    try:
        with open(md_path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception as exc:  # 兜底，绝不让推送因摘要读取失败而崩
        return f"（摘要读取失败：{exc}）"


def _resolve_report(today_str: str):
    """解析待发送的报告文件，处理时效守卫与现场重生。

    Returns:
        {"html_path", "md_path", "report_date"}  —— 可发送；
        None  —— 拒绝发送(stale 且重生失败)。

    守卫逻辑:
      - 若最新 enhanced_report 日期 == 今日: 直接使用(正常路径)。
      - 若 != 今日(缺失/过期): 现场调用 EnhancedReportBuilder 重生今日报告，
        写 enhanced_report_<today>.html + latest_report.html；重生失败则拒绝。
      - 文字摘要(smart_report)必须逐项通过 validate_summary_md（日期 == 报告日期、
        且非盘中稿），否则不附带(md_path=None)。

    前置条件：调用方必须先通过 data_readiness_gate —— 本函数不含数据就绪判定，
    不负责"该不该发"，只负责"发哪一份"。
    """
    html_path = find_latest("enhanced_report_*.html")
    md_path = find_latest("smart_report_*.md")
    if not html_path:
        _log("未找到 enhanced_report_*.html，尝试现场重生今日报告")
        html_path = None

    report_date = today_str
    if html_path:
        base = os.path.basename(html_path)
        date_str = base.replace("enhanced_report_", "").replace(".html", "")
        try:
            found_date = datetime.datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")
        except ValueError:
            found_date = date_str

        if found_date == today_str:
            # 正常: 今日报告已存在, 直接使用
            _log(f"命中今日报告: {base}")
        else:
            # stale: 当日报告缺失/过期, 现场重生
            _log(f"检测到报告日期 {found_date} 非今日 {today_str}，尝试现场重生今日报告(避免错配推送)")
            html_path = None
    else:
        found_date = None

    if html_path is None:
        # 现场重生今日报告
        try:
            from config.settings import DATABASE_PATH
            from src.utils.enhanced_report import EnhancedReportBuilder
            builder = EnhancedReportBuilder(str(DATABASE_PATH))
            # strict=True: 数据源日期不一致时直接抛错，绝不再产出拼接报告。
            fresh_html = builder.build_full_report(news_data=None, strict=True)
            stamp = today_str.replace("-", "")
            fresh_name = f"enhanced_report_{stamp}.html"
            saved = builder.save_report(fresh_html, fresh_name, news_data=None)
            html_path = saved
            report_date = today_str
            _log(f"已重生今日报告: {os.path.basename(saved)}")
        except Exception as exc:
            # 重生失败: 绝不发送错日期的旧报告, 大声报错并拒绝
            stale_base = "N/A" if found_date is None else f"enhanced_report_{found_date.replace('-','')}.html"
            _log(f"[CRITICAL] 今日报告重生失败({exc})，拒绝发送 stale 报告 {stale_base}")
            return None

    # 文字摘要必须是报告日期当天、且已过收盘时刻的收盘稿，否则不附带
    ok, reason = validate_summary_md(md_path, report_date)
    if not ok:
        _log(f"不附带文字摘要：{reason}")
        md_path = None

    return {"html_path": html_path, "md_path": md_path, "report_date": report_date}


def main() -> int:
    theme = "light"
    args = sys.argv[1:]
    if args:
        a0 = args[0].lower()
        if a0.startswith("--theme"):
            if "=" in a0:
                theme = a0.split("=", 1)[1]
            elif len(args) > 1:
                theme = args[1]
        elif a0 in ("--dark", "dark"):
            theme = "dark"
        elif a0 in ("--light", "light"):
            theme = "light"
    theme = theme.lower()

    cfg = NOTIFICATION_CONFIG.get("email", {})
    if not cfg.get("enabled"):
        _log("未启用 (EMAIL_ENABLED != true)，跳过推送")
        return 0

    today_str = datetime.datetime.now().strftime("%Y-%m-%d")

    # --- 数据就绪闸门：必须早于"现场重生"，否则会像 09-15 那样重生出一份拼接报告 ---
    from config.settings import DATABASE_PATH
    ok, reason, _summary_date = data_readiness_gate(str(DATABASE_PATH), today_str)
    if not ok:
        _log(f"[CRITICAL] 数据未就绪，拒绝生成/发送今日({today_str})日报：{reason}。"
             f"原则：宁可当天不发，也不要发错日期的报告。")
        _record_not_sent(f"数据未就绪，拒绝发送今日({today_str})日报：{reason}")
        return 1

    resolved = _resolve_report(today_str)
    if resolved is None:
        # stale 且重生失败: 拒绝发送, 由操作员介入(调度日志可见失败)
        _record_not_sent(f"报告与数据日期不符且现场重生失败，今日({today_str})日报未发送")
        return 1

    html_path = resolved["html_path"]
    md_path = resolved["md_path"]
    report_date = resolved["report_date"]

    username = (cfg.get("username") or "").strip()
    recipients = [r.strip() for r in (cfg.get("recipients") or []) if r.strip()]
    if not username or not recipients:
        _log("发件人/收件人未配置，跳过")
        return 0
    if not (cfg.get("password") or "").strip():
        _log("授权码(EMAIL_PASSWORD)未填，跳过推送")
        return 0

    # 邮件正文现场生成（默认浅色，贴合邮件客户端白底；--theme dark 可发深色版），不覆盖 dashboard 的 latest_report
    theme_label = "浅色" if theme == "light" else "深色"
    try:
        from src.utils.enhanced_report import EnhancedReportBuilder, ReportDataInconsistentError
        builder = EnhancedReportBuilder(str(DATABASE_PATH), theme=theme)
        # strict=True: 数据源日期不一致时抛错，而不是悄悄给出一份拼接报告
        html_body = builder.build_full_report(news_data=None, strict=True)
        # 邮件版用 email_report_ 前缀，避免干扰 find_latest("enhanced_report_*.html") 的日期解析
        mail_name = f"email_report_{report_date.replace('-', '')}_{theme}.html"
        from pathlib import Path as _Path
        mail_path = _Path(ROOT) / "data" / "reports" / mail_name
        mail_path.write_text(html_body, encoding="utf-8")
        html_path = str(mail_path)
        _log(f"已生成{theme_label}邮件正文: {mail_name}")
    except ReportDataInconsistentError as exc:
        _log(f"[CRITICAL] 报告数据日期不一致，拒绝发送：{exc}")
        _record_not_sent(f"报告数据日期不一致，今日({today_str})日报未发送：{exc}")
        return 1
    except Exception as exc:
        _log(f"{theme_label}正文生成失败({exc})，退回已有报告")
        with open(html_path, "r", encoding="utf-8") as f:
            html_body = f.read()

    # 兜底：无论正文来自现场生成还是退回的已有文件，只要带降级标记就一律不发。
    # 这堵死了"静默降级"这条路 —— 没有 [降级] 前缀的旁路，也没有直接发的旁路。
    if DEGRADED_MARKER in html_body:
        _log(f"[CRITICAL] 待发正文含降级标记({DEGRADED_MARKER})，数据不完整，拒绝发送 {report_date} 日报")
        _record_not_sent(f"待发正文含降级标记({DEGRADED_MARKER})，{report_date} 日报未发送")
        return 1

    text_body = build_summary(md_path)

    msg = MIMEMultipart("mixed")
    msg["From"] = formataddr((str(Header("投资组合分析系统", "utf-8")), username))
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = Header(f"投资组合智能分析报告 · {report_date}（收盘日报）", "utf-8")

    # 正文：纯文本兜底 + HTML 可视化（多部分备选，客户端择优显示）
    alt = MIMEMultipart("alternative")
    alt.attach(MIMEText(text_body, "plain", "utf-8"))
    alt.attach(MIMEText(html_body, "html", "utf-8"))
    msg.attach(alt)

    # 附件：HTML 可视化报告 + MD 文字摘要（满足"可视化报告作为附件"）
    for p in (html_path, md_path):
        if p and os.path.exists(p):
            with open(p, "rb") as f:
                part = MIMEApplication(f.read(), Name=os.path.basename(p))
            part["Content-Disposition"] = f'attachment; filename="{os.path.basename(p)}"'
            msg.attach(part)

    server = (cfg.get("smtp_server") or "smtp.qq.com").strip()
    port = int(cfg.get("smtp_port") or 587)
    try:
        if port == 465:
            smtp = smtplib.SMTP_SSL(server, port, timeout=30)
        else:
            smtp = smtplib.SMTP(server, port, timeout=30)
            smtp.starttls()
        smtp.login(username, cfg.get("password") or "")
        smtp.sendmail(username, recipients, msg.as_string())
        smtp.quit()
        attach_note = ", ".join(
            os.path.basename(p) for p in (html_path, md_path) if p and os.path.exists(p)
        )
        _log(
            f"已推送日报 {report_date} 至 {', '.join(recipients)}（附件: {attach_note}）"
        )
        return 0
    except Exception as exc:
        _log(f"发送失败: {exc}")
        _record_not_sent(f"SMTP 发送失败，{report_date} 日报未送达：{exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
