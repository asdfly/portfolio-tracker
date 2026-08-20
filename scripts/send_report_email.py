#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""收盘日报邮件推送（挂接到定时链，复用现有 15:30 触发）。

读取 .env 的 SMTP 配置（config.NOTIFICATION_CONFIG['email']），
找到最新 enhanced_report HTML + smart_report MD，发送带 HTML 正文的邮件，
并把两份报告作为附件。未启用 (EMAIL_ENABLED != true) 时静默跳过。

时效守卫(P1 修复): 若取到的最新 enhanced_report 不是"今日"，说明 run_analysis.py
当日未成功产出今日报告(被 watchdog 截断/异常)，则现场调用 EnhancedReportBuilder
重生今日报告再发，杜绝"旧 HTML + 新摘要"的日期错配推送。重生失败则拒绝发送并
大声报错(返回 1)，绝不再发错日期的 stale 报告。

免确认：走项目内 SMTP（config.NOTIFICATION_CONFIG），不经过任何需要
人工二次确认的外部连接器。
"""
from __future__ import annotations

import datetime
import glob
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


def find_latest(pattern: str):
    """在 data/reports 下按文件名排序取最新一份。"""
    files = glob.glob(os.path.join(ROOT, "data", "reports", pattern))
    if not files:
        return None
    files.sort(key=lambda p: os.path.basename(p))
    return files[-1]


def build_summary(md_path: str) -> str:
    """取 smart_report 前若干行作为纯文本摘要（客户端不支持 HTML 时的兜底）。"""
    if not md_path or not os.path.exists(md_path):
        return "（无文字摘要）"
    try:
        with open(md_path, "r", encoding="utf-8") as f:
            lines = [ln.rstrip() for ln in f if ln.strip()]
        return "\n".join(lines[:40])
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
      - 文字摘要(smart_report)若与报告日期不一致, 不附带过期摘要(md_path=None)。
    """
    html_path = find_latest("enhanced_report_*.html")
    md_path = find_latest("smart_report_*.md")
    if not html_path:
        print(f"[{datetime.datetime.now():%Y-%m-%d %H:%M:%S}] [EMAIL] 未找到 enhanced_report_*.html，尝试现场重生今日报告")
        html_path = None

    report_date = today_str
    if html_path:
        base = os.path.basename(html_path)
        date_str = base.replace("enhanced_report_", "").replace(".html", "")
        try:
            found_date = datetime.datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")
        except Exception:
            found_date = date_str

        if found_date == today_str:
            # 正常: 今日报告已存在, 直接使用
            print(f"[{datetime.datetime.now():%Y-%m-%d %H:%M:%S}] [EMAIL] 命中今日报告: {base}")
        else:
            # stale: 当日报告缺失/过期, 现场重生
            print(f"[{datetime.datetime.now():%Y-%m-%d %H:%M:%S}] [EMAIL] 检测到报告日期 {found_date} 非今日 {today_str}，"
                  f"尝试现场重生今日报告(避免错配推送)")
            html_path = None
    else:
        found_date = None

    if html_path is None:
        # 现场重生今日报告
        try:
            from config.settings import DATABASE_PATH
            from src.utils.enhanced_report import EnhancedReportBuilder
            builder = EnhancedReportBuilder(str(DATABASE_PATH))
            fresh_html = builder.build_full_report(news_data=None)
            stamp = today_str.replace("-", "")
            fresh_name = f"enhanced_report_{stamp}.html"
            saved = builder.save_report(fresh_html, fresh_name, news_data=None)
            html_path = saved
            report_date = today_str
            print(f"[{datetime.datetime.now():%Y-%m-%d %H:%M:%S}] [EMAIL] 已重生今日报告: {os.path.basename(saved)}")
        except Exception as exc:
            # 重生失败: 绝不发送错日期的旧报告, 大声报错并拒绝
            stale_base = "N/A" if found_date is None else f"enhanced_report_{found_date.replace('-','')}.html"
            print(f"[{datetime.datetime.now():%Y-%m-%d %H:%M:%S}] [EMAIL][CRITICAL] 今日报告重生失败({exc})，"
                  f"拒绝发送 stale 报告 {stale_base}")
            return None

    # 文字摘要若与报告日期不一致(来自不同日), 不附带过期摘要
    if md_path:
        mbase = os.path.basename(md_path).replace("smart_report_", "").replace(".md", "")
        try:
            mdate = datetime.datetime.strptime(mbase, "%Y%m%d").strftime("%Y-%m-%d")
        except Exception:
            mdate = mbase
        if mdate != report_date:
            print(f"[{datetime.datetime.now():%Y-%m-%d %H:%M:%S}] [EMAIL] 文字摘要日期 {mdate} 与报告日期 {report_date} 不一致, 不附带过期摘要")
            md_path = None

    return {"html_path": html_path, "md_path": md_path, "report_date": report_date}


def main() -> int:
    cfg = NOTIFICATION_CONFIG.get("email", {})
    if not cfg.get("enabled"):
        print(f"[{datetime.datetime.now():%Y-%m-%d %H:%M:%S}] [EMAIL] 未启用 (EMAIL_ENABLED != true)，跳过推送")
        return 0

    today_str = datetime.datetime.now().strftime("%Y-%m-%d")

    resolved = _resolve_report(today_str)
    if resolved is None:
        # stale 且重生失败: 拒绝发送, 由操作员介入(调度日志可见失败)
        return 1

    html_path = resolved["html_path"]
    md_path = resolved["md_path"]
    report_date = resolved["report_date"]

    username = (cfg.get("username") or "").strip()
    recipients = [r.strip() for r in (cfg.get("recipients") or []) if r.strip()]
    if not username or not recipients:
        print(f"[{datetime.datetime.now():%Y-%m-%d %H:%M:%S}] [EMAIL] 发件人/收件人未配置，跳过")
        return 0
    if not (cfg.get("password") or "").strip():
        print(f"[{datetime.datetime.now():%Y-%m-%d %H:%M:%S}] [EMAIL] 授权码(EMAIL_PASSWORD)未填，跳过推送")
        return 0

    with open(html_path, "r", encoding="utf-8") as f:
        html_body = f.read()
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
        print(
            f"[{datetime.datetime.now():%Y-%m-%d %H:%M:%S}] [EMAIL] 已推送日报 {report_date} "
            f"至 {', '.join(recipients)}（附件: {attach_note}）"
        )
        return 0
    except Exception as exc:
        print(f"[{datetime.datetime.now():%Y-%m-%d %H:%M:%S}] [EMAIL] 发送失败: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
