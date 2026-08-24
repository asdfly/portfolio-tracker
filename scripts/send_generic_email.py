#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
通用 SMTP 邮件发送工具（独立、免确认）
================================================

用途：
    作为「组合系统收盘日报」与「WorkBuddy 自动化（如元UP 2026款跟踪周报）」共用的
    发信通道。读取项目根目录 .env 的 EMAIL_* 配置，用 smtplib 直连 QQ 邮箱发送，
    不经过任何需要人工二次确认的外部连接器。

与 scripts/send_report_email.py 的关系（共用通道、互不耦合）：
    - send_report_email.py 是组合系统收盘日报的【专用】推送脚本（含时效守卫、报告重生），
      仍由 scheduled_run.bat 调用，行为保持不变。
    - 本脚本是【纯发信工具】：只负责把传入的 主题 / 正文 / 附件 发出去，
      不关心报告怎么生成、不 import 组合系统任何业务模块。
      两者都从同一个 .env 读取 SMTP 凭据，因此共用同一个发信账户 / 通道，
      但代码互相独立——任一改动不会影响另一个。

.env 需要的变量（与 config.NOTIFICATION_CONFIG['email'] 一致）：
    EMAIL_SMTP_SERVER  (默认 smtp.qq.com)
    EMAIL_SMTP_PORT    (默认 587；填 465 则走 SSL)
    EMAIL_USERNAME     (发件人，如 asdfl@qq.com)
    EMAIL_PASSWORD     (QQ 邮箱授权码)

用法：
    python send_generic_email.py \
        --to asdfl@qq.com \
        --subject "主题" \
        --body "纯文本正文" \
        [--html-file report.html]        # 可选 HTML 正文（与 --body 并存，客户端择优显示）
        [--attach a.html,b.md]           # 可选附件（逗号分隔路径）
        [--from-name "WorkBuddy 选车专题"] # 可选发件人显示名
        [--dry-run]                      # 只打印待发信息，不实际连接发送

退出码：0 成功 / 1 发送失败 / 2 配置缺失。
"""
from __future__ import annotations

import argparse
import os
import smtplib
import sys
from email.header import Header
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr
from pathlib import Path


def load_env(env_path: Path) -> None:
    """解析 .env（KEY=VALUE，忽略注释/空行，不覆盖已有环境变量）。

    与 config/settings.py 的 _load_env_file 逻辑一致，但本脚本【独立实现】，
    不 import 组合系统业务配置，避免相互耦合。
    """
    if not env_path.exists():
        return
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, _, value = line.partition("=")
                key, value = key.strip(), value.strip().strip("'\"")
                if key and key not in os.environ:
                    os.environ[key] = value


def main() -> int:
    parser = argparse.ArgumentParser(description="通用 SMTP 邮件发送工具（免确认）")
    parser.add_argument("--to", required=True, help="收件人，多个用逗号分隔")
    parser.add_argument("--subject", required=True, help="邮件主题")
    parser.add_argument("--body", default="", help="纯文本正文")
    parser.add_argument("--html-file", default=None, help="HTML 正文文件路径（可选）")
    parser.add_argument("--attach", default=None, help="附件路径，逗号分隔（可选）")
    parser.add_argument("--from-name", default="WorkBuddy 选车专题", help="发件人显示名")
    parser.add_argument("--dry-run", action="store_true", help="只打印，不实际发送")
    args = parser.parse_args()

    # 解析项目根 .env（scripts/ 的父目录 = 项目根）
    script_dir = Path(__file__).resolve().parent
    root = script_dir.parent
    load_env(root / ".env")

    server = (os.environ.get("EMAIL_SMTP_SERVER") or "smtp.qq.com").strip()
    try:
        port = int(os.environ.get("EMAIL_SMTP_PORT") or "587")
    except ValueError:
        port = 587
    username = (os.environ.get("EMAIL_USERNAME") or "").strip()
    password = (os.environ.get("EMAIL_PASSWORD") or "").strip()
    recipients = [r.strip() for r in args.to.split(",") if r.strip()]

    if not username or not password or not recipients:
        print("[SEND_GENERIC] 配置缺失（EMAIL_USERNAME/EMAIL_PASSWORD/--to），无法发送",
              file=sys.stderr)
        return 2

    # 组装邮件
    msg = MIMEMultipart("mixed")
    msg["From"] = formataddr((str(Header(args.from_name, "utf-8")), username))
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = Header(args.subject, "utf-8")

    alt = MIMEMultipart("alternative")
    alt.attach(MIMEText(args.body or "（无正文）", "plain", "utf-8"))
    if args.html_file and os.path.exists(args.html_file):
        with open(args.html_file, "r", encoding="utf-8") as f:
            alt.attach(MIMEText(f.read(), "html", "utf-8"))
    msg.attach(alt)

    attach_names = []
    if args.attach:
        for p in args.attach.split(","):
            p = p.strip()
            if p and os.path.exists(p):
                with open(p, "rb") as f:
                    part = MIMEApplication(f.read(), Name=os.path.basename(p))
                part["Content-Disposition"] = f'attachment; filename="{os.path.basename(p)}"'
                msg.attach(part)
                attach_names.append(os.path.basename(p))

    if args.dry_run:
        html_ok = bool(args.html_file and os.path.exists(args.html_file))
        print(f"[SEND_GENERIC][DRY-RUN] server={server}:{port} from={username} to={recipients}")
        print(f"[SEND_GENERIC][DRY-RUN] subject={args.subject}")
        print(f"[SEND_GENERIC][DRY-RUN] body_len={len(args.body)} html={'yes' if html_ok else 'no'} "
              f"attach={attach_names}")
        return 0

    try:
        if port == 465:
            smtp = smtplib.SMTP_SSL(server, port, timeout=30)
        else:
            smtp = smtplib.SMTP(server, port, timeout=30)
            smtp.starttls()
        smtp.login(username, password)
        smtp.sendmail(username, recipients, msg.as_string())
        smtp.quit()
        print(f"[SEND_GENERIC] 已发送 -> {', '.join(recipients)} (subject={args.subject})")
        return 0
    except Exception as exc:
        print(f"[SEND_GENERIC] 发送失败: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
