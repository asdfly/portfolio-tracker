#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
通知配置向导 - 交互式配置邮件/企业微信通知并测试发送

使用方法:
    python setup_notification.py              # 交互式配置
    python setup_notification.py --test       # 使用已有配置测试发送
    python setup_notification.py --report     # 生成HTML报告预览（不发送）
"""
import sys
import os
import json
import argparse
import smtplib
import requests
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import DATABASE_PATH
from src.utils.email_report import EmailReportBuilder


NOTIFICATION_CONFIG_PATH = PROJECT_ROOT / "config" / "notification.json"


def load_notification_config() -> dict:
    """加载通知配置"""
    if NOTIFICATION_CONFIG_PATH.exists():
        with open(NOTIFICATION_CONFIG_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {
        'email': {'enabled': False},
        'wechat': {'enabled': False}
    }


def save_notification_config(config: dict):
    """保存通知配置"""
    NOTIFICATION_CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    # 脱敏保存（密码单独存储提示）
    with open(NOTIFICATION_CONFIG_PATH, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=2, ensure_ascii=False)
    print(f"\n配置已保存到: {NOTIFICATION_CONFIG_PATH}")


def setup_email_interactive(config: dict) -> dict:
    """交互式配置邮件通知"""
    print("\n" + "=" * 50)
    print("  邮件通知配置")
    print("=" * 50)
    print("\n常用SMTP配置:")
    print("  QQ邮箱:     smtp.qq.com:465  (需开启授权码)")
    print("  163邮箱:    smtp.163.com:465  (需开启授权码)")
    print("  Gmail:      smtp.gmail.com:587 (需应用专用密码)")
    print("  Outlook:    smtp.office365.com:587")
    print()

    email_cfg = config.get('email', {})

    smtp_server = input(f"SMTP服务器 [{email_cfg.get('smtp_server', 'smtp.qq.com')}]: ").strip()
    smtp_server = smtp_server or email_cfg.get('smtp_server', 'smtp.qq.com')

    smtp_port_str = input(f"SMTP端口 [{email_cfg.get('smtp_port', 465)}]: ").strip()
    smtp_port = int(smtp_port_str) if smtp_port_str else email_cfg.get('smtp_port', 465)

    username = input(f"发件人邮箱 [{email_cfg.get('username', '')}]: ").strip()
    username = username or email_cfg.get('username', '')

    password = input("授权码/密码 (输入不会回显): ").strip()

    sender = input(f"发件人显示名 [{username}]: ").strip()
    sender = sender or username

    recipient_str = input(f"收件人邮箱 (多个用逗号分隔) [{','.join(email_cfg.get('recipients', []))}]: ").strip()
    recipients = [r.strip() for r in recipient_str.split(',') if r.strip()] if recipient_str else email_cfg.get('recipients', [])

    email_cfg = {
        'enabled': True,
        'smtp_server': smtp_server,
        'smtp_port': smtp_port,
        'username': username,
        'password': password,
        'sender': sender,
        'recipients': recipients
    }

    return email_cfg


def setup_wechat_interactive(config: dict) -> dict:
    """交互式配置企业微信通知"""
    print("\n" + "=" * 50)
    print("  企业微信机器人通知配置")
    print("=" * 50)
    print("\n获取Webhook URL:")
    print("  1. 企业微信群 -> 添加群机器人")
    print("  2. 复制Webhook地址: https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=...")
    print()

    wechat_cfg = config.get('wechat', {})
    webhook = input(f"Webhook URL [{wechat_cfg.get('webhook_url', '')}]: ").strip()
    webhook = webhook or wechat_cfg.get('webhook_url', '')

    wechat_cfg = {
        'enabled': bool(webhook),
        'webhook_url': webhook
    }

    return wechat_cfg


def test_email(email_cfg: dict):
    """测试邮件发送"""
    print("\n正在测试邮件发送...")

    if not email_cfg.get('username') or not email_cfg.get('password'):
        print("错误: 未配置邮箱账号或密码")
        return False

    if not email_cfg.get('recipients'):
        print("错误: 未配置收件人")
        return False

    try:
        # 生成测试报告
        builder = EmailReportBuilder(str(DATABASE_PATH))
        html = builder.build_daily_report()

        msg = MIMEMultipart('alternative')
        msg['Subject'] = f"[测试] 投资组合日报 - {datetime.now().strftime('%Y-%m-%d')}"
        msg['From'] = email_cfg.get('sender', email_cfg.get('username'))
        msg['To'] = ', '.join(email_cfg['recipients'])
        msg.attach(MIMEText(html, 'html', 'utf-8'))

        port = email_cfg.get('smtp_port', 465)
        if port == 465:
            server = smtplib.SMTP_SSL(email_cfg['smtp_server'], port, timeout=15)
        else:
            server = smtplib.SMTP(email_cfg['smtp_server'], port, timeout=15)
            server.starttls()

        server.login(email_cfg['username'], email_cfg['password'])
        server.send_message(msg)
        server.quit()

        print("邮件发送成功!")
        print(f"  收件人: {', '.join(email_cfg['recipients'])}")
        return True

    except smtplib.SMTPAuthenticationError:
        print("错误: SMTP认证失败，请检查邮箱账号和授权码")
        return False
    except smtplib.SMTPConnectError:
        print(f"错误: 无法连接到SMTP服务器 {email_cfg['smtp_server']}:{email_cfg.get('smtp_port')}")
        return False
    except Exception as e:
        print(f"错误: {e}")
        return False


def test_wechat(wechat_cfg: dict):
    """测试企业微信发送"""
    print("\n正在测试企业微信发送...")

    webhook_url = wechat_cfg.get('webhook_url')
    if not webhook_url:
        print("错误: 未配置Webhook URL")
        return False

    try:
        builder = EmailReportBuilder(str(DATABASE_PATH))
        summary = builder._load_summary()

        content = "📊 投资组合日报 (测试消息)\n\n"
        if summary:
            content += f"总市值: ¥{summary.get('total_value', 0):,.2f}\n"
            content += f"当日收益: {summary.get('daily_return', 0):+.2f}%\n"
            content += f"夏普比率: {summary.get('sharpe_ratio', 'N/A')}\n"
            content += f"最大回撤: {summary.get('max_drawdown', 'N/A')}%\n"
            content += f"波动率: {summary.get('volatility', 'N/A')}%\n"
        content += f"\n发送时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        content += "(这是一条测试消息，配置成功后每日定时推送)"

        payload = {
            "msgtype": "text",
            "text": {"content": content}
        }

        resp = requests.post(webhook_url, json=payload, timeout=10)
        result = resp.json()

        if result.get('errcode') == 0:
            print("企业微信消息发送成功!")
            return True
        else:
            print(f"错误: {result.get('errmsg', '未知错误')}")
            return False

    except requests.exceptions.ConnectionError:
        print("错误: 无法连接到企业微信API")
        return False
    except Exception as e:
        print(f"错误: {e}")
        return False


def generate_report_only():
    """仅生成HTML报告不发送"""
    print("\n正在生成HTML报告...")
    builder = EmailReportBuilder(str(DATABASE_PATH))
    html = builder.build_daily_report()
    filepath = builder.save_report(html)
    print(f"报告已保存: {filepath}")
    print(f"\n可以用浏览器打开查看效果: file:///{filepath.replace(os.sep, '/')}")


def apply_config_to_settings(config: dict):
    """将配置同步到settings.py"""
    settings_path = PROJECT_ROOT / "config" / "settings.py"

    with open(settings_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # 更新NOTIFICATION_CONFIG
    import re

    # 构建新的NOTIFICATION_CONFIG字符串
    email_cfg = config.get('email', {})
    wechat_cfg = config.get('wechat', {})

    new_email_username = email_cfg.get('username', 'your_email@qq.com')
    new_email_password = email_cfg.get('password', 'your_auth_code')
    new_email_sender = email_cfg.get('sender', new_email_username)
    new_email_recipients = email_cfg.get('recipients', ['recipient@example.com'])
    new_email_enabled = email_cfg.get('enabled', False)

    new_wechat_enabled = wechat_cfg.get('enabled', False)
    new_wechat_url = wechat_cfg.get('webhook_url', 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=YOUR_KEY')

    # 替换enabled
    content = re.sub(
        r"('email':\s*\{[^}]*?'enabled':\s*)(False|True)",
        f"'email': {{\n        'enabled': {new_email_enabled},",
        content
    )
    content = re.sub(
        r"('wechat':\s*\{[^}]*?'enabled':\s*)(False|True)",
        f"'wechat': {{\n        'enabled': {new_wechat_enabled},",
        content
    )

    with open(settings_path, 'w', encoding='utf-8') as f:
        f.write(content)

    print(f"settings.py 已更新")


def main():
    parser = argparse.ArgumentParser(description="通知配置向导")
    parser.add_argument('--test', action='store_true', help='使用已有配置测试发送')
    parser.add_argument('--report', action='store_true', help='仅生成HTML报告预览')
    parser.add_argument('--email', action='store_true', help='仅配置邮件')
    parser.add_argument('--wechat', action='store_true', help='仅配置企业微信')
    args = parser.parse_args()

    print("=" * 50)
    print("  投资组合跟踪系统 - 通知配置向导")
    print("=" * 50)

    if args.report:
        generate_report_only()
        return

    config = load_notification_config()

    if args.test:
        print("\n使用已有配置测试发送...")
        if config.get('email', {}).get('enabled'):
            test_email(config['email'])
        else:
            print("\n邮件通知未启用，跳过测试")

        if config.get('wechat', {}).get('enabled'):
            test_wechat(config['wechat'])
        else:
            print("企业微信通知未启用，跳过测试")
        return

    # 交互式配置
    if not args.wechat:
        want_email = input("\n是否配置邮件通知? (y/n) [y]: ").strip().lower()
        if want_email != 'n':
            config['email'] = setup_email_interactive(config)

            test_it = input("\n是否立即测试发送? (y/n) [y]: ").strip().lower()
            if test_it != 'n':
                test_email(config['email'])

    if not args.email:
        want_wechat = input("\n是否配置企业微信通知? (y/n) [y]: ").strip().lower()
        if want_wechat != 'n':
            config['wechat'] = setup_wechat_interactive(config)

            if config['wechat'].get('enabled'):
                test_it = input("\n是否立即测试发送? (y/n) [y]: ").strip().lower()
                if test_it != 'n':
                    test_wechat(config['wechat'])

    # 保存配置
    save_notification_config(config)

    # 同步到settings.py
    print("\n正在同步配置到settings.py...")
    apply_config_to_settings(config)

    print("\n配置完成!")
    print("每日定时任务执行时将自动发送通知。")


if __name__ == "__main__":
    main()