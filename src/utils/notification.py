"""
通知模块 - 支持邮件和企业微信通知
"""
import smtplib
import json
import requests
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from typing import List, Optional
import logging

logger = logging.getLogger(__name__)


class NotificationManager:
    """通知管理器"""

    def __init__(self, config: dict):
        self.config = config
        self.email_config = config.get('email', {})
        self.wechat_config = config.get('wechat', {})

    def send_portfolio_report(self, report_data: dict, recipients: List[str] = None):
        """发送投资组合报告"""
        subject = f"投资组合日报 - {datetime.now().strftime('%Y-%m-%d')}"

        # 构建邮件内容
        html_content = self._build_html_report(report_data)

        # 发送邮件
        if self.email_config.get('enabled', False):
            self._send_email(subject, html_content, recipients)

        # 发送企业微信
        if self.wechat_config.get('enabled', False):
            self._send_wechat(report_data)

    def send_alert(self, alert_type: str, message: str, level: str = "warning"):
        """发送告警通知"""
        subject = f"[{level.upper()}] 投资组合告警 - {alert_type}"

        html_content = f"""
        <html>
        <body style="font-family: Arial, sans-serif;">
            <h2 style="color: {'red' if level == 'error' else 'orange'};">⚠️ {alert_type}</h2>
            <p><strong>时间:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            <p><strong>级别:</strong> {level.upper()}</p>
            <p><strong>消息:</strong> {message}</p>
        </body>
        </html>
        """

        if self.email_config.get('enabled', False):
            self._send_email(subject, html_content)

        if self.wechat_config.get('enabled', False):
            self._send_wechat_alert(alert_type, message, level)

    def _build_html_report(self, data: dict) -> str:
        """构建HTML格式报告"""
        summary = data.get('summary', {})
        risk = data.get('risk', {})

        html = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .header {{ background: #1a73e8; color: white; padding: 20px; border-radius: 8px; }}
                .section {{ margin: 20px 0; padding: 15px; background: #f8f9fa; border-radius: 8px; }}
                .metric {{ display: inline-block; margin: 10px 20px; }}
                .positive {{ color: #28a745; }}
                .negative {{ color: #dc3545; }}
                table {{ width: 100%; border-collapse: collapse; margin: 10px 0; }}
                th, td {{ padding: 10px; text-align: left; border-bottom: 1px solid #ddd; }}
                th {{ background: #e9ecef; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>📊 投资组合日报</h1>
                <p>{datetime.now().strftime('%Y年%m月%d日')}</p>
            </div>

            <div class="section">
                <h2>💰 收益概览</h2>
                <div class="metric">
                    <strong>总市值:</strong> ¥{summary.get('total_value', 0):,.2f}
                </div>
                <div class="metric">
                    <strong>当日盈亏:</strong> 
                    <span class="{'positive' if summary.get('daily_pnl', 0) >= 0 else 'negative'}">
                        ¥{summary.get('daily_pnl', 0):,.2f} ({summary.get('daily_return', 0):.2f}%)
                    </span>
                </div>
                <div class="metric">
                    <strong>累计盈亏:</strong> 
                    <span class="{'positive' if summary.get('total_pnl', 0) >= 0 else 'negative'}">
                        ¥{summary.get('total_pnl', 0):,.2f}
                    </span>
                </div>
            </div>

            <div class="section">
                <h2>⚠️ 风险指标</h2>
                <div class="metric"><strong>夏普比率:</strong> {risk.get('sharpe_ratio', 'N/A')}</div>
                <div class="metric"><strong>最大回撤:</strong> {risk.get('max_drawdown', 'N/A')}%</div>
                <div class="metric"><strong>波动率:</strong> {risk.get('volatility', 'N/A')}%</div>
                <div class="metric"><strong>VaR(95%):</strong> {risk.get('var_95', 'N/A')}%</div>
            </div>

            <div class="section">
                <p style="color: #666; font-size: 12px;">
                    本报告由投资组合跟踪系统自动生成 | 
                    <a href="https://www.kdocs.cn/l/cdIikN6qJzWd">查看完整报告</a>
                </p>
            </div>
        </body>
        </html>
        """
        return html

    def _send_email(self, subject: str, html_content: str, recipients: List[str] = None):
        """发送邮件"""
        try:
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = self.email_config.get('sender')
            msg['To'] = ', '.join(recipients or self.email_config.get('recipients', []))

            msg.attach(MIMEText(html_content, 'html', 'utf-8'))

            with smtplib.SMTP(self.email_config.get('smtp_server'), 
                            self.email_config.get('smtp_port', 587)) as server:
                server.starttls()
                server.login(self.email_config.get('username'), 
                           self.email_config.get('password'))
                server.send_message(msg)

            logger.info(f"邮件发送成功: {subject}")
        except Exception as e:
            logger.error(f"邮件发送失败: {e}")

    def _send_wechat(self, report_data: dict):
        """发送企业微信消息"""
        try:
            webhook_url = self.wechat_config.get('webhook_url')
            if not webhook_url:
                return

            summary = report_data.get('summary', {})
            risk = report_data.get('risk', {})

            daily_pnl = summary.get('daily_pnl', 0)
            daily_return = summary.get('daily_return', 0)

            content = f"""📊 投资组合日报 {datetime.now().strftime('%Y-%m-%d')}

💰 收益概览
• 总市值: ¥{summary.get('total_value', 0):,.2f}
• 当日盈亏: {'+' if daily_pnl >= 0 else ''}¥{daily_pnl:,.2f} ({daily_return:+.2f}%)
• 累计盈亏: {'+' if summary.get('total_pnl', 0) >= 0 else ''}¥{summary.get('total_pnl', 0):,.2f}

⚠️ 风险指标
• 夏普比率: {risk.get('sharpe_ratio', 'N/A')}
• 最大回撤: {risk.get('max_drawdown', 'N/A')}%
• VaR(95%): {risk.get('var_95', 'N/A')}%"""

            payload = {
                "msgtype": "text",
                "text": {"content": content}
            }

            response = requests.post(webhook_url, json=payload, timeout=10)
            if response.status_code == 200:
                logger.info("企业微信消息发送成功")
            else:
                logger.error(f"企业微信发送失败: {response.text}")

        except Exception as e:
            logger.error(f"企业微信发送失败: {e}")

    def _send_wechat_alert(self, alert_type: str, message: str, level: str):
        """发送企业微信告警"""
        try:
            webhook_url = self.wechat_config.get('webhook_url')
            if not webhook_url:
                return

            emoji = "🚨" if level == "error" else "⚠️"
            content = f"{emoji} [{level.upper()}] {alert_type}\n\n{message}\n\n时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

            payload = {
                "msgtype": "text",
                "text": {"content": content}
            }

            requests.post(webhook_url, json=payload, timeout=10)

        except Exception as e:
            logger.error(f"告警发送失败: {e}")
