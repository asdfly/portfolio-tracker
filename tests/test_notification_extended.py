"""Tests for src/utils/notification.py."""
import pytest
from unittest.mock import patch, MagicMock

class TestNMInit:
    def test_empty(self):
        from src.utils.notification import NotificationManager
        nm = NotificationManager({})
        assert nm.config == {}

    def test_with_smtp(self):
        from src.utils.notification import NotificationManager
        nm = NotificationManager({"smtp_host": "smtp.qq.com", "smtp_port": 465})
        assert nm.config["smtp_host"] == "smtp.qq.com"

class TestBuildHtml:
    def test_basic(self):
        from src.utils.notification import NotificationManager
        nm = NotificationManager({})
        html = nm._build_html_report({"title": "Test", "body": "Content"})
        assert "投资组合" in html or "Test" in html

    def test_empty_data(self):
        from src.utils.notification import NotificationManager
        nm = NotificationManager({})
        html = nm._build_html_report({})
        assert isinstance(html, str)

class TestSendReport:
    @patch("src.utils.notification.NotificationManager._send_email")
    def test_calls_send_email(self, m):
        from src.utils.notification import NotificationManager
        nm = NotificationManager({"email": {"enabled": True, "smtp_host": "h", "smtp_port": 465, "smtp_user": "u", "smtp_pass": "p"}})
        nm.send_portfolio_report({"title": "日报"}, ["a@b.com"])
        assert m.called

class TestSendAlert:
    @patch("src.utils.notification.NotificationManager._send_email")
    def test_calls_send_email(self, m):
        from src.utils.notification import NotificationManager
        nm = NotificationManager({"email": {"enabled": True, "smtp_host": "h", "smtp_port": 465, "smtp_user": "u", "smtp_pass": "p"}})
        nm.send_alert("test", "message", "info")
        assert m.called

class TestSendEmail:
    @patch("src.utils.notification.smtplib.SMTP")
    def test_smtp_called(self, mock_smtp):
        mock_smtp.return_value.__enter__ = MagicMock()
        mock_smtp.return_value.__exit__ = MagicMock(return_value=False)
        from src.utils.notification import NotificationManager
        nm = NotificationManager({"email": {"enabled": True, "smtp_server": "h", "smtp_port": 587, "username": "u", "password": "p", "sender": "x@y.com"}})
        nm._send_email("Sub", "<p>B</p>", ["a@b.com"])
        assert mock_smtp.called

class TestWechatNoConfig:
    def test_send_wechat(self):
        from src.utils.notification import NotificationManager
        assert NotificationManager({})._send_wechat({}) is None

    def test_send_alert(self):
        from src.utils.notification import NotificationManager
        assert NotificationManager({})._send_wechat_alert("t", "m", "info") is None
