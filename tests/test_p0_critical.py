"""P0 critical module tests: monitor / notification / smart_report / run_analysis"""
import os, sys, sqlite3, time
from pathlib import Path
from unittest.mock import patch
import pytest

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


@pytest.fixture
def temp_db(tmp_path):
    """临时数据库路径，含Monitor和SmartReport所需全部表"""
    db_path = str(tmp_path / "test.db")
    conn = sqlite3.connect(db_path)
    conn.executescript("""CREATE TABLE IF NOT EXISTS execution_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT, task_name TEXT, status TEXT,
        message TEXT, duration_seconds REAL, created_at TEXT);
    CREATE TABLE IF NOT EXISTS alerts (
        id INTEGER PRIMARY KEY AUTOINCREMENT, rule_name TEXT, level TEXT,
        message TEXT, created_at TEXT, acknowledged INTEGER DEFAULT 0);
    CREATE TABLE IF NOT EXISTS advice_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT, created_at TEXT,
        advice_type TEXT, priority TEXT, title TEXT, description TEXT,
        confidence REAL, related_codes TEXT, source TEXT);
    CREATE TABLE IF NOT EXISTS daily_snapshots (
        id INTEGER PRIMARY KEY, date TEXT, total_value REAL, total_pnl REAL);
    CREATE TABLE IF NOT EXISTS index_quotes (
        id INTEGER PRIMARY KEY, date TEXT, code TEXT, name TEXT,
        close REAL, change_pct REAL);
    CREATE TABLE IF NOT EXISTS risk_indicators (
        id INTEGER PRIMARY KEY, date TEXT, indicator_name TEXT,
        indicator_value REAL);
    CREATE TABLE IF NOT EXISTS portfolio_positions (
        id INTEGER PRIMARY KEY, code TEXT, name TEXT, quantity INTEGER,
        cost_price REAL, current_price REAL, market_value REAL);
    CREATE TABLE IF NOT EXISTS etf_technical (
        id INTEGER PRIMARY KEY, code TEXT, date TEXT, rsi REAL,
        macd REAL, signal REAL, boll_upper REAL, boll_mid REAL, boll_lower REAL);""")
    conn.commit()
    conn.close()
    yield db_path


@pytest.fixture
def notify_config():
    return {"email": {"enabled": False, "smtp_host": "", "smtp_port": 587},
            "wechat": {"enabled": False, "webhook_url": ""}}


# ===== Monitor =====
class TestMonitorInit:
    def test_init_with_empty_config(self, temp_db):
        from src.utils.monitor import Monitor
        m = Monitor(str(temp_db), {})
        assert len(m.rules) == 9

    def test_init_default(self, temp_db):
        from src.utils.monitor import Monitor
        m = Monitor(temp_db)
        assert len(m.rules) == 9

    def test_init_with_threshold_override(self, temp_db):
        from src.utils.monitor import Monitor
        m = Monitor(str(temp_db), {"alert_rules": [
            {"name": "daily_loss_limit", "threshold": -5.0}]})
        rule = next(r for r in m.rules if r.name == "daily_loss_limit")
        assert rule.threshold == -5.0

    def test_init_no_crash_no_db_file(self, tmp_path):
        db_path = tmp_path / "noexist.db"
        from src.utils.monitor import Monitor
        Monitor(str(db_path))


class TestMonitorLogExecution:
    def test_log_success(self, temp_db):
        from src.utils.monitor import Monitor
        Monitor(temp_db).log_execution("t", "success", "ok", 10.5)
        row = sqlite3.connect(temp_db).execute(
            "SELECT status, duration_seconds FROM execution_logs").fetchone()
        assert row[0] == "success"
        assert row[1] == 10.5

    def test_log_failed(self, temp_db):
        from src.utils.monitor import Monitor
        Monitor(temp_db).log_execution("t", "failed", "error")
        cnt = sqlite3.connect(temp_db).execute(
            "SELECT count(*) FROM execution_logs WHERE status='failed'").fetchone()[0]
        assert cnt == 1

    def test_multiple_logs(self, temp_db):
        from src.utils.monitor import Monitor
        m = Monitor(temp_db)
        m.log_execution("t1", "success", "", 1.0)
        m.log_execution("t2", "success", "", 2.0)
        assert sqlite3.connect(temp_db).execute("SELECT count(*) FROM execution_logs").fetchone()[0] == 2


class TestMonitorCheckAlerts:
    def test_returns_list(self, temp_db):
        from src.utils.monitor import Monitor
        m = Monitor(temp_db)
        alerts = m.check_alerts(
            {"daily_return": -0.01, "total_value": 100000},
            {"max_drawdown": -0.05, "volatility": 0.02})
        assert isinstance(alerts, list)

    def test_drawdown_triggers(self, temp_db):
        from src.utils.monitor import Monitor
        m = Monitor(temp_db)
        # default drawdown threshold = -10.0
        alerts = m.check_alerts({}, {"max_drawdown": -15.0})
        triggered = [a for a in alerts if "drawdown" in a.rule_name]
        assert len(triggered) >= 1

    def test_no_alert_when_safe(self, temp_db):
        from src.utils.monitor import Monitor
        m = Monitor(temp_db)
        alerts = m.check_alerts({"daily_return": 0.5}, {"max_drawdown": -2.0})
        assert len(alerts) == 0

    def test_alert_saved_to_db(self, temp_db):
        from src.utils.monitor import Monitor
        m = Monitor(temp_db)
        m.check_alerts({}, {"max_drawdown": -15.0})
        cnt = sqlite3.connect(temp_db).execute("SELECT count(*) FROM alerts").fetchone()[0]
        assert cnt >= 1


class TestMonitorQueries:
    def test_recent_alerts_empty(self, temp_db):
        from src.utils.monitor import Monitor
        assert Monitor(temp_db).get_recent_alerts(24) == []

    def test_recent_alerts_with_data(self, temp_db):
        from src.utils.monitor import Monitor
        m = Monitor(temp_db)
        c = sqlite3.connect(temp_db)
        c.execute(
            "INSERT INTO alerts(rule_name,level,message,created_at) "
            "VALUES('t','warning','msg','2099-12-31 23:59:59')")
        c.commit()
        assert len(m.get_recent_alerts(24)) == 1

    def test_execution_stats(self, temp_db):
        from src.utils.monitor import Monitor
        m = Monitor(temp_db)
        m.log_execution("t1", "success", "", 10.0)
        m.log_execution("t1", "failed", "err", 5.0)
        stats = m.get_execution_stats(7)
        assert isinstance(stats, dict)

    def test_health_status(self, temp_db):
        from src.utils.monitor import Monitor
        assert isinstance(Monitor(temp_db).get_health_status(), dict)


# ===== NotificationManager =====
class TestNotificationInit:
    def test_with_config(self, notify_config):
        from src.utils.notification import NotificationManager
        assert NotificationManager(notify_config) is not None

    def test_default_config(self):
        from src.utils.notification import NotificationManager
        assert NotificationManager({}) is not None


class TestNotificationSend:
    def test_send_alert_no_crash(self, notify_config):
        from src.utils.notification import NotificationManager
        NotificationManager(notify_config).send_alert("T", "msg", "warning")

    def test_send_alert_error(self, notify_config):
        from src.utils.notification import NotificationManager
        NotificationManager(notify_config).send_alert("T", "critical", "error")

    def test_send_report_no_crash(self, notify_config):
        from src.utils.notification import NotificationManager
        NotificationManager(notify_config).send_portfolio_report({"date": "2026-01-01"})

    def test_build_html_structure(self, notify_config):
        from src.utils.notification import NotificationManager
        html = NotificationManager(notify_config)._build_html_report(
            {"date": "2026-01-01", "total_value": 100000,
             "daily_return": 0.02, "positions": []})
        assert isinstance(html, str) and len(html) > 50


# ===== SmartReportGenerator =====
class TestSmartReportInit:
    def test_init(self, temp_db):
        from src.report.smart_report import SmartReportGenerator
        conn = sqlite3.connect(temp_db)
        assert SmartReportGenerator(conn) is not None
        conn.close()


class TestSmartReportAdvice:
    def test_advice_summary_empty(self, temp_db):
        from src.report.smart_report import SmartReportGenerator
        assert isinstance(SmartReportGenerator(sqlite3.connect(temp_db)).get_advice_summary({}), dict)

    def test_advice_summary_with_positions(self, temp_db):
        from src.report.smart_report import SmartReportGenerator
        r = SmartReportGenerator(sqlite3.connect(temp_db)).get_advice_summary(
            {"positions": [{"code": "510300", "name": "ETF"}]})
        assert isinstance(r, dict)


class TestSmartReportBuild:
    def test_backtest_summary(self, temp_db):
        from src.report.smart_report import SmartReportGenerator
        assert isinstance(
            SmartReportGenerator(sqlite3.connect(temp_db))._generate_backtest_summary({}), dict)

    def test_build_report_str(self, temp_db):
        from src.report.smart_report import SmartReportGenerator
        bt = {"current_value": 100000, "total_return": 5000,
              "sharpe_ratio": 1.2, "max_drawdown": -5.0, "volatility": 12.0}
        r = SmartReportGenerator(sqlite3.connect(temp_db))._build_report([], bt, {})
        assert isinstance(r, str) and len(r) > 0

    def test_build_report_contains_sections(self, temp_db):
        from src.report.smart_report import SmartReportGenerator
        bt = {"current_value": 100000, "total_return": 5000,
              "sharpe_ratio": 1.2, "max_drawdown": -5.0, "volatility": 12.0}
        r = SmartReportGenerator(sqlite3.connect(temp_db))._build_report([], bt, {})
        assert "## 策略表现" in r
        assert "| 当前市值 |" in r


# ===== run_analysis helpers =====
class TestRunAnalysisHelpers:
    def test_is_trading_day_weekday(self):
        from datetime import date
        with patch("run_analysis.date") as md:
            md.today.return_value = date(2026, 6, 2)
            md.side_effect = lambda *a, **k: date(*a, **k)
            from run_analysis import is_trading_day
            assert is_trading_day() is True

    def test_is_trading_day_weekend(self):
        from datetime import date
        with patch("run_analysis.date") as md:
            md.today.return_value = date(2026, 5, 31)
            md.side_effect = lambda *a, **k: date(*a, **k)
            from run_analysis import is_trading_day
            assert is_trading_day() is False

    def test_setup_logging(self):
        import logging
        from run_analysis import setup_logging
        setup_logging()
        assert logging.getLogger("portfolio_analysis") is not None


class TestRunAnalysisStage4:
    def test_stage4_empty(self):
        from run_analysis import run_stage4_smart
        r = run_stage4_smart({}, {}, {})
        assert isinstance(r, (dict, type(None)))

    def test_stage4_with_data(self):
        from run_analysis import run_stage4_smart
        r = run_stage4_smart(
            {"positions": [{"code": "510300"}], "summary": {}},
            {"total_value": 100000}, {"max_drawdown": -0.05})
        assert isinstance(r, (dict, type(None)))
