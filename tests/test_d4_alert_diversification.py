"""
D4: 告警体系多样化 - 测试
- DEFAULT_RULES扩展(5->9条)
- 告警去重逻辑
- run_stage3_monitor check_data扩展指标
"""
import pytest
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
import sys

PROJECT_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_DIR))

from src.utils.monitor import Monitor, AlertRule, Alert


# ============================================================
# Fixtures
# ============================================================

@pytest.fixture
def monitor(tmp_path):
    db_path = tmp_path / "test_monitor.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("""CREATE TABLE IF NOT EXISTS alerts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        rule_name TEXT,
        level TEXT,
        message TEXT,
        created_at TEXT,
        acknowledged INTEGER DEFAULT 0
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS execution_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        task_name TEXT,
        status TEXT,
        message TEXT,
        duration_seconds REAL,
        created_at TEXT
    )""")
    conn.commit()
    conn.close()
    return Monitor(str(db_path), config={"dedup_interval_hours": 6})


class TestDefaultRulesSchema:
    """验证DEFAULT_RULES从5条扩展到9条"""

    def test_nine_rules_total(self):
        assert len(Monitor.DEFAULT_RULES) == 9

    def test_rule_names(self):
        expected = {
            'daily_loss_limit', 'drawdown_limit', 'concentration_risk',
            'volatility_spike', 'sharpe_low',
            'data_source_stale', 'data_quality_low',
            'position_count_change', 'total_value_drop',
        }
        actual = {r.name for r in Monitor.DEFAULT_RULES}
        assert actual == expected

    def test_new_rules_have_correct_conditions(self):
        by_name = {r.name: r for r in Monitor.DEFAULT_RULES}
        assert by_name['data_source_stale'].condition == 'stale_sources_count'
        assert by_name['data_quality_low'].condition == 'data_quality_score'
        assert by_name['position_count_change'].condition == 'position_count_change_pct'
        assert by_name['total_value_drop'].condition == 'total_value_change_pct'

    def test_new_rules_have_thresholds(self):
        by_name = {r.name: r for r in Monitor.DEFAULT_RULES}
        assert by_name['data_source_stale'].threshold == 1
        assert by_name['data_quality_low'].threshold == 60
        assert by_name['position_count_change'].threshold == 50.0
        assert by_name['total_value_drop'].threshold == -5.0

    def test_concentration_threshold_lowered(self):
        by_name = {r.name: r for r in Monitor.DEFAULT_RULES}
        assert by_name['concentration_risk'].threshold == 0.35


class TestAlertDedup:
    """验证告警去重逻辑"""

    def test_dedup_interval_hours_config(self, monitor):
        assert monitor.dedup_interval_hours == 6

    def test_dedup_custom_interval(self, tmp_path):
        db_path = tmp_path / "test2.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute("""CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rule_name TEXT, level TEXT, message TEXT,
            created_at TEXT, acknowledged INTEGER DEFAULT 0
        )""")
        conn.commit()
        conn.close()
        m = Monitor(str(db_path), config={"dedup_interval_hours": 12})
        assert m.dedup_interval_hours == 12

    def test_duplicate_alert_skipped(self, monitor):
        now_iso = datetime.now().isoformat()
        conn = sqlite3.connect(monitor.db_path)
        conn.execute(
            "INSERT INTO alerts (rule_name, level, message, created_at) VALUES (?, ?, ?, ?)",
            ('sharpe_low', 'warning', 'test', now_iso)
        )
        conn.commit()
        conn.close()
        data = {'sharpe_ratio': 0.1, 'daily_return': 0, 'max_drawdown': 0,
                'concentration_hhi': 0.1, 'volatility': 10}
        alerts = monitor.check_alerts(data, data)
        sharpe_alerts = [a for a in alerts if a.rule_name == 'sharpe_low']
        assert len(sharpe_alerts) == 0

    def test_different_rule_not_deduped(self, monitor):
        now_iso = datetime.now().isoformat()
        conn = sqlite3.connect(monitor.db_path)
        conn.execute(
            "INSERT INTO alerts (rule_name, level, message, created_at) VALUES (?, ?, ?, ?)",
            ('sharpe_low', 'warning', 'test', now_iso)
        )
        conn.commit()
        conn.close()
        data = {
            'sharpe_ratio': 2.0, 'daily_return': 0, 'max_drawdown': 0,
            'concentration_hhi': 0.1, 'volatility': 10,
            'stale_sources_count': 3, 'stale_threshold_days': 7,
        }
        alerts = monitor.check_alerts(data, data)
        stale_alerts = [a for a in alerts if a.rule_name == 'data_source_stale']
        assert len(stale_alerts) == 1

    def test_expired_alert_not_deduped(self, tmp_path):
        db_path = tmp_path / "test3.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute("""CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rule_name TEXT, level TEXT, message TEXT,
            created_at TEXT, acknowledged INTEGER DEFAULT 0
        )""")
        conn.commit()
        conn.close()
        m = Monitor(str(db_path), config={"dedup_interval_hours": 1})
        old_time = (datetime.now() - timedelta(hours=7)).isoformat()
        conn = sqlite3.connect(m.db_path)
        conn.execute(
            "INSERT INTO alerts (rule_name, level, message, created_at) VALUES (?, ?, ?, ?)",
            ('sharpe_low', 'warning', 'old test', old_time)
        )
        conn.commit()
        conn.close()
        data = {'sharpe_ratio': 0.1, 'daily_return': 0, 'max_drawdown': 0,
                'concentration_hhi': 0.1, 'volatility': 10}
        alerts = m.check_alerts(data, data)
        sharpe_alerts = [a for a in alerts if a.rule_name == 'sharpe_low']
        assert len(sharpe_alerts) == 1


class TestNewRuleTriggering:
    """验证新增规则正确触发"""

    def test_data_source_stale_triggers(self, monitor):
        data = {
            'sharpe_ratio': 2.0, 'daily_return': 0, 'max_drawdown': 0,
            'concentration_hhi': 0.1, 'volatility': 10,
            'stale_sources_count': 3, 'stale_threshold_days': 7,
        }
        alerts = monitor.check_alerts(data, data)
        names = [a.rule_name for a in alerts]
        assert 'data_source_stale' in names

    def test_data_quality_low_triggers(self, monitor):
        data = {
            'sharpe_ratio': 2.0, 'daily_return': 0, 'max_drawdown': 0,
            'concentration_hhi': 0.1, 'volatility': 10,
            'data_quality_score': 45, 'data_quality_grade': 'C',
        }
        alerts = monitor.check_alerts(data, data)
        names = [a.rule_name for a in alerts]
        assert 'data_quality_low' in names

    def test_position_count_change_triggers(self, monitor):
        data = {
            'sharpe_ratio': 2.0, 'daily_return': 0, 'max_drawdown': 0,
            'concentration_hhi': 0.1, 'volatility': 10,
            'position_count_change_pct': 80.0,
            'position_count_prev': 10, 'position_count_curr': 18,
        }
        alerts = monitor.check_alerts(data, data)
        names = [a.rule_name for a in alerts]
        assert 'position_count_change' in names

    def test_total_value_drop_triggers(self, monitor):
        data = {
            'sharpe_ratio': 2.0, 'daily_return': 0, 'max_drawdown': 0,
            'concentration_hhi': 0.1, 'volatility': 10,
            'total_value_change_pct': -8.0,
        }
        alerts = monitor.check_alerts(data, data)
        names = [a.rule_name for a in alerts]
        assert 'total_value_drop' in names


class TestThresholdDirection:
    """验证不同规则的方向判断"""

    def test_total_value_drop_less_than(self, monitor):
        data = {
            'sharpe_ratio': 2.0, 'daily_return': 0, 'max_drawdown': 0,
            'concentration_hhi': 0.1, 'volatility': 10,
            'total_value_change_pct': -3.0,
        }
        alerts = monitor.check_alerts(data, data)
        names = [a.rule_name for a in alerts]
        assert 'total_value_drop' not in names

    def test_data_source_stale_greater_than(self, monitor):
        data = {
            'sharpe_ratio': 2.0, 'daily_return': 0, 'max_drawdown': 0,
            'concentration_hhi': 0.1, 'volatility': 10,
            'stale_sources_count': 1, 'stale_threshold_days': 7,
        }
        alerts = monitor.check_alerts(data, data)
        names = [a.rule_name for a in alerts]
        assert 'data_source_stale' not in names

    def test_sharpe_less_than_direction(self, monitor):
        data = {
            'sharpe_ratio': 0.3, 'daily_return': 0, 'max_drawdown': 0,
            'concentration_hhi': 0.1, 'volatility': 10,
        }
        alerts = monitor.check_alerts(data, data)
        names = [a.rule_name for a in alerts]
        assert 'sharpe_low' in names


class TestGetRecentAlertRules:
    """验证_get_recent_alert_rules边界情况"""

    def test_empty_table_returns_empty_set(self, monitor):
        rules = monitor._get_recent_alert_rules()
        assert rules == set()

    def test_db_error_returns_empty_set(self, tmp_path):
        m = Monitor(str(tmp_path / "nonexistent.db"), config={"dedup_interval_hours": 6})
        rules = m._get_recent_alert_rules()
        assert rules == set()
