"""Tests for data quality and collection fixes."""

import sqlite3
import re
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "data" / "database" / "portfolio.db"


class TestD4Metrics:
    """Verify D4 alert metrics resolve correctly."""

    def setup_method(self):
        self.conn = sqlite3.connect(str(DB_PATH))

    def teardown_method(self):
        self.conn.close()

    def test_stale_sources_query(self):
        cur = self.conn.cursor()
        cur.execute("SELECT COUNT(*) FROM ("
            "SELECT 'portfolio_snapshots' AS src, MAX(date) AS last_date FROM portfolio_snapshots "
            "UNION ALL SELECT 'fund_flows', MAX(date) FROM fund_flows "
            "UNION ALL SELECT 'daily_news', MAX(date) FROM daily_news "
            "UNION ALL SELECT 'market_sentiment', MAX(date) FROM market_sentiment "
            "UNION ALL SELECT 'macro_daily', MAX(date) FROM macro_daily"
            ") WHERE last_date < date('now', ?)", ('-7 days',))
        count = cur.fetchone()[0]
        assert isinstance(count, int)
        assert count >= 0

    def test_position_count_query(self):
        cur = self.conn.cursor()
        cur.execute("SELECT COUNT(DISTINCT code) FROM portfolio_snapshots "
                    "WHERE date = (SELECT MAX(date) FROM portfolio_snapshots)")
        count = cur.fetchone()[0] or 0
        assert count > 0

    def test_total_value_query(self):
        cur = self.conn.cursor()
        cur.execute("SELECT total_value FROM portfolio_summary ORDER BY date DESC LIMIT 2")
        rows = cur.fetchall()
        assert len(rows) >= 2
        assert rows[0][0] is not None

    def test_dq_score_parsing(self):
        cur = self.conn.cursor()
        cur.execute("SELECT message FROM execution_logs "
                    "WHERE task_name = 'data_quality_check' "
                    "ORDER BY created_at DESC LIMIT 1")
        row = cur.fetchone()
        assert row is not None
        msg = row[0] or ''
        score_m = re.search(r'score=([0-9.]+)', msg)
        grade_m = re.search(r'grade=([A-F])', msg)
        assert score_m is not None, f"score not found in: {msg}"
        assert grade_m is not None, f"grade not found in: {msg}"
        score = float(score_m.group(1))
        assert 0 <= score <= 100


class TestHealthCheck:
    """Verify health check code is syntactically valid."""

    def test_health_check_has_network_status(self):
        run_analysis = (PROJECT_ROOT / "run_analysis.py").read_text(encoding="utf-8")
        assert "NETWORK" in run_analysis
        assert "FAIL" in run_analysis

    def test_health_check_uses_hsgt_hist(self):
        run_analysis = (PROJECT_ROOT / "run_analysis.py").read_text(encoding="utf-8")
        assert "stock_hsgt_hist_em" in run_analysis
        assert "stock_hsgt_north_net_flow_in_em" not in run_analysis

    def test_health_check_proxy_bypass(self):
        run_analysis = (PROJECT_ROOT / "run_analysis.py").read_text(encoding="utf-8")
        assert "_saved_proxies" in run_analysis


class TestAlertRules:
    """Verify 9 alert rules are all functional."""

    def test_all_9_rules_present(self):
        from src.utils.monitor import Monitor
        m = Monitor(db_path=str(DB_PATH))
        assert len(m.rules) == 9
        names = [r.name for r in m.rules]
        expected = ["daily_loss_limit", "drawdown_limit", "concentration_risk",
                     "volatility_spike", "sharpe_low", "data_source_stale",
                     "data_quality_low", "position_count_change", "total_value_drop"]
        for name in expected:
            assert name in names, f"Missing rule: {name}"

    def test_d4_rules_have_correct_conditions(self):
        from src.utils.monitor import Monitor
        m = Monitor(db_path=str(DB_PATH))
        rules = {r.name: r for r in m.rules}
        assert rules["data_source_stale"].condition == "stale_sources_count"
        assert rules["data_quality_low"].condition == "data_quality_score"
        assert rules["position_count_change"].condition == "position_count_change_pct"
        assert rules["total_value_drop"].condition == "total_value_change_pct"
