"""P1 tests: advisor feedback methods + position_reader"""
import os, sys, sqlite3
from pathlib import Path
import pytest
import pandas as pd

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

@pytest.fixture
def temp_db(tmp_path):
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
        macd REAL, signal REAL, boll_upper REAL, boll_mid REAL, boll_lower REAL);
    CREATE TABLE IF NOT EXISTS fund_flows (
        id INTEGER PRIMARY KEY AUTOINCREMENT, code TEXT, name TEXT,
        date TEXT, net_inflow REAL, category TEXT);
    CREATE TABLE IF NOT EXISTS market_sentiment (
        id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT, name TEXT,
        value REAL);
    CREATE TABLE IF NOT EXISTS macro_daily (
        id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT, name TEXT,
        value REAL);
    CREATE TABLE IF NOT EXISTS daily_news (
        id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT, title TEXT,
        content TEXT, sentiment_score REAL);
    """)
    conn.commit()
    conn.close()
    yield db_path

def _adv(db):
    from src.analysis.advisor import SmartAdvisor
    return SmartAdvisor(sqlite3.connect(db))


class TestAnalyzeFundFlows:
    def test_none_input(self, temp_db):
        assert _adv(temp_db)._analyze_fund_flows({}) == []
    def test_empty_df(self, temp_db):
        assert _adv(temp_db)._analyze_fund_flows({"fund_flows": pd.DataFrame()}) == []
    def test_small_inflow_ignored(self, temp_db):
        df = pd.DataFrame({"code": ["510300"], "net_inflow": [50000000], "date": ["2026-06-01"]})
        assert _adv(temp_db)._analyze_fund_flows({"fund_flows": df}) == []
    def test_large_inflow_triggers(self, temp_db):
        df = pd.DataFrame({"code": ["510300", "510300"], "net_inflow": [100000000, 100000000], "date": ["2026-06-01", "2026-06-02"]})
        r = _adv(temp_db)._analyze_fund_flows({"fund_flows": df, "positions": [{"code": "510300"}]})
        assert len(r) >= 1 and any("资金大幅净流入" in a.title for a in r)
    def test_large_outflow_triggers(self, temp_db):
        df = pd.DataFrame({"code": ["510300", "510300"], "net_inflow": [-100000000, -100000000], "date": ["2026-06-01", "2026-06-02"]})
        r = _adv(temp_db)._analyze_fund_flows({"fund_flows": df, "positions": [{"code": "510300"}]})
        assert len(r) >= 1 and any("资金大幅净流出" in a.title for a in r)
    def test_unit_conversion(self, temp_db):
        df = pd.DataFrame({"code": ["510300", "510300"], "net_inflow": [250000000, 250000000], "date": ["2026-06-01", "2026-06-02"]})
        r = _adv(temp_db)._analyze_fund_flows({"fund_flows": df, "positions": [{"code": "510300"}]})
        assert "5.00" in r[0].description
        assert "500000000" not in r[0].description

class TestAnalyzeMarketSentiment:
    def test_none(self, temp_db):
        assert _adv(temp_db)._analyze_market_sentiment({}) == []
    def test_empty(self, temp_db):
        assert _adv(temp_db)._analyze_market_sentiment({"market_sentiment": pd.DataFrame()}) == []
    def test_normal(self, temp_db):
        df = pd.DataFrame({"name": ["MARGIN_TOTAL", "MARGIN_BALANCE"], "value": [18000, 9000], "date": ["2026-06-01"] * 2})
        assert isinstance(_adv(temp_db)._analyze_market_sentiment({"market_sentiment": df}), list)


class TestAnalyzeMacroEnvironment:
    def test_none(self, temp_db):
        assert _adv(temp_db)._analyze_macro_environment({}) == []
    def test_empty(self, temp_db):
        assert _adv(temp_db)._analyze_macro_environment({"macro_daily": pd.DataFrame()}) == []
    def test_normal(self, temp_db):
        df = pd.DataFrame({"name": ["CPI_YOY", "PPI_YOY", "PMI"], "value": [2.5, -1.0, 50.5], "date": ["2026-06-01"] * 3})
        assert isinstance(_adv(temp_db)._analyze_macro_environment({"macro_daily": df}), list)

class TestAnalyzeNewsSentiment:
    def test_none(self, temp_db):
        assert _adv(temp_db)._analyze_news_sentiment({}) == []
    def test_empty(self, temp_db):
        assert _adv(temp_db)._analyze_news_sentiment({"daily_news": pd.DataFrame()}) == []
    def test_negative(self, temp_db):
        df = pd.DataFrame({"title": ["down"], "content": ["crash"], "sentiment_score": [-0.8], "date": ["2026-06-01"]})
        assert isinstance(_adv(temp_db)._analyze_news_sentiment({"daily_news": df}), list)
    def test_positive(self, temp_db):
        df = pd.DataFrame({"title": ["up"], "content": ["rally"], "sentiment_score": [0.8], "date": ["2026-06-01"]})
        assert isinstance(_adv(temp_db)._analyze_news_sentiment({"daily_news": df}), list)

class TestAdvisorIntegration:
    def test_empty_portfolio(self, temp_db):
        assert isinstance(_adv(temp_db).analyze_portfolio({}, {}, {}), list)
    def test_with_positions(self, temp_db):
        data = {"positions": [{"code": "510300", "name": "ETF"}], "summary": {"total_value": 100000}}
        assert isinstance(_adv(temp_db).analyze_portfolio(data, {}, {}), list)


class TestPositionReader:
    def test_init_default(self):
        from src.utils.position_reader import PositionReader
        assert PositionReader() is not None
    def test_nonexistent_file(self, tmp_path):
        from src.utils.position_reader import PositionReader
        try:
            PositionReader(str(tmp_path / "no.xls")).read_positions()
            assert False, "should raise"
        except (FileNotFoundError, OSError):
            pass
    def test_clean_code(self):
        from src.utils.position_reader import PositionReader
        r = PositionReader()
        positions = r.read_positions()
        assert isinstance(positions, list)
    def test_to_float_default(self):
        from src.utils.position_reader import PositionReader
        r = PositionReader()
        assert isinstance(r.get_summary([]), dict)
    def test_get_summary_empty(self):
        from src.utils.position_reader import PositionReader
        assert isinstance(PositionReader().get_summary([]), dict)
