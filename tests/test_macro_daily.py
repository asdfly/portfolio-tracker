"""Tests for macro_daily.py - macro/market sentiment data fetching and saving."""
import pytest
import sqlite3
import os


# ============================================================
#  Fixtures
# ============================================================

@pytest.fixture
def db_conn():
    """In-memory DB with macro_daily and market_sentiment tables."""
    conn = sqlite3.connect(":memory:")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS macro_daily (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            indicator_code TEXT NOT NULL,
            name TEXT,
            value REAL,
            change_pct REAL,
            source TEXT,
            UNIQUE(date, indicator_code)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS market_sentiment (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            indicator_code TEXT NOT NULL,
            name TEXT,
            value REAL,
            change_value REAL,
            change_pct REAL,
            source TEXT
        )
    """)
    conn.commit()
    yield conn
    conn.close()


# ============================================================
#  save_macro_daily
# ============================================================

class TestSaveMacroDaily:
    """Test save_macro_daily with in-memory DB (mock get_db_connection)."""

    def test_save_empty_records(self, db_conn):
        from src.data_sources.macro_daily import save_macro_daily
        count = save_macro_daily(db_conn, [])
        assert count == 0

    def test_save_single_record(self, db_conn):
        from src.data_sources.macro_daily import save_macro_daily
        records = [{"date": "2025-01-01", "indicator_code": "USD_CNY", "name": "美元/人民币",
                     "value": 7.25, "change_pct": 0.01, "source": "akshare"}]
        count = save_macro_daily(db_conn, records)
        assert count == 1
        cur = db_conn.execute("SELECT value FROM macro_daily WHERE indicator_code='USD_CNY'")
        assert cur.fetchone()[0] == 7.25

    def test_save_multiple_records(self, db_conn):
        from src.data_sources.macro_daily import save_macro_daily
        records = [
            {"date": "2025-01-01", "indicator_code": "CN_10Y_BOND", "name": "中国10年国债",
             "value": 2.5, "source": "akshare"},
            {"date": "2025-01-01", "indicator_code": "US_10Y_BOND", "name": "美国10年国债",
             "value": 4.3, "source": "akshare"},
            {"date": "2025-01-02", "indicator_code": "CN_10Y_BOND", "name": "中国10年国债",
             "value": 2.48, "change_pct": -0.008, "source": "akshare"},
        ]
        count = save_macro_daily(db_conn, records)
        assert count == 3
        cur = db_conn.execute("SELECT COUNT(*) FROM macro_daily")
        assert cur.fetchone()[0] == 3

    def test_save_replace_duplicate(self, db_conn):
        """INSERT OR REPLACE should update existing records."""
        from src.data_sources.macro_daily import save_macro_daily
        records_v1 = [{"date": "2025-01-01", "indicator_code": "USD_CNY", "name": "USD/CNY",
                       "value": 7.25, "source": "akshare"}]
        save_macro_daily(db_conn, records_v1)
        records_v2 = [{"date": "2025-01-01", "indicator_code": "USD_CNY", "name": "USD/CNY",
                       "value": 7.30, "change_pct": 0.007, "source": "akshare"}]
        save_macro_daily(db_conn, records_v2)
        cur = db_conn.execute("SELECT COUNT(*) FROM macro_daily")
        assert cur.fetchone()[0] == 1
        cur = db_conn.execute("SELECT value FROM macro_daily WHERE indicator_code='USD_CNY'")
        assert cur.fetchone()[0] == 7.30

    def test_save_missing_optional_fields(self, db_conn):
        """Records without change_pct/source should still save."""
        from src.data_sources.macro_daily import save_macro_daily
        records = [{"date": "2025-01-01", "indicator_code": "SHIBOR_ON", "name": "隔夜SHIBOR",
                     "value": 1.8}]
        count = save_macro_daily(db_conn, records)
        assert count == 1
        cur = db_conn.execute("SELECT value, change_pct, source FROM macro_daily")
        row = cur.fetchone()
        assert row[0] == 1.8
        assert row[1] is None
        assert row[2] is None


# ============================================================
#  save_market_sentiment
# ============================================================

class TestSaveMarketSentiment:
    """Test save_market_sentiment with in-memory DB."""

    def test_save_empty_records(self, db_conn):
        from src.data_sources.macro_daily import save_market_sentiment
        count = save_market_sentiment(db_conn, [])
        assert count == 0

    def test_save_margin_records(self, db_conn):
        from src.data_sources.macro_daily import save_market_sentiment
        records = [
            {"date": "2025-01-01", "indicator_code": "MARGIN_TOTAL", "name": "两融余额",
             "value": 18000, "change_value": 100, "change_pct": 0.0056, "source": "akshare"},
            {"date": "2025-01-01", "indicator_code": "MARGIN_BUY_上", "name": "沪市融资买入",
             "value": 500, "source": "akshare"},
        ]
        count = save_market_sentiment(db_conn, records)
        assert count == 2

    def test_save_pledge_records(self, db_conn):
        from src.data_sources.macro_daily import save_market_sentiment
        records = [
            {"date": "2025-05-15", "indicator_code": "PLEDGE_RATIO", "name": "质押比例",
             "value": 5.2, "change_pct": -0.1, "source": "akshare"},
            {"date": "2025-05-15", "indicator_code": "PLEDGE_COMPANY_COUNT", "name": "质押公司数",
             "value": 2500, "source": "akshare"},
        ]
        count = save_market_sentiment(db_conn, records)
        assert count == 2
        cur = db_conn.execute("SELECT value FROM market_sentiment WHERE indicator_code='PLEDGE_RATIO'")
        assert cur.fetchone()[0] == 5.2

    def test_save_missing_optional_fields(self, db_conn):
        """Records without change_value/change_pct/source should still save."""
        from src.data_sources.macro_daily import save_market_sentiment
        records = [{"date": "2025-01-01", "indicator_code": "MARGIN_上", "name": "沪市融资",
                     "value": 9000}]
        count = save_market_sentiment(db_conn, records)
        assert count == 1


# ============================================================
#  fetch functions (mocked network)
# ============================================================

class TestMacroFetchFunctions:
    """Test fetch functions with mocked AKShare calls."""

    def test_fetch_usd_cny_structure(self, monkeypatch):
        """fetch_usd_cny should return list of dicts with required keys."""
        import pandas as pd
        from src.data_sources import macro_daily

        mock_df = pd.DataFrame({
            "日期": ["2025-01-01", "2025-01-02"],
            "央行中间价": [725.0, 730.0],
        })
        monkeypatch.setattr("akshare.currency_boc_sina",
                            lambda symbol, start_date, end_date: mock_df)
        result = macro_daily.fetch_usd_cny()
        assert isinstance(result, list)
        assert len(result) == 2
        assert set(result[0].keys()) >= {"date", "indicator_code", "name", "value"}

    def test_fetch_bond_yields_structure(self, monkeypatch):
        """fetch_bond_yields should return CN and US bond yields + spread."""
        import pandas as pd
        from src.data_sources import macro_daily

        mock_df = pd.DataFrame({
            "日期": ["2025-01-01"],
            "中国国债收益率10年": [2.5],
            "美国国债收益率10年": [4.3],
            "中国国债收益率2年": [1.8],
            "美国国债收益率2年": [4.1],
            "中国国债收益率30年": [2.8],
            "美国国债收益率30年": [4.5],
        })
        monkeypatch.setattr("akshare.bond_zh_us_rate", lambda start_date: mock_df)
        result = macro_daily.fetch_bond_yields()
        assert isinstance(result, list)
        codes = {r["indicator_code"] for r in result}
        assert "CN_10Y_BOND" in codes
        assert "US_10Y_BOND" in codes
        assert "CN_US_SPREAD" in codes

    def test_fetch_comex_gold_structure(self, monkeypatch):
        """fetch_comex_gold should return COMEX gold records."""
        import pandas as pd
        from src.data_sources import macro_daily

        mock_df = pd.DataFrame({
            "date": ["2025-01-01"],
            "close": [2650.0],
        })
        monkeypatch.setattr("akshare.futures_foreign_hist", lambda symbol: mock_df)
        result = macro_daily.fetch_comex_gold(days=1)
        assert isinstance(result, list)
        assert all(r["indicator_code"] == "COMEX_GOLD" for r in result)

    def test_fetch_shibor_structure(self, monkeypatch):
        """fetch_shibor should return SHIBOR records."""
        import pandas as pd
        from src.data_sources import macro_daily

        mock_df = pd.DataFrame({
            "日期": ["2025-01-01"],
            "O/N-定价": [1.8],
            "O/N-涨跌幅": [0.5],
        })
        monkeypatch.setattr("akshare.macro_china_shibor_all", lambda: mock_df)
        result = macro_daily.fetch_shibor(days=1)
        assert isinstance(result, list)
        codes = {r["indicator_code"] for r in result}
        assert "SHIBOR_ON" in codes

    def test_fetch_margin_balance_structure(self, monkeypatch):
        """fetch_margin_balance should return margin trading balance records."""
        import pandas as pd
        from src.data_sources import macro_daily

        mock_df = pd.DataFrame({
            "日期": ["2025-01-01"],
            "融资余额": [18000],
            "融资买入额": [500],
        })
        # Both margin functions are called with no arguments
        monkeypatch.setattr("akshare.macro_china_market_margin_sh", lambda: mock_df)
        monkeypatch.setattr("akshare.macro_china_market_margin_sz", lambda: mock_df)
        result = macro_daily.fetch_margin_balance(days=1)
        assert isinstance(result, list)
        codes = {r["indicator_code"] for r in result}
        assert "MARGIN_TOTAL" in codes

    def test_fetch_pledge_profile_structure(self, monkeypatch):
        """fetch_pledge_profile should return pledge ratio records."""
        import pandas as pd
        from src.data_sources import macro_daily

        mock_df = pd.DataFrame({
            "交易日期": ["2025-05-15"],
            "A股质押总比例": [5.2],
            "质押公司数量": [2500],
            "质押笔数": [8000],
            "质押总股数": [5000],
            "质押总市值": [30000],
        })
        monkeypatch.setattr("akshare.stock_gpzy_profile_em", lambda: mock_df)
        result = macro_daily.fetch_pledge_profile(days=1)
        assert isinstance(result, list)
        codes = {r["indicator_code"] for r in result}
        assert "PLEDGE_RATIO" in codes
        assert "PLEDGE_COMPANY_COUNT" in codes
        assert "PLEDGE_TOTAL_MV" in codes
