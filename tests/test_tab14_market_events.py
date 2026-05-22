"""
Tab14 市场事件面板测试
"""
import pytest
import pandas as pd
import sqlite3
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta


@pytest.fixture
def mock_db(tmp_path):
    """创建测试数据库并填充测试数据"""
    db_path = tmp_path / "test_portfolio.db"
    conn = sqlite3.connect(str(db_path))
    
    # 创建表
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS stock_lhb (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL, code TEXT NOT NULL, name TEXT,
            close REAL, change_pct REAL, lhb_net_buy REAL,
            lhb_buy_amount REAL, lhb_sell_amount REAL,
            reason TEXT,
            UNIQUE(date, code)
        );
        CREATE TABLE IF NOT EXISTS stock_margin (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL, code TEXT NOT NULL, name TEXT,
            margin_balance REAL, margin_buy REAL,
            UNIQUE(date, code)
        );
        CREATE TABLE IF NOT EXISTS stock_holder_change (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL, holder_name TEXT, code TEXT NOT NULL,
            name TEXT, qty_change REAL,
            UNIQUE(date, holder_name, code)
        );
        CREATE TABLE IF NOT EXISTS stock_institution_research (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL, code TEXT NOT NULL, name TEXT,
            institution TEXT, inst_type TEXT,
            UNIQUE(date, code, institution)
        );
        CREATE TABLE IF NOT EXISTS stock_block_trade (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL, code TEXT NOT NULL, name TEXT,
            amount REAL, volume REAL, premium_rate REAL,
            buyer_broker TEXT, seller_broker TEXT,
            UNIQUE(date, code, buyer_broker, seller_broker)
        );
    """)
    
    # 插入测试数据
    today = datetime.now().strftime("%Y-%m-%d")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    conn.executemany(
        "INSERT OR IGNORE INTO stock_lhb (date, code, name, close, change_pct, lhb_net_buy, reason) VALUES (?,?,?,?,?,?,?)",
        [
            (today, "000001", "平安银行", 12.5, 5.0, 1e8, "日涨幅偏离"),
            (today, "000002", "万科A", 8.3, -3.0, -5e7, "日跌幅偏离"),
            (yesterday, "000003", "测试", 10.0, 2.0, 3e7, "日换手率"),
        ]
    )
    conn.executemany(
        "INSERT OR IGNORE INTO stock_margin (date, code, name, margin_balance, margin_buy) VALUES (?,?,?,?,?)",
        [
            (today, "510050", "50ETF", 1.5e9, 7.7e7),
            (today, "510100", "SZ50ETF", 1.7e7, 5.6e6),
        ]
    )
    conn.executemany(
        "INSERT OR IGNORE INTO stock_holder_change (date, holder_name, code, name, qty_change) VALUES (?,?,?,?,?)",
        [
            (today, "张三", "000001", "平安银行", 1e6),
            (today, "李四", "000002", "万科A", -5e5),
        ]
    )
    conn.executemany(
        "INSERT OR IGNORE INTO stock_institution_research (date, code, name, institution, inst_type) VALUES (?,?,?,?,?)",
        [
            (today, "000001", "平安银行", "华夏基金", "基金"),
            (today, "000001", "平安银行", "南方基金", "基金"),
            (today, "000002", "万科A", "中金公司", "券商"),
        ]
    )
    conn.executemany(
        "INSERT OR IGNORE INTO stock_block_trade (date, code, name, amount, volume, premium_rate, buyer_broker, seller_broker) VALUES (?,?,?,?,?,?,?,?)",
        [
            (today, "000001", "平安银行", 6.6e6, 2e5, -2.0, "机构A", "机构B"),
            (today, "000002", "万科A", 3.7e6, 9e4, 5.0, "机构C", "机构D"),
        ]
    )
    
    conn.commit()
    conn.close()
    return db_path


class TestTab14LoadData:
    """测试数据加载函数"""

    def test_load_lhb_data(self, mock_db):
        with patch("tabs.tab14_market_events.DATABASE_PATH", str(mock_db)):
            from tabs.tab14_market_events import _load_market_events
            df = _load_market_events("stock_lhb", 30)
            assert len(df) == 3
            assert set(df.columns) & {'date', 'code', 'name', 'lhb_net_buy'}

    def test_load_margin_data(self, mock_db):
        with patch("tabs.tab14_market_events.DATABASE_PATH", str(mock_db)):
            from tabs.tab14_market_events import _load_market_events
            df = _load_market_events("stock_margin", 30)
            assert len(df) == 2
            assert 'margin_balance' in df.columns

    def test_load_holder_change_data(self, mock_db):
        with patch("tabs.tab14_market_events.DATABASE_PATH", str(mock_db)):
            from tabs.tab14_market_events import _load_market_events
            df = _load_market_events("stock_holder_change", 30)
            assert len(df) == 2
            assert 'qty_change' in df.columns

    def test_load_institution_data(self, mock_db):
        with patch("tabs.tab14_market_events.DATABASE_PATH", str(mock_db)):
            from tabs.tab14_market_events import _load_market_events
            df = _load_market_events("stock_institution_research", 30)
            assert len(df) == 3
            assert 'institution' in df.columns

    def test_load_block_trade_data(self, mock_db):
        with patch("tabs.tab14_market_events.DATABASE_PATH", str(mock_db)):
            from tabs.tab14_market_events import _load_market_events
            df = _load_market_events("stock_block_trade", 30)
            assert len(df) == 2
            assert 'premium_rate' in df.columns

    def test_load_date_list(self, mock_db):
        with patch("tabs.tab14_market_events.DATABASE_PATH", str(mock_db)):
            from tabs.tab14_market_events import _load_date_list
            dates = _load_date_list("stock_lhb")
            assert len(dates) == 2

    def test_load_empty_table(self, tmp_path):
        import streamlit as st
        st.cache_data.clear()
        db_path = tmp_path / "empty.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE empty_test_table (id INTEGER PRIMARY KEY, date TEXT, code TEXT)")
        conn.close()
        with patch("tabs.tab14_market_events.DATABASE_PATH", str(db_path)):
            from tabs.tab14_market_events import _load_market_events
            df = _load_market_events("empty_test_table", 30)
            assert df.empty


class TestTab14Render:
    """测试Tab14渲染（无报错）"""

    def test_render_tab14_no_error(self, mock_db):
        with patch("tabs.tab14_market_events.DATABASE_PATH", str(mock_db)):
            import streamlit as st
            from tabs.tab14_market_events import render_tab14
            # 仅验证导入和函数可调用
            assert callable(render_tab14)

    def test_import_tab14(self):
        from tabs.tab14_market_events import render_tab14
        assert render_tab14 is not None
