"""Extended tests for tabs/_helpers.py."""
import pytest
import pandas as pd
from unittest.mock import patch
import sqlite3

def _clear():
    import streamlit as st
    st.cache_data.clear()

class TestLoadEtfDetail:
    def setup_method(self): _clear()
    @patch("tabs._helpers.get_db_connection")
    def test_empty(self, mc):
        c = sqlite3.connect(":memory:")
        c.execute("CREATE TABLE portfolio_snapshots (date TEXT,code TEXT,name TEXT,quantity REAL,cost_price REAL,current_price REAL,market_value REAL,pnl REAL,pnl_rate REAL,beta REAL)")
        mc.return_value = c
        from tabs._helpers import load_etf_detail
        df, code = load_etf_detail("510300")
        assert df.empty and code == "510300"
        c.close()
    @patch("tabs._helpers.get_db_connection")
    def test_with_data(self, mc):
        c = sqlite3.connect(":memory:")
        c.execute("CREATE TABLE portfolio_snapshots (date TEXT,code TEXT,name TEXT,quantity REAL,cost_price REAL,current_price REAL,market_value REAL,pnl REAL,pnl_rate REAL,beta REAL)")
        c.execute("INSERT INTO portfolio_snapshots VALUES ('2024-01-01','510300','ETF',1000,1.0,1.1,1100,100,0.1,0.9)")
        c.commit()
        mc.return_value = c
        from tabs._helpers import load_etf_detail
        df, code = load_etf_detail("510300")
        assert code == "510300" and len(df) == 1
        c.close()
    @patch("tabs._helpers.get_db_connection")
    def test_end_date_filter(self, mc):
        c = sqlite3.connect(":memory:")
        c.execute("CREATE TABLE portfolio_snapshots (date TEXT,code TEXT,name TEXT,quantity REAL,cost_price REAL,current_price REAL,market_value REAL,pnl REAL,pnl_rate REAL,beta REAL)")
        c.execute("INSERT INTO portfolio_snapshots VALUES ('2024-01-01','510300','E',1000,1.0,1.1,1100,100,0.1,0.9)")
        c.execute("INSERT INTO portfolio_snapshots VALUES ('2024-01-15','510300','E',1000,1.0,1.2,1200,200,0.2,0.9)")
        c.commit()
        mc.return_value = c
        from tabs._helpers import load_etf_detail
        df, code = load_etf_detail("510300", end_date="2024-01-10")
        assert len(df) == 1
        c.close()

