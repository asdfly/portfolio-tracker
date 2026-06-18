"""Extended tests for data_loader.load_etf_detail (previously in tabs/_helpers.py)."""
import pytest
import pandas as pd
from unittest.mock import patch
import sqlite3

def _clear():
    import streamlit as st
    st.cache_data.clear()

class TestLoadEtfDetail:
    """Tests for load_etf_detail now residing in data_loader module."""
    def setup_method(self): _clear()

    @patch("data_loader.get_db_connection")
    def test_empty(self, mc):
        c = sqlite3.connect(":memory:")
        c.execute("CREATE TABLE portfolio_snapshots (date TEXT,code TEXT,name TEXT,quantity REAL,cost_price REAL,current_price REAL,market_value REAL,pnl REAL,pnl_rate REAL,ytd_return REAL,beta REAL)")
        c.execute("CREATE TABLE etf_technical (date TEXT,code TEXT,rsi_value REAL,rsi_status TEXT,ma_signal TEXT,macd_signal TEXT,trend TEXT,kdj_signal TEXT,bollinger_position REAL,atr_pct REAL)")
        mc.return_value = c
        from data_loader import load_etf_detail
        df, code = load_etf_detail("510300")
        assert df.empty
        c.close()

    @patch("data_loader.get_db_connection")
    def test_with_data(self, mc):
        c = sqlite3.connect(":memory:")
        c.execute("CREATE TABLE portfolio_snapshots (date TEXT,code TEXT,name TEXT,quantity REAL,cost_price REAL,current_price REAL,market_value REAL,pnl REAL,pnl_rate REAL,ytd_return REAL,beta REAL)")
        c.execute("CREATE TABLE etf_technical (date TEXT,code TEXT,rsi_value REAL,rsi_status TEXT,ma_signal TEXT,macd_signal TEXT,trend TEXT,kdj_signal TEXT,bollinger_position REAL,atr_pct REAL)")
        c.execute("INSERT INTO portfolio_snapshots VALUES ('2024-01-01','510300','ETF',1000,1.0,1.1,1100,100,0.1,0.05,0.9)")
        c.execute("INSERT INTO etf_technical VALUES ('2024-01-01','510300',50.0,'正常','多头排列','金叉','震荡整理','中性',50.0,1.5)")
        c.commit()
        mc.return_value = c
        from data_loader import load_etf_detail
        df, code = load_etf_detail("510300")
        assert len(df) == 1
        c.close()

    @patch("data_loader.get_db_connection")
    def test_end_date_filter(self, mc):
        c = sqlite3.connect(":memory:")
        c.execute("CREATE TABLE portfolio_snapshots (date TEXT,code TEXT,name TEXT,quantity REAL,cost_price REAL,current_price REAL,market_value REAL,pnl REAL,pnl_rate REAL,ytd_return REAL,beta REAL)")
        c.execute("CREATE TABLE etf_technical (date TEXT,code TEXT,rsi_value REAL,rsi_status TEXT,ma_signal TEXT,macd_signal TEXT,trend TEXT,kdj_signal TEXT,bollinger_position REAL,atr_pct REAL)")
        c.execute("INSERT INTO portfolio_snapshots VALUES ('2024-01-01','510300','E',1000,1.0,1.1,1100,100,0.1,0.05,0.9)")
        c.execute("INSERT INTO portfolio_snapshots VALUES ('2024-01-15','510300','E',1000,1.0,1.2,1200,200,0.2,0.08,0.9)")
        c.commit()
        mc.return_value = c
        from data_loader import load_etf_detail
        df, code = load_etf_detail("510300", end_date="2024-01-10")
        assert len(df) == 1
        c.close()
