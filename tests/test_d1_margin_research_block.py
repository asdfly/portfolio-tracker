"""Tests for D1: 激活闲置数据分析"""
import pytest
import sqlite3
import pandas as pd
import numpy as np
from unittest.mock import MagicMock, patch
from datetime import datetime

DB_PATH = r"C:\Users\HUAWEI\Documents\lingxi-claw\portfolio_tracker\data\database\portfolio.db"

@pytest.fixture
def advisor():
    mock_db = MagicMock(spec=sqlite3.Connection)
    from src.analysis.advisor import SmartAdvisor
    return SmartAdvisor(mock_db)

@pytest.fixture
def sample_pd():
    return {"positions": [{"code":"510300","name":"沪深300ETF","market_value":48000},{"code":"510500","name":"中证500ETF","market_value":30000}]}

# === Margin Tests ===
class TestMargin:
    def test_exists(self, advisor):
        assert hasattr(advisor, '_analyze_margin_data')

    def test_empty_positions(self, advisor):
        assert advisor._analyze_margin_data({"positions":[]}) == []

    def test_no_data(self, advisor, sample_pd):
        with patch("pandas.read_sql_query", return_value=pd.DataFrame()):
            assert advisor._analyze_margin_data(sample_pd) == []

    def test_insufficient_rows(self, advisor, sample_pd):
        df = pd.DataFrame({"date":pd.to_datetime(["2026-05-29","2026-05-28","2026-05-27"]),"code":["510300"]*3,"name":["沪深300ETF"]*3,"margin_balance":[26e8,27e8,27e8],"margin_buy":[1.15e8,1.38e8,1.39e8],"margin_repay":[0.9e8,1.0e8,1.1e8],"short_volume":[0.68e8,0.65e8,0.71e8],"short_sell":[0.3e8,0.3e8,0.3e8],"short_repay":[0.05e8,0.05e8,0.05e8]})
        with patch("pandas.read_sql_query", return_value=df):
            assert advisor._analyze_margin_data(sample_pd) == []

    def test_surge_triggers_opportunity(self, advisor, sample_pd):
        np.random.seed(42)
        dates = pd.date_range("2026-05-01", periods=20)[::-1]
        b = list(np.random.normal(27e8,0.3e8,15)) + [30e8,32e8,34e8,36e8,40e8]
        df = pd.DataFrame({"date":dates,"code":["510300"]*20,"name":["沪深300ETF"]*20,"margin_balance":b,"margin_buy":[1.3e8]*20,"margin_repay":[1.0e8]*20,"short_volume":[0.68e8]*20,"short_sell":[0.3e8]*20,"short_repay":[0.05e8]*20})
        with patch("pandas.read_sql_query", return_value=df):
            r = advisor._analyze_margin_data(sample_pd)
        from src.analysis.advisor import AdviceType
        assert AdviceType.OPPORTUNITY in [x.type for x in r]

    def test_decline_triggers_caution(self, advisor, sample_pd):
        np.random.seed(42)
        dates = pd.date_range("2026-05-01", periods=20)[::-1]
        b = list(np.random.normal(27e8,0.3e8,15)) + [24e8,22e8,20e8,18e8,15e8]
        df = pd.DataFrame({"date":dates,"code":["510300"]*20,"name":["沪深300ETF"]*20,"margin_balance":b,"margin_buy":[1.3e8]*20,"margin_repay":[1.0e8]*20,"short_volume":[0.68e8]*20,"short_sell":[0.3e8]*20,"short_repay":[0.05e8]*20})
        with patch("pandas.read_sql_query", return_value=df):
            r = advisor._analyze_margin_data(sample_pd)
        from src.analysis.advisor import AdviceType
        assert AdviceType.CAUTION in [x.type for x in r]

    def test_related_codes(self, advisor, sample_pd):
        np.random.seed(42)
        dates = pd.date_range("2026-05-01", periods=20)[::-1]
        b = list(np.random.normal(27e8,0.3e8,15)) + [30e8,32e8,34e8,36e8,40e8]
        df = pd.DataFrame({"date":dates,"code":["510300"]*20,"name":["沪深300ETF"]*20,"margin_balance":b,"margin_buy":[1.3e8]*20,"margin_repay":[1.0e8]*20,"short_volume":[0.68e8]*20,"short_sell":[0.3e8]*20,"short_repay":[0.05e8]*20})
        with patch("pandas.read_sql_query", return_value=df):
            r = advisor._analyze_margin_data(sample_pd)
        assert any("510300" in x.related_codes for x in r)

# === Research Tests ===
class TestResearch:
    def test_exists(self, advisor):
        assert hasattr(advisor, '_analyze_institution_research')

    def test_empty_db(self, advisor, sample_pd):
        with patch("pandas.read_sql_query", return_value=pd.DataFrame()):
            assert advisor._analyze_institution_research(sample_pd) == []

    def test_few_records(self, advisor, sample_pd):
        df = pd.DataFrame({"date":pd.to_datetime(["2026-05-15"]),"code":["000001"],"name":["平安银行"],"institution":["华泰"],"inst_type":["证券"],"receive_method":["电话"],"research_date":["2026-05-14"]})
        with patch("pandas.read_sql_query", return_value=df):
            assert advisor._analyze_institution_research(sample_pd) == []

    def test_hot_triggers(self, advisor, sample_pd):
        rows = [{"date":pd.Timestamp("2026-05-20"),"code":"000001","name":"平安银行","institution":f"机构{i}","inst_type":"证券","receive_method":"调研","research_date":"2026-05-19"} for i in range(25)]
        with patch("pandas.read_sql_query", return_value=pd.DataFrame(rows)):
            r = advisor._analyze_institution_research(sample_pd)
        from src.analysis.advisor import AdviceType
        assert AdviceType.OPPORTUNITY in [x.type for x in r]

    def test_surge_detection(self, advisor, sample_pd):
        rows = []
        for i in range(12):
            rows.append({"date":pd.Timestamp("2026-05-25"),"code":"600036","name":"招商银行","institution":f"机构{i}","inst_type":"基金","receive_method":"调研","research_date":"2026-05-24"})
        for i in range(2):
            rows.append({"date":pd.Timestamp("2026-04-15"),"code":"600036","name":"招商银行","institution":f"老机构{i}","inst_type":"基金","receive_method":"调研","research_date":"2026-04-14"})
        with patch("pandas.read_sql_query", return_value=pd.DataFrame(rows)):
            r = advisor._analyze_institution_research(sample_pd)
        assert any("骤增" in x.title for x in r)

# === Block Trade Tests ===
class TestBlockTrade:
    def test_exists(self, advisor):
        assert hasattr(advisor, '_analyze_block_trade')

    def test_empty_db(self, advisor, sample_pd):
        with patch("pandas.read_sql_query", return_value=pd.DataFrame()):
            assert advisor._analyze_block_trade(sample_pd) == []

    def test_no_premium(self, advisor, sample_pd):
        df = pd.DataFrame({"date":pd.to_datetime(["2026-05-29"]),"code":["000001"],"name":["平安银行"],"change_pct":[1.5],"close":[12.0],"trade_price":[12.0],"premium_rate":[0.0],"volume":[100000],"amount":[1200000],"amount_to_float_mv":[0.005],"buyer_broker":["华泰"]})
        with patch("pandas.read_sql_query", return_value=df):
            assert advisor._analyze_block_trade(sample_pd) == []

    def test_premium_triggers(self, advisor, sample_pd):
        rows = [{"date":pd.Timestamp("2026-05-29"),"code":"000001","name":"平安银行","change_pct":2.0,"close":12.0,"trade_price":13.0,"premium_rate":0.08,"volume":500000,"amount":6500000,"amount_to_float_mv":0.02,"buyer_broker":f"机构{i}"} for i in range(3)]
        for i in range(10):
            rows.append({"date":pd.Timestamp("2026-05-28"),"code":"000001","name":"平安银行","change_pct":1.0,"close":12.0,"trade_price":13.0,"premium_rate":0.08,"volume":1000000,"amount":13000000,"amount_to_float_mv":0.03,"buyer_broker":f"机构{i}"})
        with patch("pandas.read_sql_query", return_value=pd.DataFrame(rows)):
            r = advisor._analyze_block_trade(sample_pd)
        from src.analysis.advisor import AdviceType
        assert AdviceType.OPPORTUNITY in [x.type for x in r]

    def test_discount_triggers_caution(self, advisor, sample_pd):
        rows = [{"date":pd.Timestamp("2026-05-29"),"code":"600036","name":"招商银行","change_pct":-1.0,"close":35.0,"trade_price":32.0,"premium_rate":-0.086,"volume":1000000,"amount":32000000,"amount_to_float_mv":0.01,"buyer_broker":f"机构{i}"} for i in range(10)]
        with patch("pandas.read_sql_query", return_value=pd.DataFrame(rows)):
            r = advisor._analyze_block_trade(sample_pd)
        from src.analysis.advisor import AdviceType
        assert AdviceType.CAUTION in [x.type for x in r]

# === Integration: real DB ===
class TestD1Integration:
    """使用真实数据库的集成测试"""
    def test_margin_real_db(self):
        import sqlite3
        db = sqlite3.connect(DB_PATH)
        from src.analysis.advisor import SmartAdvisor
        advisor = SmartAdvisor(db)
        pd_data = {"positions": [{"code":"510300","name":"沪深300ETF","market_value":48000}]}
        result = advisor._analyze_margin_data(pd_data)
        db.close()
        assert isinstance(result, list)
        # 真实数据不强制断言内容，只验证不崩溃
        from src.analysis.advisor import InvestmentAdvice
        assert all(isinstance(x, InvestmentAdvice) for x in result)

    def test_research_real_db(self):
        import sqlite3
        db = sqlite3.connect(DB_PATH)
        from src.analysis.advisor import SmartAdvisor
        advisor = SmartAdvisor(db)
        pd_data = {"positions": [{"code":"510300","name":"沪深300ETF","market_value":48000}]}
        result = advisor._analyze_institution_research(pd_data)
        db.close()
        assert isinstance(result, list)

    def test_block_real_db(self):
        import sqlite3
        db = sqlite3.connect(DB_PATH)
        from src.analysis.advisor import SmartAdvisor
        advisor = SmartAdvisor(db)
        pd_data = {"positions": [{"code":"510300","name":"沪深300ETF","market_value":48000}]}
        result = advisor._analyze_block_trade(pd_data)
        db.close()
        assert isinstance(result, list)
