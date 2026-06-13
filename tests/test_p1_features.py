"""P1 功能纯函数测试

测试 etf_risk_scan、trade_importer、research 模块的核心计算逻辑。
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime


class TestEtfRiskScan:
    """etf_risk_scan 模块测试。"""

    def test_score_volatility_high_atr(self):
        from src.analysis.etf_risk_scan import _score_volatility
        row = pd.Series({"atr_pct": 6.0, "bollinger_position": 95.0})
        result = _score_volatility(row)
        assert 0 <= result["score"] <= 100
        assert result["score"] >= 70  # high ATR should give high score
        assert "极高" in result["detail"]

    def test_score_volatility_low_atr(self):
        from src.analysis.etf_risk_scan import _score_volatility
        row = pd.Series({"atr_pct": 0.5, "bollinger_position": 50.0})
        result = _score_volatility(row)
        assert result["score"] < 50

    def test_score_volatility_with_hist_prices(self):
        from src.analysis.etf_risk_scan import _score_volatility
        row = pd.Series({"atr_pct": 2.0, "bollinger_position": 50.0})
        prices = pd.Series(np.random.randn(100).cumsum() + 100)
        result = _score_volatility(row, prices)
        assert "weight" in result
        assert result["weight"] == 0.25

    def test_score_discount_deep_discount(self):
        from src.analysis.etf_risk_scan import _score_discount
        row = pd.Series({"discount_rate": -2.0, "volume_ratio": 1.0, "turnover_rate": 3.0})
        result = _score_discount(row)
        assert result["score"] >= 80
        assert "大幅折价" in result["detail"]

    def test_score_discount_normal(self):
        from src.analysis.etf_risk_scan import _score_discount
        row = pd.Series({"discount_rate": 0.1, "volume_ratio": 1.0, "turnover_rate": 2.0})
        result = _score_discount(row)
        assert 30 <= result["score"] <= 60

    def test_score_liquidity_high_volume(self):
        from src.analysis.etf_risk_scan import _score_liquidity
        fund = pd.Series({"amount": 5e9, "shares": 5e9})
        tech = pd.Series({})
        result = _score_liquidity(fund, tech)
        assert result["score"] < 30  # High volume = low risk

    def test_score_liquidity_low_volume(self):
        from src.analysis.etf_risk_scan import _score_liquidity
        fund = pd.Series({"amount": 1e6, "shares": 1e6})
        tech = pd.Series({})
        result = _score_liquidity(fund, tech)
        assert result["score"] >= 70  # Low volume = high risk

    def test_score_liquidity_mini_fund(self):
        from src.analysis.etf_risk_scan import _score_liquidity
        fund = pd.Series({"amount": 1e8, "shares": 3e7})
        tech = pd.Series({})
        result = _score_liquidity(fund, tech)
        assert result["score"] >= 80  # Mini fund = high risk

    def test_score_downside_all_bearish(self):
        from src.analysis.etf_risk_scan import _score_downside
        row = pd.Series({"ma_signal": "空头排列", "macd_signal": "死叉",
                         "kdj_signal": "死叉", "trend": "下降趋势"})
        result = _score_downside(row)
        assert result["score"] >= 85
        assert "空头共振" in result["detail"]

    def test_score_downside_all_bullish(self):
        from src.analysis.etf_risk_scan import _score_downside
        row = pd.Series({"ma_signal": "多头排列", "macd_signal": "金叉",
                         "kdj_signal": "金叉", "trend": "上升趋势"})
        result = _score_downside(row)
        assert result["score"] <= 20

    def test_score_downside_mixed(self):
        from src.analysis.etf_risk_scan import _score_downside
        row = pd.Series({"ma_signal": "空头", "macd_signal": "中性",
                         "kdj_signal": "金叉", "trend": "震荡整理"})
        result = _score_downside(row)
        assert 30 <= result["score"] <= 70
        assert "多空分歧" in result["detail"]

    def test_score_deviation_normal_iopv(self):
        from src.analysis.etf_risk_scan import _score_deviation
        fund = pd.Series({"iopv": 1.0, "price": 1.001})
        result = _score_deviation(fund)
        assert result["score"] < 55

    def test_score_deviation_high_iopv_deviation(self):
        from src.analysis.etf_risk_scan import _score_deviation
        fund = pd.Series({"iopv": 1.0, "price": 1.02})
        result = _score_deviation(fund)
        assert result["score"] >= 65

    def test_score_deviation_with_beta(self):
        from src.analysis.etf_risk_scan import _score_deviation
        fund = pd.Series({"iopv": 1.0, "price": 1.001, "beta": 2.5})
        result = _score_deviation(fund)
        assert "高偏离" in result["detail"]

    def test_compute_etf_risk_scan_full(self):
        from src.analysis.etf_risk_scan import compute_etf_risk_scan
        tech = pd.Series({"atr_pct": 3.0, "bollinger_position": 50.0,
                          "ma_signal": "空头", "macd_signal": "中性",
                          "kdj_signal": "中性", "trend": "震荡整理"})
        fund = pd.Series({"discount_rate": 0.1, "volume_ratio": 1.0,
                          "turnover_rate": 2.0, "amount": 5e8, "shares": 5e8,
                          "iopv": 1.0, "price": 1.001})
        result = compute_etf_risk_scan("510300", tech, fund)
        assert "total_score" in result
        assert "risk_level" in result
        assert "grade" in result
        assert "summary" in result
        assert 0 <= result["total_score"] <= 100
        assert len(result["dimensions"]) == 5

    def test_compute_etf_risk_scan_empty(self):
        from src.analysis.etf_risk_scan import compute_etf_risk_scan
        result = compute_etf_risk_scan("999999")
        assert 40 <= result["total_score"] <= 60  # Neutral range with empty data

    def test_compute_all_etf_risk_scans_batch(self):
        from src.analysis.etf_risk_scan import compute_all_etf_risk_scans
        tech_df = pd.DataFrame({
            "code": ["A", "B"], "atr_pct": [2.0, 4.0],
            "bollinger_position": [50, 50],
            "ma_signal": ["中性", "空头"], "macd_signal": ["中性", "死叉"],
            "kdj_signal": ["中性", "死叉"], "trend": ["震荡整理", "下降趋势"],
        })
        fund_df = pd.DataFrame({
            "code": ["A", "B"], "discount_rate": [0.1, -0.5],
            "amount": [5e8, 5e7], "shares": [5e8, 5e7],
            "iopv": [1.0, 0.5], "price": [1.001, 0.502],
            "volume_ratio": [1.0, 1.0], "turnover_rate": [2.0, 1.0],
        })
        result = compute_all_etf_risk_scans(tech_df, fund_df)
        assert len(result) == 2
        assert "total_score" in result.columns

    def test_compute_all_empty_input(self):
        from src.analysis.etf_risk_scan import compute_all_etf_risk_scans
        result = compute_all_etf_risk_scans(pd.DataFrame(), pd.DataFrame())
        assert result.empty


class TestTradeImporter:
    """trade_importer 模块测试。"""

    def test_normalize_direction_buy(self):
        from src.utils.trade_importer import normalize_direction
        assert normalize_direction("买入") == "BUY"
        assert normalize_direction("BUY") == "BUY"
        assert normalize_direction("买") == "BUY"
        assert normalize_direction("证券买入") == "BUY"
        assert normalize_direction(None) is None
        assert normalize_direction("其他") is None

    def test_normalize_direction_sell(self):
        from src.utils.trade_importer import normalize_direction
        assert normalize_direction("卖出") == "SELL"
        assert normalize_direction("SELL") == "SELL"
        assert normalize_direction("卖") == "SELL"

    def test_normalize_code(self):
        from src.utils.trade_importer import normalize_code
        assert normalize_code("510300") == "510300"
        assert normalize_code("510300.SH") == "510300"
        assert normalize_code("159300") == "159300"

    def test_normalize_date(self):
        from src.utils.trade_importer import normalize_date
        assert normalize_date("2026-01-15") == "2026-01-15"
        assert normalize_date("2026/01/15") == "2026-01-15"
        assert normalize_date("20260115") == "2026-01-15"

    def test_import_from_dataframe_cms_format(self):
        from src.utils.trade_importer import import_from_dataframe
        df = pd.DataFrame({
            "发生日期": ["2026-01-15", "2026-02-20"],
            "证券代码": ["510300", "510300"],
            "证券名称": ["沪深300ETF", "沪深300ETF"],
            "操作": ["买入", "卖出"],
            "成交均价": [4.5, 5.0],
            "成交数量": [1000, 1000],
            "手续费": [5.0, 5.0],
            "备注": ["test", ""],
        })
        result = import_from_dataframe(df, dry_run=True)
        assert result["success"] == 2
        assert result["failed"] == 0
        assert len(result["preview"]) == 2

    def test_import_from_dataframe_empty(self):
        from src.utils.trade_importer import import_from_dataframe
        result = import_from_dataframe(pd.DataFrame())
        assert result["success"] == 0

    def test_import_from_dataframe_missing_columns(self):
        from src.utils.trade_importer import import_from_dataframe
        df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        result = import_from_dataframe(df)
        assert result["success"] == 0
        assert len(result["errors"]) > 0

    def test_trade_analysis_fifo(self):
        from src.utils.trade_importer import compute_trade_analysis
        # This needs DB; test with empty result
        result = compute_trade_analysis("999999")
        assert result == {}


class TestResearchModule:
    """research 模块测试。"""

    def test_match_news_to_keywords(self):
        from src.data_sources.research import _match_news_to_keywords
        assert _match_news_to_keywords("半导体行业复苏在即", ["半导体", "芯片"]) == 1
        assert _match_news_to_keywords("芯片半导体双双大涨", ["半导体", "芯片"]) == 2
        assert _match_news_to_keywords("银行板块震荡", ["半导体", "芯片"]) == 0
        assert _match_news_to_keywords(None, ["半导体"]) == 0

    def test_etf_industry_keywords_exist(self):
        from src.data_sources.research import ETF_INDUSTRY_KEYWORDS
        assert isinstance(ETF_INDUSTRY_KEYWORDS, dict)
        # Key ETFs should have keywords
        assert "588000" in ETF_INDUSTRY_KEYWORDS
        assert "512010" in ETF_INDUSTRY_KEYWORDS
        assert len(ETF_INDUSTRY_KEYWORDS["588000"]) >= 3

    def test_sector_keywords_exist(self):
        from src.data_sources.research import SECTOR_KEYWORDS
        assert isinstance(SECTOR_KEYWORDS, dict)
        assert "军工" in SECTOR_KEYWORDS
        assert "医药" in SECTOR_KEYWORDS

    def test_load_etf_industry_news_empty(self):
        from src.data_sources.research import load_etf_industry_news
        # With no matching data, should return empty
        result = load_etf_industry_news("999999", days=1)
        assert isinstance(result, pd.DataFrame)

    def test_load_sector_sentiment_empty(self):
        from src.data_sources.research import load_sector_sentiment
        result = load_sector_sentiment("999999", days=1)
        assert result["news_count"] == 0
        assert result["avg_sentiment"] == 0
        assert result["top_headlines"] == []
