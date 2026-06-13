"""P2-F tests: pre_post_market module pure functions and data_loader integration."""
import pytest
from dataclasses import dataclass
from typing import List, Dict, Tuple, Optional


class TestClassifyMacroChange:
    """Test _classify_macro_change edge cases."""

    def _import_func(self):
        from src.analysis.pre_post_market import _classify_macro_change
        return _classify_macro_change

    def test_usd_cny_warning(self):
        f = self._import_func()
        level, desc = f("USD_CNY", 7.35, -0.5)
        assert level == "warning"
        assert "7.35" in desc

    def test_usd_cny_caution(self):
        f = self._import_func()
        level, desc = f("USD_CNY", 7.25, -0.3)
        assert level == "caution"
        assert "7.25" in desc

    def test_usd_cny_info(self):
        f = self._import_func()
        level, desc = f("USD_CNY", 7.10, -0.1)
        assert level == "info"
        assert desc == ""

    def test_shibor_warning(self):
        f = self._import_func()
        level, desc = f("SHIBOR_ON", 3.0, 0.5)
        assert level == "warning"
        assert "3.0" in desc or "3.000" in desc

    def test_shibor_caution(self):
        f = self._import_func()
        level, desc = f("SHIBOR_ON", 2.5, 0.2)
        assert level == "caution"

    def test_shibor_info(self):
        f = self._import_func()
        level, desc = f("SHIBOR_ON", 2.0, 0.1)
        assert level == "info"

    def test_us_10y_warning(self):
        f = self._import_func()
        level, desc = f("US_10Y_BOND", 4.75, 0.1)
        assert level == "warning"
        assert "4.75" in desc

    def test_us_10y_caution(self):
        f = self._import_func()
        level, desc = f("USIBOR_ON", 4.5, 0.1)
        # This code is not SHIBOR_ON, falls through
        assert level == "info"

    def test_comex_gold_info(self):
        f = self._import_func()
        level, desc = f("COMEX_GOLD", 2900, 1.0)
        assert level == "info"
        assert "避险" in desc

    def test_comex_oil_warning(self):
        f = self._import_func()
        level, desc = f("COMEX_OIL", 105, 2.0)
        assert level == "warning"
        assert "通胀" in desc

    def test_comex_oil_caution(self):
        f = self._import_func()
        level, desc = f("COMEX_OIL", 55, -5.0)
        assert level == "caution"
        assert "60" in desc

    def test_comex_oil_info(self):
        f = self._import_func()
        level, desc = f("COMEX_OIL", 80, 0.5)
        assert level == "info"

    def test_unknown_code(self):
        f = self._import_func()
        level, desc = f("UNKNOWN_CODE", 42.0, 5.0)
        assert level == "info"
        assert desc == ""

    def test_boundary_usd_cny_exact(self):
        f = self._import_func()
        # Exactly 7.35 => warning
        level, _ = f("USD_CNY", 7.35, -0.5)
        assert level == "warning"

    def test_boundary_usd_cny_just_below(self):
        f = self._import_func()
        # Just below 7.35 but >= 7.25 => caution
        level, _ = f("USD_CNY", 7.34, -0.5)
        assert level == "caution"


class TestComposePreSummary:
    """Test _compose_pre_summary with mock report."""

    def _import_func(self):
        from src.analysis.pre_post_market import _compose_pre_summary
        return _compose_pre_summary

    def _make_index_change(self, code, name, pct):
        from src.analysis.pre_post_market import IndexChange
        return IndexChange(code=code, name=name, close=0.0, change_pct=pct, date="2026-01-01")

    def test_empty_report(self):
        f = self._import_func()
        from src.analysis.pre_post_market import PreMarketReport
        r = PreMarketReport(
            report_time="09:00", report_date="2026-01-01",
            index_changes=[], macro_alerts=[], etf_signals=[],
            news_sentiment={}, risk_warnings=[], summary_text=""
        )
        result = f(r)
        assert "平稳" in result

    def test_major_index_bullish(self):
        f = self._import_func()
        from src.analysis.pre_post_market import PreMarketReport
        r = PreMarketReport(
            report_time="09:00", report_date="2026-01-01",
            index_changes=[
                self._make_index_change("sh000300", "沪深300", 0.8),
                self._make_index_change("sz399001", "深证成指", 1.0),
            ],
            macro_alerts=[], etf_signals=[],
            news_sentiment={}, risk_warnings=[], summary_text=""
        )
        result = f(r)
        assert "偏多" in result

    def test_major_index_bearish(self):
        f = self._import_func()
        from src.analysis.pre_post_market import PreMarketReport
        r = PreMarketReport(
            report_time="09:00", report_date="2026-01-01",
            index_changes=[
                self._make_index_change("sh000300", "沪深300", -0.8),
                self._make_index_change("sz399001", "深证成指", -0.5),
            ],
            macro_alerts=[], etf_signals=[],
            news_sentiment={}, risk_warnings=[], summary_text=""
        )
        result = f(r)
        assert "偏空" in result

    def test_major_index_neutral(self):
        f = self._import_func()
        from src.analysis.pre_post_market import PreMarketReport
        r = PreMarketReport(
            report_time="09:00", report_date="2026-01-01",
            index_changes=[
                self._make_index_change("sh000300", "沪深300", 0.1),
                self._make_index_change("sz399001", "深证成指", -0.1),
            ],
            macro_alerts=[], etf_signals=[],
            news_sentiment={}, risk_warnings=[], summary_text=""
        )
        result = f(r)
        assert "中性" in result

    def test_risk_warnings_count(self):
        f = self._import_func()
        from src.analysis.pre_post_market import PreMarketReport, MacroAlert
        r = PreMarketReport(
            report_time="09:00", report_date="2026-01-01",
            index_changes=[],
            macro_alerts=[MacroAlert(indicator_name="USD_CNY", indicator_code="USD_CNY", current_value=7.35, previous_value=7.30, change_pct=-0.7, alert_level="warning", description="test")],
            etf_signals=[],
            news_sentiment={},
            risk_warnings=[{"code": "A"}, {"code": "B"}],
            summary_text=""
        )
        result = f(r)
        assert "2只ETF" in result

    def test_news_sentiment_positive(self):
        f = self._import_func()
        from src.analysis.pre_post_market import PreMarketReport
        r = PreMarketReport(
            report_time="09:00", report_date="2026-01-01",
            index_changes=[], macro_alerts=[], etf_signals=[],
            news_sentiment={"total": 100, "positive": 95, "negative": 2, "positive_ratio": 0.95, "negative_ratio": 0.02},
            risk_warnings=[], summary_text=""
        )
        result = f(r)
        assert "偏正面" in result

    def test_news_sentiment_negative(self):
        f = self._import_func()
        from src.analysis.pre_post_market import PreMarketReport
        r = PreMarketReport(
            report_time="09:00", report_date="2026-01-01",
            index_changes=[], macro_alerts=[], etf_signals=[],
            news_sentiment={"total": 100, "positive": 10, "negative": 50, "positive_ratio": 0.1, "negative_ratio": 0.5},
            risk_warnings=[], summary_text=""
        )
        result = f(r)
        assert "偏负面" in result

    def test_news_zero_total(self):
        f = self._import_func()
        from src.analysis.pre_post_market import PreMarketReport
        r = PreMarketReport(
            report_time="09:00", report_date="2026-01-01",
            index_changes=[], macro_alerts=[], etf_signals=[],
            news_sentiment={"total": 0, "positive": 0, "negative": 0, "positive_ratio": 0, "negative_ratio": 0},
            risk_warnings=[], summary_text=""
        )
        result = f(r)
        assert "平稳" in result


class TestComposePostSummary:
    """Test _compose_post_summary with mock report."""

    def _import_func(self):
        from src.analysis.pre_post_market import _compose_post_summary
        return _compose_post_summary

    def test_empty_report(self):
        f = self._import_func()
        from src.analysis.pre_post_market import PostMarketReport
        r = PostMarketReport(
            report_time="15:30", report_date="2026-01-01",
            portfolio_pnl=None, pnl_attribution=[], signal_changes=[],
            fund_flow_changes=[], news_highlights=[], summary_text=""
        )
        result = f(r)
        assert "平稳" in result

    def test_profit_summary(self):
        f = self._import_func()
        from src.analysis.pre_post_market import PostMarketReport
        r = PostMarketReport(
            report_time="15:30", report_date="2026-01-01",
            portfolio_pnl={"total_pnl": 10000, "total_return_pct": 2.5, "profit_count": 5, "loss_count": 2, "win_rate": 71},
            pnl_attribution=[], signal_changes=[], fund_flow_changes=[],
            news_highlights=[], summary_text=""
        )
        result = f(r)
        assert "盈利" in result
        assert "10,000" in result
        assert "+2.50%" in result

    def test_loss_summary(self):
        f = self._import_func()
        from src.analysis.pre_post_market import PostMarketReport
        r = PostMarketReport(
            report_time="15:30", report_date="2026-01-01",
            portfolio_pnl={"total_pnl": -5000, "total_return_pct": -1.2, "profit_count": 1, "loss_count": 5, "win_rate": 17},
            pnl_attribution=[], signal_changes=[], fund_flow_changes=[],
            news_highlights=[], summary_text=""
        )
        result = f(r)
        assert "亏损" in result
        assert "5,000" in result

    def test_fund_flow_divergence(self):
        f = self._import_func()
        from src.analysis.pre_post_market import PostMarketReport
        r = PostMarketReport(
            report_time="15:30", report_date="2026-01-01",
            portfolio_pnl=None, pnl_attribution=[],
            signal_changes=[],
            fund_flow_changes=[
                {"code": "A", "flow_change": 100},
                {"code": "B", "flow_change": -50},
            ],
            news_highlights=[], summary_text=""
        )
        result = f(r)
        assert "资金分化" in result

    def test_fund_flow_all_inflow(self):
        f = self._import_func()
        from src.analysis.pre_post_market import PostMarketReport
        r = PostMarketReport(
            report_time="15:30", report_date="2026-01-01",
            portfolio_pnl=None, pnl_attribution=[],
            signal_changes=[],
            fund_flow_changes=[{"code": "A", "flow_change": 100}, {"code": "B", "flow_change": 50}],
            news_highlights=[], summary_text=""
        )
        result = f(r)
        assert "资金偏多" in result

    def test_signal_changes(self):
        f = self._import_func()
        from src.analysis.pre_post_market import PostMarketReport
        r = PostMarketReport(
            report_time="15:30", report_date="2026-01-01",
            portfolio_pnl=None, pnl_attribution=[],
            signal_changes=[{"code": "A", "changes": []}, {"code": "B", "changes": []}],
            fund_flow_changes=[], news_highlights=[], summary_text=""
        )
        result = f(r)
        assert "2只ETF" in result
        assert "技术信号变化" in result

    def test_negative_news_highlights(self):
        f = self._import_func()
        from src.analysis.pre_post_market import PostMarketReport
        r = PostMarketReport(
            report_time="15:30", report_date="2026-01-01",
            portfolio_pnl=None, pnl_attribution=[],
            signal_changes=[],
            fund_flow_changes=[],
            news_highlights=[
                {"sentiment": "负面", "title": "a"},
                {"sentiment": "负面", "title": "b"},
                {"sentiment": "负面", "title": "c"},
                {"sentiment": "正面", "title": "d"},
            ],
            summary_text=""
        )
        result = f(r)
        assert "负面新闻较多" in result
        assert "3条" in result

    def test_negative_news_below_threshold(self):
        f = self._import_func()
        from src.analysis.pre_post_market import PostMarketReport
        r = PostMarketReport(
            report_time="15:30", report_date="2026-01-01",
            portfolio_pnl=None, pnl_attribution=[],
            signal_changes=[],
            fund_flow_changes=[],
            news_highlights=[{"sentiment": "负面", "title": "a"}, {"sentiment": "负面", "title": "b"}],
            summary_text=""
        )
        result = f(r)
        assert "负面新闻较多" not in result


class TestDataclasses:
    """Test pre_post_market dataclass definitions."""

    def test_index_change(self):
        from src.analysis.pre_post_market import IndexChange
        ic = IndexChange(code="sh000300", name="沪深300", close=4000.0, change_pct=0.5, date="2026-01-01")
        assert ic.code == "sh000300"
        assert ic.name == "沪深300"
        assert ic.change_pct == 0.5

    def test_macro_alert(self):
        from src.analysis.pre_post_market import MacroAlert
        ma = MacroAlert(indicator_name="USD_CNY", indicator_code="USD_CNY", current_value=7.35, previous_value=7.30, change_pct=-0.7, alert_level="warning", description="test")
        assert ma.alert_level == "warning"

    def test_etf_signal_preview(self):
        from src.analysis.pre_post_market import EtfSignalPreview
        es = EtfSignalPreview(
            code="159915", name="创业板ETF", trend="上升",
            ma_signal="多头排列", macd_signal="金叉", rsi_value=55.0, rsi_status="中性", signal_score=70, risk_score=30, fund_flow_net=1000
        )
        assert es.code == "159915"
        assert es.signal_score == 70

    def test_pre_market_report(self):
        from src.analysis.pre_post_market import PreMarketReport
        r = PreMarketReport(
            report_time="09:00", report_date="2026-01-01",
            index_changes=[], macro_alerts=[], etf_signals=[],
            news_sentiment={}, risk_warnings=[], summary_text="test"
        )
        assert r.summary_text == "test"

    def test_post_market_report(self):
        from src.analysis.pre_post_market import PostMarketReport
        r = PostMarketReport(
            report_time="15:30", report_date="2026-01-01",
            portfolio_pnl=None, pnl_attribution=[], signal_changes=[],
            fund_flow_changes=[], news_highlights=[], summary_text="test"
        )
        assert r.summary_text == "test"


class TestDataLoaderIntegration:
    """Test data_loader load functions exist and handle missing data gracefully."""

    def test_load_pre_market_report_exists(self):
        from data_loader import load_pre_market_report
        assert callable(load_pre_market_report)

    def test_load_post_market_report_exists(self):
        from data_loader import load_post_market_report
        assert callable(load_post_market_report)

    def test_load_pre_market_report_returns_none_on_error(self):
        from data_loader import load_pre_market_report
        # Should return None or a valid object, not raise
        result = load_pre_market_report()
        assert result is None or hasattr(result, "summary_text")

    def test_load_post_market_report_returns_none_on_error(self):
        from data_loader import load_post_market_report
        result = load_post_market_report()
        assert result is None or hasattr(result, "summary_text")
