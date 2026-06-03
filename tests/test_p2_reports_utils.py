"""P2 tests: report modules and utils."""
import pytest
import os
import tempfile

class TestRiskReportGenerator:
    def test_import(self):
        from src.report.risk_report import RiskReportGenerator
        assert RiskReportGenerator is not None

    def test_init(self):
        from src.report.risk_report import RiskReportGenerator
        rg = RiskReportGenerator()
        assert rg is not None

    def test_generate_risk_summary(self):
        from src.report.risk_report import RiskReportGenerator
        rg = RiskReportGenerator()
        risk_results = {"total_value":200000,"max_drawdown":0.15,"sharpe_ratio":1.2,"volatility":0.18}
        result = rg.generate_risk_summary(risk_results)
        assert result is not None

    def test_generate_risk_json(self):
        from src.report.risk_report import RiskReportGenerator
        rg = RiskReportGenerator()
        risk_results = {"total_value":200000,"max_drawdown":0.15}
        result = rg.generate_risk_json(risk_results)
        assert result is not None


class TestExcelReportGenerator:
    def test_import(self):
        from src.report.excel_report import ExcelReportGenerator
        assert ExcelReportGenerator is not None

    def test_init(self):
        from src.report.excel_report import ExcelReportGenerator
        eg = ExcelReportGenerator(db_path=":memory:")
        assert eg is not None


class TestEmailReportBuilder:
    def test_import(self):
        from src.utils.email_report import EmailReportBuilder
        assert EmailReportBuilder is not None

    def test_init(self):
        from src.utils.email_report import EmailReportBuilder
        eb = EmailReportBuilder(db_path=":memory:")
        assert eb is not None

    def test_build_alert_email(self):
        from src.utils.email_report import EmailReportBuilder
        eb = EmailReportBuilder(db_path=":memory:")
        alerts = [{"rule_name":"test","level":"warning","message":"test alert","created_at":"2026-06-01"}]
        result = eb.build_alert_email(alerts)
        assert result is not None
        assert isinstance(result, str)
        assert len(result) > 0

    def test_save_report(self):
        from src.utils.email_report import EmailReportBuilder
        eb = EmailReportBuilder(db_path=r"C:\Users\HUAWEI\Documents\lingxi-claw\portfolio_tracker\data\database\portfolio.db")
        result = eb.save_report("<h1>Test</h1>", "test_p2_report.html")
        assert result is not None and len(result) > 0


class TestHistoricalDataBackfiller:
    def test_import(self):
        from src.utils.backfill import HistoricalDataBackfiller
        assert HistoricalDataBackfiller is not None


class TestBacktest:
    def test_import(self):
        from src.analysis.backtest import StrategyBacktester
        assert StrategyBacktester is not None

    def test_init(self):
        import sqlite3
        from src.analysis.backtest import StrategyBacktester
        conn = sqlite3.connect(":memory:")
        sb = StrategyBacktester(conn)
        assert sb is not None
        conn.close()

    def test_calculate_returns(self):
        import sqlite3
        from src.analysis.backtest import StrategyBacktester
        conn = sqlite3.connect(":memory:")
        sb = StrategyBacktester(conn)
        import pandas as pd
        prices = pd.DataFrame({"close":[float(x) for x in range(100,120)]})
        result = sb.calculate_returns(prices)
        assert result is not None
        conn.close()

    def test_result_to_dict(self):
        import sqlite3
        from src.analysis.backtest import StrategyBacktester, BacktestResult
        conn = sqlite3.connect(":memory:")
        sb = StrategyBacktester(conn)
        br = BacktestResult(strategy="buy_hold", start_date="2026-01-01", end_date="2026-06-01",
            initial_value=100000, final_value=110000, total_return=0.1, annualized_return=0.2,
            volatility=0.15, sharpe_ratio=1.0, max_drawdown=0.05, calmar_ratio=0.8,
            rebalance_count=0, turnover=0.0, trades=[], daily_values=[])
        result = sb._result_to_dict(br)
        assert isinstance(result, dict)
        conn.close()


class TestPortfolioAnalyzer:
    def test_import(self):
        from src.analysis.portfolio import PortfolioAnalyzer
        assert PortfolioAnalyzer is not None

    def test_init(self):
        from src.analysis.portfolio import PortfolioAnalyzer
        pa = PortfolioAnalyzer()
        assert pa is not None


class TestFactorAttribution:
    def test_import(self):
        from src.analysis.factor_attribution import compute_factor_attribution, compute_style_exposure
        assert callable(compute_factor_attribution)
        assert callable(compute_style_exposure)

    def test_compute_style_exposure(self):
        from src.analysis.factor_attribution import compute_style_exposure
        import pandas as pd
        positions = pd.DataFrame([{"code":"510300","name":"沪深300ETF","weight":0.5,"market_value":50000},{"code":"159915","name":"创业板ETF","weight":0.5,"market_value":50000}])
        etf_categories = {"510300":{"sector":"金融","style":"value","size":"large"},"159915":{"sector":"科技","style":"growth","size":"small"}}
        result = compute_style_exposure(positions, etf_categories)
        assert isinstance(result, dict)
