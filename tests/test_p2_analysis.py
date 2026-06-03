"""P2 tests: analysis modules."""
import pytest
import numpy as np
import pandas as pd

class TestTechnicalAnalyzer:
    def test_import(self):
        from src.analysis.technical import TechnicalAnalyzer
        assert TechnicalAnalyzer is not None

    def test_init_default(self):
        from src.analysis.technical import TechnicalAnalyzer
        ta = TechnicalAnalyzer({})
        assert ta is not None

    def test_calculate_ma(self):
        from src.analysis.technical import TechnicalAnalyzer
        ta = TechnicalAnalyzer({})
        closes = np.array([10,11,12,13,14,15,16,17,18,19,20], dtype=float)
        result = ta.calculate_ma(closes, fast=5, slow=10)
        assert isinstance(result, dict)
        assert len(result) > 0

    def test_calculate_rsi(self):
        from src.analysis.technical import TechnicalAnalyzer
        ta = TechnicalAnalyzer({})
        closes = np.array([10,11,12,13,14,15,16,17,18,19,20,21,22,23,24], dtype=float)
        result = ta.calculate_rsi(closes, period=14)
        assert isinstance(result, dict)

    def test_calculate_rsi_insufficient(self):
        from src.analysis.technical import TechnicalAnalyzer
        ta = TechnicalAnalyzer({})
        closes = np.array([10, 11, 12], dtype=float)
        result = ta.calculate_rsi(closes, period=14)
        assert result == {}

    def test_calculate_bollinger(self):
        from src.analysis.technical import TechnicalAnalyzer
        ta = TechnicalAnalyzer({})
        closes = np.array([10,12,11,13,14,12,15,16,14,17,18,16,19,20,18,19,21,20,22,23], dtype=float)
        result = ta.calculate_bollinger(closes, period=20, num_std=2)
        assert isinstance(result, dict)
        assert len(result) > 0

    def test_calculate_bollinger_insufficient(self):
        from src.analysis.technical import TechnicalAnalyzer
        ta = TechnicalAnalyzer({})
        closes = np.array([10, 11, 12], dtype=float)
        result = ta.calculate_bollinger(closes, period=20, num_std=2)
        assert result == {}

    def test_calculate_atr(self):
        from src.analysis.technical import TechnicalAnalyzer
        ta = TechnicalAnalyzer({})
        n = 20
        highs = np.linspace(15, 25, n)
        lows = np.linspace(8, 12, n)
        closes = np.linspace(10, 20, n)
        result = ta.calculate_atr(highs, lows, closes, period=14)
        assert isinstance(result, dict)

    def test_calculate_atr_insufficient(self):
        from src.analysis.technical import TechnicalAnalyzer
        ta = TechnicalAnalyzer({})
        result = ta.calculate_atr(np.array([12,13]), np.array([8,9]), np.array([10,11]), period=14)
        assert result == {}

    def test_calculate_volume_ma(self):
        from src.analysis.technical import TechnicalAnalyzer
        ta = TechnicalAnalyzer({})
        v = np.array([1000,1200,1100,1300,1400,1200,1500,1100,1300,1200,1400,1500,1300,1200,1100,1400,1500,1300,1200,1400], dtype=float)
        result = ta.calculate_volume_ma(v, fast=5, slow=20)
        assert isinstance(result, dict)
        assert len(result) > 0

    def test_calculate_volume_ma_insufficient(self):
        from src.analysis.technical import TechnicalAnalyzer
        ta = TechnicalAnalyzer({})
        result = ta.calculate_volume_ma(np.array([1000,1200]), fast=5, slow=20)
        assert result == {}

    def test_analyze_trend(self):
        from src.analysis.technical import TechnicalAnalyzer
        ta = TechnicalAnalyzer({})
        closes = np.arange(20, 40, dtype=float)
        volumes = np.full(20, 1000.0)
        result = ta.analyze_trend(closes, volumes)
        assert isinstance(result, dict)

    def test_calculate_all(self):
        from src.analysis.technical import TechnicalAnalyzer
        ta = TechnicalAnalyzer({})
        n = 30
        kline = [{"high":float(15+i*10/n),"low":float(8+i*4/n),"close":float(10+i*10/n),"volume":1000.0} for i in range(n)]
        result = ta.calculate_all(kline)
        assert isinstance(result, dict)


class TestRiskAnalyzer:
    def test_import(self):
        from src.analysis.risk import RiskAnalyzer
        assert RiskAnalyzer is not None

    def test_init(self):
        from src.analysis.risk import RiskAnalyzer
        ra = RiskAnalyzer(risk_free_rate=0.03, trading_days_per_year=242)
        assert ra is not None

    def test_calculate_return_metrics(self):
        from src.analysis.risk import RiskAnalyzer
        ra = RiskAnalyzer()
        np.random.seed(42)
        r = pd.Series(np.random.normal(0.001, 0.02, 100))
        assert isinstance(ra.calculate_return_metrics(r), dict)

    def test_calculate_volatility_metrics(self):
        from src.analysis.risk import RiskAnalyzer
        ra = RiskAnalyzer()
        np.random.seed(42)
        r = pd.Series(np.random.normal(0.001, 0.02, 100))
        assert isinstance(ra.calculate_volatility_metrics(r), dict)

    def test_calculate_drawdown_metrics(self):
        from src.analysis.risk import RiskAnalyzer
        ra = RiskAnalyzer()
        prices = np.array([100,102,101,103,105,104,106,108,107,105,103,106,109], dtype=float)
        result = ra.calculate_drawdown_metrics(prices)
        assert isinstance(result, dict)
        assert "max_drawdown" in result

    def test_calculate_drawdown_insufficient(self):
        from src.analysis.risk import RiskAnalyzer
        ra = RiskAnalyzer()
        assert ra.calculate_drawdown_metrics(np.array([100.0])) == {}

    def test_calculate_risk_adjusted_metrics(self):
        from src.analysis.risk import RiskAnalyzer
        ra = RiskAnalyzer()
        np.random.seed(42)
        r = pd.Series(np.random.normal(0.001, 0.02, 100))
        assert isinstance(ra.calculate_risk_adjusted_metrics(r), dict)

    def test_calculate_var_metrics(self):
        from src.analysis.risk import RiskAnalyzer
        ra = RiskAnalyzer()
        np.random.seed(42)
        r = pd.Series(np.random.normal(0.001, 0.02, 100))
        assert isinstance(ra.calculate_var_metrics(r, confidence_levels=[0.95, 0.99]), dict)

    def test_calculate_beta_alpha(self):
        from src.analysis.risk import RiskAnalyzer
        ra = RiskAnalyzer()
        np.random.seed(42)
        r = pd.Series(np.random.normal(0.001, 0.02, 100))
        b = r + pd.Series(np.random.normal(0, 0.01, 100))
        assert isinstance(ra.calculate_beta_alpha(r, b), dict)

    def test_calculate_concentration_risk(self):
        from src.analysis.risk import RiskAnalyzer
        ra = RiskAnalyzer()
        assert isinstance(ra.calculate_concentration_risk(np.array([0.5, 0.3, 0.2])), (dict, float))

    def test_stress_test(self):
        from src.analysis.risk import RiskAnalyzer
        ra = RiskAnalyzer()
        positions = [{"code":"600001","name":"Test","market_value":100000}]
        result = ra.stress_test(200000, positions)
        assert isinstance(result, dict)
        assert len(result) > 0

    def test_stress_test_custom_scenarios(self):
        from src.analysis.risk import RiskAnalyzer
        ra = RiskAnalyzer()
        positions = [{"code":"600001","name":"Test","market_value":100000}]
        result = ra.stress_test(200000, positions, scenarios={"crash": -0.3})
        assert isinstance(result, dict)


class TestCandlePatterns:
    def test_import(self):
        from src.analysis.candle_patterns import detect_candle_patterns
        assert callable(detect_candle_patterns)

    def test_detect_returns_dataframe(self):
        from src.analysis.candle_patterns import detect_candle_patterns
        df = pd.DataFrame({"open":[10,9.5,8,12,15,14,13,16,17,14],"high":[11,10,9,14,17,15,14,18,19,15],"low":[9,8,7,11,13,13,12,15,16,13],"close":[10,8.5,8.5,13,16,13.5,13.5,17,14,14],"volume":[1000,2000,1500,3000,4000,2500,1800,3500,3000,2000]}, dtype=float)
        assert isinstance(detect_candle_patterns(df), pd.DataFrame)

    def test_detect_preserves_length(self):
        from src.analysis.candle_patterns import detect_candle_patterns
        df = pd.DataFrame({"open":[10,9.5,8,12,15,14,13,16,17,14],"high":[11,10,9,14,17,15,14,18,19,15],"low":[9,8,7,11,13,13,12,15,16,13],"close":[10,8.5,8.5,13,16,13.5,13.5,17,14,14],"volume":[1000,2000,1500,3000,4000,2500,1800,3500,3000,2000]}, dtype=float)
        assert len(detect_candle_patterns(df)) == len(df)

    def test_empty_df(self):
        from src.analysis.candle_patterns import detect_candle_patterns
        df = pd.DataFrame({"open":[],"high":[],"low":[],"close":[],"volume":[]})
        result = detect_candle_patterns(df)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

class TestPortfolioRiskAnalyzer:
    def test_import(self):
        from src.analysis.portfolio_risk import PortfolioRiskAnalyzer
        assert PortfolioRiskAnalyzer is not None

    def test_init(self):
        from src.analysis.portfolio_risk import PortfolioRiskAnalyzer
        pra = PortfolioRiskAnalyzer(risk_free_rate=0.03)
        assert pra is not None

    def test_analyze_concentration(self):
        from src.analysis.portfolio_risk import PortfolioRiskAnalyzer
        pra = PortfolioRiskAnalyzer()
        pos = [{"code":"600001","name":"A","market_value":50000},{"code":"600002","name":"B","market_value":30000},{"code":"600003","name":"C","market_value":20000}]
        assert isinstance(pra._analyze_concentration(pos), dict)

    def test_analyze_concentration_empty(self):
        from src.analysis.portfolio_risk import PortfolioRiskAnalyzer
        assert PortfolioRiskAnalyzer()._analyze_concentration([]) == {}

    def test_stress_test(self):
        from src.analysis.portfolio_risk import PortfolioRiskAnalyzer
        pra = PortfolioRiskAnalyzer()
        assert isinstance(pra._run_stress_test([{"code":"600001","name":"A","market_value":50000}]), dict)

    def test_generate_warnings(self):
        from src.analysis.portfolio_risk import PortfolioRiskAnalyzer
        pra = PortfolioRiskAnalyzer()
        assert isinstance(pra._generate_warnings({"max_drawdown":0.25,"sharpe_ratio":0.5,"concentration":{"hhi":0.5},"volatility":0.3}), list)
