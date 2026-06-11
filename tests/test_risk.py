"""Tests for src/analysis/risk.py — RiskAnalyzer pure math functions."""
import pytest, numpy as np, pandas as pd
from src.analysis.risk import RiskAnalyzer

@pytest.fixture
def ra():
    return RiskAnalyzer(risk_free_rate=0.025, trading_days_per_year=252)

class TestReturnMetrics:
    def test_basic(self, ra):
        r = ra.calculate_return_metrics(pd.Series([0.01,-0.02,0.03,-0.01,0.02]))
        assert "total_return" in r and "win_rate" in r

    def test_empty(self, ra):
        assert isinstance(ra.calculate_return_metrics(pd.Series([],dtype=float)), dict)

    def test_all_pos(self, ra):
        r = ra.calculate_return_metrics(pd.Series([0.01,0.02,0.03,0.01,0.02]))
        assert r["total_return"] > 0 and r["win_rate"] == 100.0

    def test_all_neg(self, ra):
        r = ra.calculate_return_metrics(pd.Series([-0.01,-0.02,-0.03,-0.01,-0.02]))
        assert r["total_return"] < 0 and r["win_rate"] == 0.0

    def test_single(self, ra):
        assert "total_return" in ra.calculate_return_metrics(pd.Series([0.05]))

class TestVolatilityMetrics:
    def test_basic(self, ra):
        r = ra.calculate_volatility_metrics(pd.Series([0.01,-0.02,0.03,-0.01,0.02]))
        assert "daily_volatility" in r and r["daily_volatility"] >= 0

    def test_empty(self, ra):
        assert isinstance(ra.calculate_volatility_metrics(pd.Series([],dtype=float)), dict)

    def test_single_zero(self, ra):
        # single value returns empty dict (std=0)
        assert isinstance(ra.calculate_volatility_metrics(pd.Series([0.0])), dict)

    def test_stable(self, ra):
        r = ra.calculate_volatility_metrics(pd.Series([0.001]*5))
        assert r["daily_volatility"] < 1.0

    def test_skew_kurt(self, ra):
        r = ra.calculate_volatility_metrics(pd.Series([0.05,-0.01,-0.01,-0.01,0.02]))
        assert "skewness" in r and "kurtosis" in r

class TestDrawdownMetrics:
    def test_uptrend(self, ra):
        r = ra.calculate_drawdown_metrics(pd.Series([100,101,102,103,104]))
        assert r["max_drawdown"] == 0.0

    def test_downtrend(self, ra):
        r = ra.calculate_drawdown_metrics(pd.Series([100,95,90,85,80]))
        assert r["max_drawdown"] == 20.0

    def test_v_shape(self, ra):
        r = ra.calculate_drawdown_metrics(pd.Series([100,90,80,90,100,110]))
        assert r["max_drawdown"] > 15.0 and r["current_drawdown"] < 0.01

    def test_empty(self, ra):
        assert isinstance(ra.calculate_drawdown_metrics(pd.Series([],dtype=float)), dict)

    def test_single(self, ra):
        assert isinstance(ra.calculate_drawdown_metrics(pd.Series([100.0])), dict)

    def test_current_dd(self, ra):
        r = ra.calculate_drawdown_metrics(pd.Series([100,110,105]))
        assert r["current_drawdown"] > 0

    def test_warning(self, ra):
        r = ra.calculate_drawdown_metrics(pd.Series([100,95,90,85]))
        assert r["warning"] in (True, False, 1, 0)

class TestRiskAdjustedMetrics:
    def test_pos_sharpe(self, ra):
        r = ra.calculate_risk_adjusted_metrics(pd.Series([0.02,0.01,0.03,-0.01,0.02,0.015,0.005,0.01]))
        assert "sharpe_ratio" in r and r["sharpe_ratio"] > 0

    def test_neg_sharpe(self, ra):
        r = ra.calculate_risk_adjusted_metrics(pd.Series([-0.02,-0.01,-0.03,-0.015,-0.01]))
        assert r["sharpe_ratio"] < 0

    def test_sortino(self, ra):
        r = ra.calculate_risk_adjusted_metrics(pd.Series([0.03,0.02,-0.01,0.04,0.01,0.05,0.02,-0.005]))
        assert "sortino_ratio" in r and r["sortino_ratio"] > 0

    def test_calmar(self, ra):
        assert "calmar_ratio" in ra.calculate_risk_adjusted_metrics(pd.Series([0.02,0.03,-0.01,0.04,0.01,0.02]))

    def test_empty(self, ra):
        assert isinstance(ra.calculate_risk_adjusted_metrics(pd.Series([],dtype=float)), dict)

    def test_zero_vol(self, ra):
        assert isinstance(ra.calculate_risk_adjusted_metrics(pd.Series([0.0]*4)), dict)

class TestBetaAlpha:
    def test_beta_one(self, ra):
        s = pd.Series([0.01,-0.02,0.03,-0.01,0.02])
        r = ra.calculate_beta_alpha(s, s)
        assert abs(r["beta"] - 1.0) < 0.01

    def test_alpha_pos(self, ra):
        r = ra.calculate_beta_alpha(
            pd.Series([0.03,0.02,0.04,-0.01,0.03]),
            pd.Series([0.01,0.0,0.02,-0.02,0.01]))
        assert "alpha_annual" in r and r["alpha_annual"] > 0

    def test_r_squared(self, ra):
        r = ra.calculate_beta_alpha(
            pd.Series(np.random.normal(0.001,0.02,50)),
            pd.Series(np.random.normal(0.001,0.02,50)))
        assert 0 <= r["r_squared"] <= 1

    def test_tracking_error(self, ra):
        s = pd.Series([0.01,-0.01,0.02,-0.02,0.01])
        assert ra.calculate_beta_alpha(s, s)["tracking_error"] >= 0

    def test_empty(self, ra):
        assert isinstance(ra.calculate_beta_alpha(pd.Series([],dtype=float),pd.Series([],dtype=float)), dict)

class TestConcentrationRisk:
    def test_equal(self, ra):
        r = ra.calculate_concentration_risk(pd.Series([0.25,0.25,0.25,0.25]))
        assert r["effective_n"] >= 3.5 and r["max_weight"] == 25.0

    def test_concentrated(self, ra):
        r = ra.calculate_concentration_risk(pd.Series([0.8,0.1,0.05,0.05]))
        assert r["hhi"] > 0.5 and r["effective_n"] < 2

    def test_single(self, ra):
        assert ra.calculate_concentration_risk(pd.Series([1.0]))["hhi"] == 1.0

    def test_empty(self, ra):
        assert isinstance(ra.calculate_concentration_risk(pd.Series([],dtype=float)), dict)

    def test_top5(self, ra):
        r = ra.calculate_concentration_risk(pd.Series([0.5,0.2,0.15,0.1,0.05]))
        assert r["top5_weight"] == 100.0

class TestStressTest:
    def test_defaults(self, ra):
        r = ra.stress_test(100000, [
            {"name":"A","market_value":60000,"beta":1.0},
            {"name":"B","market_value":40000,"beta":1.0}])
        assert len(r) == 4

    def test_structure(self, ra):
        r = ra.stress_test(100000, [{"name":"A","market_value":100000,"beta":1.0}])
        for name, d in r.items():
            assert "total_loss" in d and "loss_pct" in d and "remaining_value" in d

class TestGradeSharpe:
    def test_excellent(self, ra):
        assert ra._grade_sharpe(3.0) == "卓越"

    def test_poor(self, ra):
        assert ra._grade_sharpe(-0.5) == "差"

    def test_multi(self, ra):
        assert len(set(ra._grade_sharpe(s) for s in [-1,0,0.5,1,1.5,2,2.5,3])) >= 3
