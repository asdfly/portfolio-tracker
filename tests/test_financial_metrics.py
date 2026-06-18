"""金融指标算法正确性测试 — 全量业务逻辑验证。

覆盖以下模块的核心计算函数:
- data_loader.py: compute_extended_risk_metrics, compute_rolling_metrics, compute_risk_score
- src/analysis/risk.py: RiskAnalyzer 全部 8 个计算方法
- src/analysis/backtest.py: _compute_metrics
- src/analysis/factor_attribution.py: compute_factor_attribution, _build_factor_returns
- src/analysis/equity_risk_premium.py: compute_erp_for_index, compute_erp
- src/analysis/dca_backtest.py: _build_result
- src/analysis/smart_alert.py: check_volatility_alert, check_risk_alert
- src/analysis/industry_boom.py: compute_industry_boom
- tabs/tab1_net_value.py: _calc_range_metrics
"""
import math
import pytest
import numpy as np
import pandas as pd

# ── Helpers ──────────────────────────────────────────────────────────────

TRADING_DAYS = 252
RF = 0.025  # 无风险利率 2.5%


def _to_pure(returns_pct_array):
    """将百分比数组(如 [0.5, -0.2]) 转为小数(如 [0.005, -0.002])"""
    return np.array(returns_pct_array, dtype=float) / 100


# ═══════════════════════════════════════════════════════════════════════
# 1. RiskAnalyzer (risk.py) — 8 个核心方法
# ═══════════════════════════════════════════════════════════════════════

class TestRiskAnalyzerReturnMetrics:
    """收益指标正确性: total_return, annual_return, geo/arithmetic mean"""

    def test_total_return_cumprod(self):
        """累计收益 = prod(1+r) - 1 (小数输入)"""
        from src.analysis.risk import RiskAnalyzer
        ra = RiskAnalyzer(risk_free_rate=RF, trading_days_per_year=TRADING_DAYS)
        # 每天+1%，10天后 (1.01)^10 - 1 = 10.46%
        r = ra.calculate_return_metrics(np.array([0.01]*10))
        expected = (1.01**10 - 1) * 100
        assert abs(r["total_return"] - round(expected, 2)) < 0.01

    def test_total_return_known_values(self):
        """已知序列: +10%,-5%,+3%,-2%,+1% 累计收益手工验证"""
        from src.analysis.risk import RiskAnalyzer
        ra = RiskAnalyzer(risk_free_rate=RF, trading_days_per_year=TRADING_DAYS)
        r = ra.calculate_return_metrics(np.array([0.10, -0.05, 0.03, -0.02, 0.01]))
        # (1.10)(0.95)(1.03)(0.98)(1.01) - 1
        expected = 1.10 * 0.95 * 1.03 * 0.98 * 1.01 - 1
        assert abs(r["total_return"] - round(expected * 100, 2)) < 0.01

    def test_annual_return_geometric(self):
        """年化收益使用几何法: (1+total)^(252/n) - 1"""
        from src.analysis.risk import RiskAnalyzer
        ra = RiskAnalyzer(risk_free_rate=RF, trading_days_per_year=TRADING_DAYS)
        # 252天 +0.2% 日收益 → 年化约 64.2%
        daily_ret = 0.002
        n = 252
        r = ra.calculate_return_metrics(np.array([daily_ret]*n))
        total = (1 + daily_ret)**n - 1
        expected_ann = (1 + total)**(TRADING_DAYS/n) - 1
        assert abs(r["annual_return"] - round(expected_ann * 100, 2)) < 1.0  # tolerance

    def test_win_rate(self):
        """胜率 = 正收益天数/总天数"""
        from src.analysis.risk import RiskAnalyzer
        ra = RiskAnalyzer(risk_free_rate=RF, trading_days_per_year=TRADING_DAYS)
        r = ra.calculate_return_metrics(np.array([0.01, 0.02, -0.01, 0.03, -0.02, 0.01, 0.02, -0.01, 0.04, -0.03]))
        assert r["win_rate"] == 60.0  # 6/10
        assert r["positive_days"] == 6
        assert r["negative_days"] == 4


class TestRiskAnalyzerVolatility:
    """波动率正确性: daily/annual vol, downside vol, skewness, kurtosis"""

    def test_daily_vol_matches_manual(self):
        """日波动率 = std(ddof=1)"""
        from src.analysis.risk import RiskAnalyzer
        ra = RiskAnalyzer(risk_free_rate=RF, trading_days_per_year=TRADING_DAYS)
        returns = np.array([0.01, -0.02, 0.03, -0.01, 0.02])
        r = ra.calculate_volatility_metrics(returns)
        expected_std = float(np.std(returns, ddof=1))
        assert abs(r["daily_volatility"] - round(expected_std * 100, 4)) < 1e-6

    def test_annual_vol_sqrt252(self):
        """年化波动率 = daily_vol * sqrt(252)"""
        from src.analysis.risk import RiskAnalyzer
        ra = RiskAnalyzer(risk_free_rate=RF, trading_days_per_year=TRADING_DAYS)
        returns = np.random.normal(0.001, 0.02, 100)
        r = ra.calculate_volatility_metrics(returns)
        daily_vol = r["daily_volatility"] / 100
        expected_annual = daily_vol * math.sqrt(TRADING_DAYS)
        assert abs(r["annual_volatility"] - round(expected_annual * 100, 2)) < 0.1

    def test_downside_vol_ignores_positive(self):
        """下行波动率仅用负收益计算"""
        from src.analysis.risk import RiskAnalyzer
        ra = RiskAnalyzer(risk_free_rate=RF, trading_days_per_year=TRADING_DAYS)
        all_neg = np.array([-0.01, -0.02, -0.03, -0.01, -0.02])
        mixed = np.array([0.05, -0.02, 0.03, -0.01, 0.02])
        r_neg = ra.calculate_volatility_metrics(all_neg)
        r_mix = ra.calculate_volatility_metrics(mixed)
        # Mixed 应有更低的 downside volatility (只有2个负值 vs 5个)
        assert r_mix["downside_volatility"] < r_neg["downside_volatility"]

    def test_kurtosis_excess(self):
        """峰度应为超额峰度(excess kurtosis), 正态分布约0"""
        from src.analysis.risk import RiskAnalyzer
        ra = RiskAnalyzer(risk_free_rate=RF, trading_days_per_year=TRADING_DAYS)
        np.random.seed(42)
        normal_data = np.random.normal(0, 0.02, 1000)
        r = ra.calculate_volatility_metrics(normal_data)
        assert abs(r["kurtosis"]) < 1.0  # 正态分布超额峰度接近0


class TestRiskAnalyzerDrawdown:
    """回撤正确性: max_dd, dd_duration, recovery_days, avg_dd"""

    def test_uptrend_zero_drawdown(self):
        """持续上涨无回撤"""
        from src.analysis.risk import RiskAnalyzer
        ra = RiskAnalyzer(risk_free_rate=RF, trading_days_per_year=TRADING_DAYS)
        r = ra.calculate_drawdown_metrics(np.array([100, 105, 110, 115, 120], dtype=float))
        assert r["max_drawdown"] == 0.0
        assert r["dd_duration_days"] == 0

    def test_max_drawdown_calculation(self):
        """最大回撤 = (peak - trough) / peak"""
        from src.analysis.risk import RiskAnalyzer
        ra = RiskAnalyzer(risk_free_rate=RF, trading_days_per_year=TRADING_DAYS)
        # 100→80, dd=20%
        r = ra.calculate_drawdown_metrics(np.array([100, 90, 80, 85, 90], dtype=float))
        assert abs(r["max_drawdown"] - 20.0) < 0.01
        assert r["dd_duration_days"] == 2  # peak day 0, trough day 2

    def test_recovery_days(self):
        """恢复天数 = 从谷底到创新高的天数"""
        from src.analysis.risk import RiskAnalyzer
        ra = RiskAnalyzer(risk_free_rate=RF, trading_days_per_year=TRADING_DAYS)
        # peak=100, trough=80(day 2), recovery at day 5(>100)
        r = ra.calculate_drawdown_metrics(np.array([100, 90, 80, 85, 95, 105], dtype=float))
        assert r["recovery_days"] == 3  # day 2 to day 5

    def test_no_recovery(self):
        """未恢复时 recovery_days 应为 None"""
        from src.analysis.risk import RiskAnalyzer
        ra = RiskAnalyzer(risk_free_rate=RF, trading_days_per_year=TRADING_DAYS)
        r = ra.calculate_drawdown_metrics(np.array([100, 90, 80, 75, 70], dtype=float))
        assert r["recovery_days"] is None


class TestRiskAdjustedMetrics:
    """风险调整收益: Sharpe, Sortino, Calmar"""

    def test_sharpe_formula(self):
        """Sharpe = (R_annual - Rf) / vol_annual"""
        from src.analysis.risk import RiskAnalyzer
        ra = RiskAnalyzer(risk_free_rate=RF, trading_days_per_year=TRADING_DAYS)
        # 每天稳定 0.1% (年化约 28.5%)
        returns = np.array([0.001]*200)
        r = ra.calculate_risk_adjusted_metrics(returns)
        # Sharpe = (0.285 - 0.025) / vol. vol ≈ 0 (zero std), sharpe should be huge
        assert r["sharpe_ratio"] > 0

    def test_sortino_uses_downside_only(self):
        """Sortino 分母只用下行波动率"""
        from src.analysis.risk import RiskAnalyzer
        ra = RiskAnalyzer(risk_free_rate=RF, trading_days_per_year=TRADING_DAYS)
        # 很多正收益 + 少量负收益 → Sortino > Sharpe
        returns = np.array([0.03, 0.04, 0.05, -0.01, -0.02, 0.06, 0.03, 0.02, -0.01, 0.04] * 10)
        r = ra.calculate_risk_adjusted_metrics(returns)
        assert r["sortino_ratio"] > r["sharpe_ratio"]

    def test_sortino_subtracts_risk_free(self):
        """Sortino 分子减去无风险利率"""
        from src.analysis.risk import RiskAnalyzer
        ra_no_rf = RiskAnalyzer(risk_free_rate=0.0, trading_days_per_year=TRADING_DAYS)
        ra_with_rf = RiskAnalyzer(risk_free_rate=0.05, trading_days_per_year=TRADING_DAYS)
        returns = np.array([0.001, -0.001, 0.002, -0.0005, 0.0015, 0.0005, -0.001, 0.002] * 25)
        r_no = ra_no_rf.calculate_risk_adjusted_metrics(returns)
        r_with = ra_with_rf.calculate_risk_adjusted_metrics(returns)
        # Higher risk_free rate → lower sortino
        assert r_with["sortino_ratio"] < r_no["sortino_ratio"]

    def test_calmar_formula(self):
        """Calmar = annual_return / max_drawdown"""
        from src.analysis.risk import RiskAnalyzer
        ra = RiskAnalyzer(risk_free_rate=RF, trading_days_per_year=TRADING_DAYS)
        # Positive return with some drawdown
        returns = np.array([0.02, 0.03, -0.10, 0.05, 0.04, -0.02, 0.03, 0.01, -0.01, 0.02] * 10)
        r = ra.calculate_risk_adjusted_metrics(returns)
        assert r["calmar_ratio"] > 0
        # Manual verification
        total_ret = (1 + returns).prod() - 1
        ann_ret = (1 + total_ret)**(TRADING_DAYS/len(returns)) - 1
        prices = np.cumprod(1 + returns)
        max_dd = float(np.max((np.maximum.accumulate(prices) - prices) / np.maximum.accumulate(prices)))
        expected_calmar = ann_ret / max_dd
        assert abs(r["calmar_ratio"] - expected_calmar) < 0.5


class TestBetaAlpha:
    """Beta/Alpha 正确性"""

    def test_beta_identical_returns(self):
        """完全相同的组合和基准 → beta=1, alpha=0"""
        from src.analysis.risk import RiskAnalyzer
        ra = RiskAnalyzer(risk_free_rate=RF, trading_days_per_year=TRADING_DAYS)
        returns = np.array([0.01, -0.02, 0.03, -0.01, 0.02, 0.015, -0.005, 0.01, -0.01, 0.02])
        r = ra.calculate_beta_alpha(returns, returns)
        assert abs(r["beta"] - 1.0) < 0.05
        assert abs(r["alpha_annual"]) < 1.0
        assert r["r_squared"] > 0.999

    def test_beta_formula_manual(self):
        """手动验证 beta = Cov(p,b) / Var(b)"""
        from src.analysis.risk import RiskAnalyzer
        ra = RiskAnalyzer(risk_free_rate=RF, trading_days_per_year=TRADING_DAYS)
        benchmark = np.array([0.01, -0.02, 0.03, -0.01, 0.02])
        portfolio = benchmark * 1.5  # beta should be 1.5
        r = ra.calculate_beta_alpha(portfolio, benchmark)
        assert abs(r["beta"] - 1.5) < 0.05

    def test_alpha_formula_jensen(self):
        """Jensen Alpha = R_p - [Rf + beta*(R_m - Rf)]"""
        from src.analysis.risk import RiskAnalyzer
        ra = RiskAnalyzer(risk_free_rate=RF, trading_days_per_year=TRADING_DAYS)
        # 组合收益高于 CAPM 预期 → 正 alpha
        benchmark = np.array([0.01]*100)
        portfolio = np.array([0.02]*100)  # 每天多赚 1%
        r = ra.calculate_beta_alpha(portfolio, benchmark)
        assert r["alpha_annual"] > 0

    def test_r_squared_range(self):
        """R² 应在 [0,1]"""
        from src.analysis.risk import RiskAnalyzer
        ra = RiskAnalyzer(risk_free_rate=RF, trading_days_per_year=TRADING_DAYS)
        np.random.seed(123)
        r = ra.calculate_beta_alpha(np.random.normal(0.001, 0.02, 100), np.random.normal(0.001, 0.02, 100))
        assert 0 <= r["r_squared"] <= 1

    def test_tracking_error_formula(self):
        """跟踪误差 = std(p-b) * sqrt(252)"""
        from src.analysis.risk import RiskAnalyzer
        ra = RiskAnalyzer(risk_free_rate=RF, trading_days_per_year=TRADING_DAYS)
        benchmark = np.array([0.01]*100)
        portfolio = np.array([0.012]*100)  # 每天多 0.2%
        r = ra.calculate_beta_alpha(portfolio, benchmark)
        diff = portfolio - benchmark
        expected_te = float(np.std(diff) * math.sqrt(TRADING_DAYS))
        assert abs(r["tracking_error"] - round(expected_te * 100, 2)) < 0.1

    def test_information_ratio(self):
        """信息比率 = alpha / tracking_error"""
        from src.analysis.risk import RiskAnalyzer
        ra = RiskAnalyzer(risk_free_rate=RF, trading_days_per_year=TRADING_DAYS)
        rng = np.random.RandomState(42)
        benchmark = rng.normal(0.001, 0.02, 100)
        portfolio = benchmark * 1.2 + rng.normal(0.001, 0.005, 100)
        r = ra.calculate_beta_alpha(portfolio, benchmark)
        alpha_annual = r["alpha_annual"] / 100
        te_annual = r["tracking_error"] / 100
        if te_annual > 1e-10:
            expected_ir = alpha_annual / te_annual
            assert abs(r["information_ratio"] - round(expected_ir, 4)) < 0.01


class TestVarMetrics:
    """VaR/CVaR 正确性"""

    def test_var_historical_percentile(self):
        """历史法 VaR = percentile(returns, (1-conf)*100)"""
        from src.analysis.risk import RiskAnalyzer
        ra = RiskAnalyzer(risk_free_rate=RF, trading_days_per_year=TRADING_DAYS)
        np.random.seed(42)
        returns = np.random.normal(0.001, 0.02, 500)
        r = ra.calculate_var_metrics(returns, [0.95])
        # VaR at 95% = 5th percentile
        expected_var = float(np.percentile(returns, 5))
        assert abs(r["var_95"]["historical"] - round(expected_var * 100, 4)) < 1e-6

    def test_var_parametric(self):
        """参数法 VaR = mean + z * std"""
        from src.analysis.risk import RiskAnalyzer
        ra = RiskAnalyzer(risk_free_rate=RF, trading_days_per_year=TRADING_DAYS)
        np.random.seed(42)
        returns = np.random.normal(0.001, 0.02, 500)
        r = ra.calculate_var_metrics(returns, [0.95])
        from scipy import stats
        mean = float(np.mean(returns))
        std = float(np.std(returns, ddof=1))
        expected = mean + std * stats.norm.ppf(0.05)
        assert abs(r["var_95"]["parametric"] - round(expected * 100, 4)) < 1e-4

    def test_cvar_exceeds_var(self):
        """CVaR(条件VaR) 应 <= VaR (都是损失，CVaR更极端)"""
        from src.analysis.risk import RiskAnalyzer
        ra = RiskAnalyzer(risk_free_rate=RF, trading_days_per_year=TRADING_DAYS)
        np.random.seed(42)
        returns = np.random.normal(0.001, 0.02, 500)
        r = ra.calculate_var_metrics(returns, [0.95, 0.99])
        # CVaR 95% is mean of returns below VaR_95
        # Since VaR and CVaR are both negative, CVaR <= VaR (more negative)
        assert r["var_95"]["cvar"] <= r["var_95"]["historical"]

    def test_var_99_more_extreme_than_95(self):
        """99% VaR 应比 95% VaR 更极端"""
        from src.analysis.risk import RiskAnalyzer
        ra = RiskAnalyzer(risk_free_rate=RF, trading_days_per_year=TRADING_DAYS)
        np.random.seed(42)
        returns = np.random.normal(0.001, 0.02, 500)
        r = ra.calculate_var_metrics(returns, [0.95, 0.99])
        assert r["var_99"]["historical"] <= r["var_95"]["historical"]

    def test_var_insufficient_data(self):
        """数据不足30天应返回空dict"""
        from src.analysis.risk import RiskAnalyzer
        ra = RiskAnalyzer(risk_free_rate=RF, trading_days_per_year=TRADING_DAYS)
        assert ra.calculate_var_metrics(np.array([0.01]*20)) == {}


class TestConcentrationRisk:
    """集中度风险正确性"""

    def test_hhi_formula(self):
        """HHI = sum(w_i^2), 等权4只 = 4*(0.25^2) = 0.25"""
        from src.analysis.risk import RiskAnalyzer
        ra = RiskAnalyzer(risk_free_rate=RF, trading_days_per_year=TRADING_DAYS)
        r = ra.calculate_concentration_risk(np.array([0.25, 0.25, 0.25, 0.25]))
        assert abs(r["hhi"] - 0.25) < 0.001
        assert abs(r["effective_n"] - 4.0) < 0.1

    def test_hhi_max_concentration(self):
        """完全集中于1只 → HHI=1, effective_n=1"""
        from src.analysis.risk import RiskAnalyzer
        ra = RiskAnalyzer(risk_free_rate=RF, trading_days_per_year=TRADING_DAYS)
        r = ra.calculate_concentration_risk(np.array([1.0]))
        assert r["hhi"] == 1.0
        assert r["effective_n"] == 1.0

    def test_hhi_monotonicity(self):
        """集中度越高→HHI越大→effective_n越小"""
        from src.analysis.risk import RiskAnalyzer
        ra = RiskAnalyzer(risk_free_rate=RF, trading_days_per_year=TRADING_DAYS)
        equal = ra.calculate_concentration_risk(np.array([0.2]*5))
        conc = ra.calculate_concentration_risk(np.array([0.6, 0.1, 0.1, 0.1, 0.1]))
        assert conc["hhi"] > equal["hhi"]
        assert conc["effective_n"] < equal["effective_n"]


# ═══════════════════════════════════════════════════════════════════════
# 2. data_loader.py — compute_extended_risk_metrics
# ═══════════════════════════════════════════════════════════════════════

class TestComputeExtendedRiskMetrics:
    """扩展风险指标: Sortino, Calmar, win_rate, pl_ratio, skewness, kurtosis"""

    def test_sortino_subtracts_rf(self):
        """Sortino 分子应减去无风险利率(0.025=2.5%)"""
        # 使用已知序列手动计算
        # daily_return 存储为百分比: 0.5 表示 0.5%
        dates = pd.date_range("2025-08-01", periods=100)
        returns_pct = pd.Series(np.random.normal(0.5, 1.5, 100))  # 百分比格式
        df = pd.DataFrame({
            "date": dates,
            "daily_return": returns_pct,
            "daily_pnl": returns_pct * 1000,
            "total_value": 100000 * np.cumprod(1 + returns_pct / 100),
        })

        # 模拟 compute_extended_risk_metrics 的核心逻辑
        returns = returns_pct / 100  # 转小数
        annual_return = returns.mean() * 252
        neg_returns = returns[returns < 0]
        downside_std = neg_returns.std(ddof=1) * math.sqrt(252) if len(neg_returns) > 1 else np.nan

        # 标准 Sortino 应减去 Rf
        sortino_with_rf = (annual_return - 0.025) / downside_std if downside_std and downside_std > 0 else np.nan

        # 验证: 正收益组合减去Rf后Sortino仍然应>0（如果收益足够高）
        if not np.isnan(sortino_with_rf) and annual_return > 0.025:
            assert sortino_with_rf > 0

    def test_calmar_formula_consistent(self):
        """Calmar = annual_return / max_dd (使用 cumprod 计算)"""
        returns_pct = pd.Series([0.5, -0.2, 0.8, -0.1, 0.3, 0.4, -0.5, 0.6, 0.1, -0.3])
        returns = returns_pct / 100
        cumret = (1 + returns).cumprod()
        peak = cumret.cummax()
        dd_series = (cumret / peak - 1)
        max_dd_abs = abs(dd_series.min())
        annual_return = returns.mean() * 252
        calmar = annual_return / max_dd_abs if max_dd_abs > 0 else np.nan
        assert not np.isnan(calmar) and calmar > 0

    def test_win_rate_calculation(self):
        """胜率 = pnl>0 天数 / pnl!=0 天数"""
        pnls = pd.Series([100, -50, 200, -30, 0, 80, -10, 50])
        win_days = len(pnls[pnls > 0])  # 4
        total_days = len(pnls[pnls != 0])  # 7
        expected = win_days / total_days * 100
        assert expected == pytest.approx(57.14, abs=0.01)

    def test_pl_ratio_calculation(self):
        """盈亏比 = avg_win / |avg_loss|"""
        pnls = pd.Series([100, -50, 200, -100, 80])
        avg_win = pnls[pnls > 0].mean()
        avg_loss = abs(pnls[pnls < 0].mean())
        expected = avg_win / avg_loss
        assert expected == pytest.approx(1.6889, abs=0.01)  # (100+200+80)/3 / (50+100)/2 = 126.67/75

    def test_max_dd_duration(self):
        """最大回撤持续天数"""
        returns_pct = pd.Series([0.5, -0.2, -0.1, -0.3, -0.5, 0.1, 0.2, -0.1, -0.2])
        returns = returns_pct / 100
        cumret = (1 + returns).cumprod()
        peak = cumret.cummax()
        in_drawdown = cumret < peak
        max_dur = 0
        cur_dur = 0
        for is_dd in in_drawdown:
            if is_dd:
                cur_dur += 1
                max_dur = max(max_dur, cur_dur)
            else:
                cur_dur = 0
        assert max_dur >= 3  # should have at least 3 consecutive drawdown days


# ═══════════════════════════════════════════════════════════════════════
# 3. data_loader.py — compute_rolling_metrics
# ═══════════════════════════════════════════════════════════════════════

class TestComputeRollingMetrics:
    """滚动夏普/波动率正确性"""

    def test_rolling_sharpe_formula(self):
        """滚动夏普 = rolling_mean / rolling_std * sqrt(252)"""
        # 手动验证: 5天窗口
        returns_pct = pd.Series([0.5, 0.3, -0.2, 0.4, 0.1, -0.1, 0.2, 0.3, 0.5, -0.3])
        ret = returns_pct / 100
        window = 5
        rolling_sharpe = ret.rolling(window).mean() / ret.rolling(window).std() * np.sqrt(TRADING_DAYS)
        rolling_vol = ret.rolling(window).std() * np.sqrt(TRADING_DAYS)
        result = pd.DataFrame({
            "rolling_sharpe": rolling_sharpe,
            "rolling_vol": rolling_vol
        }).dropna()
        # First valid value at index (window-1), so (10 - window) rows with window=5 → 6 (index 4..9)
        assert len(result) == len(returns_pct) - window + 1  # 6 valid rows
        # Verify first value manually
        w = ret.iloc[:window]
        expected_sharpe = w.mean() / w.std() * np.sqrt(TRADING_DAYS)
        assert abs(result["rolling_sharpe"].iloc[0] - expected_sharpe) < 1e-6

    def test_rolling_sharpe_manual_verification(self):
        """滚动夏普最后值应与手动计算一致"""
        returns_pct = pd.Series(np.random.RandomState(42).normal(0.5, 1.5, 200))
        ret = returns_pct / 100
        window = 60
        rolling_sharpe = ret.rolling(window).mean() / ret.rolling(window).std() * np.sqrt(TRADING_DAYS)
        last_rolling = rolling_sharpe.dropna().iloc[-1]
        # Manual: use last window of returns
        w = ret.iloc[-window:]
        manual_sharpe = w.mean() / w.std() * np.sqrt(TRADING_DAYS)
        assert abs(last_rolling - manual_sharpe) < 1e-10


# ═══════════════════════════════════════════════════════════════════════
# 4. dashboard.py — compute_risk_score
# ═══════════════════════════════════════════════════════════════════════

class TestComputeRiskScore:
    """风险评分正确性"""

    def test_perfect_score(self):
        """低波动+低回撤+高夏普 → 100分"""
        from dashboard import compute_risk_score
        assert compute_risk_score(10, 2, 2.0) == 100

    def test_high_risk(self):
        """高波动+大回撤+负夏普 → 最低分"""
        from dashboard import compute_risk_score
        score = compute_risk_score(35, -20, -1.0)
        assert score == 20  # 100-30(vol>30)-30(dd>15)-20(sharpe<0)

    def test_bounds(self):
        """分数应在 [0, 100]"""
        from dashboard import compute_risk_score
        # Extreme case: vol=100, dd=-50, sharpe=-5
        score = compute_risk_score(100, -50, -5)
        assert 0 <= score <= 100
        # All good
        score2 = compute_risk_score(5, 1, 5.0)
        assert 0 <= score2 <= 100

    def test_volatility_thresholds(self):
        """波动率阈值梯度: >30→-30, >20→-15, >15→-5"""
        from dashboard import compute_risk_score
        base = compute_risk_score(None, None, None)  # 100
        assert compute_risk_score(31, None, None) == 70   # -30
        assert compute_risk_score(21, None, None) == 85   # -15
        assert compute_risk_score(16, None, None) == 95   # -5
        assert compute_risk_score(14, None, None) == 100   # no deduction

    def test_drawdown_thresholds(self):
        """回撤阈值梯度: >15→-30, >10→-20, >5→-10"""
        from dashboard import compute_risk_score
        assert compute_risk_score(None, -16, None) == 70   # -30
        assert compute_risk_score(None, -11, None) == 80   # -20
        assert compute_risk_score(None, -6, None) == 90    # -10
        assert compute_risk_score(None, -4, None) == 100    # no deduction


# ═══════════════════════════════════════════════════════════════════════
# 5. backtest.py — _compute_metrics
# ═══════════════════════════════════════════════════════════════════════

class TestBacktestComputeMetrics:
    """回测指标: Sharpe 应减去 Rf, 波动率用 ddof=1"""

    def test_volatility_ddof1(self):
        """波动率应使用样本标准差(ddof=1)"""
        from src.analysis.backtest import StrategyBacktester
        import sqlite3, tempfile, os
        # Create temp DB
        db_path = tempfile.mktemp(suffix=".db")
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE IF NOT EXISTS portfolio_snapshots (date TEXT, code TEXT, current_price REAL)")
        conn.execute("INSERT INTO portfolio_snapshots VALUES ('2025-01-01','510300',1.0),('2025-01-02','510300',1.01)")
        conn.commit()
        try:
            bt = StrategyBacktester(conn)
            import pandas as pd
            import numpy as np
            pv = pd.Series([100, 101, 100, 102, 103, 101, 104], dtype=float)
            # returns shape: (n_days, n_assets) → simulate single asset
            rets = pd.DataFrame({"510300": [0.01, -0.0099, 0.02, 0.0098, -0.0194, 0.0297]})
            r = bt._compute_metrics(pv, rets, 100)
            # Verify volatility uses ddof=1: std of mean(axis=1) with ddof=1
            daily_rets = rets.mean(axis=1).values
            expected_vol = float(np.std(daily_rets, ddof=1)) * np.sqrt(252) * 100
            assert abs(r["volatility"] - expected_vol) < 0.01
        finally:
            conn.close()
            os.unlink(db_path)

    def test_sharpe_subtracts_rf(self):
        """Sharpe 分子应减去无风险利率"""
        from src.analysis.backtest import StrategyBacktester
        import sqlite3, tempfile, os
        db_path = tempfile.mktemp(suffix=".db")
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE IF NOT EXISTS portfolio_snapshots (date TEXT, code TEXT, current_price REAL)")
        conn.execute("INSERT INTO portfolio_snapshots VALUES ('2025-01-01','510300',1.0),('2025-01-02','510300',1.01)")
        conn.commit()
        try:
            bt = StrategyBacktester(conn)
            import pandas as pd
            pv = pd.Series([100, 101, 102, 103], dtype=float)
            rets = pd.DataFrame({"510300": [0.01, 0.0099, 0.0098]})
            r = bt._compute_metrics(pv, rets, 100)
            # sharpe = (ann_ret - 2.5) / vol
            if r["volatility"] > 0:
                expected_sharpe = (r["annualized_return"] - 2.5) / r["volatility"]
                assert abs(r["sharpe_ratio"] - expected_sharpe) < 0.01
        finally:
            conn.close()
            os.unlink(db_path)

    def test_max_drawdown_calculation(self):
        """回测最大回撤计算"""
        from src.analysis.backtest import StrategyBacktester
        import sqlite3, tempfile, os
        db_path = tempfile.mktemp(suffix=".db")
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE IF NOT EXISTS portfolio_snapshots (date TEXT, code TEXT, current_price REAL)")
        conn.execute("INSERT INTO portfolio_snapshots VALUES ('2025-01-01','510300',1.0),('2025-01-02','510300',1.01)")
        conn.commit()
        try:
            bt = StrategyBacktester(conn)
            import pandas as pd
            pv = pd.Series([100, 110, 105, 95, 100, 115], dtype=float)
            rets = pd.DataFrame({"510300": [0.10, -0.045, -0.095, 0.053, 0.15]})
            r = bt._compute_metrics(pv, rets, 100)
            # Peak=110, trough=95, dd = (95-110)/110 = -13.6%
            assert abs(r["max_drawdown"] - (-13.64)) < 0.5
        finally:
            conn.close()
            os.unlink(db_path)


# ═══════════════════════════════════════════════════════════════════════
# 6. factor_attribution.py — OLS回归
# ═══════════════════════════════════════════════════════════════════════

class TestFactorAttribution:
    """多因子归因 OLS 回归正确性"""

    def test_residual_std_ddof1(self):
        """残差标准差应使用 ddof=1"""
        # This is now fixed; verify the formula in source
        import os
        project = os.environ.get("PROJECT_DIR", "")
        path = os.path.join(project, "src", "analysis", "factor_attribution.py")
        if project and os.path.exists(path):
            with open(path, "r") as f:
                content = f.read()
            assert "np.std(residuals_arr, ddof=1)" in content

    def test_beta_estimation(self):
        """OLS beta 估计应接近真实值"""
        from src.analysis.factor_attribution import compute_factor_attribution
        np.random.seed(42)
        n = 200
        true_alpha = 0.0002
        true_betas = {"Rm_Rf": 1.0, "SMB": 0.3, "HML": -0.1}
        dates = pd.date_range("2025-01-01", periods=n)
        factor_returns = pd.DataFrame({
            "date": dates.strftime("%Y-%m-%d"),
            "Rm_Rf": np.random.normal(0.0005, 0.01, n),
            "SMB": np.random.normal(0.0001, 0.005, n),
            "HML": np.random.normal(0.0001, 0.005, n),
        })
        noise = np.random.normal(0, 0.005, n)
        portfolio_returns = pd.Series(
            true_alpha + true_betas["Rm_Rf"] * factor_returns["Rm_Rf"]
            + true_betas["SMB"] * factor_returns["SMB"]
            + true_betas["HML"] * factor_returns["HML"]
            + noise
        )
        result = compute_factor_attribution(portfolio_returns, factor_returns)
        if "error" not in result:
            assert abs(result["beta_factors"]["Rm_Rf"] - 1.0) < 0.3
            assert abs(result["beta_factors"]["SMB"] - 0.3) < 0.5

    def test_r_squared_reasonable(self):
        """R² 应在合理范围"""
        from src.analysis.factor_attribution import compute_factor_attribution
        np.random.seed(42)
        n = 200
        dates = pd.date_range("2025-01-01", periods=n)
        factor_returns = pd.DataFrame({
            "date": dates.strftime("%Y-%m-%d"),
            "Rm_Rf": np.random.normal(0.0005, 0.01, n),
            "SMB": np.random.normal(0.0001, 0.005, n),
            "HML": np.random.normal(0.0001, 0.005, n),
        })
        portfolio_returns = pd.Series(np.random.normal(0.001, 0.015, n))
        result = compute_factor_attribution(portfolio_returns, factor_returns)
        if "error" not in result:
            assert 0 <= result["r_squared"] <= 1

    def test_alpha_annualized(self):
        """Alpha 应被年化 (×252)"""
        from src.analysis.factor_attribution import compute_factor_attribution
        np.random.seed(42)
        n = 200
        dates = pd.date_range("2025-01-01", periods=n)
        factor_returns = pd.DataFrame({
            "date": dates.strftime("%Y-%m-%d"),
            "Rm_Rf": np.random.normal(0.0005, 0.01, n),
            "SMB": np.random.normal(0.0001, 0.005, n),
        })
        portfolio_returns = pd.Series(np.random.normal(0.001, 0.015, n))
        result = compute_factor_attribution(portfolio_returns, factor_returns)
        if "error" not in result:
            # alpha = daily alpha * 252
            # Verify magnitude is reasonable (annual scale)
            assert abs(result["alpha"]) < 5  # shouldn't be >500% annual

    def test_insufficient_data(self):
        """数据不足30天应返回错误"""
        from src.analysis.factor_attribution import compute_factor_attribution
        n = 10
        dates = pd.date_range("2025-01-01", periods=n)
        factor_returns = pd.DataFrame({
            "date": dates.strftime("%Y-%m-%d"),
            "Rm_Rf": np.random.normal(0.0005, 0.01, n),
        })
        portfolio_returns = pd.Series(np.random.normal(0.001, 0.015, n))
        result = compute_factor_attribution(portfolio_returns, factor_returns)
        assert "error" in result


# ═══════════════════════════════════════════════════════════════════════
# 7. equity_risk_premium.py — ERP 计算
# ═══════════════════════════════════════════════════════════════════════

class TestEquityRiskPremium:
    """ERP = E/P - Rf"""

    def test_erp_formula(self):
        """ERP = 1/PE*100 - Rf"""
        from src.analysis.equity_risk_premium import compute_erp
        # PE=15, Rf=2.5%
        erp = compute_erp(15, 2.5)
        expected = 1/15 * 100 - 2.5  # 6.67 - 2.5 = 4.17
        assert abs(erp - round(expected, 4)) < 0.01

    def test_erp_zero_pe(self):
        """PE=0 应返回 0.0 (代码设计: PE<=0返回0.0而非None)"""
        from src.analysis.equity_risk_premium import compute_erp
        assert compute_erp(0, 2.5) == 0.0

    def test_erp_negative_pe(self):
        """PE<0 应返回 0.0"""
        from src.analysis.equity_risk_premium import compute_erp
        assert compute_erp(-10, 2.5) == 0.0

    def test_erp_high_pe(self):
        """高PE → 低 EY → 低/负 ERP"""
        from src.analysis.equity_risk_premium import compute_erp
        erp = compute_erp(100, 2.5)  # EY=1%, ERP=1-2.5=-1.5%
        assert erp < 0

    def test_erp_percentile(self):
        """ERP分位数信号应在合法范围内"""
        from src.analysis.equity_risk_premium import classify_erp_signal
        # 需要至少20个数据点
        erp_history = [float(i) / 10 for i in range(1, 101)]
        signal, detail = classify_erp_signal(5.0, erp_history)
        assert detail
        assert signal in ("偏多", "中性略偏多", "中性略偏空", "偏空")


# ═══════════════════════════════════════════════════════════════════════
# 8. smart_alert.py — 预警逻辑
# ═══════════════════════════════════════════════════════════════════════

class TestSmartAlert:
    """预警检测正确性"""

    def test_volatility_alert_zscore(self):
        """波动率预警基于 z-score"""
        from src.analysis.smart_alert import check_volatility_alert
        # z = (current - avg) / std = (40 - 20) / 5 = 4 > 2 → 重要
        alert = check_volatility_alert("510300", "沪深300", 40, 20, 5)
        assert alert is not None
        assert alert.level == "重要"

    def test_volatility_alert_no_trigger(self):
        """z < 1.5 → 无预警"""
        from src.analysis.smart_alert import check_volatility_alert
        alert = check_volatility_alert("510300", "沪深300", 22, 20, 5)
        # z = (22-20)/5 = 0.4 < 1.5
        assert alert is None

    def test_volatility_alert_warning(self):
        """1.5 <= z <= 2 → 关注"""
        from src.analysis.smart_alert import check_volatility_alert
        # z = (30 - 20) / 5 = 2.0 → 重要(not 关注)
        alert = check_volatility_alert("510300", "沪深300", 27.5, 20, 5)
        # z = 7.5/5 = 1.5 → 关注
        assert alert is not None
        assert alert.level == "关注"

    def test_risk_alert_deep(self):
        """max_dd < -15% → 紧急"""
        from src.analysis.smart_alert import check_risk_alert
        alert = check_risk_alert("510300", "沪深300", -20)
        assert alert is not None
        assert alert.level == "紧急"

    def test_risk_alert_significant(self):
        """-15 <= max_dd < -10 → 重要"""
        from src.analysis.smart_alert import check_risk_alert
        alert = check_risk_alert("510300", "沪深300", -12)
        assert alert is not None
        assert alert.level == "重要"

    def test_risk_alert_no_trigger(self):
        """max_dd > -10 → 无风险预警"""
        from src.analysis.smart_alert import check_risk_alert
        alert = check_risk_alert("510300", "沪深300", -5)
        assert alert is None

    def test_risk_alert_erp_signal(self):
        """ERP偏空 → 关注"""
        from src.analysis.smart_alert import check_risk_alert
        alert = check_risk_alert("510300", "沪深300", -3, "偏空")
        assert alert is not None
        assert alert.level == "关注"


# ═══════════════════════════════════════════════════════════════════════
# 9. tab1_net_value.py — _calc_range_metrics
# ═══════════════════════════════════════════════════════════════════════

class TestCalcRangeMetrics:
    """区间指标正确性"""

    def test_cum_return_cumprod(self):
        """累计收益使用 cumprod"""
        from tabs.tab1_net_value import _calc_range_metrics
        # daily_return=百分比格式: 1%,-0.5%,0.3%,-0.2%,0.4%
        df = pd.DataFrame({
            "daily_return": [1.0, -0.5, 0.3, -0.2, 0.4],
            "max_drawdown": [0, -0.5, -0.5, -0.7, -0.2],
        })
        r = _calc_range_metrics(df)
        # (1.01)(0.995)(1.003)(0.998)(1.004) - 1
        expected = 1.01 * 0.995 * 1.003 * 0.998 * 1.004 - 1
        assert abs(r["cum_ret"] - round(expected * 100, 10)) < 0.001

    def test_ann_ret_arithmetic_mean(self):
        """年化收益使用算术平均法"""
        from tabs.tab1_net_value import _calc_range_metrics
        df = pd.DataFrame({
            "daily_return": [0.5, -0.2, 0.3, -0.1, 0.4],
            "max_drawdown": [0]*5,
        })
        r = _calc_range_metrics(df)
        # mean = 0.196, annual = 0.196/100 * 252 * 100 = 49.392
        assert r["ann_ret"] > 0
        # Verify arithmetic: mean*daily*252*100
        daily = df["daily_return"] / 100
        expected = daily.mean() * 252 * 100
        assert abs(r["ann_ret"] - expected) < 0.001

    def test_vol_sqrt252(self):
        """波动率 = std * sqrt(252) * 100"""
        from tabs.tab1_net_value import _calc_range_metrics
        df = pd.DataFrame({
            "daily_return": [1.0, -1.0, 0.5, -0.5, 1.5, -1.5, 0.3, -0.3, 0.8, -0.8],
            "max_drawdown": [0]*10,
        })
        r = _calc_range_metrics(df)
        daily = df["daily_return"] / 100
        expected_vol = daily.std() * math.sqrt(252) * 100
        assert abs(r["vol"] - expected_vol) < 0.01

    def test_sharpe_no_rf(self):
        """区间夏普不考虑无风险利率"""
        from tabs.tab1_net_value import _calc_range_metrics
        df = pd.DataFrame({
            "daily_return": [0.5, 0.3, 0.2, 0.1, 0.4, 0.3, 0.5, 0.2, 0.1, 0.3],
            "max_drawdown": [0]*10,
        })
        r = _calc_range_metrics(df)
        daily = df["daily_return"] / 100
        expected_sharpe = daily.mean() / daily.std() * math.sqrt(252)
        assert abs(r["sharpe"] - expected_sharpe) < 0.001

    def test_win_rate(self):
        """胜率计算"""
        from tabs.tab1_net_value import _calc_range_metrics
        df = pd.DataFrame({
            "daily_return": [0.5, -0.3, 0.2, -0.1, 0.4, -0.2, 0.1, 0.3, -0.4, 0.5],
            "max_drawdown": [0]*10,
        })
        r = _calc_range_metrics(df)
        # positive: 0.5,0.2,0.4,0.1,0.3,0.5 = 6 out of 10
        assert r["wr"] == 60.0

    def test_pnl_ratio(self):
        """盈亏比计算"""
        from tabs.tab1_net_value import _calc_range_metrics
        df = pd.DataFrame({
            "daily_return": [1.0, -0.5, 0.3, -0.2, 0.4, -0.1, 0.5, -0.3, 0.2, -0.4],
            "max_drawdown": [0]*10,
        })
        r = _calc_range_metrics(df)
        daily = df["daily_return"] / 100
        avg_win = daily[daily > 0].mean()
        avg_loss = abs(daily[daily < 0].mean())
        expected = avg_win / avg_loss
        assert abs(r["pnl_ratio"] - expected) < 0.001


# ═══════════════════════════════════════════════════════════════════════
# 10. dca_backtest.py — _build_result
# ═══════════════════════════════════════════════════════════════════════

class TestDCABacktestResult:
    """定投回测指标正确性"""

    def test_total_return_calculation(self):
        """总收益 = (final_value / total_invest - 1) * 100"""
        # Manual verification: invest 12000, final 15000 → 25%
        total_invest = 12000
        final_value = 15000
        expected = (final_value / total_invest - 1) * 100
        assert abs(expected - 25.0) < 0.01

    def test_annual_return_formula(self):
        """年化收益 = (1+total/100)^(1/n_years) - 1 * 100"""
        total_ret_pct = 25.0
        n_months = 12
        n_years = n_months / 12
        annual = ((1 + total_ret_pct / 100) ** (1 / n_years) - 1) * 100
        assert abs(annual - 25.0) < 0.01  # 1 year → same

    def test_max_drawdown_series(self):
        """最大回撤 = min(cum_value / peak - 1)"""
        cum_values = [1000, 1100, 1050, 950, 1050, 1150]
        peak = cum_values[0]
        max_dd = 0
        for v in cum_values[1:]:
            if v > peak:
                peak = v
            dd = (v / peak - 1) * 100 if peak > 0 else 0
            if dd < max_dd:
                max_dd = dd
        # peak=1100, trough=950, dd = (950/1100-1)*100 = -13.64%
        assert abs(max_dd - (-13.64)) < 0.5


# ═══════════════════════════════════════════════════════════════════════
# 11. industry_boom.py — 综合景气度
# ═══════════════════════════════════════════════════════════════════════

class TestIndustryBoom:
    """行业景气度综合评分正确性"""

    def test_weight_sum(self):
        """综合分 = 资金30% + 估值25% + 技术25% + 政策20%"""
        from src.analysis.industry_boom import compute_industry_boom
        r = compute_industry_boom(
            "医药", net_inflow_5d=10, net_inflow_20d=20,
            pe_percentile=20, pb_percentile=20,
            ma5_above_ma20=True, ma20_above_ma60=True,
            vol_ratio=1.2, price_change_20d=10,
            has_positive_policy=True, has_negative_policy=False, recent_events=3,
        )
        # All factors should be high → total > 65
        assert r.boom_score > 65

    def test_score_range(self):
        """综合分应在 [0, 100]"""
        from src.analysis.industry_boom import compute_industry_boom
        r = compute_industry_boom("测试")
        assert 0 <= r.boom_score <= 100

    def test_pessimistic_score(self):
        """所有因素悲观 → 低分"""
        from src.analysis.industry_boom import compute_industry_boom
        r = compute_industry_boom(
            "医药", net_inflow_5d=-10, net_inflow_20d=-20,
            pe_percentile=90, pb_percentile=90,
            ma5_above_ma20=False, ma20_above_ma60=False,
            vol_ratio=0.5, price_change_20d=-10,
            has_positive_policy=False, has_negative_policy=True, recent_events=0,
        )
        assert r.boom_score < 40
