"""P0: 多因子综合决策引擎测试。"""
import pytest
import numpy as np
import pandas as pd


class TestScoreTechnical:
    def _fn(self):
        from src.analysis.multi_factor_score import _score_technical
        return _score_technical

    def test_strong_bull(self):
        r = self._fn()(85.0)
        assert r.score == 85.0 and r.level == "强烈看多"

    def test_neutral(self):
        r = self._fn()(50.0)
        assert r.level == "中性"

    def test_strong_bear(self):
        r = self._fn()(10.0)
        assert r.level == "强烈看空"


class TestScoreRisk:
    def _fn(self):
        from src.analysis.multi_factor_score import _score_risk
        return _score_risk

    def test_low_risk(self):
        r = self._fn()(20.0)
        assert r.score == 80.0 and r.level == "极低风险"

    def test_high_risk(self):
        r = self._fn()(80.0)
        assert r.score == 20.0 and r.level == "高风险"

    def test_medium_risk(self):
        r = self._fn()(55.0)
        assert r.level == "中风险" and r.score == 45.0


class TestScoreFundFlow:
    def _fn(self):
        from src.analysis.multi_factor_score import _score_fund_flow
        return _score_fund_flow

    def test_none_df(self):
        r = self._fn()(None)
        assert r.score == 50.0 and r.level == "数据不足"

    def test_empty_df(self):
        r = self._fn()(pd.DataFrame())
        assert r.score == 50.0

    def test_positive_flow(self):
        df = pd.DataFrame({"net_amount": [2e8, 1.5e8, 1e8, 0.5e8, 0.3e8]})
        r = self._fn()(df)
        assert r.score > 50.0 and "净流入" in r.detail

    def test_negative_flow(self):
        df = pd.DataFrame({"net_amount": [-2e8, -1.5e8, -1e8, -0.5e8, -0.3e8]})
        r = self._fn()(df)
        assert r.score < 50.0 and "净流出" in r.detail

    def test_score_clamped(self):
        r = self._fn()(pd.DataFrame({"net_amount": [1e10] * 5}))
        assert 0 <= r.score <= 100


class TestScoreFundamental:
    def _fn(self):
        from src.analysis.multi_factor_score import _score_fundamental
        return _score_fundamental

    def test_empty_dict(self):
        r = self._fn()({})
        assert r.score == 50.0 and r.level == "数据不足"

    def test_none_dict(self):
        r = self._fn()(None)
        assert r.score == 50.0

    def test_low_pe(self):
        r = self._fn()({"pe_ratio": 10.0})
        assert r.score > 50.0 and "低估" in r.level

    def test_high_pe(self):
        r = self._fn()({"pe_ratio": 40.0})
        assert r.score < 50.0 and "高估" in r.level

    def test_discount(self):
        r = self._fn()({"discount_rate": -0.8})
        assert r.score > 50.0

    def test_premium(self):
        r = self._fn()({"discount_rate": 1.5})
        assert r.score < 50.0

    def test_combined(self):
        r = self._fn()({"pe_ratio": 11.0, "dividend_yield": 4.5, "discount_rate": -0.3})
        assert r.score > 60.0

    def test_nan_pe(self):
        r = self._fn()({"pe_ratio": float("nan")})
        assert r.score == 50.0


class TestRiskConstraint:
    def _fn(self):
        from src.analysis.multi_factor_score import compute_multi_factor_score
        return compute_multi_factor_score

    def test_high_risk_blocks_buy(self):
        r = self._fn()("A", "TA", signal_score_val=85, risk_score_val=70)
        assert r.risk_constrained is True
        assert r.action != "买入"

    def test_high_risk_blocks_add(self):
        r = self._fn()("B", "TB", signal_score_val=70, risk_score_val=70)
        assert r.risk_constrained is True
        assert r.action != "加仓"

    def test_low_risk_no_constraint(self):
        r = self._fn()("C", "TC", signal_score_val=85, risk_score_val=20)
        assert r.risk_constrained is False
        assert r.action == "加仓"

    def test_sell_not_constrained(self):
        r = self._fn()("E", "TE", signal_score_val=5, risk_score_val=95)
        assert r.action == "卖出"


class TestComputeMultiFactorScore:
    def _fn(self):
        from src.analysis.multi_factor_score import compute_multi_factor_score
        return compute_multi_factor_score

    def test_all_neutral(self):
        r = self._fn()("X", "TX", 50, 50)
        assert r.total_score == 50.0 and r.action == "持有"

    def test_bullish_all_factors(self):
        r = self._fn()("Y", "TY", 90, 10,
             fund_flow_df=pd.DataFrame({"net_amount": [5e8]*5}),
             fund_data={"pe_ratio": 8.0, "discount_rate": -1.0})
        assert r.total_score >= 75.0 and r.action == "买入"

    def test_bearish_all_factors(self):
        r = self._fn()("Z", "TZ", 10, 90,
             fund_flow_df=pd.DataFrame({"net_amount": [-5e8]*5}),
             fund_data={"pe_ratio": 50.0})
        assert r.total_score <= 25.0 and r.action == "卖出"

    def test_score_range(self):
        f = self._fn()
        for sig in [0, 25, 50, 75, 100]:
            for risk in [0, 25, 50, 75, 100]:
                r = f("T", "T", sig, risk)
                assert 0 <= r.total_score <= 100

    def test_reasons_populated(self):
        r = self._fn()("R", "TR", 80, 20,
             fund_flow_df=pd.DataFrame({"net_amount": [3e8]*5}),
             fund_data={"pe_ratio": 12.0})
        assert len(r.reasons) > 0

    def test_dataclass_compat(self):
        r = self._fn()("D", "TD", 60, 40)
        assert r["code"] == "D" and r.get("name") == "TD"
        assert "total_score" in r.keys()


class TestActionGeneration:
    def _fn(self):
        from src.analysis.multi_factor_score import _generate_action
        return _generate_action

    def test_buy_threshold(self):
        a, u = self._fn()(78)
        assert a == "买入" and u == "强烈建议"

    def test_add_threshold(self):
        a, _ = self._fn()(65)
        assert a == "加仓"

    def test_hold_threshold(self):
        a, _ = self._fn()(50)
        assert a == "持有"

    def test_watch_threshold(self):
        a, _ = self._fn()(35)
        assert a == "观望"

    def test_sell_threshold(self):
        a, _ = self._fn()(15)
        assert a == "卖出"

    def test_boundaries(self):
        f = self._fn()
        assert f(75)[0] == "买入"
        assert f(62)[0] == "加仓"
        assert f(42)[0] == "持有"
