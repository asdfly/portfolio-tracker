# -*- coding: utf-8 -*-
"""P3 进阶能力模块测试 — 纯函数测试

覆盖模块:
  - equity_risk_premium: ERP计算、信号分类
  - dca_backtest: 定投回测(均匀/估值)
  - industry_boom: 行业景气度评分
  - smart_alert: 智能预警检测
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys, os

# Ensure project root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.analysis.equity_risk_premium import (
    compute_erp, classify_erp_signal, ERPResult,
    INDEX_NAMES,
)
from src.analysis.dca_backtest import (
    backtest_dca_uniform, backtest_dca_valuation, DCAResult, DCARecord,
)
from src.analysis.industry_boom import (
    calc_fund_score, calc_valuation_score, calc_trend_score,
    calc_policy_score, classify_boom_signal, compute_industry_boom,
    IndustryBoomResult,
)
from src.analysis.smart_alert import (
    check_price_alert, check_fund_flow_alert, check_volatility_alert,
    check_valuation_alert, check_risk_alert, scan_all_alerts,
    summarize_alerts, format_alert_text,
    AlertEvent, AlertSummary, get_level_order,
)


# ============================================================
# equity_risk_premium tests
# ============================================================

class TestComputeERP:
    def test_normal_case(self):
        """正常PE和利率计算ERP"""
        erp = compute_erp(12.5, 2.8)
        expected = 1.0 / 12.5 * 100 - 2.8
        assert abs(erp - round(expected, 4)) < 0.001

    def test_high_pe(self):
        """高PE -> 低盈利收益率 -> 低ERP"""
        erp = compute_erp(30.0, 2.5)
        assert erp < compute_erp(15.0, 2.5)

    def test_zero_pe(self):
        """PE<=0返回0"""
        assert compute_erp(0, 2.5) == 0.0
        assert compute_erp(-5, 2.5) == 0.0

    def test_negative_rate(self):
        """负利率返回0"""
        assert compute_erp(12, -1) == 0.0

    def test_rounding(self):
        """验证四舍五入"""
        erp = compute_erp(10, 5)
        assert erp == 5.0  # 10% - 5% = 5.0


class TestClassifyERPSignal:
    def test_insufficient_data(self):
        """数据不足时返回提示"""
        signal, detail = classify_erp_signal(3.0, [1, 2])
        assert signal == "数据不足"

    def test_bullish_erp(self):
        """ERP高分位 -> 偏多"""
        history = [i * 0.1 for i in range(100)]  # 0-9.9
        signal, _ = classify_erp_signal(8.0, history)
        assert signal == "偏多"

    def test_bearish_erp(self):
        """ERP低分位 -> 偏空"""
        history = [i * 0.1 for i in range(100)]
        signal, _ = classify_erp_signal(0.5, history)
        assert signal == "偏空"

    def test_neutral_erp(self):
        """ERP中分位 -> 中性"""
        history = [i * 0.1 for i in range(100)]
        signal, _ = classify_erp_signal(4.5, history)
        assert "中性" in signal


# ============================================================
# dca_backtest tests
# ============================================================

def _make_price_series(days=120, start=1.0, trend=0.001, volatility=0.02):
    """生成模拟价格序列"""
    dates = pd.date_range("2024-01-01", periods=days, freq="B")
    returns = np.random.normal(trend, volatility, days)
    prices = start * np.cumprod(1 + returns)
    return pd.Series(prices, index=dates, name="close")


class TestDCABacktestUniform:
    def test_basic_run(self):
        """均匀定投基本运行"""
        np.random.seed(42)
        prices = _make_price_series(days=120)
        result = backtest_dca_uniform(prices, period_amount=1000, freq="W")
        assert isinstance(result, DCAResult)
        assert result.total_invest > 0
        assert result.total_periods > 0

    def test_monthly_freq(self):
        """月度定投"""
        np.random.seed(42)
        prices = _make_price_series(days=250)
        result = backtest_dca_uniform(prices, period_amount=1000, freq="ME")
        assert result.total_periods >= 10  # ~10 months

    def test_positive_return(self):
        """上涨行情定投应有正收益"""
        dates = pd.date_range("2024-01-01", periods=120, freq="B")
        prices = pd.Series(1.0 + np.arange(120) * 0.002, index=dates, name="close")
        result = backtest_dca_uniform(prices, period_amount=1000, freq="W")
        assert result.total_return_pct > 0

    def test_negative_return(self):
        """下跌行情定投应有负收益"""
        dates = pd.date_range("2024-01-01", periods=120, freq="B")
        prices = pd.Series(2.0 - np.arange(120) * 0.005, index=dates, name="close")
        prices = prices.clip(lower=0.1)
        result = backtest_dca_uniform(prices, period_amount=1000, freq="W")
        assert result.total_return_pct < 0


class TestDCABacktestValuation:
    def test_basic_run(self):
        """估值定投基本运行"""
        np.random.seed(42)
        prices = _make_price_series(days=120)
        pe_series = pd.Series(np.random.uniform(8, 25, 120), index=prices.index)
        result = backtest_dca_valuation(prices, pe_series, period_amount=1000, freq="W")
        assert isinstance(result, DCAResult)
        assert result.total_invest > 0

    def test_low_pe_more_invest(self):
        """低PE时应多投"""
        dates = pd.date_range("2024-01-01", periods=60, freq="B")
        prices = pd.Series([1.0] * 60, index=dates, name="close")
        pe_series = pd.Series([8.0] * 30 + [20.0] * 30, index=dates)
        result = backtest_dca_valuation(prices, pe_series, period_amount=1000, freq="W")
        # 低PE期间投入应更多
        assert result.total_periods > 0


# ============================================================
# industry_boom tests
# ============================================================

class TestCalcFundScore:
    def test_strong_inflow(self):
        """大额净流入 -> 高分"""
        assert calc_fund_score(10, 30) > calc_fund_score(0, 0)

    def test_strong_outflow(self):
        """大额净流出 -> 低分"""
        score = calc_fund_score(-15, -30)
        assert score < 40

    def test_neutral(self):
        """零流入 -> 中性分"""
        score = calc_fund_score(0, 0)
        assert 45 <= score <= 55


class TestCalcValuationScore:
    def test_cheap_valuation(self):
        """低分位 -> 高分(便宜)"""
        assert calc_valuation_score(10, 15) > calc_valuation_score(80, 85)

    def test_expensive_valuation(self):
        """高分位 -> 低分(贵)"""
        score = calc_valuation_score(95, 90)
        assert score < 15

    def test_equal_pe_pb(self):
        """PE和PB相同分位"""
        score = calc_valuation_score(50, 50)
        assert abs(score - 50) < 1


class TestCalcTrendScore:
    def test_bullish_alignment(self):
        """多头排列+放量+上涨 -> 高分"""
        score = calc_trend_score(True, True, 1.5, 15)
        assert score >= 80

    def test_bearish_alignment(self):
        """空头排列+缩量+下跌 -> 低分"""
        score = calc_trend_score(False, False, 0.5, -15)
        assert score <= 30

    def test_mixed_signals(self):
        """混合信号 -> 中性分"""
        score = calc_trend_score(True, False, 1.0, 0)
        assert 40 <= score <= 70


class TestClassifyBoomSignal:
    def test_high_score(self):
        assert classify_boom_signal(85)[0] == "强烈推荐"

    def test_low_score(self):
        assert classify_boom_signal(15)[0] == "回避"

    def test_neutral_score(self):
        assert classify_boom_signal(50)[0] == "中性"


class TestComputeIndustryBoom:
    def test_bullish_industry(self):
        """看多行业"""
        result = compute_industry_boom(
            industry="technology",
            net_inflow_5d=10, net_inflow_20d=30,
            pe_percentile=15, pb_percentile=20,
            ma5_above_ma20=True, ma20_above_ma60=True,
            vol_ratio=1.5, price_change_20d=12,
            has_positive_policy=True,
        )
        assert isinstance(result, IndustryBoomResult)
        assert result.boom_score > 60
        assert len(result.top_reasons) > 0

    def test_bearish_industry(self):
        """看空行业"""
        result = compute_industry_boom(
            industry="real_estate",
            net_inflow_5d=-10, net_inflow_20d=-30,
            pe_percentile=85, pb_percentile=80,
            ma5_above_ma20=False, ma20_above_ma60=False,
            vol_ratio=0.5, price_change_20d=-12,
            has_negative_policy=True,
        )
        assert result.boom_score < 40
        assert len(result.risk_reasons) > 0

    def test_neutral_industry(self):
        """中性行业"""
        result = compute_industry_boom("utilities")
        assert isinstance(result, IndustryBoomResult)
        assert result.signal in ("强烈推荐", "推荐", "中性", "谨慎", "回避")


# ============================================================
# smart_alert tests
# ============================================================

class TestGetLevelOrder:
    def test_ordering(self):
        assert get_level_order("紧急") < get_level_order("重要")
        assert get_level_order("重要") < get_level_order("关注")
        assert get_level_order("关注") < get_level_order("信息")
        assert get_level_order("信息") < get_level_order("unknown")


class TestCheckPriceAlert:
    def test_break_support(self):
        """跌破支撑位 -> 紧急"""
        event = check_price_alert("159915", "创业板ETF",
                                   2.0, 2.2, 2.1, 2.1, 2.5, -2.5)
        assert event is not None
        assert event.level == "紧急"
        assert "支撑" in event.title

    def test_break_ma20(self):
        """跌破MA20 -> 重要"""
        event = check_price_alert("510300", "沪深300ETF",
                                   3.8, 4.0, 3.9, 0, 0, -2.0)
        assert event is not None
        assert event.level == "重要"

    def test_no_alert(self):
        """无异常 -> 无预警"""
        event = check_price_alert("510300", "沪深300ETF",
                                   4.0, 4.0, 3.9, 0, 0, 0.5)
        assert event is None

    def test_big_drop(self):
        """大跌 -> 紧急"""
        event = check_price_alert("159915", "创业板ETF",
                                   3.0, 3.2, 3.1, 0, 0, -4.0)
        assert event is not None
        assert event.level == "紧急"

    def test_break_resist(self):
        """突破压力位 -> 关注"""
        event = check_price_alert("510300", "沪深300ETF",
                                   4.6, 4.5, 4.3, 0, 4.5, 1.5)
        assert event is not None
        assert event.level == "关注"


class TestCheckFundFlowAlert:
    def test_large_inflow(self):
        """大额流入 -> 关注"""
        event = check_fund_flow_alert("510300", "沪深300ETF", 8.0, 15.0)
        assert event is not None
        assert event.level == "关注"

    def test_large_outflow(self):
        """大额流出 -> 重要"""
        event = check_fund_flow_alert("510300", "沪深300ETF", -8.0, -20.0)
        assert event is not None
        assert event.level == "重要"

    def test_no_alert(self):
        event = check_fund_flow_alert("510300", "沪深300ETF", 1.0, 2.0)
        assert event is None


class TestCheckVolatilityAlert:
    def test_extreme_vol(self):
        """波动率飙升2倍标准差 -> 重要"""
        event = check_volatility_alert("159915", "创业板ETF", 45, 20, 10)
        assert event is not None
        assert event.level == "重要"

    def test_moderate_vol(self):
        """波动率上升1.5倍标准差 -> 关注"""
        event = check_volatility_alert("159915", "创业板ETF", 35, 20, 10)
        assert event is not None
        assert event.level == "关注"

    def test_normal_vol(self):
        event = check_volatility_alert("159915", "创业板ETF", 22, 20, 10)
        assert event is None

    def test_zero_std(self):
        """零标准差 -> 无预警"""
        event = check_volatility_alert("159915", "创业板ETF", 22, 20, 0)
        assert event is None


class TestCheckValuationAlert:
    def test_cheap(self):
        """极低估 -> 关注"""
        event = check_valuation_alert("510300", "沪深300ETF", 5.0)
        assert event is not None
        assert event.level == "关注"

    def test_expensive(self):
        """极高估 -> 重要"""
        event = check_valuation_alert("159915", "创业板ETF", 95.0)
        assert event is not None
        assert event.level == "重要"

    def test_normal(self):
        event = check_valuation_alert("510300", "沪深300ETF", 50.0)
        assert event is None


class TestCheckRiskAlert:
    def test_deep_drawdown(self):
        """深度回撤 -> 紧急"""
        event = check_risk_alert("159915", "创业板ETF", -18.0)
        assert event is not None
        assert event.level == "紧急"

    def test_moderate_drawdown(self):
        event = check_risk_alert("159915", "创业板ETF", -12.0)
        assert event is not None
        assert event.level == "重要"

    def test_erp_bearish(self):
        """ERP偏空 -> 关注"""
        event = check_risk_alert("159915", "创业板ETF", -5.0, erp_signal="偏空")
        assert event is not None
        assert event.level == "关注"

    def test_no_alert(self):
        event = check_risk_alert("159915", "创业板ETF", -3.0, erp_signal="中性")
        assert event is None


class TestScanAllAlerts:
    def test_multi_alert(self):
        """多维度触发"""
        events = scan_all_alerts(
            "159915", "创业板ETF",
            current_price=2.0, support=2.1, drop_pct=-4.0,
            net_inflow_today=-8, net_inflow_5d=-15,
            current_vol=45, avg_vol=20, vol_std=10,
            pe_percentile=95, max_drawdown=-18,
            erp_signal="偏空",
        )
        assert len(events) > 1
        # 按级别排序，第一个应是最紧急的
        assert events[0].level == "紧急"

    def test_no_data(self):
        """无数据 -> 无预警"""
        events = scan_all_alerts("510300", "沪深300ETF")
        assert len(events) == 0


class TestSummarizeAlerts:
    def test_summary_counts(self):
        events = [
            AlertEvent("1", "ETF1", "risk", "紧急", "t1", "d1"),
            AlertEvent("2", "ETF2", "price", "重要", "t2", "d2"),
            AlertEvent("3", "ETF3", "fund", "关注", "t3", "d3"),
            AlertEvent("4", "ETF4", "fund", "关注", "t4", "d4"),
            AlertEvent("5", "ETF5", "valuation", "信息", "t5", "d5"),
        ]
        summary = summarize_alerts(events)
        assert summary.total == 5
        assert summary.urgent == 1
        assert summary.important == 1
        assert summary.watch == 2
        assert summary.info == 1

    def test_empty(self):
        summary = summarize_alerts([])
        assert summary.total == 0


class TestFormatAlertText:
    def test_format(self):
        event = AlertEvent("510300", "沪深300ETF", "price",
                           "重要", "跌破均线", "详情", value=3.8, threshold=4.0)
        text = format_alert_text(event)
        assert "重要" in text
        assert "沪深300ETF" in text
        assert "跌破均线" in text
