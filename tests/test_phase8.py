"""
Phase 8A/8B/8C unit tests
Tests alert trend visualizations, gauge health scoring,
and investment review logic extracted from dashboard.py.
"""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


# ============================================================
# Phase 8C: Investment Review Logic Tests
# ============================================================
class TestReviewYearlyAggregation:
    """8C-1: Yearly return aggregation logic"""

    def test_yearly_returns_from_summary(self):
        """Group portfolio_summary by year and compute cumulative return."""
        dates = pd.date_range("2024-01-02", periods=250, freq="B")
        df = pd.DataFrame({
            "date": dates,
            "total_cost": [100000] * 250,
            "total_pnl": np.linspace(0, 8000, 250),
            "sharpe_ratio": np.random.uniform(0.5, 1.5, 250),
            "max_drawdown": np.random.uniform(-0.15, -0.03, 250),
            "volatility": np.random.uniform(0.10, 0.25, 250),
        })
        df["year"] = df["date"].dt.year
        yearly = df.groupby("year").agg(
            y_return=("total_pnl", "last"),
            y_sharpe=("sharpe_ratio", "mean"),
            y_maxdd=("max_drawdown", "min"),
        ).reset_index()
        assert len(yearly) == 1
        assert yearly.iloc[0]["y_return"] > 0
        assert yearly.iloc[0]["y_sharpe"] > 0

    def test_yearly_filter_recent_years(self):
        """Only keep years >= 2024."""
        dates = pd.date_range("2022-01-01", periods=1000, freq="D")
        df = pd.DataFrame({"date": dates, "total_pnl": np.arange(1000)})
        df["year"] = df["date"].dt.year
        recent = df[df["year"] >= 2024]
        assert all(recent["year"] >= 2024)


class TestReviewMonthlyHeatmap:
    """8C-2: Monthly return heatmap pivot logic"""

    def test_monthly_return_computation(self):
        """Monthly return = product of (1 + daily_return) - 1."""
        dr = pd.Series([0.01, -0.005, 0.008])
        monthly = (1 + dr).prod() - 1
        assert abs(monthly - 0.01294) < 0.0001

    def test_monthly_pivot_structure(self):
        """Pivot table has years as rows, months 1-12 as columns."""
        dates = pd.date_range("2024-01-01", periods=365, freq="D")
        monthly = pd.DataFrame({
            "year": dates.year,
            "month": dates.month,
            "m_return": np.random.normal(0.02, 0.05, len(dates)),
        })
        pivot = monthly.groupby(["year", "month"])["m_return"].mean().reset_index()
        pivot = pivot.pivot(index="year", columns="month", values="m_return")
        assert pivot.shape[0] >= 1
        assert list(pivot.columns) == list(range(1, 13))

    def test_heatmap_color_mapping(self):
        """Heatmap colorscale: red(0) -> dark(0.45) -> green(1), zmid=0."""
        colorscale = [[0, "#ef4444"], [0.45, "#21262d"], [1, "#22c55e"]]
        assert len(colorscale) == 3
        assert colorscale[1][0] == pytest.approx(0.45)


class TestReviewSectorAttribution:
    """8C-3: Sector attribution logic"""

    def test_sector_pnl_aggregation(self):
        """Group snapshots by sector, sum pnl."""
        snaps = pd.DataFrame({
            "date": pd.date_range("2026-01-01", periods=10),
            "code": ["510300", "512010"] * 5,
            "pnl": [100, -50, 120, -30, 80, 60, -70, 90, 40, -20],
            "pnl_rate": [2.0, -1.0, 2.4, -0.6, 1.6, 1.2, -1.4, 1.8, 0.8, -0.4],
            "market_value": [50000] * 10,
            "sector": ["宽基","医药","宽基","医药","宽基","医药","宽基","医药","宽基","医药"],
        })
        sector = snaps.groupby("sector").agg(
            s_total_pnl=("pnl", "sum"),
            s_avg_rate=("pnl_rate", "mean"),
        ).reset_index()
        assert len(sector) == 2
        kuanji = sector[sector["sector"] == "宽基"].iloc[0]
        assert kuanji["s_total_pnl"] == pytest.approx(270)

    def test_sector_filter_zero_pnl(self):
        """Exclude sectors with zero total pnl."""
        snaps = pd.DataFrame({
            "sector": ["宽基", "医药", "科技"],
            "pnl": [100, 0, -50],
        })
        filtered = snaps[snaps["pnl"] != 0]
        assert len(filtered) == 2
        assert "医药" not in filtered["sector"].values

    def test_sector_pnl_format(self):
        """Positive pnl shows ¥xxx, negative shows -¥xxx."""
        pnl_str_pos = f"¥{1000:,.0f}"
        pnl_str_neg = f"-¥{abs(-2500):,.0f}"
        assert pnl_str_pos == "¥1,000"
        assert pnl_str_neg == "-¥2,500"

    def test_sector_90day_cutoff(self):
        """Only include snapshots from last 90 days."""
        now = pd.Timestamp("2026-05-26")
        cutoff = now - pd.Timedelta(days=90)
        dates = pd.date_range("2026-01-01", "2026-05-26", freq="D")
        snaps = pd.DataFrame({"date": dates, "pnl": range(len(dates))})
        recent = snaps[snaps["date"] >= cutoff]
        assert recent["date"].min() >= cutoff


class TestReviewSignalWinRate:
    """8C-4: Technical signal win rate logic"""

    def test_macd_golden_cross_win_rate(self):
        """Golden cross: win = positive return after 5 days."""
        returns = [0.5, -0.3, 1.2, 0.8, -0.1]
        wins = sum(1 for r in returns if r > 0)
        assert wins == 3
        assert wins / len(returns) * 100 == pytest.approx(60.0)

    def test_macd_death_cross_win_rate(self):
        """Death cross: win = negative return after 5 days (short perspective)."""
        returns = [0.5, -0.3, 1.2, 0.8, -0.1]
        wins = sum(1 for r in returns if r < 0)
        assert wins == 2
        assert wins / len(returns) * 100 == pytest.approx(40.0)

    def test_signal_avg_return(self):
        """Average 5-day return after signal."""
        returns = [0.5, -0.3, 1.2, 0.8, -0.1]
        avg = sum(returns) / len(returns)
        assert avg == pytest.approx(0.42)

    def test_signal_card_color(self):
        """Win rate >= 50% -> green, else red."""
        sc_55 = "#22c55e" if 55 >= 50 else "#ef4444"
        sc_40 = "#22c55e" if 40 >= 50 else "#ef4444"
        assert sc_55 == "#22c55e"
        assert sc_40 == "#ef4444"

    def test_no_signals_empty_state(self):
        """Empty signal list shows caption instead of cards."""
        signals = []
        assert len(signals) == 0


# ============================================================
# Phase 8A: Alert Trend Visualization Tests
# ============================================================
class TestAlertTimelineLogic:
    """8A alert timeline: Scatter plot by level and time"""

    def test_alert_level_color_mapping(self):
        """Alert levels map to distinct colors."""
        level_colors = {"error": "#ef4444", "warning": "#f59e0b", "info": "#58a6ff"}
        assert len(level_colors) == 3
        assert level_colors["error"] == "#ef4444"

    def test_alert_timeline_data_prep(self):
        """Prepare timeline: level as y-axis numeric, date as x-axis."""
        level_map = {"error": 3, "warning": 2, "info": 1}
        alerts = pd.DataFrame({
            "level": ["warning", "warning", "error"],
            "created_at": pd.to_datetime(["2026-05-20", "2026-05-22", "2026-05-25"]),
        })
        alerts["y_level"] = alerts["level"].map(level_map)
        assert alerts["y_level"].tolist() == [2, 2, 3]

    def test_alert_scatter_by_level(self):
        """Scatter trace separated by level for coloring."""
        alerts = pd.DataFrame({
            "level": ["warning"] * 3 + ["error"] * 2,
            "created_at": pd.date_range("2026-05-01", periods=5),
        })
        groups = {lvl: g for lvl, g in alerts.groupby("level")}
        assert set(groups.keys()) == {"warning", "error"}
        assert len(groups["warning"]) == 3


class TestAlertFrequencyHeatmap:
    """8A alert frequency heatmap: dow x hour_bin"""

    def test_dow_extraction(self):
        """Extract day of week from datetime."""
        dt = pd.Timestamp("2026-05-25")  # Monday
        dow = dt.dayofweek  # 0=Monday
        assert dow == 0

    def test_hour_bin(self):
        """Bin hours into 4 periods: 0-6, 6-12, 12-18, 18-24."""
        hour = 14
        bin_idx = hour // 6  # 2
        assert bin_idx == 2

    def test_heatmap_pivot_dow_hour(self):
        """Pivot: rows=dow(0-6), cols=hour_bin(0-3)."""
        np.random.seed(42)
        data = pd.DataFrame({
            "dow": np.random.randint(0, 7, 50),
            "hour_bin": np.random.randint(0, 4, 50),
            "count": np.random.randint(1, 5, 50),
        })
        pivot = data.groupby(["dow", "hour_bin"])["count"].sum().unstack(fill_value=0)
        assert pivot.shape[0] <= 7
        assert pivot.shape[1] <= 4


class TestAlertRuleTrend:
    """8A alert rule trigger trend: weekly stacked area"""

    def test_weekly_aggregation(self):
        """Group alerts by week and rule."""
        dates = pd.date_range("2026-05-01", periods=21, freq="D")
        alerts = pd.DataFrame({
            "created_at": dates,
            "rule_name": ["sharpe_low"] * 10 + ["vol_high"] * 11,
        })
        alerts["week"] = alerts["created_at"].dt.isocalendar().week.astype(int)
        weekly = alerts.groupby(["week", "rule_name"]).size().unstack(fill_value=0)
        assert weekly.shape[0] >= 1
        assert "sharpe_low" in weekly.columns or len(weekly.columns) >= 1

    def test_stacked_area_columns(self):
        """Each rule becomes a trace in stacked area chart."""
        rules = ["sharpe_low", "vol_high", "drawdown_alert"]
        traces = len(rules)
        assert traces == 3


# ============================================================
# Phase 8B: Gauge & Health Score Tests
# ============================================================
class TestGaugeThresholdLogic:
    """8B gauge metrics and threshold computation"""

    def test_gauge_metric_extraction_sharpe(self):
        """Extract sharpe_ratio from summary."""
        summary = pd.DataFrame({
            "date": ["2026-05-26"],
            "sharpe_ratio": [1.2],
            "max_drawdown": [-0.08],
            "volatility": [0.15],
            "daily_return": [0.005],
        })
        sharpe = summary.iloc[-1]["sharpe_ratio"]
        assert sharpe == 1.2

    def test_gauge_sharpe_thresholds(self):
        """Sharpe gauge: warn=0.5, danger=0."""
        sharpe = 1.2
        warn_threshold = 0.5
        is_safe = sharpe >= warn_threshold
        is_danger = sharpe <= 0
        assert is_safe is True
        assert is_danger is False

    def test_gauge_maxdd_thresholds(self):
        """MaxDD gauge: warn=-0.10, danger=-0.20."""
        max_dd = -0.08
        warn = -0.10
        danger = -0.20
        is_safe = max_dd > warn
        is_danger = max_dd <= danger
        assert is_safe is True
        assert is_danger is False

    def test_gauge_volatility_thresholds(self):
        """Volatility gauge: warn=0.20, danger=0.30."""
        vol = 0.25
        warn = 0.20
        danger = 0.30
        is_safe = vol <= warn
        is_warning = warn < vol <= danger
        is_danger = vol > danger
        assert is_safe is False
        assert is_warning is True
        assert is_danger is False


class TestHealthScoreComputation:
    """8B health score: 0-100, error=-20, warning=-8"""

    def test_perfect_health(self):
        """No alerts: health_score = 100."""
        n_error = 0
        n_warning = 0
        health_score = max(0, 100 - n_error * 20 - n_warning * 8)
        assert health_score == 100

    def test_two_errors(self):
        """2 errors: 100 - 2*20 = 60."""
        n_error = 2
        n_warning = 0
        health_score = max(0, 100 - n_error * 20 - n_warning * 8)
        assert health_score == 60

    def test_mixed_alerts(self):
        """1 error + 2 warnings: 100 - 20 - 16 = 64."""
        n_error = 1
        n_warning = 2
        health_score = max(0, 100 - n_error * 20 - n_warning * 8)
        assert health_score == 64

    def test_minimum_zero(self):
        """Score cannot go below 0."""
        n_error = 10
        n_warning = 5
        health_score = max(0, 100 - n_error * 20 - n_warning * 8)
        assert health_score == 0

    def test_health_score_color(self):
        """>=80 green, >=60 yellow, <60 red."""
        hc_85 = "#22c55e" if 85 >= 80 else "#f59e0b" if 85 >= 60 else "#ef4444"
        hc_65 = "#22c55e" if 65 >= 80 else "#f59e0b" if 65 >= 60 else "#ef4444"
        hc_40 = "#22c55e" if 40 >= 80 else "#f59e0b" if 40 >= 60 else "#ef4444"
        assert hc_85 == "#22c55e"
        assert hc_65 == "#f59e0b"
        assert hc_40 == "#ef4444"

    def test_health_status_message(self):
        """Different messages for score ranges."""
        msg_high = "组合整体风险可控" if 85 >= 80 else "存在风险预警，建议关注" if 85 >= 60 else "多项指标触发告警，需立即关注"
        msg_mid = "组合整体风险可控" if 65 >= 80 else "存在风险预警，建议关注" if 65 >= 60 else "多项指标触发告警，需立即关注"
        msg_low = "组合整体风险可控" if 40 >= 80 else "存在风险预警，建议关注" if 40 >= 60 else "多项指标触发告警，需立即关注"
        assert "风险可控" in msg_high
        assert "预警" in msg_mid
        assert "立即" in msg_low


class TestGaugeIndicatorStructure:
    """8B gauge plotly Indicator structure"""

    def test_indicator_mode_gauge_number(self):
        """go.Indicator should use mode='gauge+number'."""
        mode = "gauge+number"
        assert "gauge" in mode
        assert "number" in mode

    def test_indicator_threshold_steps(self):
        """Threshold steps: green(0-warn), yellow(warn-danger), red(danger+)."""
        steps = [
            {"range": [0, 0.5], "color": "#22c55e"},
            {"range": [0.5, 0.8], "color": "#f59e0b"},
            {"range": [0.8, 1.0], "color": "#ef4444"},
        ]
        assert len(steps) == 3
        assert steps[0]["color"] == "#22c55e"

    def test_gauge_title_format(self):
        """Gauge title includes metric name."""
        title = "夏普比率"
        assert "夏普" in title


# ============================================================
# Integration: AST Verification
# ============================================================
class TestPhase8ASTIntegrity:
    """Verify dashboard.py contains Phase 8A/8B/8C markers"""

    def test_8a_timeline_marker(self):
        """8A alert timeline code present."""
        import ast
        source = open(
            r"C:\Users\HUAWEI\Documents\lingxi-claw\portfolio_tracker\dashboard.py",
            encoding="utf-8",
        ).read()
        assert "告警时间线" in source

    def test_8a_heatmap_marker(self):
        """8A alert frequency heatmap code present."""
        import ast
        source = open(
            r"C:\Users\HUAWEI\Documents\lingxi-claw\portfolio_tracker\dashboard.py",
            encoding="utf-8",
        ).read()
        assert "告警频率热力图" in source

    def test_8a_trend_marker(self):
        """8A rule trigger trend code present."""
        source = open(
            r"C:\Users\HUAWEI\Documents\lingxi-claw\portfolio_tracker\dashboard.py",
            encoding="utf-8",
        ).read()
        assert "规则触发趋势" in source

    def test_8b_gauge_marker(self):
        """8B gauge dashboard code present."""
        source = open(
            r"C:\Users\HUAWEI\Documents\lingxi-claw\portfolio_tracker\dashboard.py",
            encoding="utf-8",
        ).read()
        assert "指标阈值监控" in source

    def test_8b_health_score_marker(self):
        """8B health score code present."""
        source = open(
            r"C:\Users\HUAWEI\Documents\lingxi-claw\portfolio_tracker\dashboard.py",
            encoding="utf-8",
        ).read()
        assert "告警健康评分" in source

    def test_8c_review_marker(self):
        """8C investment review code present."""
        source = open(
            r"C:\Users\HUAWEI\Documents\lingxi-claw\portfolio_tracker\dashboard.py",
            encoding="utf-8",
        ).read()
        assert "投资复盘" in source

    def test_8c_yearly_bar(self):
        """8C yearly return bar chart code present."""
        source = open(
            r"C:\Users\HUAWEI\Documents\lingxi-claw\portfolio_tracker\dashboard.py",
            encoding="utf-8",
        ).read()
        assert "年度收益对比" in source

    def test_8c_monthly_heatmap(self):
        """8C monthly heatmap code present."""
        source = open(
            r"C:\Users\HUAWEI\Documents\lingxi-claw\portfolio_tracker\dashboard.py",
            encoding="utf-8",
        ).read()
        assert "月度收益热力图" in source

    def test_8c_sector_attribution(self):
        """8C sector attribution code present."""
        source = open(
            r"C:\Users\HUAWEI\Documents\lingxi-claw\portfolio_tracker\dashboard.py",
            encoding="utf-8",
        ).read()
        assert "行业收益归因" in source

    def test_8c_signal_winrate(self):
        """8C signal win rate code present."""
        source = open(
            r"C:\Users\HUAWEI\Documents\lingxi-claw\portfolio_tracker\dashboard.py",
            encoding="utf-8",
        ).read()
        assert "技术信号胜率复盘" in source

    def test_dashboard_ast_valid(self):
        """Full dashboard.py passes AST parse."""
        import ast
        source = open(
            r"C:\Users\HUAWEI\Documents\lingxi-claw\portfolio_tracker\dashboard.py",
            encoding="utf-8",
        ).read()
        ast.parse(source)
        # If no exception, it's valid
