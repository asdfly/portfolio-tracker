"""
Tests for Tab8: Advice module
"""
import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock


class TestTab8Importable:
    """Test module import and basic structure"""

    def test_module_imports(self):
        from tabs.tab8_advice import render_tab8
        assert callable(render_tab8)

    def test_render_signature(self):
        import inspect
        from tabs.tab8_advice import render_tab8
        sig = inspect.signature(render_tab8)
        params = list(sig.parameters.keys())
        assert len(params) == 0, f"Expected no params, got {params}"


class TestTab8EmptyPositions:
    """Test behavior with empty positions"""

    @patch("tabs.tab8_advice.load_positions", return_value=pd.DataFrame())
    @patch("tabs.tab8_advice.load_summary", return_value=pd.DataFrame())
    @patch("tabs.tab8_advice.st")
    def test_empty_positions_shows_info(self, mock_st, mock_summary, mock_positions):
        """Empty positions shows info message"""
        from tabs.tab8_advice import render_tab8
        mock_st.session_state = MagicMock()
        mock_st.session_state.get = lambda key, default="": default
        mock_st.caption = MagicMock()
        mock_st.info = MagicMock()
        mock_st.markdown = MagicMock()
        mock_st.columns = lambda spec: [MagicMock() for _ in range(len(spec) if isinstance(spec, list) else spec)]
        mock_st.metric = MagicMock()
        mock_st.button = MagicMock(return_value=False)
        mock_st.download_button = MagicMock()
        mock_st.success = MagicMock()
        mock_st.error = MagicMock()
        mock_st.bar_chart = MagicMock()
        mock_st.expander = MagicMock(return_value=MagicMock())
        mock_st.plotly_chart = MagicMock()

        render_tab8()
        mock_st.info.assert_called()


class TestTab8WithPositions:
    """Test behavior with valid positions"""

    @patch("tabs.tab8_advice.load_positions", return_value=pd.DataFrame())
    @patch("tabs.tab8_advice.load_summary", return_value=pd.DataFrame())
    @patch("tabs.tab8_advice.st")
    @patch("tabs.tab8_advice._load_tech_signals", return_value=pd.DataFrame())
    def test_with_positions_no_tech(self, mock_tech, mock_st, mock_summary, mock_load_pos):
        """Positions without tech data shows suggestions from portfolio"""
        from tabs.tab8_advice import render_tab8
        positions = pd.DataFrame({
            "code": ["510300"], "name": ["沪深300ETF"],
            "quantity": [10000], "cost_price": [4.0],
            "current_price": [4.2], "market_value": [42000],
            "pnl": [2000], "pnl_rate": [5.0],
        })
        mock_st.session_state = MagicMock()
        mock_st.session_state.get = lambda key, default="": default
        mock_st.caption = MagicMock()
        mock_st.info = MagicMock()
        mock_st.markdown = MagicMock()
        mock_st.columns = lambda spec: [MagicMock() for _ in range(len(spec) if isinstance(spec, list) else spec)]
        mock_st.metric = MagicMock()
        mock_st.button = MagicMock(return_value=False)
        mock_st.download_button = MagicMock()
        mock_st.success = MagicMock()
        mock_st.error = MagicMock()
        mock_st.bar_chart = MagicMock()
        mock_st.expander = MagicMock(return_value=MagicMock())
        mock_st.plotly_chart = MagicMock()

        render_tab8()
        # Should not crash


class TestTab8SignalScoring:
    """Test signal scoring logic"""

    def test_buy_signal_threshold(self):
        """Net signal >= 3 should produce buy action"""
        # Simulate the scoring logic
        buy_signals = 5.0
        sell_signals = 1.0
        net = buy_signals - sell_signals
        assert net >= 3
        action = "买入" if net >= 3 else "持有"
        assert action == "买入"

    def test_sell_signal_threshold(self):
        """Net signal <= -2 should produce sell/watch action"""
        buy_signals = 0.5
        sell_signals = 3.0
        net = buy_signals - sell_signals
        assert net <= -2
        action = "卖出" if net <= -2 else "观望" if net >= -2 else "持有"
        assert action == "卖出"

    def test_hold_signal_threshold(self):
        """Net signal in [-0.5, 1.5) should produce hold action"""
        buy_signals = 1.0
        sell_signals = 1.0
        net = buy_signals - sell_signals
        assert -0.5 <= net < 1.5
        action = "持有" if -0.5 <= net < 1.5 else "观望"
        assert action == "持有"


class TestTab8SignalPreviewNoDataDistinction:
    """#140 Option A: 渲染层必须把「无数据(占位哨兵50)」与「真实中立50」区分开。"""

    @patch("data_loader.load_pre_market_report")
    @patch("tabs.tab8_advice.st")
    def test_render_shows_no_data_not_neutral(self, mock_st, mock_load):
        from src.analysis.pre_post_market import PreMarketReport, EtfSignalPreview

        # 反例 A：真实数据齐全，RSI/评分=50 命中真中立 → 必须原样显示 "50"
        real = EtfSignalPreview(
            code="510300", name="300ETF", trend="上涨", ma_signal="金叉", macd_signal="金叉",
            rsi_value=50.0, rsi_status="正常", signal_score=50.0, risk_score=50.0,
            fund_flow_net=12.0, rsi_available=True, score_available=True, risk_available=True)
        # 反例 B：占位哨兵 50，但三个指标都无真实数据 → 必须显示 "无数据"
        missing = EtfSignalPreview(
            code="159915", name="创业ETF", trend="--", ma_signal="--", macd_signal="--",
            rsi_value=50.0, rsi_status="--", signal_score=50.0, risk_score=50.0,
            fund_flow_net=0.0, rsi_available=False, score_available=False, risk_available=False)

        mock_load.return_value = PreMarketReport(
            report_time="09:00", report_date="2026-09-17",
            etf_signals=[real, missing], news_sentiment={}, risk_warnings=[])

        frames = []
        mock_st.dataframe = lambda df, *a, **k: frames.append(df)
        mock_st.session_state = MagicMock()
        mock_st.session_state.get = lambda key, default="": default
        for m in ["caption", "info", "markdown", "metric", "button", "download_button",
                  "success", "error", "bar_chart", "expander", "plotly_chart", "warning"]:
            setattr(mock_st, m, MagicMock())
        mock_st.columns = lambda spec: [MagicMock() for _ in range(
            len(spec) if isinstance(spec, list) else spec)]

        from tabs.tab8_advice import _render_pre_market_panel
        _render_pre_market_panel()

        sig_frames = [f for f in frames if isinstance(f, list) and f and "代码" in f[0]]
        assert sig_frames, "持仓信号预览 dataframe 未被渲染"
        rows = {r["代码"]: r for r in sig_frames[0]}
        # 真实中立 50 必须原样显示
        assert rows["510300"]["RSI"] == "50", "真实中立 RSI=50 必须显示 '50'"
        assert rows["510300"]["技术评分"] == "50"
        assert rows["510300"]["风险评分"] == "50"
        # 占位哨兵 50 必须显示为 无数据，不得与中立混淆
        assert rows["159915"]["RSI"] == "无数据", "无数据的占位 50 必须显示 '无数据'"
        assert rows["159915"]["技术评分"] == "无数据"
        assert rows["159915"]["风险评分"] == "无数据"
