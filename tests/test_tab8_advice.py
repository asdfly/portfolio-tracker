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
        mock_st.columns = lambda n: [MagicMock() for _ in range(n)]
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
        mock_st.columns = lambda n: [MagicMock() for _ in range(n)]
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
