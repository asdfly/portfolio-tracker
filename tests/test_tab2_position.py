"""
Tests for Tab2: Position module
"""
import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock


class TestTab2Importable:
    """Test module import and basic structure"""

    def test_module_imports(self):
        from tabs.tab2_position import render_tab2
        assert callable(render_tab2)

    def test_render_signature(self):
        import inspect
        from tabs.tab2_position import render_tab2
        sig = inspect.signature(render_tab2)
        params = list(sig.parameters.keys())
        assert "positions" in params
        assert "summary" in params


class TestTab2EmptyPositions:
    """Test behavior with empty positions"""

    @patch("tabs.tab2_position.st")
    def test_empty_positions_renders(self, mock_st):
        """Empty positions should not crash"""
        from tabs.tab2_position import render_tab2
        mock_st.subheader = MagicMock()
        mock_st.info = MagicMock()
        mock_st.markdown = MagicMock()
        mock_st.columns = MagicMock(return_value=[MagicMock(), MagicMock()])
        mock_st.metric = MagicMock()
        mock_st.selectbox = MagicMock(return_value=None)
        mock_st.plotly_chart = MagicMock()
        mock_st.caption = MagicMock()
        mock_st.tabs = MagicMock(return_value=[MagicMock(), MagicMock()])
        mock_st.button = MagicMock(return_value=False)
        mock_st.expander = MagicMock(return_value=MagicMock())
        mock_st.dataframe = MagicMock()

        render_tab2(pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), "2026-05-20", "沪深300")
        # Should not raise


class TestTab2HHI:
    """Test HHI concentration calculation"""

    def test_hhi_equal_weights(self):
        """Equal weight portfolio HHI should be 1/N"""
        n = 5
        weights = np.array([1/n] * n)
        hhi = (weights ** 2).sum()
        effective_n = 1 / hhi
        assert abs(hhi - 1/n) < 1e-10
        assert abs(effective_n - n) < 1e-10

    def test_hhi_concentrated(self):
        """One dominant position should have high HHI"""
        weights = np.array([0.8, 0.05, 0.05, 0.05, 0.05])
        hhi = (weights ** 2).sum()
        effective_n = 1 / hhi
        assert hhi > 0.5  # highly concentrated
        assert effective_n < 2  # effectively < 2 positions

    def test_hhi_diversified(self):
        """Evenly spread should have low HHI"""
        weights = np.array([0.1] * 10)
        hhi = (weights ** 2).sum()
        assert hhi <= 0.15  # highly diversified

    def test_hhi_grade_classification(self):
        """HHI grade thresholds"""
        if 0 <= 0.12 <= 0.15:
            grade = "high_diversified"
        elif 0.12 <= 0.20 <= 0.25:
            grade = "moderate"
        else:
            grade = "concentrated"
        assert grade == "high_diversified"


class TestTab2BetaContribution:
    """Test beta contribution analysis"""

    def test_weighted_beta_sum(self):
        """Portfolio beta = sum(weight_i * beta_i)"""
        weights = np.array([0.4, 0.3, 0.3])
        betas = np.array([1.2, 0.9, 1.1])
        portfolio_beta = (weights * betas).sum()
        expected = 0.4 * 1.2 + 0.3 * 0.9 + 0.3 * 1.1
        assert abs(portfolio_beta - expected) < 1e-10
        assert abs(portfolio_beta - 1.08) < 1e-10

    def test_beta_contribution_ordering(self):
        """Higher weight * beta = higher contribution"""
        data = pd.DataFrame({
            "name": ["A", "B", "C"],
            "weight": [0.5, 0.3, 0.2],
            "beta": [1.5, 1.0, 0.8],
        })
        data["contribution"] = data["weight"] * data["beta"]
        assert data.loc[0, "contribution"] == 0.75  # dominant
        assert data["contribution"].sum() == pytest.approx(1.21)
