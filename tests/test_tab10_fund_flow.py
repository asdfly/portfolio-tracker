"""
Tests for Tab10: Fund Flow module
"""
import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock
from tabs.tab10_fund_flow import render_tab10


@pytest.fixture
def mock_positions():
    """Minimal positions DataFrame for tab10 rendering"""
    return pd.DataFrame({
        "code": ["510300", "159915"],
        "name": ["沪深300ETF", "创业板ETF"],
        "quantity": [10000, 5000],
        "cost_price": [4.0, 2.5],
        "current_price": [4.2, 2.3],
        "market_value": [42000, 11500],
        "pnl": [2000, -1000],
        "pnl_rate": [5.0, -4.0],
    })


@pytest.fixture
def mock_summary():
    return pd.DataFrame({
        "date": pd.date_range("2026-05-01", periods=5),
        "total_value": [50000, 51000, 50500, 52000, 53000],
        "daily_return": [0, 0.02, -0.01, 0.03, 0.02],
    })


@pytest.fixture
def mock_index_quotes():
    return pd.DataFrame()


class TestTab10Importable:
    """Test module import and basic structure"""

    def test_module_imports(self):
        from tabs.tab10_fund_flow import render_tab10
        assert callable(render_tab10)

    def test_render_signature(self):
        import inspect
        sig = inspect.signature(render_tab10)
        params = list(sig.parameters.keys())
        assert "positions" in params
        assert "summary" in params


class TestTab10SectorFlow:
    """Test sector fund flow rendering"""

    @patch("tabs.tab10_fund_flow.pd.read_sql_query", return_value=pd.DataFrame())
    def test_sector_flow_empty_db(self, mock_sql):
        """Empty fund_flows table renders without error"""
        with patch("tabs.tab10_fund_flow.get_db_connection") as mock_db:
            mock_conn = MagicMock()
            mock_db.return_value.__enter__ = MagicMock(return_value=mock_conn)
            mock_db.return_value.__exit__ = MagicMock(return_value=False)
            # st.tabs needs to return 3 mock tabs
            mock_tab = MagicMock()
            mock_tab.__enter__ = MagicMock(return_value=mock_tab)
            mock_tab.__exit__ = MagicMock(return_value=False)
            with patch("tabs.tab10_fund_flow.st") as mock_st:
                mock_st.tabs.return_value = [mock_tab, mock_tab, mock_tab]
                mock_st.subheader = MagicMock()
                mock_st.info = MagicMock()
                mock_st.metric = MagicMock()
                mock_st.markdown = MagicMock()
                mock_st.plotly_chart = MagicMock()
                mock_st.caption = MagicMock()
                mock_st.dataframe = MagicMock()
                mock_st.expander = MagicMock()
                mock_st.columns = MagicMock(return_value=[mock_tab, mock_tab])
                mock_st.button = MagicMock(return_value=False)
                render_tab10(pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), "2026-05-20", "沪深300")


class TestTab10ETFFlow:
    """Test ETF fund flow rendering"""

    @patch("tabs.tab10_fund_flow.get_db_connection")
    def test_etf_flow_empty(self, mock_conn):
        """Empty ETF flow shows info"""
        with patch("tabs.tab10_fund_flow.pd.read_sql_query", return_value=pd.DataFrame()):
            render_tab10(pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), "2026-05-20", "沪深300")


class TestTab10MainFund:
    """Test main fund flow rendering"""

    @patch("tabs.tab10_fund_flow.get_db_connection")
    def test_main_fund_empty(self, mock_conn):
        """Empty main fund shows info"""
        with patch("tabs.tab10_fund_flow.pd.read_sql_query", return_value=pd.DataFrame()):
            render_tab10(pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), "2026-05-20", "沪深300")

    @patch("tabs.tab10_fund_flow.get_db_connection")
    def test_main_fund_with_data(self, mock_conn):
        """Main fund with data renders metrics"""
        mf_df = pd.DataFrame({
            "date": pd.date_range("2026-05-01", periods=5),
            "net_inflow": [1e9, -5e8, 2e9, 8e8, 1.5e9],
            "super_large_inflow": [5e8, -2e8, 1e9, 4e8, 7e8],
            "large_inflow": [5e8, -3e8, 1e9, 4e8, 8e8],
            "medium_inflow": [2e8, 1e8, 3e8, 1e8, 2e8],
            "small_inflow": [1e8, 2e8, 1e8, 1e8, 1e8],
            "net_inflow_pct": [0.1, -0.05, 0.2, 0.08, 0.15],
        })
        with patch("tabs.tab10_fund_flow.pd.read_sql_query", return_value=mf_df):
            with patch("tabs.tab10_fund_flow.pd.read_sql_query", return_value=mf_df):
                render_tab10(pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), "2026-05-20", "沪深300")
