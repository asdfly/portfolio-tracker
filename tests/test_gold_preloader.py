"""黄金模块预加载器单元测试"""
import pytest


@pytest.fixture(autouse=True)
def _clean_gold_preload():
    """每个测试前后清理 session_state 中的预加载数据"""
    import streamlit as st
    st.session_state.pop("gold_preload", None)
    yield
    st.session_state.pop("gold_preload", None)
import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import pytest
from unittest.mock import patch


class TestPreloader:

    def _fake_bench(self):
        return pd.DataFrame({"date": pd.date_range("2026-01-01", periods=10), "close": range(10)})

    def _fake_etf(self, years=2):
        return pd.DataFrame({"date": pd.date_range("2026-01-01", periods=5), "total_holdings": [100]*5})

    def _fake_comex(self):
        return pd.DataFrame({"date": pd.date_range("2026-01-01", periods=8), "inventory_ton": [50]*8})

    @patch("tabs.gold_components.gold_utils.fetch_sge_benchmark")
    @patch("tabs.gold_components.gold_utils.fetch_global_etf_holdings")
    @patch("tabs.gold_components.gold_utils.fetch_comex_inventory")
    def test_preload_returns_all_keys(self, mock_comex, mock_etf, mock_bench):
        import streamlit as st
        st.session_state = {}
        mock_bench.return_value = self._fake_bench()
        mock_etf.return_value = self._fake_etf()
        mock_comex.return_value = self._fake_comex()
        from tabs.gold_components.gold_preloader import preload_gold_data
        result = preload_gold_data()
        assert "sge_benchmark" in result
        assert "global_etf_holdings" in result
        assert "comex_inventory" in result
        assert "version" in result

    @patch("tabs.gold_components.gold_utils.fetch_sge_benchmark")
    @patch("tabs.gold_components.gold_utils.fetch_global_etf_holdings")
    @patch("tabs.gold_components.gold_utils.fetch_comex_inventory")
    def test_preload_caches_in_session(self, mock_comex, mock_etf, mock_bench):
        import streamlit as st
        st.session_state = {}
        mock_bench.return_value = self._fake_bench()
        mock_etf.return_value = self._fake_etf()
        mock_comex.return_value = self._fake_comex()
        from tabs.gold_components.gold_preloader import preload_gold_data
        preload_gold_data()
        preload_gold_data()
        assert mock_bench.call_count == 1

    def test_get_preloaded_returns_default(self):
        import streamlit as st
        st.session_state = {}
        from tabs.gold_components.gold_preloader import get_preloaded
        assert get_preloaded("nonexistent") is None
        assert get_preloaded("nonexistent", default="fallback") == "fallback"

    @patch("tabs.gold_components.gold_utils.fetch_sge_benchmark")
    @patch("tabs.gold_components.gold_utils.fetch_global_etf_holdings")
    @patch("tabs.gold_components.gold_utils.fetch_comex_inventory")
    def test_preload_handles_failure(self, mock_comex, mock_etf, mock_bench):
        import streamlit as st
        st.session_state = {}
        mock_bench.side_effect = Exception("network error")
        mock_etf.return_value = self._fake_etf()
        mock_comex.return_value = self._fake_comex()
        from tabs.gold_components.gold_preloader import preload_gold_data, PRELOAD_VERSION
        result = preload_gold_data()
        assert result["sge_benchmark"] is None
        assert result["global_etf_holdings"] is not None
        assert result["version"] == PRELOAD_VERSION
