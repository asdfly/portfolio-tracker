"""Extended tests for gold_utils - base_layout and fetch function success paths."""
import pytest
import pandas as pd
from unittest.mock import patch


class TestBaseLayout:
    def test_default_layout(self):
        from tabs.gold_components.gold_utils import base_layout, DARK_BG
        layout = base_layout()
        assert layout["plot_bgcolor"] == DARK_BG

    def test_overrides(self):
        from tabs.gold_components.gold_utils import base_layout
        layout = base_layout(title="Test", plot_bgcolor="#fff")
        assert layout["title"] == "Test"

    def test_does_not_mutate_common(self):
        from tabs.gold_components.gold_utils import base_layout, COMMON_LAYOUT
        base_layout(title="X")
        assert "title" not in COMMON_LAYOUT


class TestFetchSgeBenchmark:
    @patch("akshare.spot_golden_benchmark_sge")
    def test_normal_chinese(self, m):
        m.return_value = pd.DataFrame({"日期": ["2024-01-01"], "收盘": [480.0]})
        from tabs.gold_components.gold_utils import fetch_sge_benchmark
        df = fetch_sge_benchmark()
        assert df is not None and "date" in df.columns and "close" in df.columns

    @patch("akshare.spot_golden_benchmark_sge")
    def test_empty(self, m):
        m.return_value = pd.DataFrame()
        from tabs.gold_components.gold_utils import fetch_sge_benchmark
        assert fetch_sge_benchmark() is None

    @patch("akshare.spot_golden_benchmark_sge")
    def test_none(self, m):
        m.return_value = None
        from tabs.gold_components.gold_utils import fetch_sge_benchmark
        assert fetch_sge_benchmark() is None


class TestFetchSgeHist:
    @patch("akshare.spot_hist_sge")
    def test_normal(self, m):
        m.return_value = pd.DataFrame({"日期": ["2024-01-01"], "收盘": [480.0]})
        from tabs.gold_components.gold_utils import fetch_sge_hist
        assert fetch_sge_hist() is not None

    @patch("akshare.spot_hist_sge")
    def test_none(self, m):
        m.return_value = None
        from tabs.gold_components.gold_utils import fetch_sge_hist
        assert fetch_sge_hist() is None


class TestFetchChinaReserve:
    @patch("akshare.macro_china_fx_gold")
    def test_normal(self, m):
        m.return_value = pd.DataFrame({
            "月份": ["2024年01月份"], "黄金储备-数值": [2200], "外汇储备-数值": [32000]
        })
        from tabs.gold_components.gold_utils import fetch_china_reserve
        df = fetch_china_reserve()
        assert df is not None and "date" in df.columns

    @patch("akshare.macro_china_fx_gold")
    def test_empty(self, m):
        m.return_value = pd.DataFrame()
        from tabs.gold_components.gold_utils import fetch_china_reserve
        assert fetch_china_reserve() is None


class TestFetchUsdcnyHist:
    @patch("akshare.currency_boc_safe")
    def test_divides_by_100(self, m):
        m.return_value = pd.DataFrame({"日期": ["2024-01-01"], "美元": [720.0]})
        from tabs.gold_components.gold_utils import fetch_usdcny_hist
        df = fetch_usdcny_hist()
        assert df is not None and abs(df["close"].iloc[0] - 7.2) < 0.01

    @patch("akshare.currency_boc_safe")
    def test_none(self, m):
        m.return_value = None
        from tabs.gold_components.gold_utils import fetch_usdcny_hist
        assert fetch_usdcny_hist() is None


class TestFetchBondYields:
    @patch("akshare.bond_zh_us_rate")
    def test_normal(self, m):
        m.return_value = pd.DataFrame({
            "日期": pd.date_range("2024-01-01", periods=100).astype(str),
            "中国国债收益率10年": [2.5]*100, "美国国债收益率10年": [4.0]*100
        })
        from tabs.gold_components.gold_utils import fetch_bond_yields
        df = fetch_bond_yields()
        assert "cn_10y" in df.columns and "us_10y" in df.columns

    @patch("akshare.bond_zh_us_rate")
    def test_filters_by_years(self, m):
        m.return_value = pd.DataFrame({
            "日期": pd.date_range("2020-01-01", periods=1000).astype(str),
            "中国国债收益率10年": [2.5]*1000, "美国国债收益率10年": [4.0]*1000
        })
        from tabs.gold_components.gold_utils import fetch_bond_yields
        assert len(fetch_bond_yields(years=1)) < 1000


class TestFetchChinaCpi:
    @patch("akshare.macro_china_cpi")
    def test_normal(self, m):
        m.return_value = pd.DataFrame({
            "月份": ["2024年01月份"], "全国-当月-同比增长": [0.5], "全国-当月-环比增长": [0.2]
        })
        from tabs.gold_components.gold_utils import fetch_china_cpi
        assert fetch_china_cpi() is not None and "date" in fetch_china_cpi().columns

    @patch("akshare.macro_china_cpi")
    def test_empty(self, m):
        m.return_value = pd.DataFrame()
        from tabs.gold_components.gold_utils import fetch_china_cpi
        assert fetch_china_cpi() is None


class TestFetchChinaReserveData:
    @patch("akshare.macro_china_fx_gold")
    def test_normal(self, m):
        m.return_value = pd.DataFrame({"月份": ["2024年1月份"], "黄金储备-数值": [2200]})
        from tabs.gold_components.gold_utils import fetch_china_reserve_data
        assert fetch_china_reserve_data() is not None


class TestFetchComexInventory:
    @patch("akshare.futures_comex_inventory")
    def test_normal(self, m):
        m.return_value = pd.DataFrame({"日期": ["2024-01-01"], "多单": [1000], "空单": [800]})
        from tabs.gold_components.gold_utils import fetch_comex_inventory
        df = fetch_comex_inventory()
        assert df is not None and "date" in df.columns

    @patch("akshare.futures_comex_inventory")
    def test_none(self, m):
        m.return_value = None
        import streamlit as st
        st.cache_data.clear()
        from tabs.gold_components.gold_utils import fetch_comex_inventory
        assert fetch_comex_inventory() is None
