"""Tests for gold_components sub-modules."""
import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch
from datetime import datetime

def _gold_df(n=30):
    dates = pd.date_range(end=datetime.now(), periods=n, freq="D")
    return pd.DataFrame({"date": dates, "close": np.random.uniform(450, 500, n)})

def _bond_df(n=30):
    dates = pd.date_range(end=datetime.now(), periods=n, freq="D")
    return pd.DataFrame({"date": dates, "cn_10y": np.random.uniform(2,3,n), "us_10y": np.random.uniform(3,5,n)})

def _clear():
    import streamlit as st; st.cache_data.clear()

class TestFetchFactorGold:
    def setup_method(self): _clear()
    @patch("tabs.gold_components.correlation.fetch_sge_hist")
    def test_none(self, m):
        m.return_value = None
        from tabs.gold_components.correlation import _fetch_factor_gold
        assert _fetch_factor_gold(30) is None
    @patch("tabs.gold_components.correlation.fetch_sge_hist")
    def test_df(self, m):
        m.return_value = _gold_df(50)
        from tabs.gold_components.correlation import _fetch_factor_gold
        r = _fetch_factor_gold(30)
        assert r is not None and "gold_price" in r.columns

class TestFetchFactorBonds:
    def setup_method(self): _clear()
    @patch("tabs.gold_components.correlation.fetch_bond_yields")
    def test_none(self, m):
        m.return_value = None
        from tabs.gold_components.correlation import _fetch_factor_bonds
        assert _fetch_factor_bonds() is None
    @patch("tabs.gold_components.correlation.fetch_bond_yields")
    def test_df(self, m):
        m.return_value = _bond_df(30)
        from tabs.gold_components.correlation import _fetch_factor_bonds
        r = _fetch_factor_bonds()
        assert r is not None and "spread" in r.columns

class TestLoadAllFactors:
    def setup_method(self): _clear()
    @patch("tabs.gold_components.correlation.fetch_bond_yields")
    @patch("tabs.gold_components.correlation.fetch_sge_hist")
    def test_gold_only(self, mg, mb):
        mg.return_value = _gold_df(50); mb.return_value = None
        from tabs.gold_components.correlation import _load_all_factors
        r = _load_all_factors(30)
        assert r is not None and "daily" in r
    @patch("tabs.gold_components.correlation.fetch_bond_yields")
    @patch("tabs.gold_components.correlation.fetch_sge_hist")
    def test_both(self, mg, mb):
        mg.return_value = _gold_df(50); mb.return_value = _bond_df(50)
        from tabs.gold_components.correlation import _load_all_factors
        r = _load_all_factors(30)
        assert r is not None and "daily" in r

class TestCBT:
    def test_cards_none(self):
        from tabs.gold_components.central_bank_trends import _render_reserve_cards
        _render_reserve_cards(None, None)
    def test_china_trend(self):
        from tabs.gold_components.central_bank_trends import _render_china_reserve_trend
        _render_china_reserve_trend(pd.DataFrame({"month": pd.date_range("2024-01-01", periods=10), "gold_reserve": [2200]*10}))
    def test_reserve_ratio(self):
        from tabs.gold_components.central_bank_trends import _render_reserve_ratio
        _render_reserve_ratio(pd.DataFrame({"month": pd.date_range("2024-01-01", periods=10), "gold_reserve": [2200]*10, "fx_reserve": [32000]*10}))
    def test_etf_trend(self):
        from tabs.gold_components.central_bank_trends import _render_global_etf_trend
        _render_global_etf_trend(pd.DataFrame({"date": pd.date_range("2024-01-01", periods=10), "total_holdings": [900]*10, "change": [1]*10}))

class TestGPC:
    def test_importable(self):
        from tabs.gold_components.gold_portfolio_correlation import render_gold_portfolio_correlation
        assert callable(render_gold_portfolio_correlation)

class TestSD:
    def test_cards_none(self):
        from tabs.gold_components.supply_demand import _render_supply_demand_cards
        _render_supply_demand_cards(None, None, None)
    def test_comex(self):
        from tabs.gold_components.supply_demand import _render_comex_inventory_trend
        _render_comex_inventory_trend(pd.DataFrame({"date": pd.date_range("2024-01-01", periods=10), "inventory_ton": [1000]*10}))
    def test_etf_flow(self):
        from tabs.gold_components.supply_demand import _render_etf_monthly_flow
        _render_etf_monthly_flow(pd.DataFrame({"date": pd.date_range("2024-01-01", periods=10), "change": [50]*10}))
    def test_importable(self):
        from tabs.gold_components.supply_demand import render_supply_demand
        assert callable(render_supply_demand)
