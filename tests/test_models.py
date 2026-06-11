"""测试 src/models.py — 5 个 dataclass 的 dict 兼容接口"""

import numpy as np
import pandas as pd
import pytest
from src.models import (
    RiskMetrics, MonteCarloResult, ReturnAttribution,
    RebalanceTrade, RebalanceSuggestion,
)


class TestRiskMetrics:
    def test_default_values_are_nan(self):
        r = RiskMetrics()
        assert pd.isna(r.sortino)
        assert pd.isna(r.calmar)
        assert r.max_consec_win == 0

    def test_getitem_existing_key(self):
        r = RiskMetrics(sortino=1.5, calmar=0.8)
        assert r["sortino"] == 1.5

    def test_getitem_missing_key_raises(self):
        r = RiskMetrics()
        with pytest.raises(KeyError):
            _ = r["nonexistent"]

    def test_get_existing(self):
        r = RiskMetrics(sortino=2.0)
        assert r.get("sortino") == 2.0

    def test_get_missing_default(self):
        r = RiskMetrics()
        assert r.get("nonexistent") is None
        assert r.get("nonexistent", 42) == 42

    def test_keys_values_items(self):
        r = RiskMetrics(sortino=1.0, calmar=0.5)
        assert "sortino" in r.keys()
        assert 1.0 in r.values()
        items = dict(r.items())
        assert items["sortino"] == 1.0

    def test_contains(self):
        r = RiskMetrics(sortino=1.0)
        assert "sortino" in r
        assert "nonexistent" not in r

    def test_iter(self):
        r = RiskMetrics(sortino=1.0, calmar=0.5)
        keys = list(r)
        assert "sortino" in keys

    def test_bool_empty(self):
        r = RiskMetrics()
        assert not r

    def test_bool_with_value(self):
        r = RiskMetrics(sortino=1.5)
        assert r

    def test_empty_classmethod(self):
        r = RiskMetrics.empty()
        assert pd.isna(r.sortino)
        assert not r


class TestMonteCarloResult:
    def test_default_values(self):
        m = MonteCarloResult()
        assert m.paths is None
        assert m.sample_count == 0

    def test_bool_none_paths(self):
        m = MonteCarloResult()
        assert not m

    def test_bool_with_paths(self):
        m = MonteCarloResult(paths=np.array([[1, 2, 3]]))
        assert m

    def test_dict_access(self):
        m = MonteCarloResult(last_value=100.0, mean_return=0.05)
        assert m["last_value"] == 100.0
        assert m.get("mean_return") == 0.05
        assert m.get("missing", -1) == -1
        assert "last_value" in m


class TestReturnAttribution:
    def test_default_empty_dicts(self):
        r = ReturnAttribution()
        assert r.allocation_effect == {}

    def test_bool_empty(self):
        r = ReturnAttribution()
        assert not r

    def test_bool_with_sectors(self):
        r = ReturnAttribution(sector_returns={"tech": 0.05})
        assert r

    def test_dict_access_nested(self):
        r = ReturnAttribution(allocation_effect={"tech": 0.02})
        assert r["allocation_effect"]["tech"] == 0.02


class TestRebalanceTrade:
    def test_default_values(self):
        t = RebalanceTrade()
        assert t.sector == ""
        assert t.shares == 0

    def test_dict_access(self):
        t = RebalanceTrade(sector="科技", code="159915", diff=0.05)
        assert t["sector"] == "科技"
        assert t.get("missing") is None
        assert "sector" in t


class TestRebalanceSuggestion:
    def test_default_empty(self):
        s = RebalanceSuggestion()
        assert s.current_weights == {}
        assert not s

    def test_bool_with_weights(self):
        s = RebalanceSuggestion(current_weights={"tech": 0.3})
        assert s

    def test_dict_access(self):
        s = RebalanceSuggestion(total_value=100000, threshold=0.05)
        assert s["total_value"] == 100000
        assert s.get("threshold") == 0.05
