"""P1: 仓位管理+估值分位数测试。"""
import pytest
import numpy as np
import pandas as pd


class TestPositionAdvice:
    def _fn(self):
        from src.analysis.position_advisor import compute_position_advice
        return compute_position_advice

    def test_add_position(self):
        a = self._fn()("A", "TA", "科技", 5.0, 50000, 80, 10.0)
        assert a.adjust_action == "加仓" and a.adjust_min_pct > 0

    def test_reduce_position(self):
        a = self._fn()("B", "TB", "医药", 15.0, 150000, 20, 15.0)
        assert a.adjust_action == "减仓"

    def test_hold_position(self):
        a = self._fn()("C", "TC", "宽基", 10.0, 100000, 50, 30.0)
        assert a.adjust_action == "维持"

    def test_sector_constraint_blocks_add(self):
        a = self._fn()("D", "TD", "科技", 10.0, 100000, 80, 35.0)
        assert a.adjust_action == "维持" and "超限" in a.advice_text

    def test_risk_constraint_blocks_add(self):
        a = self._fn()("E", "TE", "科技", 5.0, 50000, 80, 10.0, risk_constrained=True)
        assert a.adjust_action == "维持"

    def test_advice_text_populated(self):
        a = self._fn()("G", "TG", "新能源", 12.0, 120000, 25, 25.0)
        assert len(a.advice_text) > 0

    def test_dataclass_compat(self):
        a = self._fn()("H", "TH", "医药", 10.0, 100000, 50, 10.0)
        assert a["code"] == "H" and "adjust_action" in a.keys()


class TestSectorExposure:
    def test_normal_exposure(self):
        from src.analysis.position_advisor import compute_sector_exposures
        positions = pd.DataFrame({
            "code": ["510300", "510500"], "name": ["沪深300", "中证500"],
            "market_value": [200000, 200000]
        })
        cats = {"510300": {"sector": "宽基"}, "510500": {"sector": "宽基"}}
        result = compute_sector_exposures(positions, cats)
        assert len(result) > 0 and result[0].sector == "宽基"

    def test_over_exposure(self):
        from src.analysis.position_advisor import compute_sector_exposures
        positions = pd.DataFrame({
            "code": ["512010", "159992", "510300"], "name": ["医药ETF", "创新药", "沪深300"],
            "market_value": [300000, 200000, 100000]
        })
        cats = {"512010": {"sector": "医药"}, "159992": {"sector": "医药"},
                "510300": {"sector": "宽基"}}
        result = compute_sector_exposures(positions, cats)
        med = [e for e in result if e.sector == "医药"][0]
        assert med.status == "超限"

    def test_empty_positions(self):
        from src.analysis.position_advisor import compute_sector_exposures
        assert compute_sector_exposures(pd.DataFrame()) == []
