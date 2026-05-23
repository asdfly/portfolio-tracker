"""Tests for market event signal engine and advisor integration."""
import pytest
import sqlite3
import pandas as pd
import numpy as np
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "database", "portfolio.db")
PROJ = os.path.dirname(os.path.dirname(__file__))


class TestSignalEngine:
    """Test MarketEventSignalEngine with real database."""

    @pytest.fixture(scope="class")
    def conn(self):
        c = sqlite3.connect(DB_PATH)
        yield c
        c.close()

    def test_import(self):
        from src.analysis.market_event_signals import (
            MarketEventSignalEngine, MarketSignal, SignalType, SignalLevel
        )
        assert hasattr(MarketEventSignalEngine, "generate_all_signals")

    def test_generate_signals_returns_list(self, conn):
        from src.analysis.market_event_signals import MarketEventSignalEngine
        engine = MarketEventSignalEngine(conn)
        signals = engine.generate_all_signals(end_date="2026-05-21", lookback_days=5)
        assert isinstance(signals, list)

    def test_signal_fields(self, conn):
        from src.analysis.market_event_signals import MarketEventSignalEngine, MarketSignal
        engine = MarketEventSignalEngine(conn)
        signals = engine.generate_all_signals(end_date="2026-05-21", lookback_days=3)
        if signals:
            s = signals[0]
            assert isinstance(s, MarketSignal)
            assert s.source in ("lhb", "margin", "holder_change", "block_trade", "institution")
            assert isinstance(s.title, str)
            assert len(s.title) > 0
            assert 0 <= s.confidence <= 1

    def test_summary_structure(self, conn):
        from src.analysis.market_event_signals import MarketEventSignalEngine
        engine = MarketEventSignalEngine(conn)
        signals = engine.generate_all_signals(end_date="2026-05-21", lookback_days=3)
        summary = engine.get_signal_summary(signals)
        assert "total" in summary
        assert "by_type" in summary
        assert "by_level" in summary
        assert summary["total"] == len(signals)

    def test_portfolio_report_empty_codes(self, conn):
        from src.analysis.market_event_signals import MarketEventSignalEngine
        engine = MarketEventSignalEngine(conn)
        signals = engine.generate_all_signals(end_date="2026-05-21", lookback_days=3)
        rpt = engine.get_portfolio_signal_report(signals, [])
        assert rpt["related_count"] == 0
        assert rpt["portfolio_risk_level"] == "low"

    def test_portfolio_report_with_codes(self, conn):
        from src.analysis.market_event_signals import MarketEventSignalEngine
        engine = MarketEventSignalEngine(conn)
        signals = engine.generate_all_signals(end_date="2026-05-21", lookback_days=3)
        # Use a code that exists in signals
        if signals:
            test_code = signals[0].code
            rpt = engine.get_portfolio_signal_report(signals, [test_code])
            assert rpt["related_count"] >= 0


class TestAdvisorIntegration:
    """Test advisor.py market event signal integration."""

    def test_analyze_market_event_signals_method_exists(self):
        from src.analysis.advisor import SmartAdvisor
        assert hasattr(SmartAdvisor, "analyze_market_event_signals")

    def test_analyze_returns_list(self):
        from src.analysis.advisor import SmartAdvisor
        from src.analysis.market_event_signals import MarketSignal, SignalType, SignalLevel
        # Create mock signal
        sig = MarketSignal(
            source="lhb", signal_type=SignalType.RISK, level=SignalLevel.HIGH,
            code="000001", name="测试", date="2026-05-21",
            title="测试信号", description="测试描述"
        )
        # Use a minimal mock db
        import sqlite3
        conn = sqlite3.connect(":memory:")
        advisor = SmartAdvisor(conn)
        advices = advisor.analyze_market_event_signals([sig])
        assert isinstance(advices, list)
        assert len(advices) == 1
        assert advices[0].title == "测试信号"
        conn.close()
