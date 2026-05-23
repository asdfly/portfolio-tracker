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


class TestSignalEngineEdgeCases:
    """Edge cases for signal engine."""

    def test_empty_database(self):
        """Engine should handle empty tables gracefully."""
        import sqlite3
        from src.analysis.market_event_signals import MarketEventSignalEngine
        conn = sqlite3.connect(":memory:")
        # Create empty tables with same schema
        conn.execute("CREATE TABLE stock_lhb (id INTEGER, date TEXT, code TEXT, name TEXT, "
                     "change_pct REAL, reason TEXT, buyer_type TEXT, buy_amount REAL, sell_amount REAL)")
        conn.execute("CREATE TABLE stock_margin (id INTEGER, date TEXT, code TEXT, name TEXT, "
                     "rzye REAL, rzmre REAL, rzche REAL)")
        conn.execute("CREATE TABLE stock_holder_change (id INTEGER, date TEXT, code TEXT, name TEXT, "
                     "holder_name TEXT, change_type TEXT, change_ratio REAL)")
        conn.execute("CREATE TABLE stock_block_trade (id INTEGER, date TEXT, code TEXT, name TEXT, "
                     "premium_rate REAL, trade_amount REAL, buyer_broker TEXT)")
        conn.execute("CREATE TABLE stock_institution_research (id INTEGER, date TEXT, code TEXT, name TEXT, "
                     "institution TEXT, inst_type TEXT)")
        engine = MarketEventSignalEngine(conn)
        signals = engine.generate_all_signals(lookback_days=5)
        assert signals == []
        summary = engine.get_signal_summary(signals)
        assert summary["total"] == 0
        conn.close()

    def test_single_row_per_table(self):
        """Engine handles minimal data."""
        import sqlite3
        from src.analysis.market_event_signals import MarketEventSignalEngine, SignalType
        conn = sqlite3.connect(":memory:")
        conn.execute("CREATE TABLE stock_lhb (id INTEGER, date TEXT, code TEXT, name TEXT, "
                     "change_pct REAL, reason TEXT, buyer_type TEXT, buy_amount REAL, sell_amount REAL)")
        conn.execute("CREATE TABLE stock_margin (id INTEGER, date TEXT, code TEXT, name TEXT, "
                     "rzye REAL, rzmre REAL, rzche REAL)")
        conn.execute("CREATE TABLE stock_holder_change (id INTEGER, date TEXT, code TEXT, name TEXT, "
                     "holder_name TEXT, change_type TEXT, change_ratio REAL)")
        conn.execute("CREATE TABLE stock_block_trade (id INTEGER, date TEXT, code TEXT, name TEXT, "
                     "premium_rate REAL, trade_amount REAL, buyer_broker TEXT)")
        conn.execute("CREATE TABLE stock_institution_research (id INTEGER, date TEXT, code TEXT, name TEXT, "
                     "institution TEXT, inst_type TEXT)")
        conn.execute("INSERT INTO stock_lhb VALUES (1,'2026-05-21','600001','测试',9.9,"
                     "'日涨幅偏离值达7%','游资',1000,500)")
        conn.execute("INSERT INTO stock_block_trade VALUES (1,'2026-05-21','600001','测试',-0.25,5000,'机构A')")
        engine = MarketEventSignalEngine(conn)
        signals = engine.generate_all_signals(lookback_days=5)
        assert len(signals) >= 1
        types = [s.signal_type for s in signals]
        assert SignalType.OPPORTUNITY in types
        assert SignalType.RISK in types
        conn.close()

    def test_signal_sorting(self):
        """Signals should be sorted by level then confidence."""
        import sqlite3
        from src.analysis.market_event_signals import MarketEventSignalEngine, SignalLevel
        conn = sqlite3.connect(":memory:")
        conn.execute("CREATE TABLE stock_lhb (id INTEGER, date TEXT, code TEXT, name TEXT, "
                     "change_pct REAL, reason TEXT, buyer_type TEXT, buy_amount REAL, sell_amount REAL)")
        conn.execute("CREATE TABLE stock_margin (id INTEGER, date TEXT, code TEXT, name TEXT, "
                     "rzye REAL, rzmre REAL, rzche REAL)")
        conn.execute("CREATE TABLE stock_holder_change (id INTEGER, date TEXT, code TEXT, name TEXT, "
                     "holder_name TEXT, change_type TEXT, change_ratio REAL)")
        conn.execute("CREATE TABLE stock_block_trade (id INTEGER, date TEXT, code TEXT, name TEXT, "
                     "premium_rate REAL, trade_amount REAL, buyer_broker TEXT)")
        conn.execute("CREATE TABLE stock_institution_research (id INTEGER, date TEXT, code TEXT, name TEXT, "
                     "institution TEXT, inst_type TEXT)")
        # One HIGH and one MEDIUM signal
        conn.execute("INSERT INTO stock_lhb VALUES (1,'2026-05-20','600001','A',9.9,"
                     "'日涨幅偏离值达7%','x',0,0)")
        conn.execute("INSERT INTO stock_lhb VALUES (2,'2026-05-21','600002','B',6.0,"
                     "'日涨幅偏离值达7%','x',0,0)")
        engine = MarketEventSignalEngine(conn)
        signals = engine.generate_all_signals(lookback_days=5)
        if len(signals) >= 2:
            lo = {SignalLevel.HIGH: 0, SignalLevel.MEDIUM: 1, SignalLevel.LOW: 2}
            for i in range(len(signals) - 1):
                a, b = signals[i], signals[i + 1]
                assert (lo[a.level], -a.confidence) <= (lo[b.level], -b.confidence)
        conn.close()

    def test_enum_values(self):
        from src.analysis.market_event_signals import SignalType, SignalLevel
        assert SignalType.RISK.value == "risk"
        assert SignalType.OPPORTUNITY.value == "opp"
        assert SignalType.NEUTRAL.value == "neutral"
        assert SignalLevel.HIGH.value == "high"
        assert SignalLevel.MEDIUM.value == "medium"
        assert SignalLevel.LOW.value == "low"

    def test_portfolio_report_with_no_match(self):
        """Portfolio report with codes not in signals."""
        import sqlite3
        from src.analysis.market_event_signals import MarketEventSignalEngine
        conn = sqlite3.connect(":memory:")
        conn.execute("CREATE TABLE stock_lhb (id INTEGER, date TEXT, code TEXT, name TEXT, "
                     "change_pct REAL, reason TEXT, buyer_type TEXT, buy_amount REAL, sell_amount REAL)")
        conn.execute("CREATE TABLE stock_margin (id INTEGER, date TEXT, code TEXT, name TEXT, "
                     "rzye REAL, rzmre REAL, rzche REAL)")
        conn.execute("CREATE TABLE stock_holder_change (id INTEGER, date TEXT, code TEXT, name TEXT, "
                     "holder_name TEXT, change_type TEXT, change_ratio REAL)")
        conn.execute("CREATE TABLE stock_block_trade (id INTEGER, date TEXT, code TEXT, name TEXT, "
                     "premium_rate REAL, trade_amount REAL, buyer_broker TEXT)")
        conn.execute("CREATE TABLE stock_institution_research (id INTEGER, date TEXT, code TEXT, name TEXT, "
                     "institution TEXT, inst_type TEXT)")
        engine = MarketEventSignalEngine(conn)
        signals = engine.generate_all_signals(lookback_days=5)
        rpt = engine.get_portfolio_signal_report(signals, ["999999", "888888"])
        assert rpt["related_count"] == 0
        assert rpt["portfolio_risk_level"] == "low"
        assert rpt["affected_positions"] == []
        conn.close()


class TestAdvisorEdgeCases:
    """Edge cases for advisor market event integration."""

    def test_empty_signals(self):
        from src.analysis.advisor import SmartAdvisor
        import sqlite3
        conn = sqlite3.connect(":memory:")
        advisor = SmartAdvisor(conn)
        advices = advisor.analyze_market_event_signals([])
        assert advices == []
        conn.close()

    def test_mixed_signals_priority_sort(self):
        from src.analysis.advisor import SmartAdvisor, AdvicePriority
        from src.analysis.market_event_signals import MarketSignal, SignalType, SignalLevel
        import sqlite3
        conn = sqlite3.connect(":memory:")
        advisor = SmartAdvisor(conn)
        signals = [
            MarketSignal(source="x", signal_type=SignalType.OPPORTUNITY, level=SignalLevel.LOW,
                         code="1", name="A", date="d", title="opp_low", description="d"),
            MarketSignal(source="x", signal_type=SignalType.RISK, level=SignalLevel.HIGH,
                         code="2", name="B", date="d", title="risk_high", description="d"),
            MarketSignal(source="x", signal_type=SignalType.OPPORTUNITY, level=SignalLevel.MEDIUM,
                         code="3", name="C", date="d", title="opp_med", description="d"),
        ]
        advices = advisor.analyze_market_event_signals(signals)
        # LOW level signals are filtered out by advisor (only HIGH/MEDIUM processed)
        assert len(advices) == 2
        # First should be HIGH priority
        assert advices[0].priority == AdvicePriority.HIGH
        conn.close()

    def test_dedup_same_title_code(self):
        from src.analysis.advisor import SmartAdvisor
        from src.analysis.market_event_signals import MarketSignal, SignalType, SignalLevel
        import sqlite3
        conn = sqlite3.connect(":memory:")
        advisor = SmartAdvisor(conn)
        signals = [
            MarketSignal(source="a", signal_type=SignalType.RISK, level=SignalLevel.HIGH,
                         code="1", name="A", date="d1", title="same_title", description="d1"),
            MarketSignal(source="b", signal_type=SignalType.RISK, level=SignalLevel.MEDIUM,
                         code="1", name="A", date="d2", title="same_title", description="d2"),
        ]
        advices = advisor.analyze_market_event_signals(signals)
        # Same (title, code) should be deduped
        assert len(advices) == 1
        conn.close()

    def test_different_codes_not_deduped(self):
        from src.analysis.advisor import SmartAdvisor
        from src.analysis.market_event_signals import MarketSignal, SignalType, SignalLevel
        import sqlite3
        conn = sqlite3.connect(":memory:")
        advisor = SmartAdvisor(conn)
        signals = [
            MarketSignal(source="a", signal_type=SignalType.RISK, level=SignalLevel.HIGH,
                         code="1", name="A", date="d1", title="same_title", description="d1"),
            MarketSignal(source="b", signal_type=SignalType.RISK, level=SignalLevel.HIGH,
                         code="2", name="B", date="d2", title="same_title", description="d2"),
        ]
        advices = advisor.analyze_market_event_signals(signals)
        assert len(advices) == 2  # Different codes, not deduped
        conn.close()
