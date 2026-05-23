"""Tests for db_schema.py - centralized DDL management."""
import pytest
import sqlite3


class TestDbSchema:
    """Test db_schema table definitions and initialization."""

    def test_import(self):
        from src.utils.db_schema import TABLE_DEFS, QUALITY_CHECK_TABLES
        assert isinstance(TABLE_DEFS, list)
        assert isinstance(QUALITY_CHECK_TABLES, dict)

    def test_all_17_tables_defined(self):
        from src.utils.db_schema import get_all_table_names
        tables = get_all_table_names()
        expected = {
            "portfolio_snapshots", "portfolio_summary", "index_quotes", "etf_technical",
            "fund_flows", "macro_daily", "market_sentiment", "daily_news",
            "alerts", "execution_logs", "custom_indicators",
            "indicator_backtest_results",
            "stock_lhb", "stock_margin", "stock_holder_change",
            "stock_institution_research", "stock_block_trade",
        }
        assert set(tables) == expected
        assert len(tables) == 17

    def test_ddl_syntax_valid(self):
        """Each DDL should be executable SQL."""
        from src.utils.db_schema import TABLE_DEFS
        conn = sqlite3.connect(":memory:")
        for name, ddl, _indexes in TABLE_DEFS:
            try:
                conn.execute(ddl)
            except Exception as e:
                pytest.fail(f"DDL for {name} failed: {e}")
        conn.close()

    def test_indexes_valid(self):
        """Each index should reference existing table and column."""
        from src.utils.db_schema import TABLE_DEFS
        conn = sqlite3.connect(":memory:")
        for name, ddl, indexes in TABLE_DEFS:
            conn.execute(ddl)
            for idx_sql in indexes:
                try:
                    conn.execute(idx_sql)
                except Exception as e:
                    pytest.fail(f"Index on {name} failed: {e}")
        # Count total indexes
        total = sum(len(idx_list) for _, _, idx_list in TABLE_DEFS)
        assert total >= 19

    def test_quality_check_tables_subset(self):
        """QUALITY_CHECK_TABLES should be a subset of all tables."""
        from src.utils.db_schema import get_all_table_names, QUALITY_CHECK_TABLES
        all_tables = set(get_all_table_names())
        for t in QUALITY_CHECK_TABLES:
            assert t in all_tables, f"{t} not in registered tables"

    def test_init_all_tables(self):
        """init_all_tables should create all 17 tables."""
        from src.utils.db_schema import init_all_tables
        conn = sqlite3.connect(":memory:")
        init_all_tables(conn)
        cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = {r[0] for r in cur.fetchall()}
        tables.discard("sqlite_sequence")
        assert len(tables) == 17

    def test_market_event_tables_unique_constraints(self):
        """New market event tables should have UNIQUE constraints."""
        from src.utils.db_schema import TABLE_DEFS
        unique_tables = {
            "stock_lhb": ["date", "code"],
            "stock_margin": ["date", "code"],
            "stock_holder_change": ["date", "holder_name", "code"],
            "stock_institution_research": ["date", "code", "institution"],
            "stock_block_trade": ["date", "code", "buyer_broker", "seller_broker"],
        }
        ddl_map = {name: ddl for name, ddl, _ in TABLE_DEFS}
        for table, expected_cols in unique_tables.items():
            ddl = ddl_map[table].upper()
            for col in expected_cols:
                assert col.upper() in ddl, f"{table} missing UNIQUE on {col}"
            assert "UNIQUE" in ddl, f"{table} missing UNIQUE constraint"

    def test_macro_daily_unique_constraint(self):
        from src.utils.db_schema import TABLE_DEFS
        ddl_map = {name: ddl for name, ddl, _ in TABLE_DEFS}
        ddl = ddl_map["macro_daily"].upper()
        assert "UNIQUE" in ddl
        assert "DATE" in ddl
        assert "INDICATOR_CODE" in ddl
