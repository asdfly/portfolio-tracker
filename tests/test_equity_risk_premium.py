# -*- coding: utf-8 -*-
"""equity_risk_premium.py 测试 — 覆盖 DB 依赖函数 + 边界条件
目标覆盖率 70%+
"""
import sqlite3
from unittest.mock import patch, MagicMock, call

import numpy as np
import pandas as pd
import pytest

from src.analysis.equity_risk_premium import (
    ERPResult,
    INDEX_NAMES,
    INDEX_PE_API,
    classify_erp_signal,
    compute_erp,
    compute_erp_for_index,
    compute_erp_multi,
    load_index_pe_from_db,
    load_risk_free_rate,
)

# ============================================================
# Helpers
# ============================================================

def _make_mock_conn(tables=None):
    """Create a MagicMock connection that simulates sqlite3 connection.
    
    tables: dict mapping SQL to (fetchall_result, columns) tuples
    E.g. {"SELECT ...": ([row1, row2], ["date", "pe"])}
    """
    conn = MagicMock(spec=sqlite3.Connection)
    conn.close = MagicMock()
    # Default: no tables, queries return empty
    conn.cursor.return_value.fetchall.return_value = []
    conn.cursor.return_value.description = None
    if tables:
        # Store tables for query matching
        conn._tables = tables
    
    def _execute_side_effect(sql, params=None):
        mock_cur = MagicMock()
        if tables and sql.strip() in tables:
            rows, cols = tables[sql.strip()]
            mock_cur.fetchall.return_value = rows
            mock_cur.description = [(c,) for c in cols] if cols else None
        else:
            mock_cur.fetchall.return_value = []
            mock_cur.description = None
        return mock_cur
    
    conn.cursor.side_effect = _execute_side_effect
    return conn

# ============================================================
# compute_erp — 边界测试
# ============================================================

class TestComputeERPEdge:
    def test_very_high_pe(self):
        erp = compute_erp(1000, 2.5)
        assert erp == pytest.approx(-2.4, abs=0.01)

    def test_very_low_pe(self):
        erp = compute_erp(5.0, 2.5)
        assert erp == pytest.approx(17.5, abs=0.01)

    def test_pe_equals_one(self):
        erp = compute_erp(1.0, 2.5)
        assert erp == pytest.approx(97.5, abs=0.01)

    def test_zero_risk_free_rate(self):
        erp = compute_erp(10, 0)
        assert erp == pytest.approx(10.0, abs=0.01)

    def test_high_risk_free_rate(self):
        erp = compute_erp(100, 5.0)
        assert erp < 0

    def test_zero_pe(self):
        erp = compute_erp(0, 2.5)
        assert erp == 0.0

    def test_negative_pe(self):
        erp = compute_erp(-10, 2.5)
        assert erp == 0.0

    def test_negative_risk_free(self):
        erp = compute_erp(10, -1)
        assert erp == 0.0

# ============================================================
# classify_erp_signal — 边界测试
# ============================================================

class TestClassifyERPSignalEdge:
    def test_exactly_70_percentile(self):
        history = list(range(100))
        signal, detail = classify_erp_signal(70, history)
        assert signal == "偏多"
        assert "70%" in detail

    def test_exactly_50_percentile(self):
        history = list(range(100))
        signal, detail = classify_erp_signal(50, history)
        assert "中性略偏多" in signal

    def test_exactly_30_percentile(self):
        history = list(range(100))
        signal, detail = classify_erp_signal(30, history)
        assert "中性略偏空" in signal

    def test_below_30_percentile(self):
        history = list(range(100))
        signal, detail = classify_erp_signal(20, history)
        assert signal == "偏空"

    def test_empty_history(self):
        signal, detail = classify_erp_signal(3.0, [])
        assert signal == "数据不足"

    def test_single_element(self):
        signal, _ = classify_erp_signal(3.0, [1.0])
        assert signal == "数据不足"

    def test_exactly_19_elements(self):
        signal, _ = classify_erp_signal(3.0, [float(i) for i in range(19)])
        assert signal == "数据不足"

    def test_exactly_20_elements(self):
        history = [float(i) for i in range(20)]
        signal, detail = classify_erp_signal(19.0, history)
        assert signal != "数据不足"

    def test_all_same_values(self):
        history = [5.0] * 100
        signal, detail = classify_erp_signal(5.0, history)
        assert signal is not None

    def test_negative_erp(self):
        history = [5.0, 6.0, 7.0, 8.0, 9.0] * 20
        signal, detail = classify_erp_signal(-1.0, history)
        assert signal == "偏空"

# ============================================================
# ERPResult dataclass
# ============================================================

class TestERPResult:
    def test_creation(self):
        r = ERPResult(
            index_code="sh000300", index_name="沪深300",
            current_pe=12.5, earnings_yield=8.0,
            risk_free_rate=2.8, erp=5.2,
            erp_percentile=60.0, signal="中性略偏多",
            detail="测试详情"
        )
        assert r.erp == 5.2
        assert r.signal == "中性略偏多"

    def test_field_access(self):
        r = ERPResult(
            index_code="sh000300", index_name="沪深300",
            current_pe=12.5, earnings_yield=8.0,
            risk_free_rate=2.8, erp=5.2,
            erp_percentile=60.0, signal="中性略偏多",
            detail="测试详情"
        )
        assert r.index_code == "sh000300"
        assert r.current_pe == 12.5

    def test_index_names(self):
        assert INDEX_NAMES["sh000300"] == "沪深300"
        assert len(INDEX_NAMES) >= 10

    def test_index_pe_api(self):
        assert INDEX_PE_API["sh000300"] == "000300"
        assert len(INDEX_PE_API) >= 6

# ============================================================
# load_index_pe_from_db — Mock 测试
# ============================================================

class TestLoadIndexPEFromDB:
    def _pe_query(self, index_code="sh000300", days=1095):
        return (
            "SELECT date, pe FROM index_pe_history WHERE index_code='{}' "
            "AND date >= date('now', '-{} days') ORDER BY date"
        ).format(index_code, days)

    def test_with_data(self):
        """pd.read_sql_query succeeds and returns a DataFrame"""
        test_df = pd.DataFrame({
            "date": pd.date_range("2024-01-01", periods=100),
            "pe": [12.0 + 3.0 * np.sin(i / 15.0) for i in range(100)]
        })
        with patch("src.utils.database.get_db_connection") as mock_get_db:
            mock_conn = MagicMock()
            mock_get_db.return_value = mock_conn
            with patch("pandas.read_sql_query", return_value=test_df):
                df = load_index_pe_from_db("sh000300")
                assert isinstance(df, pd.DataFrame)
                assert len(df) == 100
                assert "pe" in df.columns
            mock_conn.close.assert_called_once()

    def test_empty_result(self):
        with patch("src.utils.database.get_db_connection") as mock_get_db:
            mock_conn = MagicMock()
            mock_get_db.return_value = mock_conn
            with patch("pandas.read_sql_query", return_value=pd.DataFrame()):
                df = load_index_pe_from_db("sh000300")
                assert isinstance(df, pd.DataFrame)
                assert df.empty

    def test_db_operational_error(self):
        with patch("src.utils.database.get_db_connection") as mock_get_db:
            mock_conn = MagicMock()
            mock_get_db.return_value = mock_conn
            with patch("pandas.read_sql_query",
                       side_effect=pd.errors.DatabaseError("no such table")):
                df = load_index_pe_from_db("sh000300")
                assert isinstance(df, pd.DataFrame)
                assert df.empty
            mock_conn.close.assert_called_once()

    def test_db_missing_table_error(self):
        with patch("src.utils.database.get_db_connection") as mock_get_db:
            mock_conn = MagicMock()
            mock_get_db.return_value = mock_conn
            with patch("pandas.read_sql_query",
                       side_effect=pd.errors.DatabaseError("table missing")):
                df = load_index_pe_from_db("sh000300")
                assert df.empty

# ============================================================
# load_risk_free_rate — Mock 测试
# ============================================================

class TestLoadRiskFreeRate:
    def test_primary_indicator(self):
        df = pd.DataFrame({"value": [2.8]})
        with patch("src.utils.database.get_db_connection") as mock_get_db:
            mock_conn = MagicMock()
            mock_get_db.return_value = mock_conn
            with patch("pandas.read_sql_query", return_value=df):
                rate = load_risk_free_rate()
                assert rate == 2.8
            mock_conn.close.assert_called_once()

    def test_fallback_indicator(self):
        """First indicator empty, second has data"""
        call_count = [0]
        def _read_sql_side(sql, conn, params=None):
            call_count[0] += 1
            if call_count[0] == 1:
                return pd.DataFrame()  # 10Y_BOND empty
            return pd.DataFrame({"value": [3.1]})  # BOND_10Y fallback

        with patch("src.utils.database.get_db_connection") as mock_get_db:
            mock_conn = MagicMock()
            mock_get_db.return_value = mock_conn
            with patch("pandas.read_sql_query", side_effect=_read_sql_side):
                rate = load_risk_free_rate()
                assert rate == 3.1

    def test_no_data(self):
        with patch("src.utils.database.get_db_connection") as mock_get_db:
            mock_conn = MagicMock()
            mock_get_db.return_value = mock_conn
            with patch("pandas.read_sql_query", return_value=pd.DataFrame()):
                rate = load_risk_free_rate()
                assert rate is None

    def test_nan_value(self):
        df = pd.DataFrame({"value": [pd.NA]})
        with patch("src.utils.database.get_db_connection") as mock_get_db:
            mock_conn = MagicMock()
            mock_get_db.return_value = mock_conn
            with patch("pandas.read_sql_query", return_value=df):
                rate = load_risk_free_rate()
                assert rate is None

    def test_db_error(self):
        with patch("src.utils.database.get_db_connection") as mock_get_db:
            mock_conn = MagicMock()
            mock_get_db.return_value = mock_conn
            with patch("pandas.read_sql_query",
                       side_effect=pd.errors.DatabaseError("error")):
                rate = load_risk_free_rate()
                assert rate is None

# ============================================================
# compute_erp_for_index — 集成测试 (mock load functions)
# ============================================================

class TestComputeERPForIndex:
    def test_normal_result(self):
        pe_df = pd.DataFrame({
            "date": pd.date_range("2024-01-01", periods=100),
            "pe": [12.0 + 3.0 * np.sin(i / 15.0) for i in range(100)]
        })
        with patch("src.analysis.equity_risk_premium.load_index_pe_from_db", return_value=pe_df), \
             patch("src.analysis.equity_risk_premium.load_risk_free_rate", return_value=2.8):
            result = compute_erp_for_index("sh000300")
            assert isinstance(result, ERPResult)
            assert result.index_code == "sh000300"
            assert result.index_name == "沪深300"
            assert result.current_pe > 0
            assert result.risk_free_rate == 2.8
            assert result.signal in ("偏多", "中性略偏多", "中性略偏空", "偏空")
            assert 0 <= result.erp_percentile <= 100

    def test_custom_risk_free_rate(self):
        pe_df = pd.DataFrame({"date": pd.date_range("2024-01-01", periods=50), "pe": [15.0] * 50})
        with patch("src.analysis.equity_risk_premium.load_index_pe_from_db", return_value=pe_df), \
             patch("src.analysis.equity_risk_premium.load_risk_free_rate", return_value=3.0):
            result = compute_erp_for_index("sh000300", risk_free_rate=3.0)
            assert result.risk_free_rate == 3.0

    def test_empty_pe_data(self):
        with patch("src.analysis.equity_risk_premium.load_index_pe_from_db", return_value=pd.DataFrame()):
            result = compute_erp_for_index("sh000999")
            assert result is None

    def test_zero_pe(self):
        pe_df = pd.DataFrame({"date": pd.date_range("2024-01-01", periods=5), "pe": [0.0, 1.0, 2.0, 3.0, 0.0]})
        with patch("src.analysis.equity_risk_premium.load_index_pe_from_db", return_value=pe_df):
            result = compute_erp_for_index("sh000300", risk_free_rate=2.5)
            assert result is None

    def test_negative_pe(self):
        pe_df = pd.DataFrame({"date": pd.date_range("2024-01-01", periods=5), "pe": [1.0, 2.0, 3.0, 4.0, -5.0]})
        with patch("src.analysis.equity_risk_premium.load_index_pe_from_db", return_value=pe_df):
            result = compute_erp_for_index("sh000300", risk_free_rate=2.5)
            assert result is None

    def test_no_risk_free_rate_uses_default(self):
        pe_df = pd.DataFrame({"date": pd.date_range("2024-01-01", periods=50), "pe": [12.0] * 50})
        with patch("src.analysis.equity_risk_premium.load_index_pe_from_db", return_value=pe_df), \
             patch("src.analysis.equity_risk_premium.load_risk_free_rate", return_value=None):
            result = compute_erp_for_index("sh000300")
            assert result is not None
            assert result.risk_free_rate == 2.5

    def test_unknown_index_code(self):
        pe_df = pd.DataFrame({"date": pd.date_range("2024-01-01", periods=50), "pe": [15.0] * 50})
        with patch("src.analysis.equity_risk_premium.load_index_pe_from_db", return_value=pe_df), \
             patch("src.analysis.equity_risk_premium.load_risk_free_rate", return_value=2.5):
            result = compute_erp_for_index("UNKNOWN")
            assert result is not None
            assert result.index_name == "UNKNOWN"

    def test_single_pe_point(self):
        """Edge: only one data point -> erp_history has only 1 element, classify returns 数据不足"""
        pe_df = pd.DataFrame({"date": ["2024-01-01"], "pe": [15.0]})
        with patch("src.analysis.equity_risk_premium.load_index_pe_from_db", return_value=pe_df), \
             patch("src.analysis.equity_risk_premium.load_risk_free_rate", return_value=2.5):
            result = compute_erp_for_index("sh000300")
            assert result is not None
            assert result.signal == "数据不足"

# ============================================================
# compute_erp_multi — 批量测试
# ============================================================

class TestComputeERPMulti:
    def _make_pe_df(self, idx):
        return pd.DataFrame({
            "date": pd.date_range("2024-01-01", periods=50),
            "pe": [12.0 + 3.0 * np.sin(i / 10.0) + (hash(idx) % 5) for i in range(50)]
        })

    def test_default_indices(self):
        with patch("src.analysis.equity_risk_premium.load_risk_free_rate", return_value=2.8), \
             patch("src.analysis.equity_risk_premium.load_index_pe_from_db", side_effect=self._make_pe_df):
            results = compute_erp_multi()
            assert isinstance(results, list)
            assert len(results) > 0
            assert all(isinstance(r, ERPResult) for r in results)

    def test_custom_indices(self):
        with patch("src.analysis.equity_risk_premium.load_risk_free_rate", return_value=2.8), \
             patch("src.analysis.equity_risk_premium.load_index_pe_from_db", side_effect=self._make_pe_df):
            results = compute_erp_multi(["sh000300"])
            assert len(results) == 1
            assert results[0].index_code == "sh000300"

    def test_empty_indices(self):
        results = compute_erp_multi([])
        assert results == []

    def test_nonexistent_indices(self):
        with patch("src.analysis.equity_risk_premium.load_index_pe_from_db", return_value=pd.DataFrame()):
            results = compute_erp_multi(["NONEXISTENT"])
            assert results == []

    def test_mixed_valid_invalid(self):
        def _side_effect(code, **kw):
            if code == "sh000300":
                return self._make_pe_df("sh000300")
            return pd.DataFrame()

        with patch("src.analysis.equity_risk_premium.load_risk_free_rate", return_value=2.8), \
             patch("src.analysis.equity_risk_premium.load_index_pe_from_db", side_effect=_side_effect):
            results = compute_erp_multi(["sh000300", "FAKE"])
            assert len(results) == 1
            assert results[0].index_code == "sh000300"
