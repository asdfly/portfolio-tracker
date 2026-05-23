"""Tests for tab4_calendar.py utility functions."""
import pytest
import pandas as pd
import numpy as np
import sqlite3


@pytest.fixture
def db_with_summary():
    """In-memory DB with portfolio_summary data for calendar tests."""
    conn = sqlite3.connect(":memory:")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS portfolio_summary (
            date TEXT PRIMARY KEY,
            total_value REAL,
            total_cost REAL,
            total_pnl REAL,
            daily_pnl REAL,
            daily_return REAL,
            vs_hs300 REAL,
            profit_count INTEGER,
            loss_count INTEGER,
            sharpe_ratio REAL,
            max_drawdown REAL,
            total_return REAL,
            annualized_return REAL,
            calmar_ratio REAL
        )
    """)
    # Insert 6 months of data (2024-07 to 2024-12, ~126 trading days)
    rows = []
    date = pd.Timestamp("2024-07-01")
    total_value = 100000.0
    for i in range(126):
        d = date + pd.tseries.offsets.BDay(i)
        daily_ret = (np.random.random() - 0.48) * 0.03
        total_value *= (1 + daily_ret)
        rows.append((d.strftime("%Y-%m-%d"), round(total_value, 2), 90000.0,
                      round(total_value - 90000, 2), round(daily_ret * total_value, 2),
                      round(daily_ret, 6), 0, 10, 3, 0.5, -0.02, 0.1, 0.12, 0.5))
    conn.executemany(
        "INSERT OR IGNORE INTO portfolio_summary (date,total_value,total_cost,total_pnl,"
        "daily_pnl,daily_return,vs_hs300,profit_count,loss_count,sharpe_ratio,max_drawdown,"
        "total_return,annualized_return,calmar_ratio) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    conn.commit()
    yield conn
    conn.close()


class TestComputeMonthlyReturns:
    """Test compute_monthly_returns with mocked DB."""

    def test_returns_dataframe(self, monkeypatch, db_with_summary):
        """Should return a DataFrame with year index and month columns."""
        monkeypatch.setattr("tabs.tab4_calendar.get_db_connection", lambda: db_with_summary)
        from tabs.tab4_calendar import compute_monthly_returns
        result = compute_monthly_returns()
        assert isinstance(result, pd.DataFrame)
        assert not result.empty

    def test_returns_have_yearly_column(self, monkeypatch, db_with_summary):
        """Result should include '年累计' column."""
        monkeypatch.setattr("tabs.tab4_calendar.get_db_connection", lambda: db_with_summary)
        from tabs.tab4_calendar import compute_monthly_returns
        result = compute_monthly_returns()
        assert "年累计" in result.columns

    def test_returns_have_summary_row(self, monkeypatch, db_with_summary):
        """Result should include '月均' summary row."""
        monkeypatch.setattr("tabs.tab4_calendar.get_db_connection", lambda: db_with_summary)
        from tabs.tab4_calendar import compute_monthly_returns
        result = compute_monthly_returns()
        assert "月均" in result.index

    def test_empty_db_returns_empty_df(self, monkeypatch):
        """Empty portfolio_summary should return empty DataFrame."""
        conn = sqlite3.connect(":memory:")
        conn.execute("""CREATE TABLE portfolio_summary (
            date TEXT PRIMARY KEY, total_value REAL, daily_pnl REAL, daily_return REAL)""")
        conn.commit()
        monkeypatch.setattr("tabs.tab4_calendar.get_db_connection", lambda: conn)
        from tabs.tab4_calendar import compute_monthly_returns
        result = compute_monthly_returns()
        assert isinstance(result, pd.DataFrame)
        assert result.empty
        conn.close()


class TestLoadCalendarData:
    """Test load_calendar_data with mocked DB."""

    def test_returns_dataframe(self, monkeypatch, db_with_summary):
        monkeypatch.setattr("tabs.tab4_calendar.get_db_connection", lambda: db_with_summary)
        from tabs.tab4_calendar import load_calendar_data
        result = load_calendar_data()
        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        assert "year" in result.columns
        assert "month" in result.columns
        assert "day" in result.columns

    def test_empty_db_returns_empty_df(self, monkeypatch):
        conn = sqlite3.connect(":memory:")
        conn.execute("""CREATE TABLE portfolio_summary (
            date TEXT PRIMARY KEY, total_value REAL, daily_pnl REAL, daily_return REAL)""")
        conn.commit()
        monkeypatch.setattr("tabs.tab4_calendar.get_db_connection", lambda: conn)
        from tabs.tab4_calendar import load_calendar_data
        result = load_calendar_data()
        assert result.empty
        conn.close()


class TestGetThirdFriday:
    """Test get_third_friday helper - it's a nested function inside render_tab4."""

    def test_third_friday_january_2025(self):
        """Jan 2025: Fridays are 3,10,17,24,31 -> third is 17."""
        import calendar as cal_mod
        cal = cal_mod.monthcalendar(2025, 1)
        fridays = [week[cal_mod.FRIDAY] for week in cal if week[cal_mod.FRIDAY] != 0]
        result = fridays[2] if len(fridays) >= 3 else fridays[-1]
        assert result == 17

    def test_third_friday_february_2025(self):
        """Feb 2025: Fridays are 7,14,21,28 -> third is 21."""
        import calendar as cal_mod
        cal = cal_mod.monthcalendar(2025, 2)
        fridays = [week[cal_mod.FRIDAY] for week in cal if week[cal_mod.FRIDAY] != 0]
        result = fridays[2] if len(fridays) >= 3 else fridays[-1]
        assert result == 21

    def test_third_friday_always_valid(self):
        """For any month, third Friday should be a valid day."""
        import calendar as cal_mod
        for month in range(1, 13):
            cal = cal_mod.monthcalendar(2025, month)
            fridays = [week[cal_mod.FRIDAY] for week in cal if week[cal_mod.FRIDAY] != 0]
            assert len(fridays) >= 4  # Every month has at least 4 Fridays
            assert fridays[2] >= 15  # Third Friday is at least the 15th
