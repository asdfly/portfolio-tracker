"""DataQualityChecker 单元测试"""
import sys, os, sqlite3
from pathlib import Path
import pytest

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


@pytest.fixture
def sample_db(tmp_path):
    """创建带样本数据的测试数据库（schema 匹配实际 portfolio.db）"""
    db_path = str(tmp_path / "test_portfolio.db")
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    c.executescript("""
        CREATE TABLE portfolio_summary (
            date TEXT, total_value REAL, total_cost REAL, total_pnl REAL,
            daily_pnl REAL, daily_return REAL, vs_hs300 REAL,
            profit_count INTEGER, loss_count INTEGER,
            sharpe_ratio REAL, max_drawdown REAL, volatility REAL
        );
        CREATE TABLE portfolio_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT, code TEXT, name TEXT, quantity REAL,
            cost_price REAL, current_price REAL, market_value REAL,
            pnl REAL, pnl_rate REAL, ytd_return REAL, beta REAL
        );
        CREATE TABLE daily_news (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT, category TEXT, title TEXT, source TEXT, url TEXT,
            summary TEXT, publish_time TEXT, created_at TEXT
        );
        CREATE TABLE fund_flows (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT, code TEXT, name TEXT, net_inflow REAL,
            buy_amount REAL, sell_amount REAL, category TEXT,
            created_at TIMESTAMP, net_inflow_pct REAL,
            super_large_inflow REAL, super_large_pct REAL,
            large_inflow REAL, large_pct REAL,
            medium_inflow REAL, medium_pct REAL,
            small_inflow REAL, small_pct REAL
        );
        CREATE TABLE etf_technical (
            date TEXT, code TEXT, ma_signal TEXT, macd_signal TEXT,
            rsi_value REAL, rsi_status TEXT, kdj_signal TEXT,
            bollinger_position REAL, atr_pct REAL, trend TEXT
        );
        CREATE TABLE index_quotes (
            date TEXT, code TEXT, name TEXT, close REAL,
            change_pct REAL, volume REAL, amount REAL
        );
        CREATE TABLE macro_daily (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT, indicator_code TEXT, name TEXT, value REAL,
            change_pct REAL, source TEXT, created_at TIMESTAMP
        );
        CREATE TABLE market_sentiment (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT, indicator_code TEXT, name TEXT, value REAL,
            change_value REAL, change_pct REAL, source TEXT, created_at TIMESTAMP
        );
        CREATE TABLE alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rule_name TEXT, level TEXT, message TEXT,
            created_at TIMESTAMP, acknowledged BOOLEAN
        );
        CREATE TABLE execution_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_name TEXT, status TEXT, message TEXT,
            duration_seconds REAL, created_at TIMESTAMP
        );
        CREATE TABLE custom_indicators (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT, formula TEXT, description TEXT,
            created_at TIMESTAMP, is_template BOOLEAN
        );
        CREATE TABLE indicator_backtest_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            indicator_id INTEGER, test_period TEXT,
            total_signals INTEGER, win_count INTEGER, loss_count INTEGER,
            win_rate REAL, avg_pnl REAL, sharpe REAL, created_at TIMESTAMP
        );
    """)

    import pandas as pd
    dates = pd.date_range("2026-05-01", periods=15, freq="D").strftime("%Y-%m-%d")
    for d in dates:
        c.execute("INSERT INTO portfolio_summary VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                  (d, 100000, 90000, 10000, 1000, 0.01, 0.02, 5, 3, 1.0, 0.05, 0.15))
        c.execute("INSERT INTO portfolio_snapshots (date,code,name,quantity,cost_price,current_price,market_value,pnl,pnl_rate,ytd_return,beta) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                  (d, "510300", "沪深300ETF", 1000, 4.5, 4.8, 4800, 300, 6.67, 10.5, 0.95))
        c.execute("INSERT INTO etf_technical VALUES (?,?,?,?,?,?,?,?,?,?)",
                  (d, "510300", "多头排列", "金叉", 50.0, "中性", "超买", 0.5, 1.2, "上涨"))
        c.execute("INSERT INTO fund_flows (date,code,name,net_inflow,buy_amount,sell_amount,category) VALUES (?,?,?,?,?,?,?)",
                  (d, "510300", "沪深300ETF", 5000.0, 3000.0, 2000.0, "ETF"))
        c.execute("INSERT INTO index_quotes VALUES (?,?,?,?,?,?,?)",
                  (d, "sh000300", "沪深300", 3500.0, 1.5, 100000000.0, 5000000000.0))
    c.execute("INSERT INTO daily_news (date,category,title,source,url,summary,publish_time,created_at) VALUES (?,?,?,?,?,?,?,?)",
              ("2026-05-15", "ETF", "测试新闻", "东方财富", "https://example.com/1", "摘要", "10:00", "2026-05-15 10:00:00"))
    for i, d in enumerate(dates):
        c.execute("INSERT INTO macro_daily (date,indicator_code,name,value,change_pct,source,created_at) VALUES (?,?,?,?,?,?,?)",
                  (d, "USD_CNY", "美元兑人民币", 7.25 + i * 0.001, 0.01, "东方财富", "2026-05-15"))
        c.execute("INSERT INTO market_sentiment (date,indicator_code,name,value,change_value,change_pct,source,created_at) VALUES (?,?,?,?,?,?,?,?)",
                  (d, "MARGIN_TOTAL", "两融余额", 18000 + i * 10, 10.0, 0.05, "东方财富", "2026-05-15"))
    c.execute("INSERT INTO custom_indicators (name,formula,description,created_at,is_template) VALUES (?,?,?,?,?)",
              ("MA_CROSS", "MA5>MA20", "均线金叉", "2026-05-01", 1))
    c.execute("INSERT INTO indicator_backtest_results (indicator_id,test_period,total_signals,win_count,loss_count,win_rate,avg_pnl,sharpe,created_at) VALUES (?,?,?,?,?,?,?,?,?)",
              (1, "2025-01-01~2025-06-30", 100, 55, 45, 0.55, 0.02, 1.2, "2026-05-15"))
    conn.commit()
    conn.close()
    return db_path


@pytest.fixture
def empty_db(tmp_path):
    """创建空表结构的测试数据库"""
    db_path = str(tmp_path / "empty_test.db")
    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE portfolio_summary (date TEXT, total_value REAL, total_cost REAL, total_pnl REAL,
            daily_pnl REAL, daily_return REAL, vs_hs300 REAL,
            profit_count INTEGER, loss_count INTEGER,
            sharpe_ratio REAL, max_drawdown REAL, volatility REAL);
        CREATE TABLE portfolio_snapshots (date TEXT, code TEXT);
        CREATE TABLE daily_news (date TEXT);
        CREATE TABLE fund_flows (date TEXT, code TEXT);
        CREATE TABLE etf_technical (date TEXT, code TEXT);
        CREATE TABLE index_quotes (date TEXT, code TEXT);
        CREATE TABLE macro_daily (date TEXT, indicator_code TEXT);
        CREATE TABLE market_sentiment (date TEXT, indicator_code TEXT);
        CREATE TABLE alerts (id INTEGER PRIMARY KEY);
        CREATE TABLE execution_logs (id INTEGER PRIMARY KEY);
        CREATE TABLE custom_indicators (id INTEGER PRIMARY KEY, name TEXT, is_template INTEGER);
        CREATE TABLE indicator_backtest_results (id INTEGER PRIMARY KEY, indicator_id INTEGER, test_period TEXT,
            total_signals INTEGER, win_count INTEGER, loss_count INTEGER, win_rate REAL, avg_pnl REAL, sharpe REAL);
    """)
    conn.close()
    return db_path


class TestDataQualityChecker:

    def test_init_with_db_path(self, sample_db):
        from src.utils.data_quality import DataQualityChecker
        checker = DataQualityChecker(sample_db)
        assert checker.db_path == sample_db

    def test_check_table_freshness_structure(self, sample_db):
        from src.utils.data_quality import DataQualityChecker
        checker = DataQualityChecker(sample_db)
        freshness = checker.check_table_freshness()
        assert isinstance(freshness, list)
        assert len(freshness) > 0
        for item in freshness:
            assert "table" in item
            assert "label" in item
            assert "latest_date" in item
            assert "days_lag" in item
            assert item["status"] in ("OK", "WARN", "STALE", "EMPTY", "ERROR")

    def test_check_table_freshness_labels(self, sample_db):
        from src.utils.data_quality import DataQualityChecker
        checker = DataQualityChecker(sample_db)
        freshness = checker.check_table_freshness()
        labels = [f["label"] for f in freshness]
        assert "交易日快照" in labels
        assert "技术指标" in labels

    def test_check_data_coverage_structure(self, sample_db):
        from src.utils.data_quality import DataQualityChecker
        checker = DataQualityChecker(sample_db)
        coverage = checker.check_data_coverage()
        assert isinstance(coverage, dict)
        assert "portfolio_snapshots" in coverage
        for table, info in coverage.items():
            assert "total_rows" in info
            assert "distinct_codes" in info
            assert "date_range" in info

    def test_check_data_coverage_values(self, sample_db):
        from src.utils.data_quality import DataQualityChecker
        checker = DataQualityChecker(sample_db)
        coverage = checker.check_data_coverage()
        assert coverage["portfolio_snapshots"]["total_rows"] == 15

    def test_check_indicator_backtest(self, sample_db):
        from src.utils.data_quality import DataQualityChecker
        checker = DataQualityChecker(sample_db)
        result = checker.check_indicator_backtest()
        assert isinstance(result, dict)
        assert "template_count" in result
        assert "result_count" in result
        assert result["template_count"] == 1
        assert result["result_count"] == 1

    def test_compute_quality_score_structure(self, sample_db):
        from src.utils.data_quality import DataQualityChecker
        checker = DataQualityChecker(sample_db)
        score = checker.compute_quality_score()
        assert isinstance(score, dict)
        for key in ["total_score", "grade", "freshness_score", "coverage_score", "backtest_score"]:
            assert key in score

    def test_compute_quality_score_ranges(self, sample_db):
        from src.utils.data_quality import DataQualityChecker
        checker = DataQualityChecker(sample_db)
        score = checker.compute_quality_score()
        assert 0 <= score["total_score"] <= 100
        assert score["grade"] in ("A", "B+", "B", "C", "D")

    def test_run_full_check_equals_compute(self, sample_db):
        from src.utils.data_quality import DataQualityChecker
        checker = DataQualityChecker(sample_db)
        full = checker.run_full_check()
        computed = checker.compute_quality_score()
        assert full["total_score"] == computed["total_score"]

    def test_get_freshness_summary_returns_str(self, sample_db):
        from src.utils.data_quality import DataQualityChecker
        checker = DataQualityChecker(sample_db)
        summary = checker.get_freshness_summary()
        assert isinstance(summary, str)
        assert len(summary) > 0

    def test_empty_database_no_crash(self, empty_db):
        from src.utils.data_quality import DataQualityChecker
        checker = DataQualityChecker(empty_db)
        freshness = checker.check_table_freshness()
        for item in freshness:
            assert item["status"] in ("EMPTY", "ERROR")
        score = checker.compute_quality_score()
        assert score is not None
        assert score["total_score"] < 100

    def test_empty_database_coverage_zero(self, empty_db):
        from src.utils.data_quality import DataQualityChecker
        checker = DataQualityChecker(empty_db)
        coverage = checker.check_data_coverage()
        for table, info in coverage.items():
            assert info["total_rows"] == 0
