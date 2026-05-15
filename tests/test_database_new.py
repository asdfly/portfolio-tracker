"""L2 - 数据库完整性 + DatabaseManager 测试"""
import sys
import sqlite3
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def test_database_file_exists():
    """portfolio.db 文件存在"""
    import config.settings as s
    assert s.DATABASE_PATH.is_dir() or s.DATABASE_PATH.with_suffix(".db").exists() or s.DATABASE_PATH.exists(),         f"数据库路径无效: {s.DATABASE_PATH}"


def test_all_four_tables_exist():
    """4 张核心表都存在"""
    import sqlite3, config.settings as s
    db_path = str(s.DATABASE_PATH)
    if not db_path.endswith(".db"):
        db_path += ".db"
    conn = sqlite3.connect(db_path)
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = {row[0] for row in cur.fetchall()}
    conn.close()
    required = {"portfolio_snapshots", "portfolio_summary", "index_quotes", "etf_technical"}
    missing = required - tables
    assert not missing, f"缺少表: {missing}"


def test_snapshots_schema():
    """portfolio_snapshots 包含预期的列"""
    import sqlite3, config.settings as s
    db_path = str(s.DATABASE_PATH)
    conn = sqlite3.connect(db_path)
    cur = conn.execute("PRAGMA table_info(portfolio_snapshots)")
    cols = {row[1] for row in cur.fetchall()}
    conn.close()
    required = {"date", "code", "name", "quantity", "cost_price", "current_price", "market_value", "pnl"}
    missing = required - cols
    assert not missing, f"portfolio_snapshots 缺少列: {missing}"


def test_index_quotes_schema():
    """index_quotes 包含预期的列"""
    import sqlite3, config.settings as s
    db_path = str(s.DATABASE_PATH)
    conn = sqlite3.connect(db_path)
    cur = conn.execute("PRAGMA table_info(index_quotes)")
    cols = {row[1] for row in cur.fetchall()}
    conn.close()
    required = {"date", "code", "close", "change_pct"}
    missing = required - cols
    assert not missing, f"index_quotes 缺少列: {missing}"


def test_etf_technical_schema():
    """etf_technical 包含预期的列"""
    import sqlite3, config.settings as s
    db_path = str(s.DATABASE_PATH)
    conn = sqlite3.connect(db_path)
    cur = conn.execute("PRAGMA table_info(etf_technical)")
    cols = {row[1] for row in cur.fetchall()}
    conn.close()
    required = {"date", "code", "ma_signal", "macd_signal", "rsi_value", "rsi_status", "trend"}
    missing = required - cols
    assert not missing, f"etf_technical 缺少列: {missing}"


def test_indexes_exist():
    """6 个索引存在"""
    import sqlite3, config.settings as s
    db_path = str(s.DATABASE_PATH)
    conn = sqlite3.connect(db_path)
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%'")
    indexes = {row[0] for row in cur.fetchall()}
    conn.close()
    expected = {"idx_snap_date", "idx_snap_code_date", "idx_summary_date",
                "idx_idx_quote_code_date", "idx_tech_date", "idx_tech_code_date"}
    missing = expected - indexes
    assert not missing, f"缺少索引: {missing}"


class TestDatabaseManager:
    """DatabaseManager CRUD 测试（使用临时数据库）"""

    def _make_temp_db(self, tmp_path):
        import sqlite3
        db_file = tmp_path / "test_portfolio.db"
        conn = sqlite3.connect(str(db_file))
        conn.close()
        return db_file

    def test_init_creates_tables(self, tmp_path):
        """初始化创建所有表"""
        db_file = self._make_temp_db(tmp_path)
        from src.utils.database import DatabaseManager
        dm = DatabaseManager(str(db_file))
        conn = sqlite3.connect(str(db_file))
        cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row[0] for row in cur.fetchall()}
        conn.close()
        assert "portfolio_snapshots" in tables
        assert "etf_technical" in tables

    def test_save_and_load_snapshot(self, tmp_path):
        """保存持仓快照后可读取"""
        db_file = self._make_temp_db(tmp_path)
        from src.utils.database import DatabaseManager
        dm = DatabaseManager(str(db_file))
        holdings = [
            {"code": "510300", "name": "沪深300ETF", "quantity": 1000,
             "cost_price": 4.5, "current_price": 4.8, "market_value": 4800,
             "pnl": 300, "pnl_rate": 6.67, "beta": 0.95}
        ]
        dm.save_portfolio_snapshot("2024-06-01", holdings)
        result = dm.get_latest_portfolio("2024-06-01")
        assert len(result) == 1
        assert result[0]["code"] == "510300"
        assert result[0]["pnl"] == 300

    def test_save_and_load_summary(self, tmp_path):
        """保存汇总后可读取"""
        import sqlite3
        db_file = self._make_temp_db(tmp_path)
        from src.utils.database import DatabaseManager
        dm = DatabaseManager(str(db_file))
        summary = {"total_value": 100000, "total_cost": 95000, "total_pnl": 5000,
                   "daily_pnl": 200, "daily_return": 0.002, "vs_hs300": 0.001}
        dm.save_portfolio_summary("2024-06-01", summary)
        conn = sqlite3.connect(str(db_file))
        cur = conn.execute("SELECT * FROM portfolio_summary WHERE date='2024-06-01'")
        row = cur.fetchone()
        conn.close()
        assert row is not None

    def test_save_index_quotes(self, tmp_path):
        """保存指数行情"""
        import sqlite3
        db_file = self._make_temp_db(tmp_path)
        from src.utils.database import DatabaseManager
        dm = DatabaseManager(str(db_file))
        quotes = {"sh000300": {"price": 3500.0, "change_pct": 0.5, "volume": 1e8, "amount": 1e10}}
        dm.save_index_quotes("2024-06-01", quotes)
        conn = sqlite3.connect(str(db_file))
        cur = conn.execute("SELECT * FROM index_quotes WHERE code='sh000300'")
        row = cur.fetchone()
        conn.close()
        assert row is not None

    def test_save_technical_indicators(self, tmp_path):
        """保存技术指标"""
        import sqlite3
        db_file = self._make_temp_db(tmp_path)
        from src.utils.database import DatabaseManager
        dm = DatabaseManager(str(db_file))
        indicators = {"ma": {"signal": "买入"}, "macd": {"signal": "金叉"},
                       "rsi": {"RSI": 45.0, "status": "neutral"},
                       "kdj": {"signal": "超卖"}, "bollinger": {"position": 0.3},
                       "atr": {"ATR_pct": 1.5}, "trend": {"trend": "上涨"}}
        dm.save_technical_indicators("2024-06-01", "510300", indicators)
        conn = sqlite3.connect(str(db_file))
        cur = conn.execute("SELECT * FROM etf_technical WHERE code='510300'")
        row = cur.fetchone()
        conn.close()
        assert row is not None

    def test_get_portfolio_history_order(self, tmp_path):
        """历史数据按时间正序返回"""
        import sqlite3
        db_file = self._make_temp_db(tmp_path)
        from src.utils.database import DatabaseManager
        dm = DatabaseManager(str(db_file))
        for i, day in enumerate(["2024-06-01", "2024-06-02", "2024-06-03"]):
            dm.save_portfolio_summary(day, {"total_value": 100000 + i * 100})
        result = dm.get_portfolio_history(days=3)
        assert len(result) == 3
        dates = [r["date"] for r in result]
        assert dates == sorted(dates), "应按时间正序"
