"""数据库连接测试（已修复）"""
import sqlite3
import pytest


def test_get_db_connection(db_connection):
    """测试数据库连接"""
    assert db_connection is not None
    assert isinstance(db_connection, sqlite3.Connection)


def test_database_tables_exist(db_connection):
    """测试核心表是否存在"""
    cur = db_connection.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = {row[0] for row in cur.fetchall()}
    # 至少应有这4张表
    required = {"portfolio_snapshots", "portfolio_summary", "index_quotes", "etf_technical"}
    missing = required - tables
    assert not missing, f"缺少表: {missing}"
