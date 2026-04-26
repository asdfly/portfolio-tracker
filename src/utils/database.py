"""
数据库模块 - SQLite数据持久化
"""
import sqlite3
import json
from datetime import datetime, date
from typing import Dict, List, Any, Optional
from pathlib import Path
import logging

from config.settings import DATABASE_PATH

logger = logging.getLogger(__name__)


class DatabaseManager:
    """数据库管理器"""

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or str(DATABASE_PATH)
        self._init_db()

    def _init_db(self):
        """初始化数据库表结构"""
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # 持仓快照表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS portfolio_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT NOT NULL,
                    code TEXT NOT NULL,
                    name TEXT,
                    quantity REAL,
                    cost_price REAL,
                    current_price REAL,
                    market_value REAL,
                    pnl REAL,
                    pnl_rate REAL,
                    ytd_return REAL,
                    beta REAL,
                    UNIQUE(date, code)
                )
            """)

            # 组合汇总表
            cursor.execute("""
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
                    volatility REAL
                )
            """)

            # 指数行情表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS index_quotes (
                    date TEXT NOT NULL,
                    code TEXT NOT NULL,
                    name TEXT,
                    close REAL,
                    change_pct REAL,
                    volume REAL,
                    amount REAL,
                    PRIMARY KEY (date, code)
                )
            """)

            # ETF技术指标表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS etf_technical (
                    date TEXT NOT NULL,
                    code TEXT NOT NULL,
                    ma_signal TEXT,
                    macd_signal TEXT,
                    rsi_value REAL,
                    rsi_status TEXT,
                    kdj_signal TEXT,
                    bollinger_position REAL,
                    atr_pct REAL,
                    trend TEXT,
                    PRIMARY KEY (date, code)
                )
            """)

            # 创建索引
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_snapshot_date ON portfolio_snapshots(date)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_index_date ON index_quotes(date)")

            conn.commit()
            logger.info("数据库初始化完成")

    def save_portfolio_snapshot(self, date_str: str, holdings: List[Dict[str, Any]]):
        """保存持仓快照"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            for holding in holdings:
                cursor.execute("""
                    INSERT OR REPLACE INTO portfolio_snapshots 
                    (date, code, name, quantity, cost_price, current_price, 
                     market_value, pnl, pnl_rate, ytd_return, beta)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    date_str,
                    holding.get('code'),
                    holding.get('name'),
                    holding.get('quantity'),
                    holding.get('cost_price'),
                    holding.get('current_price'),
                    holding.get('market_value'),
                    holding.get('pnl'),
                    holding.get('pnl_rate'),
                    holding.get('ytd_return'),
                    holding.get('beta')
                ))

            conn.commit()
            logger.info(f"保存持仓快照: {date_str}, {len(holdings)}条记录")

    def save_portfolio_summary(self, date_str: str, summary: Dict[str, Any]):
        """保存组合汇总"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO portfolio_summary 
                (date, total_value, total_cost, total_pnl, daily_pnl, daily_return,
                 vs_hs300, profit_count, loss_count, sharpe_ratio, max_drawdown, volatility)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                date_str,
                summary.get('total_value'),
                summary.get('total_cost'),
                summary.get('total_pnl'),
                summary.get('daily_pnl'),
                summary.get('daily_return'),
                summary.get('vs_hs300'),
                summary.get('profit_count'),
                summary.get('loss_count'),
                summary.get('sharpe_ratio'),
                summary.get('max_drawdown'),
                summary.get('volatility')
            ))
            conn.commit()

    def save_index_quotes(self, date_str: str, quotes: Dict[str, Dict[str, Any]]):
        """保存指数行情"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            from config.settings import INDEX_CODES
            for code, quote in quotes.items():
                std_name = INDEX_CODES.get(code, quote.get('name', ''))
                cursor.execute("""
                    INSERT OR REPLACE INTO index_quotes
                    (date, code, name, close, change_pct, volume, amount)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    date_str,
                    code,
                    std_name,
                    quote.get('price'),
                    quote.get('change_pct'),
                    quote.get('volume'),
                    quote.get('amount')
                ))

            conn.commit()

    def save_technical_indicators(self, date_str: str, code: str, 
                                   indicators: Dict[str, Any]):
        """保存技术指标"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            ma = indicators.get('ma', {})
            macd = indicators.get('macd', {})
            rsi = indicators.get('rsi', {})
            kdj = indicators.get('kdj', {})
            bollinger = indicators.get('bollinger', {})
            atr = indicators.get('atr', {})
            trend = indicators.get('trend', {})

            cursor.execute("""
                INSERT OR REPLACE INTO etf_technical 
                (date, code, ma_signal, macd_signal, rsi_value, rsi_status,
                 kdj_signal, bollinger_position, atr_pct, trend)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                date_str, code,
                ma.get('signal'),
                macd.get('signal'),
                rsi.get('RSI'),
                rsi.get('status'),
                kdj.get('signal'),
                bollinger.get('position'),
                atr.get('ATR_pct'),
                trend.get('trend')
            ))
            conn.commit()

    def get_latest_portfolio(self, date_str: Optional[str] = None) -> List[Dict[str, Any]]:
        """获取最新持仓数据"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            if date_str:
                cursor.execute("""
                    SELECT * FROM portfolio_snapshots WHERE date = ?
                """, (date_str,))
            else:
                cursor.execute("""
                    SELECT * FROM portfolio_snapshots 
                    WHERE date = (SELECT MAX(date) FROM portfolio_snapshots)
                """)

            rows = cursor.fetchall()
            return [dict(row) for row in rows]

    def get_portfolio_history(self, days: int = 30) -> List[Dict[str, Any]]:
        """获取组合历史数据"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute("""
                SELECT * FROM portfolio_summary
                ORDER BY date DESC
                LIMIT ?
            """, (days,))

            rows = cursor.fetchall()
            return [dict(row) for row in rows]

    def get_price_history(self, code: str, days: int = 60) -> List[Dict[str, Any]]:
        """获取价格历史"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute("""
                SELECT date, current_price, market_value, pnl
                FROM portfolio_snapshots
                WHERE code = ?
                ORDER BY date DESC
                LIMIT ?
            """, (code, days))

            rows = cursor.fetchall()
            return [dict(row) for row in rows]
