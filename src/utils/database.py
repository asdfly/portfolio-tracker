"""
数据库模块 - SQLite数据持久化
"""
import sqlite3
import json
from datetime import datetime, date
from typing import Dict, List, Any, Optional
from pathlib import Path
import logging

import streamlit as st
from config.settings import DATABASE_PATH

logger = logging.getLogger(__name__)

class DatabaseManager:
    """数据库管理器"""

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or str(DATABASE_PATH)
        self._init_db()

    def _init_db(self):
        """初始化数据库表结构（统一从 db_schema 执行全部 DDL）"""
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)

        from src.utils.db_schema import init_all_tables
        with sqlite3.connect(self.db_path) as conn:
            init_all_tables(conn)
            logger.info("数据库初始化完成")

    def save_portfolio_snapshot(self, date_str: str, holdings: List[Dict[str, Any]]):
        """保存持仓快照"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            for holding in holdings:
                # 优先使用实时行情数据（来自新浪API），确保与summary一致
                current_price = holding.get('realtime_price', holding.get('current_price'))
                market_value = holding.get('realtime_market_value', holding.get('market_value'))
                pnl = holding.get('realtime_pnl', holding.get('pnl'))

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
                    current_price,
                    market_value,
                    pnl,
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
            # 按时间正序返回（DESC查询取最近N条后需反转，确保收益率计算方向正确）
            return [dict(row) for row in reversed(rows)]

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


_indexes_created = False

def get_db_connection():
    """获取数据库连接，首次调用时创建索引"""
    global _indexes_created
    import sqlite3
    from config.settings import DATABASE_PATH
    conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
    if not _indexes_created:
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_snap_date ON portfolio_snapshots(date)",
            "CREATE INDEX IF NOT EXISTS idx_snap_code_date ON portfolio_snapshots(code, date)",
            "CREATE INDEX IF NOT EXISTS idx_summary_date ON portfolio_summary(date)",
            "CREATE INDEX IF NOT EXISTS idx_idx_quote_code_date ON index_quotes(code, date)",
            "CREATE INDEX IF NOT EXISTS idx_tech_date ON etf_technical(date)",
            "CREATE INDEX IF NOT EXISTS idx_tech_code_date ON etf_technical(code, date)",
        ]
        for sql in indexes:
            try:
                conn.execute(sql)
            except Exception:
                pass
        conn.commit()
        _indexes_created = True
    return conn

