"""
资金流数据采集模块 - Phase 5 Batch 4
使用 AKShare 采集行业资金流、ETF资金流、北向资金数据。
"""

import akshare as ak
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
from typing import Optional, List, Dict
import logging

logger = logging.getLogger(__name__)


def get_db_connection() -> sqlite3.Connection:
    from config.settings import DATABASE_PATH
    return sqlite3.connect(str(DATABASE_PATH))


def fetch_sector_fund_flow(date_str: str = None) -> pd.DataFrame:
    """获取行业板块资金流向"""
    try:
        df = ak.stock_sector_fund_flow_rank(indicator="今日", sector_type="行业资金流")
        if df is not None and not df.empty:
            df = df.rename(columns={
                '名称': 'name', '代码': 'code',
                '最新涨跌幅': 'change_pct',
                '主力净流入-净额': 'net_inflow',
                '主力净流入-净占比': 'net_inflow_pct',
                '超大单净流入-净额': 'super_large_inflow',
                '超大单净流入-净占比': 'super_large_pct',
                '大单净流入-净额': 'large_inflow',
                '大单净流入-净占比': 'large_pct',
                '中单净流入-净额': 'medium_inflow',
                '中单净流入-净占比': 'medium_pct',
                '小单净流入-净额': 'small_inflow',
                '小单净流入-净占比': 'small_pct',
            })
            df['category'] = 'sector'
            df['date'] = date_str or datetime.now().strftime('%Y-%m-%d')
            return df
    except Exception as e:
        logger.warning(f"获取行业资金流失败: {e}")
    return pd.DataFrame()


def fetch_etf_fund_flow(code: str, name: str = '') -> pd.DataFrame:
    """获取单只ETF资金流向"""
    try:
        df = ak.stock_individual_fund_flow(stock=code, market="sh" if code.startswith('5') or code.startswith('15') else "sz")
        if df is not None and not df.empty:
            df = df.rename(columns={
                '日期': 'date', '收盘价': 'close', '涨跌幅': 'change_pct',
                '主力净流入-净额': 'net_inflow',
                '主力净流入-净占比': 'net_inflow_pct',
                '超大单净流入-净额': 'super_large_inflow',
                '大单净流入-净额': 'large_inflow',
                '中单净流入-净额': 'medium_inflow',
                '小单净流入-净额': 'small_inflow',
            })
            df['code'] = code
            df['name'] = name
            df['category'] = 'etf'
            keep_cols = ['date', 'code', 'name', 'close', 'change_pct',
                         'net_inflow', 'net_inflow_pct', 'category']
            df = df[[c for c in keep_cols if c in df.columns]]
            return df
    except Exception as e:
        logger.warning(f"获取ETF {code} 资金流失败: {e}")
    return pd.DataFrame()


def fetch_north_flow(days: int = 30) -> pd.DataFrame:
    """获取北向资金净流入数据"""
    try:
        df = ak.stock_hsgt_north_net_flow_in_em(symbol="北向资金")
        if df is not None and not df.empty:
            df = df.rename(columns={
                '日期': 'date', '当日成交净买额': 'net_inflow',
                '当日资金流入': 'buy_amount', '当日资金流出': 'sell_amount',
            })
            df['code'] = 'north'
            df['name'] = '北向资金'
            df['category'] = 'north'
            keep_cols = ['date', 'code', 'name', 'net_inflow', 'buy_amount', 'sell_amount', 'category']
            df = df[[c for c in keep_cols if c in df.columns]]
            if len(df) > days:
                df = df.tail(days)
            return df
    except Exception as e:
        logger.warning(f"获取北向资金失败: {e}")
    return pd.DataFrame()


def save_fund_flows(conn: sqlite3.Connection, df: pd.DataFrame):
    """保存资金流数据到数据库（upsert）"""
    if df.empty:
        return 0
    required = ['date', 'category']
    for col in required:
        if col not in df.columns:
            return 0

    cursor = conn.cursor()
    count = 0
    for _, row in df.iterrows():
        date_val = str(row.get('date', ''))
        code_val = str(row.get('code', ''))
        cat_val = str(row.get('category', ''))

        cursor.execute("""
            SELECT id FROM fund_flows
            WHERE date = ? AND code = ? AND category = ?
        """, (date_val, code_val, cat_val))
        existing = cursor.fetchone()

        if existing:
            # 更新
            cursor.execute("""
                UPDATE fund_flows SET name=?, net_inflow=?,
                    buy_amount=?, sell_amount=?
                WHERE id=?
            """, (
                str(row.get('name', '')),
                float(row.get('net_inflow', 0)) if pd.notna(row.get('net_inflow')) else 0,
                float(row.get('buy_amount', 0)) if pd.notna(row.get('buy_amount')) else 0,
                float(row.get('sell_amount', 0)) if pd.notna(row.get('sell_amount')) else 0,
                existing[0],
            ))
        else:
            cursor.execute("""
                INSERT INTO fund_flows (date, code, name, net_inflow, buy_amount, sell_amount, category)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                date_val, code_val, str(row.get('name', '')),
                float(row.get('net_inflow', 0)) if pd.notna(row.get('net_inflow')) else 0,
                float(row.get('buy_amount', 0)) if pd.notna(row.get('buy_amount')) else 0,
                float(row.get('sell_amount', 0)) if pd.notna(row.get('sell_amount')) else 0,
                cat_val,
            ))
        count += 1

    conn.commit()
    return count
