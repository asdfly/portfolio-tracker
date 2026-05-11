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
import os

logger = logging.getLogger(__name__)

# 模块加载时清除代理环境变量，避免 AKShare 请求被本地代理拦截
for _proxy_key in ['http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY',
                    'all_proxy', 'ALL_PROXY']:
    os.environ.pop(_proxy_key, None)

# Monkey-patch requests.Session: 所有新创建的 Session 默认禁用系统代理
# 这确保 AKShare 内部创建的 Session 也不会走本地代理
import requests as _requests
_OrigSession = _requests.Session

class _NoProxySession(_OrigSession):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.trust_env = False

_requests.Session = _NoProxySession


def get_db_connection() -> sqlite3.Connection:
    from config.settings import DATABASE_PATH
    return sqlite3.connect(str(DATABASE_PATH))


def _no_proxy_session():
    """返回一个禁用系统代理的 requests.Session"""
    import requests
    s = requests.Session()
    s.trust_env = False
    return s


def fetch_sector_fund_flow(date_str: str = None) -> pd.DataFrame:
    """获取行业板块资金流向（使用AKShare stock_fund_flow_industry接口）"""
    try:
        df = ak.stock_fund_flow_industry(symbol='即时')
        if df is None or df.empty:
            return pd.DataFrame()

        # 列名映射（兼容不同版本 AKShare）
        col_map = {
            '序号': 'code',
            '行业': 'name',
            '行业-涨跌幅': 'change_pct',
            '净额': 'net_inflow',
            '流入资金': 'buy_amount',
            '流出资金': 'sell_amount',
        }
        df = df.rename(columns=col_map)

        df['category'] = 'sector'
        df['date'] = date_str or datetime.now().strftime('%Y-%m-%d')

        # 保留目标列
        keep_cols = ['date', 'code', 'name', 'change_pct', 'net_inflow',
                     'buy_amount', 'sell_amount', 'category']
        df = df[[c for c in keep_cols if c in df.columns]]
        return df
    except Exception as e:
        logger.warning(f"获取行业资金流失败: {e}")
    return pd.DataFrame()


def fetch_etf_fund_flow(code: str, name: str = '') -> pd.DataFrame:
    """获取单只ETF资金流向"""
    try:
        market = "sh" if code.startswith('5') or code.startswith('15') or code.startswith('56') or code.startswith('58') else "sz"
        df = ak.stock_individual_fund_flow(stock=code, market=market)
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


def fetch_north_flow(days: int = 60) -> pd.DataFrame:
    """获取北向资金净流入数据（合并沪股通+深股通）"""
    all_dfs = []
    for symbol in ["沪股通", "深股通"]:
        try:
            df = ak.stock_hsgt_hist_em(symbol=symbol)
            if df is not None and not df.empty:
                df = df.rename(columns={
                    '日期': 'date',
                    '当日成交净买额': 'net_inflow',
                    '买入成交额': 'buy_amount',
                    '卖出成交额': 'sell_amount',
                })
                df['code'] = 'north'
                df['name'] = '北向资金'
                df['category'] = 'north'
                df['source'] = symbol
                keep_cols = ['date', 'code', 'name', 'net_inflow',
                             'buy_amount', 'sell_amount', 'category']
                df = df[[c for c in keep_cols if c in df.columns]]
                all_dfs.append(df)
        except Exception as e:
            logger.warning(f"获取{symbol}数据失败: {e}")

    if not all_dfs:
        return pd.DataFrame()

    combined = pd.concat(all_dfs, ignore_index=True)
    # 合并同一日的沪股通+深股通
    agg = combined.groupby('date').agg({
        'code': 'first',
        'name': 'first',
        'net_inflow': 'sum',
        'buy_amount': 'sum',
        'sell_amount': 'sum',
        'category': 'first',
    }).reset_index()

    # 过滤掉全是 NaN 的行
    agg = agg.dropna(subset=['net_inflow', 'buy_amount', 'sell_amount'], how='all')

    # 按日期排序取最近 days 天
    agg = agg.sort_values('date').tail(days).reset_index(drop=True)
    return agg


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
