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
    """获取行业板块资金流向（直接调用东方财富API，绕过AKShare兼容性问题）"""
    try:
        import requests as _req
        _s = _req.Session()
        _s.trust_env = False
        url = 'https://push2.eastmoney.com/api/qt/clist/get'
        params = {
            'pn': '1', 'pz': '500', 'po': '1', 'np': '1',
            'ut': 'b2884a393a59ad64002292a3e90d46a5',
            'fltt': '2', 'invt': '2', 'fid0': 'f62',
            'fs': 'm:90+t:2', 'stat': '1',
            'fields': 'f12,f14,f2,f3,f62,f184,f66,f69,f72,f75,f78,f81,f84,f87',
        }
        r = _s.get(url, params=params, timeout=15)
        data = r.json()
        items = data.get('data', {}).get('diff', [])
        if not items:
            return pd.DataFrame()

        rows = []
        for item in items:
            rows.append({
                'code': str(item.get('f12', '')),
                'name': item.get('f14', ''),
                'change_pct': item.get('f2', 0) or 0,
                'net_inflow': item.get('f62', 0) or 0,
                'net_inflow_pct': item.get('f184', 0) or 0,
                'super_large_inflow': item.get('f66', 0) or 0,
                'large_inflow': item.get('f69', 0) or 0,
                'medium_inflow': item.get('f72', 0) or 0,
                'small_inflow': item.get('f75', 0) or 0,
            })
        df = pd.DataFrame(rows)
        df['category'] = 'sector'
        df['date'] = date_str or datetime.now().strftime('%Y-%m-%d')
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
