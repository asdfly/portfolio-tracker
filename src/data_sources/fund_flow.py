"""
资金流数据采集模块
使用 AKShare / 东方财富 datacenter-web 采集行业资金流、ETF资金流、主力资金数据。
"""

import pandas as pd
import sqlite3
from datetime import datetime
from typing import Optional
import logging
import os
import urllib.request
import json
import time
import requests as _requests

logger = logging.getLogger(__name__)

for _proxy_key in ['http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY', 'all_proxy', 'ALL_PROXY']:
    os.environ.pop(_proxy_key, None)

# 禁用 requests 代理。
# 使用 deepcopy 保存原始 __init__ 的完整副本，避免在共享 Python 进程中
# （如 akshare 可能也 patch Session）形成递归调用链。
import copy
_OriginalSessionInit = copy.deepcopy(_requests.Session.__init__)

def _NoProxySessionInit(self, *args, **kwargs):
    _OriginalSessionInit(self, *args, **kwargs)
    self.trust_env = False

_requests.Session.__init__ = _NoProxySessionInit

def get_db_connection() -> sqlite3.Connection:
    from config.settings import DATABASE_PATH
    return sqlite3.connect(str(DATABASE_PATH))

def _urllib_get_json(url, retries=2, delay=1.0):
    proxy_handler = urllib.request.ProxyHandler({})
    opener = urllib.request.build_opener(proxy_handler)
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            resp = opener.open(req, timeout=15)
            return json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                logger.warning(f"urllib GET failed {url}: {e}")
                return None

def fetch_sector_fund_flow(date_str=None) -> pd.DataFrame:
    try:
        import akshare as ak
        df = ak.stock_fund_flow_industry(symbol='即时')
        if df is None or df.empty:
            return pd.DataFrame()
        col_map = {
            '序号': 'code', '行业': 'name', '行业-涨跌幅': 'change_pct',
            '净额': 'net_inflow', '流入资金': 'buy_amount', '流出资金': 'sell_amount',
        }
        df = df.rename(columns=col_map)
        # 同花顺原始单位为亿元，数据库约定统一为元 → ×1e8
        for _col in ['net_inflow', 'buy_amount', 'sell_amount']:
            if _col in df.columns:
                df[_col] = df[_col].apply(lambda v: float(v) * 1e8 if pd.notna(v) else None)
        df['category'] = 'sector'
        df['date'] = date_str or datetime.now().strftime('%Y-%m-%d')
        keep_cols = ['date', 'code', 'name', 'change_pct', 'net_inflow', 'buy_amount', 'sell_amount', 'category']
        df = df[[c for c in keep_cols if c in df.columns]]
        return df
    except Exception as e:
        logger.warning(f"获取行业资金流失败: {e}")
    return pd.DataFrame()

def fetch_etf_fund_flow(code: str, name: str = '') -> pd.DataFrame:
    try:
        import akshare as ak
        market = "sh" if code.startswith('5') or code.startswith('15') or code.startswith('56') or code.startswith('58') else "sz"
        df = ak.stock_individual_fund_flow(stock=code, market=market)
        if df is not None and not df.empty:
            df = df.rename(columns={
                '日期': 'date', '收盘价': 'close', '涨跌幅': 'change_pct',
                '主力净流入-净额': 'net_inflow', '主力净流入-净占比': 'net_inflow_pct',
                '超大单净流入-净额': 'super_large_inflow', '大单净流入-净额': 'large_inflow',
                '中单净流入-净额': 'medium_inflow', '小单净流入-净额': 'small_inflow',
            })
            df['code'] = code
            df['name'] = name
            df['category'] = 'etf'
            keep_cols = ['date', 'code', 'name', 'close', 'change_pct', 'net_inflow', 'net_inflow_pct', 'category']
            df = df[[c for c in keep_cols if c in df.columns]]
            return df
    except Exception as e:
        logger.warning(f"获取ETF {code} 资金流失败: {e}")
    return pd.DataFrame()

def fetch_main_fund_flow(days: int = 120) -> pd.DataFrame:
    """获取A股大盘主力资金净流入数据。
    方案A: push2his urllib 直连（完整数据，含超大单/大单/中单/小单细分）
    方案B: 同花顺行业资金流聚合（仅当日快照，无细分，作为 fallback）
    """
    # --- 方案A: push2his ---
    try:
        url = (
            "https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get"
            "?lmt=0&klt=101&secid=1.000001&secid2=0.399001"
            "&fields1=f1,f2,f3,f7&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64,f65"
            f"&ut=b2884a393a59ad64002292a3e90d46a5&_={int(time.time()*1000)}"
        )
        data = _urllib_get_json(url)
        if data and data.get("data", {}).get("klines"):
            klines = data["data"]["klines"]
            df = pd.DataFrame([item.split(",") for item in klines])
            df.columns = [
                "date", "main_net", "small_net", "medium_net",
                "large_net", "super_large_net", "main_pct",
                "small_pct", "medium_pct", "large_pct",
                "super_large_pct", "_sh", "_shc", "_sz", "_szc",
            ]
            for col in df.columns[1:]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            df = df.rename(columns={
                'main_net': 'net_inflow', 'main_pct': 'net_inflow_pct',
                'super_large_net': 'super_large_inflow', 'super_large_pct': 'super_large_pct',
                'large_net': 'large_inflow', 'large_pct': 'large_pct',
                'medium_net': 'medium_inflow', 'medium_pct': 'medium_pct',
                'small_net': 'small_inflow', 'small_pct': 'small_pct',
            })
            df['code'] = 'main_fund'
            df['name'] = '主力资金'
            df['category'] = 'main_fund'
            keep = ['date','code','name','net_inflow','net_inflow_pct',
                     'super_large_inflow','super_large_pct','large_inflow','large_pct',
                     'medium_inflow','medium_pct','small_inflow','small_pct','category']
            df = df[[c for c in keep if c in df.columns]]
            df = df.dropna(subset=['net_inflow']).sort_values('date').tail(days).reset_index(drop=True)
            logger.info(f"主力资金(push2his): {len(df)} 条")
            return df
    except Exception as e:
        logger.debug(f"push2his 方案失败: {e}")

    # --- 方案B: 同花顺行业资金流聚合 ---
    logger.info("push2his 不可用, 回退到同花顺行业资金流聚合")
    try:
        import akshare as ak
        df = ak.stock_fund_flow_industry(symbol='即时')
    except Exception as e:
        logger.warning(f"同花顺也失败: {e}")
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()
    # 同花顺原始单位为亿元，数据库约定统一为元 -> x1e8
    tb = pd.to_numeric(df.get('流入资金',0), errors='coerce').sum() * 1e8
    ts = pd.to_numeric(df.get('流出资金',0), errors='coerce').sum() * 1e8
    tn = pd.to_numeric(df.get('净额',0), errors='coerce').sum() * 1e8
    np_ = round(tn/tb*100,2) if tb > 0 else 0.0
    today = datetime.now().strftime('%Y-%m-%d')
    result = pd.DataFrame([{
        'date': today, 'code': 'main_fund', 'name': '主力资金',
        'net_inflow': round(tn,2), 'net_inflow_pct': np_,
        'buy_amount': round(tb,2), 'sell_amount': round(ts,2),
        'super_large_inflow': 0.0, 'super_large_pct': 0.0,
        'large_inflow': 0.0, 'large_pct': 0.0,
        'medium_inflow': 0.0, 'medium_pct': 0.0,
        'small_inflow': round(-tn,2), 'small_pct': 0.0,
        'category': 'main_fund',
    }])
    logger.info(f"主力资金(同花顺聚合): date={today}, 净流入={tn/1e8:.2f}亿 -> {tn:.0f}元")
    return result


def fetch_north_flow(days: int = 60) -> pd.DataFrame:
    """获取北向资金（2024-08-19起停更，保留以备恢复）。"""
    all_dfs = []
    for symbol in ["沪股通", "深股通"]:
        try:
            import akshare as ak
            df = ak.stock_hsgt_hist_em(symbol=symbol)
            if df is not None and not df.empty:
                df = df.rename(columns={'日期': 'date', '当日成交净买额': 'net_inflow', '买入成交额': 'buy_amount', '卖出成交额': 'sell_amount'})
                df['code'] = 'north'; df['name'] = '北向资金'; df['category'] = 'north'
                keep_cols = ['date', 'code', 'name', 'net_inflow', 'buy_amount', 'sell_amount', 'category']
                df = df[[c for c in keep_cols if c in df.columns]]
                all_dfs.append(df)
        except Exception as e:
            logger.warning(f"获取{symbol}数据失败: {e}")
    if not all_dfs:
        return pd.DataFrame()
    combined = pd.concat(all_dfs, ignore_index=True)
    agg = combined.groupby('date').agg({'code': 'first', 'name': 'first', 'net_inflow': 'sum', 'buy_amount': 'sum', 'sell_amount': 'sum', 'category': 'first'}).reset_index()
    agg = agg.dropna(subset=['net_inflow', 'buy_amount', 'sell_amount'], how='all')
    agg = agg.sort_values('date').tail(days).reset_index(drop=True)
    return agg

def save_fund_flows(conn: sqlite3.Connection, df: pd.DataFrame):
    """保存资金流数据到数据库（upsert），支持扩展列"""
    if df.empty:
        return 0
    for col in ['date', 'category']:
        if col not in df.columns:
            return 0
    extra_cols = ['net_inflow_pct', 'super_large_inflow', 'super_large_pct',
                  'large_inflow', 'large_pct', 'medium_inflow', 'medium_pct', 'small_inflow', 'small_pct']
    cursor = conn.cursor()
    count = 0
    for _, row in df.iterrows():
        date_val = str(row.get('date', ''))
        code_val = str(row.get('code', ''))
        cat_val = str(row.get('category', ''))
        def _float(v):
            return float(v) if pd.notna(v) else None
        cursor.execute("SELECT id FROM fund_flows WHERE date = ? AND code = ? AND category = ?", (date_val, code_val, cat_val))
        existing = cursor.fetchone()
        base_vals = (str(row.get('name', '')), _float(row.get('net_inflow')), _float(row.get('buy_amount')), _float(row.get('sell_amount')))
        if existing:
            set_parts = ["name=?", "net_inflow=?", "buy_amount=?", "sell_amount=?"]
            vals = list(base_vals)
            for ec in extra_cols:
                if ec in row.index:
                    set_parts.append(f"{ec}=?")
                    vals.append(_float(row.get(ec)))
            vals.append(existing[0])
            cursor.execute(f"UPDATE fund_flows SET {', '.join(set_parts)} WHERE id=?", vals)
        else:
            ins_cols = ['date', 'code', 'name', 'net_inflow', 'buy_amount', 'sell_amount', 'category']
            ins_vals = [date_val, code_val, str(row.get('name', '')), _float(row.get('net_inflow')), _float(row.get('buy_amount')), _float(row.get('sell_amount')), cat_val]
            placeholders = '?,?,?,?,?,?,?'
            for ec in extra_cols:
                if ec in row.index:
                    ins_cols.append(ec)
                    ins_vals.append(_float(row.get(ec)))
                    placeholders += ',?'
            cursor.execute(f"INSERT INTO fund_flows ({', '.join(ins_cols)}) VALUES ({placeholders})", ins_vals)
        count += 1
    conn.commit()
    return count
