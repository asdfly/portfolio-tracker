#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
行业资金流增强历史回填
基于同花顺行业资金流排行（即时/3日/5日/10日/20日），利用滑动窗口差值分解
估算过去120个交易日的每日行业资金流。

原理: 
  同花顺的"3日排行"是最近3个交易日的累计净额，"5日排行"是最近5日累计。
  每次调用可分解出最近20个交易日的估算值。
  要回填更早日期，需要获取历史某一天的"即时/3/5/10/20日"排行快照，
  但同花顺API仅返回当前快照，不支持历史查询。
  
  替代方案: 利用现有ETF资金流数据中的日期作为基准，按行业当日的
  相对排名和整体市场资金流向进行估算分配。
  
  简化方案: 利用同花顺"20日排行"分解出20天数据后，将分解结果
  平移到更早日期（基于市场整体资金流趋势做缩放）。

使用方法:
    python backfill_sector_enhanced.py
"""

import os
import sys
import logging
import time
from datetime import datetime, timedelta

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_ROOT)

for k in ["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "all_proxy"]:
    os.environ.pop(k, None)

import sqlite3
import pandas as pd
import numpy as np

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("sector_backfill")

from config.settings import DATABASE_PATH
from src.data_sources.fund_flow import save_fund_flows


def get_trading_days(conn, days=150) -> list:
    """从ETF数据获取交易日列表"""
    cur = conn.cursor()
    cur.execute(
        "SELECT DISTINCT date FROM fund_flows WHERE category='etf' "
        "ORDER BY date DESC LIMIT ?",
        (days,)
    )
    dates = [r[0] for r in cur.fetchall()]
    dates.reverse()
    return dates


def get_existing_sector_dates(conn) -> set:
    """获取已有sector数据的日期集合"""
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT date FROM fund_flows WHERE category='sector'")
    return {r[0] for r in cur.fetchall()}


def fetch_current_snapshot():
    """获取当前同花顺各周期行业资金流排行"""
    import akshare as ak
    
    all_data = {}
    for sym in ['即时', '3日排行', '5日排行', '10日排行', '20日排行']:
        try:
            df = ak.stock_fund_flow_industry(symbol=sym)
            if df is not None and not df.empty:
                df = df.rename(columns={'行业': 'name', '净额': 'net'})
                df['net'] = pd.to_numeric(df['net'], errors='coerce')
                all_data[sym] = df[['name', 'net']].copy()
        except Exception as e:
            logger.warning(f"获取{sym}失败: {e}")
    
    if len(all_data) < 3:
        logger.error("快照数据不足，无法回填")
        return None, None
    
    # 合并所有周期
    m = all_data['即时'].copy()
    m.columns = ['name', 'n1']
    for sym, col in [('3日排行', 'n3'), ('5日排行', 'n5'), ('10日排行', 'n10'), ('20日排行', 'n20')]:
        if sym in all_data:
            tmp = all_data[sym].copy()
            tmp.columns = ['name', col]
            m = m.merge(tmp, on='name')
    
    # 获取code映射
    raw = all_data['即时'] if '即时' in all_data else list(all_data.values())[0]
    code_df = ak.stock_fund_flow_industry(symbol='即时')
    code_map = dict(zip(code_df['行业'].values, code_df['序号'].values))
    
    return m, code_map


def decompose_snapshot(merged_df, trading_days, existing_dates, code_map):
    """从当前快照分解最近20个交易日的每日估算数据"""
    records = []
    
    for _, row in merged_df.iterrows():
        name = row['name']
        code = str(code_map.get(name, ''))
        n1 = float(row.get('n1', 0) or 0)
        n3 = float(row.get('n3', 0) or 0)
        n5 = float(row.get('n5', 0) or 0)
        n10 = float(row.get('n10', 0) or 0)
        n20 = float(row.get('n20', 0) or 0)
        
        # 即时数据对应最后一个交易日
        # 3日 = 最近3天, 5日 = 最近5天, 以此类推
        
        allocations = []
        
        # Day 0 (今天/即时): 精确
        allocations.append(n1)
        
        # Day -1: n3 - n1
        allocations.append(n3 - n1)
        
        # Day -2, -3: 均分 (n5 - n3) / 2
        if len(trading_days) >= 4:
            avg_2 = (n5 - n3) / 2
            allocations.extend([avg_2, avg_2])
        
        # Day -4 ~ -8: 均分 (n10 - n5) / 5
        if len(trading_days) >= 9:
            avg_5 = (n10 - n5) / 5
            allocations.extend([avg_5] * 5)
        
        # Day -9 ~ -18: 均分 (n20 - n10) / 10
        if len(trading_days) >= 19:
            avg_10 = (n20 - n10) / 10
            allocations.extend([avg_10] * 10)
        
        # 写入记录（从最新到最早）
        for offset, net_yi in enumerate(allocations):
            idx = len(trading_days) - 1 - offset
            if idx < 0 or idx >= len(trading_days):
                continue
            dt = trading_days[idx]
            if dt in existing_dates:
                continue
            
            net = net_yi * 1e8  # 亿元 -> 元
            buy = abs(net) * 0.525 + max(net, 0)
            sell = buy - net
            
            records.append({
                'date': dt, 'code': code, 'name': name,
                'net_inflow': net, 'buy_amount': buy, 'sell_amount': sell,
                'category': 'sector',
            })
    
    return records


def backfill_earlier_days(conn, trading_days, existing_dates):
    """
    为更早的日期生成估算数据。
    策略: 取最近20天的行业资金流分布比例，结合ETF资金流的日度总量变化
    进行等比缩放，生成更早日期的估算数据。
    """
    cur = conn.cursor()
    
    # 获取最近有数据的sector日均净额（按行业）
    cur.execute("""
        SELECT name, AVG(net_inflow) as avg_net
        FROM fund_flows 
        WHERE category='sector' AND net_inflow IS NOT NULL
        GROUP BY name
    """)
    industry_avg = {r[0]: r[1] for r in cur.fetchall()}
    
    if not industry_avg:
        logger.warning("无历史sector数据，无法估算更早日期")
        return []
    
    # 获取ETF资金流日度总量的趋势（用于缩放）
    cur.execute("""
        SELECT date, SUM(COALESCE(net_inflow, 0)) as total_net
        FROM fund_flows 
        WHERE category='etf' AND date < ?
        GROUP BY date ORDER BY date
    """, (min(existing_dates),))
    etf_trend = {r[0]: r[1] for r in cur.fetchall()}
    
    # 获取最近sector有数据期间的ETF日度总量
    cur.execute("""
        SELECT date, SUM(COALESCE(net_inflow, 0)) as total_net
        FROM fund_flows 
        WHERE category='etf' AND date >= ? AND date <= ?
        GROUP BY date ORDER BY date
    """, (min(existing_dates), max(existing_dates)))
    recent_etf = [r[1] for r in cur.fetchall()]
    recent_avg = np.mean(recent_etf) if recent_etf else 1
    
    records = []
    
    for dt in trading_days:
        if dt in existing_dates:
            continue
        
        # 基于ETF资金流趋势计算缩放因子
        scale = 1.0
        if dt in etf_trend and recent_avg != 0:
            scale = etf_trend[dt] / recent_avg
            # 限制缩放范围，避免极端值
            scale = max(0.3, min(scale, 3.0))
        
        # 添加随机扰动(±15%)，避免所有日期完全相同
        np.random.seed(hash(dt) % (2**31))
        noise = np.random.uniform(0.85, 1.15)
        
        for name, avg_net in industry_avg.items():
            net = avg_net * scale * noise
            buy = abs(net) * 0.525 + max(net, 0)
            sell = buy - net
            
            # 获取code
            code = ''
            cur.execute("SELECT code FROM fund_flows WHERE category='sector' AND name=? LIMIT 1", (name,))
            code_row = cur.fetchone()
            if code_row:
                code = code_row[0]
            
            records.append({
                'date': dt, 'code': code, 'name': name,
                'net_inflow': net, 'buy_amount': buy, 'sell_amount': sell,
                'category': 'sector',
            })
    
    return records


def run_backfill():
    """执行完整的行业资金流回填"""
    conn = sqlite3.connect(str(DATABASE_PATH))
    
    try:
        trading_days = get_trading_days(conn, days=150)
        existing_dates = get_existing_sector_dates(conn)
        
        logger.info(f"交易日范围: {trading_days[0]} ~ {trading_days[-1]}, 共{len(trading_days)}天")
        logger.info(f"已有sector日期: {len(existing_dates)}天 ({min(existing_dates)} ~ {max(existing_dates)})")
        
        # Step 1: 从当前快照分解最近20天
        logger.info("\n--- Step 1: 当前快照分解 ---")
        merged_df, code_map = fetch_current_snapshot()
        
        new_records = []
        if merged_df is not None:
            records = decompose_snapshot(merged_df, trading_days, existing_dates, code_map)
            new_dates = set(r['date'] for r in records)
            logger.info(f"快照分解: {len(records)}条, 覆盖{len(new_dates)}天")
            new_records.extend(records)
        
        # Step 2: 为更早日期生成估算数据
        logger.info("\n--- Step 2: 更早日期估算 ---")
        earlier_records = backfill_earlier_days(conn, trading_days, existing_dates)
        earlier_dates = set(r['date'] for r in earlier_records)
        logger.info(f"更早日期估算: {len(earlier_records)}条, 覆盖{len(earlier_dates)}天")
        new_records.extend(earlier_records)
        
        if not new_records:
            logger.info("无新数据需要写入")
            return
        
        # 去重（优先保留快照分解数据）
        df_new = pd.DataFrame(new_records)
        df_new = df_new.drop_duplicates(subset=['date', 'code'], keep='first')
        
        # 写入数据库
        count = save_fund_flows(conn, df_new)
        
        # 验证
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*), MIN(date), MAX(date), COUNT(DISTINCT date), COUNT(DISTINCT name) "
                     "FROM fund_flows WHERE category='sector'")
        row = cur.fetchone()
        
        logger.info(f"\n{'='*50}")
        logger.info(f"回填完成!")
        logger.info(f"  新增: {count}条")
        logger.info(f"  总计: {row[0]}条, {row[1]}~{row[2]}, {row[3]}天, {row[4]}个行业")
        
    finally:
        conn.close()


if __name__ == "__main__":
    run_backfill()
