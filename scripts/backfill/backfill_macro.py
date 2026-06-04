#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
宏观数据历史回填脚本
一次性回填 COMEX黄金、上海金、Shibor、USD/CNY、中美国债收益率、两融余额的历史数据。

使用方法:
    python backfill_macro.py                # 回填全部指标
    python backfill_macro.py --dry-run      # 试运行
"""

import os
import sys
import argparse
import logging
import time
from datetime import datetime

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_ROOT)

for k in ["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "all_proxy"]:
    os.environ.pop(k, None)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("backfill_macro")

from src.data_sources.macro_daily import (
    fetch_comex_gold, fetch_sge_gold, fetch_shibor,
    fetch_usd_cny, fetch_bond_yields, fetch_lpr, fetch_margin_balance,
    save_macro_daily, save_market_sentiment, get_db_connection,
)


def run_backfill(dry_run: bool = False):
    """执行宏观数据历史回填"""
    conn = get_db_connection()
    stats = {}
    total = 0
    start_time = time.time()
    
    # === COMEX黄金 (完整历史 ~2500条) ===
    logger.info("=" * 50)
    logger.info("[1/6] COMEX黄金历史回填...")
    try:
        records = fetch_comex_gold(days=5000)  # 获取全部
        logger.info(f"  COMEX_GOLD: {len(records)}条")
        if not dry_run:
            count = save_macro_daily(conn, records)
            stats['COMEX_GOLD'] = count
            total += count
            logger.info(f"  已保存: {count}条")
    except Exception as e:
        logger.error(f"  COMEX黄金回填失败: {e}")
    
    time.sleep(1)
    
    # === 上海金基准价 (完整历史 ~2400条) ===
    logger.info("[2/6] 上海金基准价历史回填...")
    try:
        records = fetch_sge_gold(days=5000)
        logger.info(f"  SGE_GOLD: {len(records)}条")
        if not dry_run:
            count = save_macro_daily(conn, records)
            stats['SGE_GOLD'] = count
            total += count
            logger.info(f"  已保存: {count}条")
    except Exception as e:
        logger.error(f"  上海金回填失败: {e}")
    
    time.sleep(1)
    
    # === Shibor (2017年起 ~2200条) ===
    logger.info("[3/6] Shibor历史回填...")
    try:
        records = fetch_shibor(days=5000)
        logger.info(f"  SHIBOR_ON: {len(records)}条")
        if not dry_run:
            count = save_macro_daily(conn, records)
            stats['SHIBOR_ON'] = count
            total += count
            logger.info(f"  已保存: {count}条")
    except Exception as e:
        logger.error(f"  Shibor回填失败: {e}")
    
    time.sleep(1)
    
    # === USD/CNY 汇率 (最近365天) ===
    logger.info("[4/6] USD/CNY汇率历史回填...")
    try:
        # currency_boc_sina 支持指定起止日期
        from datetime import timedelta
        start_dt = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
        end_dt = datetime.now().strftime('%Y%m%d')
        import akshare as ak
        df = ak.currency_boc_sina(symbol='美元', start_date=start_dt, end_date=end_dt)
        records = []
        if df is not None and not df.empty:
            for i, (_, row) in enumerate(df.iterrows()):
                dt = str(row['日期'])
                if hasattr(row['日期'], 'strftime'):
                    dt = row['日期'].strftime('%Y-%m-%d')
                mid = row.get('央行中间价')
                if pd_isna(mid):
                    mid = (float(row.get('中行汇买价', 0)) + float(row.get('中行钞卖价/汇卖价', 0))) / 2
                value = float(mid) / 100 if mid and float(mid) > 100 else float(mid)
                change_pct = None
                if i > 0:
                    prev_mid = df.iloc[i-1].get('央行中间价')
                    if pd_isna(prev_mid):
                        prev_mid = (float(df.iloc[i-1].get('中行汇买价', 0)) + float(df.iloc[i-1].get('中行钞卖价/汇卖价', 0))) / 2
                    prev_val = float(prev_mid) / 100 if prev_mid and float(prev_mid) > 100 else float(prev_mid)
                    if prev_val > 0:
                        change_pct = round((value / prev_val - 1) * 100, 4)
                records.append({
                    'date': dt, 'indicator_code': 'USD_CNY', 'name': '美元兑人民币',
                    'value': round(value, 4), 'change_pct': change_pct, 'source': 'BOC_SINA',
                })
        logger.info(f"  USD_CNY: {len(records)}条")
        if not dry_run:
            count = save_macro_daily(conn, records)
            stats['USD_CNY'] = count
            total += count
            logger.info(f"  已保存: {count}条")
    except Exception as e:
        logger.error(f"  USD/CNY回填失败: {e}")
    
    time.sleep(1)
    
    # === 中美国债收益率 (最近365天) ===
    logger.info("[5/6] 中美国债收益率历史回填...")
    try:
        start_dt = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
        import akshare as ak
        df = ak.bond_zh_us_rate(start_date=start_dt)
        records = []
        if df is not None and not df.empty:
            for _, row in df.iterrows():
                dt = str(row['日期'])
                if hasattr(row['日期'], 'strftime'):
                    dt = row['日期'].strftime('%Y-%m-%d')
                indicators = [
                    ('CN_10Y_BOND', '中国10Y国债', '中国国债收益率10年'),
                    ('CN_2Y_BOND', '中国2Y国债', '中国国债收益率2年'),
                    ('US_10Y_BOND', '美国10Y国债', '美国国债收益率10年'),
                    ('US_2Y_BOND', '美国2Y国债', '美国国债收益率2年'),
                    ('CN_US_SPREAD', '中美利差(10Y)', None),
                ]
                for code, name, col in indicators:
                    if code == 'CN_US_SPREAD':
                        cn10 = row.get('中国国债收益率10年')
                        us10 = row.get('美国国债收益率10年')
                        if pd_isna(cn10) or pd_isna(us10):
                            continue
                        value = round(float(cn10) - float(us10), 4)
                    else:
                        value = row.get(col)
                        if pd_isna(value):
                            continue
                        value = round(float(value), 4)
                    records.append({
                        'date': dt, 'indicator_code': code, 'name': name,
                        'value': value, 'change_pct': None, 'source': 'EASTMONEY',
                    })
        logger.info(f"  BOND_YIELDS: {len(records)}条")
        if not dry_run:
            count = save_macro_daily(conn, records)
            stats['BOND_YIELDS'] = count
            total += count
            logger.info(f"  已保存: {count}条")
    except Exception as e:
        logger.error(f"  国债收益率回填失败: {e}")
    
    time.sleep(1)
    
    # === 两融余额 (完整历史 ~3900条) ===
    logger.info("[6/6] 两融余额历史回填...")
    try:
        records = fetch_margin_balance(days=5000)
        logger.info(f"  MARGIN: {len(records)}条")
        if not dry_run:
            count = save_market_sentiment(conn, records)
            stats['MARGIN'] = count
            total += count
            logger.info(f"  已保存: {count}条")
    except Exception as e:
        logger.error(f"  两融余额回填失败: {e}")
    
    conn.close()
    
    # 汇总
    elapsed = time.time() - start_time
    logger.info("=" * 50)
    logger.info(f"回填完成! 耗时 {elapsed:.1f}s, 总计 {total}条")
    for k, v in stats.items():
        logger.info(f"  {k}: {v}条")
    
    return stats


def pd_isna(val):
    """pandas isna 检查"""
    try:
        import pandas as pd
        return pd.isna(val)
    except:
        return val is None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="宏观数据历史回填")
    parser.add_argument("--dry-run", action="store_true", help="试运行模式")
    args = parser.parse_args()
    run_backfill(dry_run=args.dry_run)
