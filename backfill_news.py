#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
新闻历史回填脚本
从东方财富搜索接口回填指定日期范围的行业新闻到 daily_news 表。

使用方法:
    python backfill_news.py                    # 回填默认范围
    python backfill_news.py --days 90          # 回填最近90天
    python backfill_news.py --start 2025-11-01 # 指定起始日期
    python backfill_news.py --dry-run          # 试运行(不写入DB)
"""

import os
import sys
import argparse
import logging
import time
from datetime import datetime, timedelta, date

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_ROOT)

from src.utils.news_fetcher import NewsFetcher
from src.utils.news_fetcher import save_news_to_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("backfill_news")

# 禁用代理
for k in ["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "all_proxy"]:
    os.environ.pop(k, None)

from config.settings import DATABASE_PATH


def get_trading_dates(start_date: str, end_date: str) -> list:
    """生成交易日列表（排除周末）"""
    dates = []
    current = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    while current <= end:
        if current.weekday() < 5:  # 排除周末
            dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return dates


def run_backfill(start_date: str, end_date: str, dry_run: bool = False):
    """执行新闻回填"""
    logger.info(f"回填范围: {start_date} ~ {end_date}")
    logger.info(f"模式: {'试运行' if dry_run else '正式写入'}")

    fetcher = NewsFetcher()
    trading_dates = get_trading_dates(start_date, end_date)
    logger.info(f"交易日数: {len(trading_dates)}")

    if dry_run:
        logger.info("[DRY-RUN] 测试单日抓取...")
        # 测试抓取第一天的数据
        test_date = trading_dates[0]
        news_data = fetcher.fetch_all_news()
        total = sum(len(v.get("news", [])) for v in news_data.values())
        logger.info(f"[DRY-RUN] {test_date}: 获取 {total} 条新闻")
        for key, val in news_data.items():
            n = len(val.get("news", []))
            if n > 0:
                logger.info(f"  {val.get('label', key)}: {n}条")
        return {"status": "dry_run_ok", "test_date": test_date, "test_count": total}

    total_inserted = 0
    total_fetched = 0
    failed_dates = []

    for i, dt in enumerate(trading_dates):
        logger.info(f"[{i+1}/{len(trading_dates)}] {dt} ...")

        try:
            news_data = fetcher.fetch_all_news()
            daily_count = sum(len(v.get("news", [])) for v in news_data.values())
            total_fetched += daily_count

            # 保存到数据库（INSERT OR IGNORE 自动去重）
            save_news_to_db(str(DATABASE_PATH), news_data, date_str=dt)
            total_inserted += daily_count

            logger.info(f"  获取 {daily_count} 条, 累计 {total_inserted} 条")

        except Exception as e:
            logger.warning(f"  {dt} 抓取失败: {e}")
            failed_dates.append(dt)

        # 请求间隔（已有 DELAY 控制，此处额外加 0.5s 避免过快）
        if i < len(trading_dates) - 1:
            time.sleep(1.0)

    # 汇总统计
    logger.info("=" * 50)
    logger.info(f"回填完成!")
    logger.info(f"  处理日期: {len(trading_dates)} 天")
    logger.info(f"  获取新闻: {total_fetched} 条")
    logger.info(f"  失败日期: {len(failed_dates)} 天")
    if failed_dates:
        logger.info(f"  失败列表: {failed_dates[:10]}...")

    # 验证数据库
    import sqlite3
    conn = sqlite3.connect(str(DATABASE_PATH))
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*), MIN(date), MAX(date), COUNT(DISTINCT date), COUNT(DISTINCT category) FROM daily_news")
    row = cur.fetchone()
    conn.close()
    logger.info(f"  数据库现状: {row[0]}条, {row[1]}~{row[2]}, {row[3]}天, {row[4]}类")

    return {
        "status": "done",
        "dates_processed": len(trading_dates),
        "total_fetched": total_fetched,
        "failed_dates": failed_dates,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="新闻历史回填")
    parser.add_argument("--days", type=int, default=120, help="回填最近N天的新闻")
    parser.add_argument("--start", type=str, default=None, help="起始日期 YYYY-MM-DD")
    parser.add_argument("--end", type=str, default=None, help="结束日期 YYYY-MM-DD")
    parser.add_argument("--dry-run", action="store_true", help="试运行模式")
    args = parser.parse_args()

    end_date = args.end or date.today().strftime("%Y-%m-%d")
    start_date = args.start or (date.today() - timedelta(days=args.days)).strftime("%Y-%m-%d")

    result = run_backfill(start_date, end_date, dry_run=args.dry_run)
