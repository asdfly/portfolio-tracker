#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
新闻历史回填脚本 (v2)
从东方财富搜索接口回填指定日期范围的行业新闻到 daily_news 表。

修复:
  v1 的问题：fetch_all_news() 每次都抓取实时最新新闻，不按日期回溯，
  导致 120 天回填实际上每天都写入相同的一批新闻，被 title 唯一索引去重后
  只保留了最新一天的少量数据。

  v2 的方案：直接调用东方财富搜索 API，用 sort=asc + 翻页逐页获取历史新闻，
  按 publish_time 的日期分组写入数据库。搜索 API 最多返回 100 页×10 条=1000 条，
  不同关键词可回溯到 2025-12 左右。

使用方法:
    python backfill_news.py                    # 回填所有 topic 默认范围
    python backfill_news.py --days 90          # 回填最近90天的新闻
    python backfill_news.py --start 2025-12-01 # 指定起始日期
    python backfill_news.py --topics market,pharma  # 只回填指定 topic
    python backfill_news.py --dry-run          # 试运行
"""

import os
import sys
import argparse
import logging
import re
import json
import time
from datetime import datetime, timedelta, date
from typing import Dict, List, Any, Optional

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


def fetch_historical_news(
    keyword: str,
    start_date: str,
    end_date: str,
    max_pages: int = 100,
    page_size: int = 10,
    delay: float = 0.5,
) -> List[Dict[str, Any]]:
    """
    通过东方财富搜索API翻页获取历史新闻，按 publish_time 过滤日期范围。

    API 限制: sort=asc 时最多翻 100 页，每页 10 条，单次最多 1000 条。
    不同关键词的可回溯范围不同，热门关键词（hits=10000）通常能到 ~3个月前。

    Args:
        keyword: 搜索关键词
        start_date: 起始日期 YYYY-MM-DD
        end_date: 结束日期 YYYY-MM-DD
        max_pages: 最大翻页数（API 上限 100）
        page_size: 每页条数
        delay: 翻页间隔（秒）

    Returns:
        新闻列表 [{title, source, url, summary, publish_time, category}, ...]
    """
    import requests

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    })

    api_url = "https://search-api-web.eastmoney.com/search/jsonp"
    all_items = []
    seen_titles = set()

    for page in range(1, max_pages + 1):
        param_obj = {
            "uid": "",
            "keyword": keyword,
            "type": ["cmsArticleWebOld"],
            "client": "web",
            "clientType": "web",
            "clientVersion": "curr",
            "param": {
                "cmsArticleWebOld": {
                    "searchScope": "default",
                    "sort": "asc",
                    "pageIndex": page,
                    "pageSize": page_size,
                    "preTag": "",
                    "postTag": "",
                }
            },
        }
        params = {"cb": "jQuery_callback", "param": json.dumps(param_obj, ensure_ascii=True)}

        try:
            resp = session.get(api_url, params=params, timeout=10)
            m = re.search(r"jQuery_callback\((.+)\)", resp.text)
            if not m:
                logger.debug(f"  Page {page}: JSONP parse failed")
                break

            data = json.loads(m.group(1))
            articles = data.get("result", {}).get("cmsArticleWebOld", [])

            if not isinstance(articles, list) or len(articles) == 0:
                logger.debug(f"  Page {page}: empty, stop paging")
                break

            page_has_valid = False

            for art in articles:
                title = art.get("title", "").strip()
                if not title or len(title) < 8:
                    continue
                if title in seen_titles:
                    continue

                pub_date_str = art.get("date", "")
                pub_date = _parse_date(pub_date_str)
                if not pub_date:
                    continue

                seen_titles.add(title)

                if pub_date > end_date:
                    # asc排序下不应出现，但防御性跳过
                    continue

                if pub_date < start_date:
                    # 太旧 — 标记但不 break，看本页后面是否还有更新的
                    continue

                page_has_valid = True

                all_items.append({
                    "title": title,
                    "source": art.get("mediaName", "东方财富"),
                    "url": art.get("url", ""),
                    "summary": (art.get("content", "") or "")[:80].strip(),
                    "publish_time": pub_date_str[:16] if len(pub_date_str) >= 16 else pub_date_str,
                    "category": keyword,
                })

            # asc排序下，如果本页没有有效数据，说明所有新闻都太旧了，停止
            if not page_has_valid and articles:
                last_date = _parse_date(articles[-1].get("date", ""))
                first_date = _parse_date(articles[0].get("date", ""))
                if last_date and first_date and last_date < start_date and first_date < start_date:
                    logger.debug(f"  Page {page}: all articles older than {start_date}, stop")
                    break

            if page % 10 == 0:
                logger.info(f"  Page {page}: accumulated {len(all_items)} articles")

            time.sleep(delay)

        except requests.RequestException as e:
            logger.warning(f"  Page {page}: request failed - {e}")
            break
        except Exception as e:
            logger.warning(f"  Page {page}: error - {e}")
            break

    return all_items


def _parse_date(date_str: str) -> Optional[str]:
    """Parse date string to YYYY-MM-DD, return None on failure"""
    if not date_str:
        return None
    date_str = str(date_str).strip()
    try:
        if date_str.isdigit():
            ts = int(date_str)
            if ts > 1e12:
                ts = ts // 1000
            return datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
    except (ValueError, OSError):
        pass
    for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"]:
        try:
            return datetime.strptime(date_str[:19], fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def run_backfill(start_date: str, end_date: str, topic_keys: List[str] = None, dry_run: bool = False):
    """Execute news historical backfill"""
    logger.info(f"Backfill range: {start_date} ~ {end_date}")

    fetcher = NewsFetcher()
    topics = fetcher.NEWS_TOPICS

    if topic_keys:
        topics = [t for t in topics if t["key"] in topic_keys]
        if not topics:
            logger.error(f"No matching topics: {topic_keys}")
            return {"status": "error", "msg": "no matching topics"}

    logger.info(f"Topics: {len(topics)}")
    logger.info(f"Mode: {'DRY-RUN' if dry_run else 'WRITE'}")

    total_by_topic = {}
    total_all = 0

    for i, topic in enumerate(topics):
        key = topic["key"]
        label = topic["label"]
        keyword = topic["keywords"][0]

        logger.info(f"[{i + 1}/{len(topics)}] {label} ({keyword}) ...")

        try:
            items = fetch_historical_news(keyword, start_date, end_date, max_pages=100)
        except Exception as e:
            logger.warning(f"  {label} failed: {e}")
            total_by_topic[key] = 0
            continue

        if not items:
            logger.info(f"  0 articles")
            total_by_topic[key] = 0
            continue

        if dry_run:
            dates_in_items = set(it.get("publish_time", "")[:10] for it in items if it.get("publish_time"))
            logger.info(f"  [DRY-RUN] {len(items)} articles, dates: {min(dates_in_items)} ~ {max(dates_in_items)}")
            total_by_topic[key] = len(items)
            total_all += len(items)
            continue

        # Group by publish date
        by_date: Dict[str, List[Dict]] = {}
        for item in items:
            pub_date = _parse_date(item.get("publish_time", ""))
            if pub_date and start_date <= pub_date <= end_date:
                by_date.setdefault(pub_date, []).append(item)

        # Write day by day
        written = 0
        for dt in sorted(by_date.keys()):
            news_data = {key: {"label": label, "news": by_date[dt]}}
            save_news_to_db(str(DATABASE_PATH), news_data, date_str=dt)
            written += len(by_date[dt])

        logger.info(f"  fetched {len(items)}, written {written} ({len(by_date)} days)")
        total_by_topic[key] = written
        total_all += written
        time.sleep(1.0)

    # Summary
    logger.info("=" * 50)
    logger.info("Backfill complete!")
    logger.info(f"  Total: {total_all} articles")
    for key, cnt in total_by_topic.items():
        t = next((t for t in topics if t["key"] == key), {})
        logger.info(f"    {t.get('label', key):12s}: {cnt}")

    if not dry_run:
        import sqlite3
        conn = sqlite3.connect(str(DATABASE_PATH))
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*), MIN(date), MAX(date), COUNT(DISTINCT date), COUNT(DISTINCT category) "
            "FROM daily_news"
        )
        row = cur.fetchone()
        conn.close()
        logger.info(f"  DB: {row[0]} rows, {row[1]}~{row[2]}, {row[3]} days, {row[4]} categories")

    return {"status": "done", "total": total_all, "by_topic": total_by_topic}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="News historical backfill (v2)")
    parser.add_argument("--days", type=int, default=120, help="Backfill recent N days")
    parser.add_argument("--start", type=str, default=None, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", type=str, default=None, help="End date YYYY-MM-DD")
    parser.add_argument("--topics", type=str, default=None,
                        help="Comma-separated topic keys, e.g. market,pharma,gold")
    parser.add_argument("--dry-run", action="store_true", help="Dry run mode")
    args = parser.parse_args()

    end_date = args.end or date.today().strftime("%Y-%m-%d")
    start_date = args.start or (date.today() - timedelta(days=args.days)).strftime("%Y-%m-%d")
    topic_keys = args.topics.split(",") if args.topics else None

    result = run_backfill(start_date, end_date, topic_keys=topic_keys, dry_run=args.dry_run)
