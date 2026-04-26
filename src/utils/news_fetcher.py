# -*- coding: utf-8 -*-
"""
行业新闻抓取模块 - 通过多关键词搜索获取行业资讯
不依赖外部API，使用requests直接抓取东方财富等公开数据源
"""
import logging
import re
import time
import requests
from typing import Dict, List, Any
from datetime import datetime, date
from dataclasses import dataclass, asdict

logger = logging.getLogger(__name__)


@dataclass
class NewsItem:
    """新闻条目"""
    title: str
    source: str
    url: str
    summary: str
    publish_time: str
    category: str


class NewsFetcher:
    """行业新闻抓取器"""

    # 新闻搜索关键词分组（与灵犀任务对应）
    NEWS_TOPICS = [
        {"key": "market", "keywords": ["A股 市场行情"], "label": "大盘行情"},
        {"key": "pharma", "keywords": ["医药 创新药 行业"], "label": "医药板块"},
        {"key": "broker", "keywords": ["券商 证券 行业"], "label": "券商板块"},
        {"key": "ai", "keywords": ["人工智能 AI 行业"], "label": "AI板块"},
        {"key": "military", "keywords": ["军工 航天 国防"], "label": "军工板块"},
        {"key": "etf", "keywords": ["ETF 基金 市场"], "label": "ETF市场"},
    ]

    # 每个分组取的新闻条数
    MAX_NEWS_PER_TOPIC = 3

    # 请求超时
    TIMEOUT = 8

    # 请求间隔（秒）
    DELAY = 0.5

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        })

    def fetch_all_news(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        抓取所有分组的行业新闻
        返回: {"market": [news_dict, ...], "pharma": [...], ...}
        """
        results = {}
        for topic in self.NEWS_TOPICS:
            key = topic["key"]
            label = topic["label"]
            try:
                news_list = self._fetch_topic_news(topic["keywords"][0])
                results[key] = {
                    "label": label,
                    "news": [asdict(n) for n in news_list[:self.MAX_NEWS_PER_TOPIC]]
                }
                logger.info(f"[新闻] {label}: 获取{len(results[key]['news'])}条")
            except Exception as e:
                logger.warning(f"[新闻] {label} 抓取失败: {e}")
                results[key] = {"label": label, "news": []}

            time.sleep(self.DELAY)

        return results

    def _fetch_topic_news(self, keyword: str) -> List[NewsItem]:
        """
        通过东方财富搜索接口抓取新闻
        """
        items = []

        # 方式1: 东方财富搜索API
        try:
            items = self._fetch_from_eastmoney_search(keyword)
            if items:
                return items
        except Exception as e:
            logger.debug(f"东方财富搜索失败: {e}")

        # 方式2: 东方财富资讯频道
        try:
            items = self._fetch_from_eastmoney_news(keyword)
            if items:
                return items
        except Exception as e:
            logger.debug(f"东方财富资讯失败: {e}")

        return items

    def _fetch_from_eastmoney_search(self, keyword: str) -> List[NewsItem]:
        """通过东方财富搜索接口获取新闻"""
        url = "https://search-api-web.eastmoney.com/search/jsonp"
        import json as _json
        _param_obj = {
            "uid": "",
            "keyword": keyword,
            "type": ["cmsArticleWebOld"],
            "client": "web",
            "clientType": "web",
            "clientVersion": "curr",
            "param": {
                "cmsArticleWebOld": {
                    "searchScope": "default",
                    "sort": "default",
                    "pageIndex": 1,
                    "pageSize": 5,
                    "preTag": "",
                    "postTag": ""
                }
            }
        }
        params = {
            "cb": "jQuery_callback",
            "param": _json.dumps(_param_obj, ensure_ascii=True)
        }

        resp = self.session.get(url, params=params, timeout=self.TIMEOUT)
        # 提取JSONP
        text = resp.text
        json_str = re.search(r"jQuery_callback\((.+)\)", text)
        if not json_str:
            json_str = re.search(r"jQuery_callback\((.+)\)", text.replace("\\", "\\"))
        if not json_str:
            return []

        import json
        data = json.loads(json_str.group(1))
        _articles_raw = data.get("result", {}).get("cmsArticleWebOld", [])
        articles = _articles_raw.get("list", []) if isinstance(_articles_raw, dict) else _articles_raw

        items = []
        for art in articles:
            title = art.get("title", "").strip()
            if not title or len(title) < 8:
                continue
            items.append(NewsItem(
                title=title,
                source=art.get("mediaName", "东方财富"),
                url=art.get("url", ""),
                summary=art.get("content", "")[:80].strip(),
                publish_time=self._format_time(art.get("date", "")),
                category=keyword
            ))

        return items

    def _fetch_from_eastmoney_news(self, keyword: str) -> List[NewsItem]:
        """通过东方财富资讯页面获取新闻（备用）"""
        # 使用东方财富快讯接口
        url = "https://np-listapi.eastmoney.com/comm/web/getNewsByColumns"
        params = {
            "client": "web",
            "biz": "web_news_col",
            "column": "350",
            "order": "1",
            "page_index": "1",
            "page_size": "10",
        }

        resp = self.session.get(url, params=params, timeout=self.TIMEOUT)
        if resp.status_code != 200:
            return []

        import json
        data = resp.json()
        news_list = data.get("data", {}).get("list", [])

        items = []
        for n in news_list:
            title = n.get("title", "").strip()
            if not title or len(title) < 8:
                continue
            # 简单关键词过滤
            if keyword and not any(kw in title for kw in keyword.split()):
                continue
            items.append(NewsItem(
                title=title,
                source=n.get("source", "东方财富"),
                url=n.get("url", n.get("wap_url", "")),
                summary=n.get("digest", n.get("summary", ""))[:80].strip(),
                publish_time=self._format_time(n.get("showtime", "")),
                category=keyword
            ))
            if len(items) >= self.MAX_NEWS_PER_TOPIC:
                break

        return items

    def _format_time(self, time_str: str) -> str:
        """格式化时间字符串"""
        if not time_str:
            return ""
        time_str = str(time_str)
        # 处理时间戳（秒或毫秒）
        try:
            if time_str.isdigit():
                ts = int(time_str)
                if ts > 1e12:
                    ts = ts // 1000
                return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")
        except (ValueError, OSError):
            pass
        # 处理标准格式
        for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%m-%d %H:%M"]:
            try:
                return datetime.strptime(time_str, fmt).strftime("%Y-%m-%d %H:%M")
            except ValueError:
                continue
        return time_str[:16]

    def analyze_news_impact(self, news_data: Dict[str, List[Dict]], 
                           positions: List[Dict]) -> List[Dict[str, Any]]:
        """
        分析新闻对持仓的影响
        返回影响评估列表
        """
        impacts = []

        # 持仓行业关键词映射
        industry_keywords = {
            "医药": ["医药", "创新药", "生物", "制药", "医疗"],
            "券商": ["券商", "证券", "资本", "金融"],
            "AI": ["人工智能", "AI", "大模型", "芯片", "算力", "GPU"],
            "军工": ["军工", "航天", "国防", "航空", "导弹"],
            "新能源": ["新能源", "电池", "锂电", "光伏", "储能"],
            "科技": ["科技", "半导体", "集成电路", "电子"],
            "消费": ["消费", "零售", "白酒", "食品"],
            "红利": ["红利", "高股息", "债券", "利率"],
        }

        # 持仓所属行业判断（基于名称关键词）
        pos_industries = []
        for pos in positions:
            name = pos.get("name", "")
            pos_ind = "其他"
            for ind, kws in industry_keywords.items():
                if any(kw in name for kw in kws):
                    pos_ind = ind
                    break
            pos_industries.append((pos["name"], pos_ind))

        # 遍历新闻，评估影响
        for topic_key, topic_data in news_data.items():
            topic_label = topic_data.get("label", "")
            for news_item in topic_data.get("news", []):
                title = news_item.get("title", "")
                # 判断新闻情感倾向
                positive_words = ["上涨", "反弹", "利好", "增长", "突破", "新高", "提振", "回暖", "强势", "超预期"]
                negative_words = ["下跌", "暴跌", "利空", "下滑", "风险", "回调", "疲软", "收紧", "制裁", "低迷"]

                pos_score = sum(1 for w in positive_words if w in title)
                neg_score = sum(1 for w in negative_words if w in title)

                if pos_score == 0 and neg_score == 0:
                    sentiment = "neutral"
                elif pos_score > neg_score:
                    sentiment = "positive"
                else:
                    sentiment = "negative"

                # 关联受影响的持仓
                affected = []
                for pos_name, pos_ind in pos_industries:
                    if any(kw in title for kw in industry_keywords.get(pos_ind, [])):
                        affected.append(pos_name)

                if affected or sentiment != "neutral":
                    impacts.append({
                        "title": title,
                        "source": news_item.get("source", ""),
                        "sentiment": sentiment,
                        "affected_positions": affected[:3],
                        "topic": topic_label,
                        "url": news_item.get("url", "")
                    })

        # 按影响程度排序（有受影响持仓的排前面）
        impacts.sort(key=lambda x: (len(x["affected_positions"]) > 0, x["sentiment"] != "neutral"), reverse=True)

        return impacts[:10]

    def generate_rotation_analysis(self, positions: List[Dict], 
                                   index_quotes: Dict[str, Dict]) -> Dict[str, Any]:
        """
        行业轮动分析 - 基于持仓日涨跌和指数表现
        """
        rotation = {
            "leaders": [],    # 今日领涨
            "laggards": [],   # 今日领跌
            "sector_performance": [],  # 板块表现排名
            "trend": ""       # 资金流向趋势
        }

        # 按日涨跌幅排序持仓
        sorted_pos = sorted(positions, 
                           key=lambda x: x.get("realtime_change_pct", x.get("daily_change_pct", 0)), 
                           reverse=True)

        # 领涨TOP5
        for pos in sorted_pos[:5]:
            chg = pos.get("realtime_change_pct", pos.get("daily_change_pct", 0))
            rotation["leaders"].append({
                "name": pos["name"],
                "code": pos["code"],
                "change_pct": round(chg, 2),
                "market_value": pos.get("realtime_market_value", pos.get("market_value", 0))
            })

        # 领跌TOP5
        for pos in sorted_pos[-5:]:
            chg = pos.get("realtime_change_pct", pos.get("daily_change_pct", 0))
            rotation["laggards"].append({
                "name": pos["name"],
                "code": pos["code"],
                "change_pct": round(chg, 2),
                "market_value": pos.get("realtime_market_value", pos.get("market_value", 0))
            })

        # 指数表现
        for code, name in [
            ("sh000300", "沪深300"), ("sz399006", "创业板指"), 
            ("sh000905", "中证500"), ("sz399987", "中证酒"),
            ("sh000688", "科创50"), ("sh000015", "红利指数")
        ]:
            quote = index_quotes.get(code, {})
            chg = quote.get("change_pct", 0)
            if chg:
                rotation["sector_performance"].append({
                    "name": name,
                    "change_pct": round(chg, 2)
                })

        rotation["sector_performance"].sort(key=lambda x: x["change_pct"], reverse=True)

        # 资金流向趋势判断
        if rotation["sector_performance"]:
            avg_change = sum(s["change_pct"] for s in rotation["sector_performance"]) / len(rotation["sector_performance"])
            positive_count = len([s for s in rotation["sector_performance"] if s["change_pct"] > 0])
            total = len(rotation["sector_performance"])

            if avg_change > 1.0 and positive_count / total > 0.6:
                rotation["trend"] = "市场整体偏强，多数板块上涨，资金呈流入态势"
            elif avg_change < -1.0 and positive_count / total < 0.4:
                rotation["trend"] = "市场整体偏弱，多数板块下跌，资金呈流出态势"
            elif positive_count / total > 0.5:
                rotation["trend"] = "市场分化，部分板块表现活跃"
            else:
                rotation["trend"] = "市场震荡，板块涨跌互现"

        return rotation


def save_news_to_db(db_path: str, news_data: Dict[str, Any], date_str: str = None):
    """
    将新闻数据保存到数据库
    """
    import sqlite3
    if date_str is None:
        date_str = date.today().strftime("%Y-%m-%d")

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 创建新闻表（如果不存在）
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_news (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            category TEXT,
            title TEXT,
            source TEXT,
            url TEXT,
            summary TEXT,
            publish_time TEXT,
            created_at TEXT
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_news_date ON daily_news(date)")

    # 保存新闻
    for topic_key, topic_data in news_data.items():
        label = topic_data.get("label", topic_key)
        for news in topic_data.get("news", []):
            cursor.execute("""
                INSERT OR REPLACE INTO daily_news (date, category, title, source, url, summary, publish_time, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                date_str, label, news.get("title", ""), news.get("source", ""),
                news.get("url", ""), news.get("summary", ""), news.get("publish_time", ""),
                datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            ))

    conn.commit()
    conn.close()
    logger.info(f"新闻数据已保存: {date_str}")


def load_news_from_db(db_path: str, date_str: str = None) -> Dict[str, Any]:
    """
    从数据库加载新闻数据
    """
    import sqlite3
    if date_str is None:
        date_str = date.today().strftime("%Y-%m-%d")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("""
        SELECT category, title, source, url, summary, publish_time 
        FROM daily_news WHERE date = ? ORDER BY category, id
    """, (date_str,))

    rows = cursor.fetchall()
    conn.close()

    # 按分类分组
    result = {}
    for row in rows:
        cat = row["category"]
        if cat not in result:
            result[cat] = {"label": cat, "news": []}
        result[cat]["news"].append({
            "title": row["title"],
            "source": row["source"],
            "url": row["url"],
            "summary": row["summary"],
            "publish_time": row["publish_time"]
        })

    return result
