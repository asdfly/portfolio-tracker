"""券商研报与行业观点聚合模块

基于 daily_news 表的关键词匹配，聚合持仓 ETF 对应行业的新闻/研报观点。
预留 AKShare 研报 API 接口（stock_research_report_em 恢复后可激活）。
"""

import re
import pandas as pd
import sqlite3
from typing import Dict, List, Optional
from datetime import datetime, timedelta

from src.utils.database import get_db_connection


# ETF 代码到行业关键词的映射
ETF_INDUSTRY_KEYWORDS = {
    "510300": ["沪深300", "大盘", "蓝筹", "上证50", "A股"],
    "159300": ["沪深300", "大盘", "蓝筹", "A股"],
    "510500": ["中证500", "中盘", "中小盘", "成长"],
    "512100": ["中证1000", "小盘", "中小盘", "微盘"],
    "159949": ["创业板", "创业板50", "成长", "科技"],
    "588000": ["科创板", "科创50", "半导体", "硬科技", "芯片"],
    "512010": ["医药", "创新药", "医疗", "生物医药", "CXO", "中药"],
    "159992": ["创新药", "医药", "生物医药", "CXO"],
    "515120": ["医药", "医疗", "生物医药", "中药"],
    "515010": ["金融", "银行", "券商", "保险"],
    "512810": ["军工", "国防", "航天", "航空", "军工"],
    "159267": ["军工", "国防", "航天", "航空", "卫星"],
    "516160": ["新能源", "光伏", "风电", "碳中和", "储能"],
    "561910": ["新能源", "光伏", "储能", "锂电"],
    "159796": ["新能源", "电池", "锂电", "储能", "动力电池"],
    "159819": ["人工智能", "AI", "大模型", "ChatGPT", "机器人"],
    "159770": ["机器人", "智能制造", "自动化", "工业4.0"],
    "159732": ["消费电子", "半导体", "芯片", "苹果产业链", "华为"],
    "159220": ["红利", "高股息", "港股", "央企"],
    "563020": ["红利", "高股息", "国企", "央企"],
    "511520": ["债券", "国债", "利率", "债市", "固收"],
    "159650": ["债券", "国开债", "利率债", "债市"],
    "511380": ["债券", "信用债", "公司债", "债市"],
}

# 行业分类到通用关键词
SECTOR_KEYWORDS = {
    "军工": ["军工", "国防", "航天", "航空", "导弹", "军舰", "雷达"],
    "医药": ["医药", "医疗", "创新药", "生物医药", "CXO", "中药", "仿制药", "集采"],
    "金融": ["银行", "券商", "保险", "金融", "信贷", "利率"],
    "新能源": ["新能源", "光伏", "风电", "碳中和", "储能", "锂电"],
    "科技": ["人工智能", "AI", "大模型", "芯片", "半导体", "机器人", "智能制造"],
    "红利": ["红利", "高股息", "国企", "央企", "分红"],
    "债券": ["债券", "国债", "利率债", "信用债", "债市", "固收"],
}


def _match_news_to_keywords(title: str, keywords: List[str]) -> int:
    """计算标题与关键词列表的匹配度。

    Returns
    -------
    int : 匹配的关键词数量
    """
    if not title or pd.isna(title):
        return 0
    count = 0
    for kw in keywords:
        if kw in title:
            count += 1
    return count


def load_etf_industry_news(code: str, days: int = 30) -> pd.DataFrame:
    """加载与 ETF 相关的行业新闻/观点。

    Parameters
    ----------
    code : str
        ETF 代码
    days : int
        回溯天数

    Returns
    -------
    pd.DataFrame : 匹配的新闻列表，按相关度排序
    """
    keywords = ETF_INDUSTRY_KEYWORDS.get(code, [])
    if not keywords:
        # 回退到 sector 关键词
        sector = _get_etf_sector(code)
        if sector:
            keywords = SECTOR_KEYWORDS.get(sector, [])
    if not keywords:
        return pd.DataFrame()

    conn = get_db_connection()
    try:
        cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        df = pd.read_sql_query(
            "SELECT id, date, title, source, summary, sentiment_score "
            "FROM daily_news WHERE date >= ? ORDER BY date DESC",
            conn, params=(cutoff,)
        )
        if df.empty:
            return pd.DataFrame()

        # 匹配关键词
        df["relevance"] = df["title"].apply(
            lambda t: _match_news_to_keywords(str(t), keywords)
        )
        matched = df[df["relevance"] > 0].sort_values(
            ["relevance", "date"], ascending=[False, False]
        )
        return matched.head(20)
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()


def _get_etf_sector(code: str) -> str:
    """获取 ETF 的行业分类。"""
    try:
        from config.settings import ETF_CATEGORIES
        info = ETF_CATEGORIES.get(str(code), {})
        return info.get("sector", "")
    except Exception:
        return ""


def load_sector_sentiment(code: str, days: int = 30) -> Dict:
    """计算 ETF 对应行业的新闻情绪。

    Parameters
    ----------
    code : str
        ETF 代码
    days : int
        回溯天数

    Returns
    -------
    dict : {avg_sentiment, positive_count, negative_count, neutral_count, news_count, top_headlines}
    """
    news_df = load_etf_industry_news(code, days)
    if news_df.empty:
        return {
            "avg_sentiment": 0,
            "positive_count": 0,
            "negative_count": 0,
            "neutral_count": 0,
            "news_count": 0,
            "top_headlines": [],
        }

    sentiments = news_df["sentiment_score"].dropna()
    if sentiments.empty:
        avg = 0
        pos = neg = neu = 0
    else:
        avg = round(float(sentiments.mean()), 2)
        pos = int((sentiments > 0.1).sum())
        neg = int((sentiments < -0.1).sum())
        neu = int(len(sentiments) - pos - neg)

    top_headlines = news_df.head(5)[["date", "title", "source", "sentiment_score"]].to_dict("records")

    return {
        "avg_sentiment": avg,
        "positive_count": pos,
        "negative_count": neg,
        "neutral_count": neu,
        "news_count": len(news_df),
        "top_headlines": top_headlines,
    }


# === 预留研报 API 接口 ===
def fetch_research_reports(industry: str = "", limit: int = 50) -> pd.DataFrame:
    """从 AKShare 获取券商研报列表（预留接口）。

    当前 AKShare stock_research_report_em 存在 API 问题，
    待修复后激活此函数。

    Parameters
    ----------
    industry : str
        行业关键词过滤
    limit : int
        返回条数

    Returns
    -------
    pd.DataFrame : 研报列表
    """
    try:
        import akshare as ak
        df = ak.stock_research_report_em(symbol="行业")
        if df.empty:
            return pd.DataFrame()
        if industry:
            mask = df.apply(lambda r: industry in str(r.values), axis=1)
            df = df[mask]
        return df.head(limit)
    except Exception:
        return pd.DataFrame()
