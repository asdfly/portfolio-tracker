# -*- coding: utf-8 -*-
"""
新闻情绪分析模块 — P2 新闻升级

基于 daily_news 表的 sentiment_score，提供：
1. 情感强度分级（极度乐观/乐观/中性/悲观/极度悲观）
2. 时效衰减因子（越近的新闻权重越高）
3. 板块情绪聚合（按板块分组计算加权情绪）
4. 情绪趋势图数据（N日情绪变化）
5. 持仓 ETF 板块情绪匹配
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple


# ======== 情感强度分级 ========

def classify_sentiment(score):
    """将 0-1 的 sentiment_score 映射为情感标签和等级。

    Returns (label, level):
        level: extreme_high/high/neutral/low/extreme_low
    """
    if score >= 0.80:
        return ("极度乐观", "extreme_high")
    elif score >= 0.62:
        return ("乐观", "high")
    elif score >= 0.42:
        return ("中性", "neutral")
    elif score >= 0.22:
        return ("悲观", "low")
    else:
        return ("极度悲观", "extreme_low")


def classify_sentiment_batch(scores):
    """批量分类情感强度。"""
    if scores.empty:
        return pd.DataFrame(columns=["sentiment_score", "label", "level"])
    results = scores.apply(classify_sentiment)
    df = pd.DataFrame(results.tolist(), columns=["label", "level"], index=scores.index)
    df["sentiment_score"] = scores
    return df[["sentiment_score", "label", "level"]]


# ======== 时效衰减因子 ========

HALF_LIFE_DAYS = 3  # 半衰期 3 天


def time_decay_factor(news_date, reference_date=None):
    """计算单条新闻的时效衰减因子 (0.0~1.0)。"""
    try:
        nd = datetime.strptime(str(news_date)[:10], "%Y-%m-%d").date()
        rd = (reference_date and datetime.strptime(str(reference_date)[:10], "%Y-%m-%d").date()) or datetime.now().date()
        days_diff = max((rd - nd).days, 0)
        return round(0.5 ** (days_diff / HALF_LIFE_DAYS), 6)
    except (ValueError, TypeError):
        return 0.5


def time_decay_weights(dates, reference_date=None):
    """批量计算时效衰减权重。"""
    return dates.apply(lambda d: time_decay_factor(d, reference_date))


# ======== 板块情绪聚合 ========

@dataclass
class SectorSentiment:
    """板块情绪聚合结果"""
    sector: str
    news_count: int
    avg_score: float
    weighted_score: float
    optimism_ratio: float
    pessimism_ratio: float
    label: str
    level: str
    top_positive: list
    top_negative: list


def compute_sector_sentiment(news_df, category_sector_map=None, top_n=3):
    """按板块聚合新闻情绪。

    Parameters
    ----------
    news_df : pd.DataFrame — daily_news 数据
    category_sector_map : dict — 新闻分类→板块映射
    top_n : int — 每板块返回 top N 条正/负面新闻标题

    Returns list[SectorSentiment]
    """
    if news_df.empty:
        return []

    df = news_df.copy()
    for col in ["category", "sentiment_score", "date"]:
        if col not in df.columns:
            return []

    df = df[df["sentiment_score"].notna()].copy()
    df["decay_weight"] = time_decay_weights(df["date"])

    if category_sector_map:
        df["sector"] = df["category"].map(category_sector_map).fillna(df["category"])
    else:
        df["sector"] = df["category"]

    df["weighted_score"] = df["sentiment_score"] * df["decay_weight"]

    results = []
    for sector, group in df.groupby("sector"):
        count = len(group)
        avg_score = group["sentiment_score"].mean()
        weighted_sum = group["weighted_score"].sum()
        weight_total = group["decay_weight"].sum()
        weighted_score = weighted_sum / weight_total if weight_total > 0 else avg_score

        classified = classify_sentiment_batch(group["sentiment_score"])
        optimism_ratio = float((classified["level"].isin(["extreme_high", "high"])).mean())
        pessimism_ratio = float((classified["level"].isin(["extreme_low", "low"])).mean())

        label, level = classify_sentiment(weighted_score)

        top_pos = group.nlargest(min(top_n, len(group)), "sentiment_score")["title"].tolist()
        top_neg = group.nsmallest(min(top_n, len(group)), "sentiment_score")["title"].tolist()

        results.append(SectorSentiment(
            sector=sector, news_count=count,
            avg_score=round(avg_score, 4),
            weighted_score=round(weighted_score, 4),
            optimism_ratio=round(optimism_ratio, 4),
            pessimism_ratio=round(pessimism_ratio, 4),
            label=label, level=level,
            top_positive=top_pos, top_negative=top_neg,
        ))

    return sorted(results, key=lambda x: x.weighted_score, reverse=True)


# ======== 情绪趋势 ========

def compute_sentiment_trend(news_df, days=30, category_sector_map=None):
    """计算情绪趋势（N 日内按日聚合的情绪变化）。

    Returns pd.DataFrame: date, sector, avg_score, news_count
    """
    if news_df.empty:
        return pd.DataFrame()

    df = news_df[news_df["sentiment_score"].notna()].copy()
    if category_sector_map:
        df["sector"] = df["category"].map(category_sector_map).fillna(df["category"])
    else:
        df["sector"] = df["category"]

    try:
        cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        df = df[df["date"] >= cutoff]
    except (ValueError, TypeError):
        pass

    if df.empty:
        return pd.DataFrame()

    trend = df.groupby(["date", "sector"]).agg(
        avg_score=("sentiment_score", "mean"),
        news_count=("sentiment_score", "count"),
    ).reset_index()

    trend["avg_score"] = trend["avg_score"].round(4)
    return trend.sort_values(["date", "sector"])


# ======== 持仓板块情绪匹配 ========

def match_portfolio_sentiment(sector_sentiments, held_sectors):
    """将持仓板块与情绪分析结果匹配。"""
    sentiment_map = {ss.sector: ss for ss in sector_sentiments}
    matched = {}
    for sector in held_sectors:
        if sector in sentiment_map:
            matched[sector] = sentiment_map[sector]
        else:
            for ss in sector_sentiments:
                if sector in ss.sector or ss.sector in sector:
                    matched[sector] = ss
                    break
    return matched


# ======== 组合情绪综合摘要 ========

@dataclass
class PortfolioSentimentSummary:
    """组合情绪综合摘要"""
    total_news: int
    overall_score: float
    overall_label: str
    overall_level: str
    sector_details: dict
    trend_df: object  # Optional[pd.DataFrame]


def compute_portfolio_sentiment(news_df, held_sectors, category_sector_map=None, trend_days=30):
    """计算持仓组合的综合情绪摘要。"""
    all_sectors = compute_sector_sentiment(news_df, category_sector_map)
    matched = match_portfolio_sentiment(all_sectors, held_sectors)

    sector_details = {}
    total_weighted = 0.0
    total_weight = 0.0

    for sector, ss in matched.items():
        sector_details[sector] = {
            "news_count": ss.news_count,
            "avg_score": ss.avg_score,
            "weighted_score": ss.weighted_score,
            "optimism_ratio": ss.optimism_ratio,
            "pessimism_ratio": ss.pessimism_ratio,
            "label": ss.label,
            "level": ss.level,
        }
        total_weighted += ss.weighted_score * ss.news_count
        total_weight += ss.news_count

    overall_score = total_weighted / total_weight if total_weight > 0 else 0.5
    overall_label, overall_level = classify_sentiment(overall_score)

    trend_df = compute_sentiment_trend(news_df, trend_days, category_sector_map)
    total_news = sum(ss.news_count for ss in matched.values())

    return PortfolioSentimentSummary(
        total_news=total_news,
        overall_score=round(overall_score, 4),
        overall_label=overall_label,
        overall_level=overall_level,
        sector_details=sector_details,
        trend_df=trend_df if not trend_df.empty else None,
    )
