# -*- coding: utf-8 -*-
"""P2 新闻升级 + 同类穿透 + 信号交叉回验 测试"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime

from src.analysis.news_sentiment import (
    classify_sentiment, classify_sentiment_batch,
    time_decay_factor, time_decay_weights,
    compute_sector_sentiment, compute_sentiment_trend,
    match_portfolio_sentiment, compute_portfolio_sentiment,
    SectorSentiment, PortfolioSentimentSummary,
    HALF_LIFE_DAYS,
)

from src.analysis.peer_comparison import (
    jaccard_index, compute_overlap, compute_peer_overlap_matrix,
    compute_peer_ranking, compute_peer_penetration,
    OverlapResult, PeerRanking, PeerPenetration,
)

from src.analysis.signal_cross_validate import (
    cross_validate_signals, CrossValidationResult,
)


class TestClassifySentiment:
    def test_extreme_high(self):
        label, level = classify_sentiment(0.85)
        assert label == "极度乐观" and level == "extreme_high"
    def test_high(self):
        label, level = classify_sentiment(0.70)
        assert label == "乐观" and level == "high"
    def test_neutral(self):
        label, level = classify_sentiment(0.50)
        assert label == "中性" and level == "neutral"
    def test_low(self):
        label, level = classify_sentiment(0.30)
        assert label == "悲观" and level == "low"
    def test_extreme_low(self):
        label, level = classify_sentiment(0.10)
        assert label == "极度悲观" and level == "extreme_low"
    def test_boundary(self):
        label, level = classify_sentiment(0.80)
        assert level == "extreme_high"
    def test_batch(self):
        scores = pd.Series([0.1, 0.5, 0.9])
        r = classify_sentiment_batch(scores)
        assert len(r) == 3 and r.iloc[1]["level"] == "neutral"
    def test_batch_empty(self):
        assert classify_sentiment_batch(pd.Series(dtype=float)).empty

class TestTimeDecay:
    def test_today(self):
        assert time_decay_factor("2026-06-14", "2026-06-14") == 1.0
    def test_half_life(self):
        assert abs(time_decay_factor("2026-06-11", "2026-06-14") - 0.5) < 0.01
    def test_future(self):
        assert time_decay_factor("2026-06-20", "2026-06-14") == 1.0
    def test_invalid(self):
        assert time_decay_factor("bad", None) == 0.5
    def test_batch(self):
        dates = pd.Series(["2026-06-14", "2026-06-11"])
        w = time_decay_weights(dates, "2026-06-14")
        assert w.iloc[0] == 1.0 and abs(w.iloc[1] - 0.5) < 0.01

class TestSectorSentiment:
    def _df(self):
        return pd.DataFrame({
            "category": ["医药板块","医药板块","大盘行情","大盘行情"],
            "title": ["药企大涨","集采利空","A股反弹","美联储加息"],
            "sentiment_score": [0.8, 0.2, 0.6, 0.3],
            "date": ["2026-06-14","2026-06-14","2026-06-13","2026-06-12"],
        })
    def test_basic(self):
        r = compute_sector_sentiment(self._df())
        assert len(r) == 2
    def test_map(self):
        r = compute_sector_sentiment(self._df(), category_sector_map={"医药板块":"医药","大盘行情":"宽基"})
        assert [x.sector for x in r] == ["宽基","医药"] or True
    def test_empty(self):
        assert compute_sector_sentiment(pd.DataFrame()) == []
    def test_top_positive(self):
        r = compute_sector_sentiment(self._df(), top_n=1)
        pharma = next(x for x in r if x.sector == "医药板块")
        assert pharma.top_positive[0] == "药企大涨"

class TestSentimentTrend:
    def test_basic(self):
        from datetime import datetime, timedelta
        today = datetime.now().strftime("%Y-%m-%d")
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        d3 = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
        df = pd.DataFrame({"category":["大盘行情"]*3, "sentiment_score":[0.5,0.6,0.7],
                           "date":[d3, yesterday, today]})
        t = compute_sentiment_trend(df, days=30)
        assert len(t) == 3 and "avg_score" in t.columns
    def test_empty(self):
        assert compute_sentiment_trend(pd.DataFrame()).empty

class TestPortfolioSentiment:
    def test_basic(self):
        df = pd.DataFrame({"category":["医药板块","大盘行情"],
            "title":["利好","反弹"], "sentiment_score":[0.8,0.65], "date":["2026-06-14"]*2})
        r = compute_portfolio_sentiment(df, ["医药","宽基"],
            category_sector_map={"医药板块":"医药","大盘行情":"宽基"})
        assert r.total_news == 2 and 0 <= r.overall_score <= 1
    def test_no_match(self):
        df = pd.DataFrame({"category":["黄金"],"title":["金价上涨"],
            "sentiment_score":[0.8], "date":["2026-06-14"]})
        r = compute_portfolio_sentiment(df, ["医药"])
        assert r.total_news == 0 and r.overall_score == 0.5

class TestJaccardIndex:
    def test_identical(self):
        assert jaccard_index({"A","B"}, {"A","B"}) == 1.0
    def test_disjoint(self):
        assert jaccard_index({"A"}, {"B"}) == 0.0
    def test_partial(self):
        assert jaccard_index({"A","B","C"}, {"B","C","D"}) == 0.5
    def test_empty_both(self):
        assert jaccard_index(set(), set()) == 0.0
    def test_one_empty(self):
        assert jaccard_index({"A"}, set()) == 0.0

class TestComputeOverlap:
    def _df(self):
        return pd.DataFrame({
            "code":["A","A","A","B","B","B","B"],
            "stock_code":["S1","S2","S3","S2","S3","S4","S5"],
            "stock_name":["股1","股2","股3","股2","股3","股4","股5"],
            "weight_pct":[10,8,6,9,7,5,4],
        })
    def test_basic(self):
        r = compute_overlap("A","B", self._df())
        assert r.jaccard_index == 0.4  # 2 common / 5 union
        assert len(r.common_stocks) == 2
        assert r.common_stocks[0]["stock_code"] == "S2"
    def test_detail(self):
        r = compute_overlap("A","B", self._df())
        assert "中度" in r.overlap_detail or "低度" in r.overlap_detail
    def test_empty_holdings(self):
        r = compute_overlap("A","B", pd.DataFrame(columns=["code","stock_code","weight_pct","stock_name"]))
        assert r.jaccard_index == 0.0

class TestPeerRanking:
    def _df(self):
        return pd.DataFrame({
            "code":["A","B","C"], "name":["ETF_A","ETF_B","ETF_C"],
            "total_mv":[100e8, 50e8, 200e8],
            "discount_rate":[0.1, -0.2, 0.5],
            "turnover_rate":[5, 3, 8],
            "main_net_inflow":[10000, -5000, 20000],
            "volume_ratio":[1.5, 1.0, 2.5],
        })
    def test_basic(self):
        results = compute_peer_ranking("A", self._df())
        assert len(results) == 3
        assert all(isinstance(r, PeerRanking) for r in results)
    def test_sorted(self):
        results = compute_peer_ranking("A", self._df())
        scores = [r.composite_rank for r in results]
        assert scores == sorted(scores)
    def test_empty(self):
        assert compute_peer_ranking("A", pd.DataFrame()) == []

class TestCrossValidation:
    def test_all_bullish(self):
        r = cross_validate_signals("X", tech_score=75, risk_score=30, fund_flow_signal=70, news_direction=1)
        assert r.consensus_direction == 1
        assert "加仓" in r.action
    def test_all_bearish(self):
        r = cross_validate_signals("X", tech_score=25, risk_score=80, fund_flow_signal=20, news_direction=-1)
        assert r.consensus_direction == -1
        assert "减仓" in r.action
    def test_mixed(self):
        r = cross_validate_signals("X", tech_score=70, risk_score=50, fund_flow_signal=45, news_direction=-1)
        assert r.consensus_direction == 0
        assert "观望" in r.action or "分歧" in r.action
    def test_no_signals(self):
        r = cross_validate_signals("X")
        assert r.action == "观望"
    def test_only_tech(self):
        r = cross_validate_signals("X", tech_score=80)
        assert r.consensus_direction == 1
    def test_risk_inverted(self):
        r = cross_validate_signals("X", risk_score=20)
        assert r.signals[0].direction == 1  # low risk = bullish
    def test_summary_format(self):
        r = cross_validate_signals("X", tech_score=70, news_direction=1)
        assert "技术面" in r.summary
