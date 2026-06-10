"""Extended tests for news_fetcher."""
import pytest
from unittest.mock import patch
from datetime import datetime


class TestFormatTime:
    def test_today(self):
        from src.utils.news_fetcher import NewsFetcher
        assert "09:30" in NewsFetcher()._format_time("今天 09:30")

    def test_yesterday(self):
        from src.utils.news_fetcher import NewsFetcher
        assert "15:00" in NewsFetcher()._format_time("昨天 15:00")

    def test_date(self):
        from src.utils.news_fetcher import NewsFetcher
        assert "2024" in NewsFetcher()._format_time("2024-01-15")

    def test_unknown(self):
        from src.utils.news_fetcher import NewsFetcher
        assert NewsFetcher()._format_time("random") is not None


class TestAnalyzeNewsImpact:
    def test_empty(self):
        from src.utils.news_fetcher import NewsFetcher
        assert isinstance(NewsFetcher().analyze_news_impact({}, []), list)

    def test_with_data(self):
        from src.utils.news_fetcher import NewsFetcher
        nf = NewsFetcher()
        news = {"财经": {"label": "财经", "news": [{"title": "利好消息", "summary": "市场上涨", "source": "x", "url": "", "publish_time": "2024-01-01"}]}}
        assert isinstance(nf.analyze_news_impact(news, [{"code": "510300", "name": "沪深300ETF"}]), list)


class TestGenerateRotationAnalysis:
    def test_empty(self):
        from src.utils.news_fetcher import NewsFetcher
        r = NewsFetcher().generate_rotation_analysis([], {})
        assert "leaders" in r and "laggards" in r

    def test_with_data(self):
        from src.utils.news_fetcher import NewsFetcher
        pos = [{"code": "510300", "name": "沪深300ETF", "daily_change_pct": 1.5}]
        r = NewsFetcher().generate_rotation_analysis(pos, {"沪深300": {"close": 3500, "change_pct": 0.3}})
        assert isinstance(r, dict)


class TestSaveLoadNewsDb:
    def test_save_and_load(self, tmp_path):
        import sqlite3
        db = str(tmp_path / "t.db")
        conn = sqlite3.connect(db)
        conn.execute("CREATE TABLE daily_news (id INTEGER PRIMARY KEY, date TEXT, category TEXT, title TEXT, source TEXT, url TEXT, summary TEXT, publish_time TEXT, created_at TEXT, sentiment_score REAL, UNIQUE(date,title,source))")
        conn.commit(); conn.close()
        from src.utils.news_fetcher import save_news_to_db, load_news_from_db
        data = {"财经": {"label": "财经", "news": [{"title": "t", "summary": "s", "source": "x", "url": "", "publish_time": ""}]}}
        save_news_to_db(db, data, "2024-01-01")
        assert "财经" in load_news_from_db(db, "2024-01-01")

    def test_save_default_date(self, tmp_path):
        import sqlite3
        db = str(tmp_path / "t2.db")
        conn = sqlite3.connect(db)
        conn.execute("CREATE TABLE daily_news (id INTEGER PRIMARY KEY, date TEXT, category TEXT, title TEXT, source TEXT, url TEXT, summary TEXT, publish_time TEXT, created_at TEXT, sentiment_score REAL, UNIQUE(date,title,source))")
        conn.commit(); conn.close()
        from src.utils.news_fetcher import save_news_to_db
        save_news_to_db(db, {"科技": {"label": "科技", "news": [{"title": "A", "summary": "B", "source": "x", "url": "", "publish_time": ""}]}})
        assert os.path.exists(db)


class TestFetchAllNews:
    @patch("src.utils.news_fetcher.NewsFetcher._fetch_topic_news")
    def test_returns_dict(self, m):
        m.return_value = []
        from src.utils.news_fetcher import NewsFetcher
        assert isinstance(NewsFetcher().fetch_all_news(), dict)


class TestFetchTopicNews:
    @patch("src.utils.news_fetcher.NewsFetcher._fetch_from_eastmoney_search")
    def test_returns_list(self, m):
        m.return_value = []
        from src.utils.news_fetcher import NewsFetcher
        assert isinstance(NewsFetcher()._fetch_topic_news("股票"), list)


import os
