"""Tab12 (宏观市场) + Tab13 (数据质量) render 测试"""
import sys, os
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# 清除已加载模块
_rm = [k for k in list(sys.modules) if k.startswith("streamlit") or k.startswith("tabs") or k.startswith("dashboard") or k.startswith("src")]
for m in _rm:
    del sys.modules[m]

import pandas as pd, numpy as np
from unittest.mock import MagicMock, patch

# === Mock Streamlit ===
mock_st = MagicMock()
mock_st.session_state = {}

def _mock_columns(n):
    """st.columns(n) 返回 n 个 MagicMock 容器，每个支持 with 上下文管理。
    支持参数为 int 或 list（如 st.columns([3,1])）。
    """
    count = n if isinstance(n, int) else len(n)
    return [MagicMock(__enter__=MagicMock(return_value=MagicMock()), __exit__=MagicMock(return_value=False)) for _ in range(count)]

mock_st.columns = _mock_columns

for _a in ["plotly_chart","markdown","info","success","warning","error","metric",
           "dataframe","title","subheader","header","divider","caption","text","write","json","code"]:
    setattr(mock_st, _a, MagicMock())
mock_st.expander = MagicMock(return_value=MagicMock())
# tabs 返回支持 __getitem__ 的列表
mock_st.tabs = MagicMock(return_value=[MagicMock(__enter__=MagicMock(return_value=MagicMock()), __exit__=MagicMock(return_value=False)) for _ in range(5)])
mock_st.container = MagicMock(return_value=MagicMock(__enter__=MagicMock(return_value=MagicMock()), __exit__=MagicMock(return_value=False)))
mock_st.spinner = MagicMock(return_value=MagicMock(__enter__=MagicMock(return_value=MagicMock()), __exit__=MagicMock(return_value=False)))
mock_st.empty = MagicMock(return_value=MagicMock())
def _mock_selectbox(label, options=None, index=0, **kw):
    if options and isinstance(options, list):
        return options[min(index, len(options)-1)]
    return "sh000300"
mock_st.selectbox = _mock_selectbox
mock_st.slider = MagicMock(return_value=365)
mock_st.checkbox = MagicMock(return_value=True)
mock_st.button = MagicMock(return_value=False)
mock_st.number_input = MagicMock(return_value=365)
class _CacheDec:
    def __call__(self, func=None, **kw):
        return func if func else (lambda f: f)
    def __getattr__(self, name):
        return self
mock_st.cache_data = _CacheDec()
mock_st.cache_resource = _CacheDec()
mock_st.set_page_config = MagicMock()
mock_st.set_option = MagicMock()
sys.modules["streamlit"] = mock_st


# === Tab12: 宏观市场 ===
class TestTab12Macro:

    def test_render_tab12_no_error(self):
        """Tab12 主渲染函数不抛异常"""
        from tabs.tab12_macro import render_tab12
        render_tab12()

    def test_load_macro_data_empty(self):
        """_load_macro_data 空数据库返回空 DataFrame"""
        from tabs.tab12_macro import _load_macro_data
        with patch("tabs.tab12_macro.get_db_connection") as mock_db:
            mock_conn = MagicMock()
            mock_db.return_value = mock_conn
            with patch("pandas.read_sql_query", return_value=pd.DataFrame()):
                df = _load_macro_data(["USD_CNY"], days=30)
                assert isinstance(df, pd.DataFrame)
                assert df.empty

    def test_load_sentiment_data_empty(self):
        """_load_sentiment_data 空数据库返回空 DataFrame"""
        from tabs.tab12_macro import _load_sentiment_data
        with patch("tabs.tab12_macro.get_db_connection") as mock_db:
            mock_conn = MagicMock()
            mock_db.return_value = mock_conn
            with patch("pandas.read_sql_query", return_value=pd.DataFrame()):
                df = _load_sentiment_data(["MARGIN_TOTAL"], days=30)
                assert isinstance(df, pd.DataFrame)
                assert df.empty

    def test_style_fig_returns_figure(self):
        """_style_fig 返回 plotly Figure"""
        from tabs.tab12_macro import _style_fig
        import plotly.graph_objects as go
        fig = go.Figure()
        result = _style_fig(fig, title="测试")
        assert hasattr(result, 'update_layout')

    def test_render_exchange_rate_no_data(self):
        """_render_exchange_rate 空数据不崩溃"""
        from tabs.tab12_macro import _render_exchange_rate
        with patch("tabs.tab12_macro._load_macro_data", return_value=pd.DataFrame()):
            _render_exchange_rate(days=30)

    def test_render_bond_yields_no_data(self):
        """_render_bond_yields 空数据不崩溃"""
        from tabs.tab12_macro import _render_bond_yields
        with patch("tabs.tab12_macro._load_macro_data", return_value=pd.DataFrame()):
            _render_bond_yields(days=30)

    def test_render_gold_benchmark_no_data(self):
        """_render_gold_benchmark 空数据不崩溃"""
        from tabs.tab12_macro import _render_gold_benchmark
        with patch("tabs.tab12_macro._load_macro_data", return_value=pd.DataFrame()):
            _render_gold_benchmark(days=30)

    def test_render_interest_rates_no_data(self):
        """_render_interest_rates 空数据不崩溃"""
        from tabs.tab12_macro import _render_interest_rates
        with patch("tabs.tab12_macro._load_macro_data", return_value=pd.DataFrame()):
            _render_interest_rates(days=30)

    def test_render_margin_data_no_data(self):
        """_render_margin_data 空数据不崩溃"""
        from tabs.tab12_macro import _render_margin_data
        with patch("tabs.tab12_macro._load_sentiment_data", return_value=pd.DataFrame()):
            _render_margin_data(days=30)

    def test_render_exchange_rate_with_data(self):
        """_render_exchange_rate 有数据时正常渲染"""
        from tabs.tab12_macro import _render_exchange_rate
        dates = pd.date_range("2026-01-01", periods=10, freq="D")
        df = pd.DataFrame({
            "date": dates, "indicator_code": ["USD_CNY"]*10,
            "name": ["美元兑人民币"]*10,
            "value": [7.1 + i*0.01 for i in range(10)],
            "change_pct": [0.01]*10,
        })
        with patch("tabs.tab12_macro._load_macro_data", return_value=df):
            _render_exchange_rate(days=30)

    def test_render_bond_yields_with_data(self):
        """_render_bond_yields 有数据时正常渲染"""
        from tabs.tab12_macro import _render_bond_yields
        dates = pd.date_range("2026-01-01", periods=10, freq="D").strftime("%Y-%m-%d")
        rows = []
        for code in ["CN_10Y_BOND", "US_10Y_BOND"]:
            for i, d in enumerate(dates):
                rows.append({"date": d, "indicator_code": code, "name": code, "value": 2.0 + i*0.01, "change_pct": 0.01})
        df = pd.DataFrame(rows)
        with patch("tabs.tab12_macro._load_macro_data", return_value=df):
            _render_bond_yields(days=30)

    def test_render_interest_rates_with_none_values(self):
        """_render_interest_rates 含 None 值不崩溃"""
        from tabs.tab12_macro import _render_interest_rates
        dates = pd.date_range("2026-01-01", periods=10, freq="D").strftime("%Y-%m-%d")
        df = pd.DataFrame({
            "date": dates, "indicator_code": ["SHIBOR_ON"]*10, "name": ["Shibor"]*10,
            "value": [None, 2.0, None, 2.1, None, 2.2, None, 2.3, None, 2.4],
            "change_pct": [0.01]*10,
        })
        with patch("tabs.tab12_macro._load_macro_data", return_value=df):
            _render_interest_rates(days=30)

    def test_render_margin_data_with_data(self):
        """_render_margin_data 有数据时正常渲染"""
        from tabs.tab12_macro import _render_margin_data
        dates = pd.date_range("2026-01-01", periods=10, freq="D").strftime("%Y-%m-%d")
        rows = []
        for code in ["MARGIN_TOTAL", "MARGIN_上", "MARGIN_深"]:
            for i, d in enumerate(dates):
                rows.append({
                    "date": d, "indicator_code": code, "name": code,
                    "value": 18000.0 + i*10, "change_pct": 0.05, "change_value": 10.0,
                })
        df = pd.DataFrame(rows)
        with patch("tabs.tab12_macro._load_sentiment_data", return_value=df):
            _render_margin_data(days=30)


# === Tab13: 数据质量 ===
class TestTab13DataQuality:

    def test_score_ring_normal(self):
        """_score_ring 正常分数返回 Figure"""
        from tabs.tab13_data_quality import _score_ring
        import plotly.graph_objects as go
        fig = _score_ring(85.0, "A")
        assert isinstance(fig, go.Figure)

    def test_score_ring_zero(self):
        """_score_ring 零分不崩溃"""
        from tabs.tab13_data_quality import _score_ring
        _score_ring(0, "D")

    def test_score_ring_full(self):
        """_score_ring 满分不崩溃"""
        from tabs.tab13_data_quality import _score_ring
        _score_ring(100.0, "A+")

    def test_score_ring_none_score(self):
        """_score_ring score=None 不崩溃"""
        from tabs.tab13_data_quality import _score_ring
        _score_ring(None, "N/A")

    def test_freshness_heatmap_empty(self):
        """_freshness_heatmap 空数据不崩溃"""
        from tabs.tab13_data_quality import _freshness_heatmap
        fig = _freshness_heatmap([])
        assert fig is not None

    def test_freshness_heatmap_with_data(self):
        """_freshness_heatmap 有数据正常渲染（使用 label/status/days_lag 键）"""
        from tabs.tab13_data_quality import _freshness_heatmap
        data = [
            {"table": "portfolio_summary", "label": "交易日快照", "latest_date": "2026-05-20", "days_lag": 0, "status": "OK"},
            {"table": "daily_news", "label": "新闻资讯", "latest_date": "2026-05-19", "days_lag": 1, "status": "OK"},
            {"table": "macro_daily", "label": "宏观数据", "latest_date": "2026-05-18", "days_lag": 2, "status": "WARN"},
            {"table": "empty_table", "label": "空表", "latest_date": "N/A", "days_lag": 999, "status": "EMPTY"},
        ]
        fig = _freshness_heatmap(data)
        assert fig is not None

    def test_coverage_table_empty(self):
        """_coverage_table 空数据返回空 DataFrame"""
        from tabs.tab13_data_quality import _coverage_table
        df = _coverage_table({})
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_coverage_table_with_data(self):
        """_coverage_table 有数据正确构建（使用 total_rows/distinct_codes/date_range 键）"""
        from tabs.tab13_data_quality import _coverage_table
        data = {
            "portfolio_snapshots": {"total_rows": 3394, "distinct_codes": 5, "date_range": "2024-01-01 ~ 2026-05-20"},
            "etf_technical": {"total_rows": 5000, "distinct_codes": 5, "date_range": "2024-01-01 ~ 2026-05-20"},
            "macro_daily": {"total_rows": 8000, "distinct_codes": 0, "date_range": "2025-01-01 ~ 2026-05-20"},
        }
        df = _coverage_table(data)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert "数据表" in df.columns
        assert "记录数" in df.columns

    def test_backtest_summary_empty(self):
        """_backtest_summary 空数据返回空 DataFrame"""
        from tabs.tab13_data_quality import _backtest_summary
        df = _backtest_summary({})
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_backtest_summary_with_data(self):
        """_backtest_summary 有数据正确构建（使用 periods_per_indicator 键）"""
        from tabs.tab13_data_quality import _backtest_summary
        data = {
            "template_count": 3, "result_count": 200, "covered_indicators": 2,
            "periods_per_indicator": {"MA_CROSS": 5, "MACD_SIGNAL": 3}
        }
        df = _backtest_summary(data)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert "指标名称" in df.columns
