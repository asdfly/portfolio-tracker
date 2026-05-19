"""L4 - 交互式分支覆盖测试

确保 selectbox/button/expander 等交互组件的非默认路径也能正确执行，
防止 _render_etf_detail_panel 等函数因缺少 import 而在运行时崩溃。
"""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

_rm = [
    k for k in list(sys.modules)
    if k.startswith("streamlit") or k.startswith("tabs")
    or k.startswith("dashboard_main") or k.startswith("src")
]
for m in _rm:
    del sys.modules[m]

import pandas as pd
import numpy as np
from unittest.mock import MagicMock

def _mock_columns(n):
    return [MagicMock() for _ in range(n if isinstance(n, int) else len(n))]

def _identity(f):
    return f

def _ctx_mgr():
    return MagicMock(
        __enter__=MagicMock(return_value=MagicMock()),
        __exit__=MagicMock(return_value=False),
    )

mock_st = MagicMock()
mock_st.columns = _mock_columns
mock_st.session_state = {}
for _a in [
    "plotly_chart", "markdown", "info", "success", "warning", "error",
    "metric", "dataframe", "title", "subheader", "header", "divider",
    "caption", "text", "write", "json", "code", "progress", "delta",
]:
    setattr(mock_st, _a, MagicMock())

mock_st.expander = MagicMock(return_value=_ctx_mgr())
mock_st.container = MagicMock(return_value=_ctx_mgr())
mock_st.spinner = MagicMock(return_value=_ctx_mgr())
mock_st.empty = MagicMock(return_value=MagicMock())
mock_st.set_page_config = MagicMock()
mock_st.set_option = MagicMock()

class _CacheDecorator:
    def __call__(self, func=None, **kw):
        return func if func is not None else _identity
    def __getattr__(self, name):
        return self

mock_st.cache_data = _CacheDecorator()
mock_st.cache_resource = _CacheDecorator()

_selectbox_return_values = []

def _mock_selectbox(label, options=None, index=0, **kw):
    if options and isinstance(options, list) and len(options) > 1:
        if _selectbox_return_values:
            val = _selectbox_return_values.pop(0)
            if val in options:
                return val
        return options[index]
    return options[0] if options and isinstance(options, list) else "sh000300"

mock_st.selectbox = _mock_selectbox
mock_st.date_input = MagicMock(return_value="2024-01-30")
mock_st.number_input = MagicMock(return_value=30)
mock_st.slider = MagicMock(return_value=30)
mock_st.checkbox = MagicMock(return_value=True)
mock_st.button = MagicMock(return_value=False)
mock_st.tabs = lambda labels: [MagicMock() for _ in labels]
mock_st.toggle = MagicMock(return_value=True)
mock_st.radio = MagicMock(return_value="option1")
mock_st.multiselect = MagicMock(return_value=[])

sys.modules["streamlit"] = mock_st


def _empty_df():
    return pd.DataFrame()

def _make_positions():
    return pd.DataFrame({
        "code": ["510300", "510500", "159915"],
        "name": ["沪深300ETF", "中证500ETF", "创业板ETF"],
        "quantity": [1000, 500, 800],
        "cost_price": [4.5, 6.2, 2.1],
        "current_price": [4.8, 6.0, 2.3],
        "market_value": [4800.0, 3000.0, 1840.0],
        "pnl": [300.0, -100.0, 160.0],
        "pnl_rate": [6.67, -3.23, 9.52],
        "beta": [0.95, 1.05, 1.15],
    })

def _make_summary(n=30):
    np.random.seed(42)
    dates = pd.date_range("2024-01-01", periods=n, freq="D").strftime("%Y-%m-%d")
    return pd.DataFrame({
        "date": dates,
        "total_value": np.random.normal(100000, 5000, n).round(2),
        "total_pnl": np.random.normal(1000, 500, n).round(2),
        "daily_return": np.random.normal(0.001, 0.02, n).round(6),
    })

def _make_index_quotes(n=10):
    np.random.seed(42)
    dates = pd.date_range("2024-01-20", periods=n, freq="D")
    return pd.DataFrame([
        {"date": d.strftime("%Y-%m-%d"), "code": "sh000300",
         "name": "沪深300", "close": round(3500 + np.random.randn() * 30, 2),
         "change_pct": round(np.random.randn() * 1.5, 2),
         "volume": int(np.random.rand() * 1e8),
         "amount": int(np.random.rand() * 1e10)}
        for d in dates
    ])

KW = {"selected_date": "2024-01-30", "selected_benchmark": "sh000300"}


class TestTab2ETFDetail:
    """Tab2 selectbox 选中 ETF 后应触发 _render_etf_detail_panel 而不崩溃"""

    def test_etf_detail_panel_triggered(self):
        """selectbox 选中 ETF -> _render_etf_detail_panel 执行（验证 import go 等）"""
        global _selectbox_return_values
        _selectbox_return_values = [
            "全部", "全部", "全部", "沪深300ETF（510300）",
        ]
        from tabs.tab2_position import render_tab2
        render_tab2(_make_positions(), _make_summary(), _make_index_quotes(), **KW)


class TestTab3Expander:
    """Tab3 expander 返回上下文管理器时应正确展开"""

    def test_expander_brinson_content(self):
        """expander 内 Brinson 归因内容不崩溃"""
        from tabs.tab3_risk import render_tab3
        render_tab3(_make_positions(), _make_summary(), _make_index_quotes(), **KW)


class TestTab5Advanced:
    """Tab5 高级分析工具的各种交互路径"""

    def test_monte_carlo_expander(self):
        from tabs.tab5_advanced import render_tab5
        render_tab5(_make_positions(), _make_summary(), _make_index_quotes(), **KW)

    def test_stress_test_expander(self):
        from tabs.tab5_advanced import render_tab5
        render_tab5(_make_positions(), _make_summary(), _make_index_quotes(), **KW)


class TestTab7NewsDetail:
    """Tab7 selectbox 选中后加载资讯详情"""

    def test_news_selectbox_selected(self):
        global _selectbox_return_values
        _selectbox_return_values = ["沪深300ETF"] * 5
        from tabs.tab7_news import render_tab7
        render_tab7(_make_positions(), _make_summary(), _make_index_quotes(), **KW)


class TestTab10ButtonPaths:
    """Tab10 button 按下后的数据刷新路径"""

    def test_refresh_button_pressed(self):
        mock_st.button = MagicMock(return_value=True)
        from tabs.tab10_fund_flow import render_tab10
        render_tab10(_make_positions(), _make_summary(), _make_index_quotes(), **KW)
        mock_st.button = MagicMock(return_value=False)
