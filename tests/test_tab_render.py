"""L3 - Tab mock streamlit"""
import sys, importlib, os
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# 清除已加载的项目模块，确保用mock的streamlit重新导入
_rm = [k for k in list(sys.modules) if k.startswith("streamlit") or k.startswith("tabs") or k.startswith("dashboard_main") or k.startswith("src")]
for m in _rm:
    del sys.modules[m]

import pandas as pd, numpy as np
from unittest.mock import MagicMock

def _mock_columns(n): return [MagicMock() for _ in range(n if isinstance(n, int) else len(n))]
def _identity(f): return f
def _ctx_mgr(): return MagicMock(__enter__=MagicMock(return_value=MagicMock()), __exit__=MagicMock(return_value=False))

mock_st = MagicMock()
mock_st.columns = _mock_columns
mock_st.session_state = {}
for _a in ["plotly_chart","markdown","info","success","warning","error","metric","dataframe","title","subheader","header","divider","caption","text","write","json","code","progress","delta","empty"]: setattr(mock_st, _a, MagicMock())
mock_st.expander = MagicMock(return_value=MagicMock())
def _mock_selectbox(label, options=None, index=0, **kw):
    if options and isinstance(options, list):
        return options[index if index < len(options) else 0]
    return "sh000300"
mock_st.selectbox = _mock_selectbox
mock_st.date_input = MagicMock(return_value="2024-01-30")
class _SliderReturn(int):
    pass
mock_st.slider = MagicMock(return_value=_SliderReturn(30))
mock_st.checkbox = MagicMock(return_value=True)
mock_st.button = MagicMock(return_value=False)
def _mock_tabs(labels): return [MagicMock() for _ in labels]
mock_st.tabs = _mock_tabs
mock_st.container = MagicMock(return_value=_ctx_mgr())
mock_st.spinner = MagicMock(return_value=_ctx_mgr())
mock_st.empty = MagicMock(return_value=MagicMock())
mock_st.number_input = MagicMock(return_value=30)
class _CacheDecorator:
    def __call__(self, func=None, **kw):
        if func is not None:
            return func
        return _identity
    def __getattr__(self, name):
        return self
mock_st.cache_data = _CacheDecorator()
mock_st.cache_resource = _CacheDecorator()
mock_st.set_page_config = MagicMock()
mock_st.set_option = MagicMock()
sys.modules["streamlit"] = mock_st

def _empty_df(): return pd.DataFrame()

def sample_positions():
    return pd.DataFrame({"code":["510300","510500","159915"],"name":["沪深300ETF","中证500ETF","创业板ETF"],"quantity":[1000,500,800],"cost_price":[4.5,6.2,2.1],"current_price":[4.8,6.0,2.3],"market_value":[4800.0,3000.0,1840.0],"pnl":[300.0,-100.0,160.0],"pnl_rate":[6.67,-3.23,9.52],"beta":[0.95,1.05,1.15]})

def sample_summary(n=10):
    np.random.seed(42); dates = pd.date_range("2024-01-01",periods=n,freq="D").strftime("%Y-%m-%d")
    return pd.DataFrame({"date":dates,"total_value":np.random.normal(100000,5000,n).round(2),"total_pnl":np.random.normal(1000,500,n).round(2),"daily_return":np.random.normal(0.001,0.02,n).round(6)})

def _sample_idx(n=10):
    np.random.seed(42); dates=pd.date_range("2024-01-20",periods=n,freq="D")
    return pd.DataFrame([{"date":d.strftime("%Y-%m-%d"),"code":"sh000300","name":"沪深300","close":round(3500+np.random.randn()*30,2),"change_pct":round(np.random.randn()*1.5,2),"volume":int(np.random.rand()*1e8),"amount":int(np.random.rand()*1e10)} for d in dates])

KW = {"selected_date":"2024-01-30","selected_benchmark":"sh000300"}

# 空数据边界 11个Tab
def test_tab1_empty():
    from tabs.tab1_net_value import render_tab1; render_tab1(_empty_df(),_empty_df(),_empty_df(),**KW)
def test_tab2_empty():
    from tabs.tab2_position import render_tab2; render_tab2(_empty_df(),_empty_df(),_empty_df(),**KW)
def test_tab3_empty():
    from tabs.tab3_risk import render_tab3; render_tab3(_empty_df(),_empty_df(),_empty_df(),**KW)
def test_tab4_empty():
    from tabs.tab4_calendar import render_tab4; render_tab4(_empty_df(),_empty_df(),_empty_df(),**KW)
def test_tab5_empty():
    from tabs.tab5_advanced import render_tab5; render_tab5(_empty_df(),_empty_df(),_empty_df(),**KW)
def test_tab6_empty():
    from tabs.tab6_technical import render_tab6; render_tab6(_empty_df(),_empty_df(),_empty_df(),**KW)
def test_tab7_empty():
    from tabs.tab7_news import render_tab7; render_tab7(_empty_df(),_empty_df(),_empty_df(),**KW)
def test_tab8_empty():
    from tabs.tab8_advice import render_tab8; render_tab8(_empty_df(),_empty_df(),_empty_df(),**KW)
def test_tab9_empty():
    from tabs.tab9_custom import render_tab9; render_tab9(_empty_df(),_empty_df(),_empty_df(),**KW)
def test_tab10_empty(sample_index_quotes, sample_positions, sample_summary):
    from tabs.tab10_fund_flow import render_tab10; render_tab10(_empty_df(),_empty_df(),_empty_df(),**KW)
def test_tab11_empty(sample_index_quotes, sample_positions, sample_summary, monkeypatch):
    import akshare as ak
    for attr in ["spot_hist_sge","spot_golden_benchmark_sge","bond_zh_us_rate",
                  "macro_china_cpi","macro_cons_gold","macro_china_fx_gold",
                  "futures_comex_inventory","forex_hist_em","macro_fx_sentiment"]:
        if hasattr(ak, attr):
            monkeypatch.setattr(ak, attr, lambda *a, **kw: pd.DataFrame())
    from tabs.tab11_gold import render_tab11; render_tab11(_empty_df(),_empty_df(),_empty_df(),**KW)

# 正常数据 5个关键Tab
def test_tab1_normal(sample_index_quotes, sample_positions, sample_summary):
    from tabs.tab1_net_value import render_tab1; render_tab1(sample_positions,sample_summary,sample_index_quotes,**KW)
def test_tab2_normal(sample_index_quotes, sample_positions, sample_summary):
    from tabs.tab2_position import render_tab2; render_tab2(sample_positions,sample_summary,sample_index_quotes,**KW)
def test_tab3_normal(sample_index_quotes, sample_positions, sample_summary):
    from tabs.tab3_risk import render_tab3; render_tab3(sample_positions,sample_summary,sample_index_quotes,**KW)
def test_tab5_normal(sample_index_quotes, sample_positions, sample_summary):
    from tabs.tab5_advanced import render_tab5; render_tab5(sample_positions,sample_summary,sample_index_quotes,**KW)
def test_tab7_normal(sample_index_quotes, sample_positions, sample_summary):
    from tabs.tab7_news import render_tab7; render_tab7(sample_positions,sample_summary,sample_index_quotes,**KW)

# 部分数据边界
def test_tab3_summary_one_row(sample_positions, sample_summary):
    from tabs.tab3_risk import render_tab3; render_tab3(sample_positions,sample_summary.iloc[:1],_empty_df(),**KW)
def test_tab6_no_technical(sample_positions, sample_summary):
    from tabs.tab6_technical import render_tab6; render_tab6(sample_positions,sample_summary,_empty_df(),**KW)
def test_tab8_empty_advice(sample_positions, sample_summary):
    from tabs.tab8_advice import render_tab8; render_tab8(sample_positions,sample_summary.iloc[:5],_empty_df(),**KW)
def test_tab9_empty_custom(sample_index_quotes, sample_positions, sample_summary):
    from tabs.tab9_custom import render_tab9; render_tab9(sample_positions,sample_summary.iloc[:5],_empty_df(),**KW)
def test_tab10_empty_flow(sample_index_quotes, sample_positions, sample_summary):
    from tabs.tab10_fund_flow import render_tab10; render_tab10(sample_positions,sample_summary.iloc[:5],_empty_df(),**KW)
# 补充正常数据渲染 - tab4/tab8/tab9/tab10
def test_tab4_normal(sample_index_quotes, sample_positions, sample_summary):
    from tabs.tab4_calendar import render_tab4; render_tab4(sample_positions,sample_summary,sample_index_quotes,**KW)
def test_tab8_normal(sample_index_quotes, sample_positions, sample_summary):
    from tabs.tab8_advice import render_tab8; render_tab8(sample_positions,sample_summary,sample_index_quotes,**KW)
def test_tab9_normal(sample_index_quotes, sample_positions, sample_summary):
    from tabs.tab9_custom import render_tab9; render_tab9(sample_positions,sample_summary,sample_index_quotes,**KW)
def test_tab10_normal(sample_index_quotes, sample_positions, sample_summary):
    from tabs.tab10_fund_flow import render_tab10; render_tab10(sample_positions,sample_summary,sample_index_quotes,**KW)
