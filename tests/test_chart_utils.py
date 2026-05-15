"""L1 - chart_utils 纯函数单元测试"""
import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from src.utils.chart_utils import (
    downsample, _add_min_max_annotations, _cleanse_daily_returns,
    _fmt, _fmt_cell, _sig, _rsi_c, _boll_c, _atr_c,
)

# ---- downsample ----
def test_downsample_no_reduction():
    df = pd.DataFrame({"date": range(10), "val": range(10)})
    assert len(downsample(df, max_points=20)) == 10

def test_downsample_exact():
    df = pd.DataFrame({"date": range(1000), "val": range(1000)})
    result = downsample(df, max_points=50)
    assert 48 <= len(result) <= 55

def test_downsample_preserves_boundaries():
    df = pd.DataFrame({"date": range(1000), "val": range(1000)})
    result = downsample(df, max_points=50)
    assert result.iloc[0]["val"] == 0 and result.iloc[-1]["val"] == 999

def test_downsample_empty():
    assert len(downsample(pd.DataFrame(), max_points=50)) == 0

def test_downsample_custom_date_col():
    df = pd.DataFrame({"my_date": range(10), "val": range(10)})
    result = downsample(df, date_col="my_date", max_points=5)
    assert "my_date" in result.columns

# ---- _add_min_max_annotations ----
def test_annotations_basic():
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=[1,2,3], y=[10,5,15]))
    n = len(fig.data)
    _add_min_max_annotations(fig, [1,2,3], [10,5,15])
    assert len(fig.data) == n + 2

def test_annotations_all_nan():
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=[1,2,3], y=[np.nan, np.nan, np.nan]))
    n = len(fig.data)
    _add_min_max_annotations(fig, [1,2,3], [np.nan, np.nan, np.nan])
    assert len(fig.data) == n

def test_annotations_single_point():
    fig = go.Figure()
    _add_min_max_annotations(fig, [1], [5])
    assert len(fig.data) == 0

def test_annotations_subplot():
    from plotly.subplots import make_subplots
    fig = make_subplots(rows=2, cols=1)
    fig.add_trace(go.Scatter(x=[1,2,3], y=[10,5,15]), row=1, col=1)
    _add_min_max_annotations(fig, [1,2,3], [10,5,15], row=1, col=1)
    assert len(fig.data) >= 2

# ---- _cleanse_daily_returns ----
def test_cleanse_normal():
    np.random.seed(42)
    df = pd.DataFrame({"daily_return": np.random.normal(0, 0.02, 100)})
    result, stats = _cleanse_daily_returns(df, threshold=5.0)
    assert len(result) > 90
    assert "original" in stats and "after_filter" in stats

def test_cleanse_with_outliers():
    df = pd.DataFrame({"daily_return": [0.01]*90 + [10.0, -10.0]})
    result, stats = _cleanse_daily_returns(df, threshold=5.0)
    assert stats["filtered"] >= 2
    assert len(result) < len(df)

def test_cleanse_empty():
    result, stats = _cleanse_daily_returns(pd.DataFrame({"daily_return": []}))
    assert len(result) == 0 and stats["original"] == 0

def test_cleanse_stats_keys():
    _, stats = _cleanse_daily_returns(pd.DataFrame({"daily_return": [0.01]*10}))
    assert {"original","after_filter","after_tail","filtered","tailed"}.issubset(stats.keys())

# ---- _fmt / _fmt_cell ----
def test_fmt_positive():
    assert "#22c55e" in _fmt(1.5)

def test_fmt_negative():
    assert "#ef4444" in _fmt(-2.3)

def test_fmt_zero():
    assert "#c9d1d9" in _fmt(0)

def test_fmt_nan():
    """NaN 不触发 except，float(nan) 成功但比较行为异常，输出含 nan"""
    result = _fmt(float("nan"))
    assert "nan" in result.lower() or "--" in result

def test_fmt_cell_non_numeric():
    assert "--" in _fmt_cell("abc")

# ---- _sig ----
def test_sig_bull():
    assert "#22c55e" in _sig("买入", bull={"买入"}, bear={"卖出"})

def test_sig_bear():
    assert "#ef4444" in _sig("卖出", bull={"买入"}, bear={"卖出"})

def test_sig_warn():
    assert "#f59e0b" in _sig("观望", bull={"买入"}, bear={"卖出"}, warn={"观望"})

def test_sig_unknown():
    assert "#8b949e" in _sig("未知", bull={"买入"}, bear={"卖出"})

# ---- _rsi_c / _boll_c / _atr_c ----
def test_rsi_c_boundaries():
    assert "#3b82f6" in _rsi_c(20), "RSI<=20 应蓝色(超卖)"
    assert "#c9d1d9" in _rsi_c(50), "RSI=50 应灰色(中性)"
    assert "#ef4444" in _rsi_c(80), "RSI>=80 应红色(超买)"
    assert "#f59e0b" in _rsi_c(75), "RSI 70-80 应黄色"
    assert "#f59e0b" in _rsi_c(25), "RSI 20-30 应黄色"

def test_boll_c_boundaries():
    assert "#ef4444" in _boll_c(80), "boll>=80 应红色"
    assert "#22c55e" in _boll_c(60), "boll 60-79 应绿色"
    assert "#3b82f6" in _boll_c(20), "boll<=20 应蓝色"
    assert "#f59e0b" in _boll_c(40), "boll 20-40 应黄色"
    assert "#c9d1d9" in _boll_c(50), "boll 41-59 应灰色"

def test_atr_c_boundaries():
    assert "#22c55e" in _atr_c(0.5), "ATR<2% 应绿色"
    assert "#c9d1d9" in _atr_c(2.5), "ATR 2-3% 应灰色"
    assert "#f59e0b" in _atr_c(5.0), "ATR>=3% 应黄色"
