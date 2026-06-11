"""测试 src/utils/chart_utils.py 纯函数"""
import numpy as np, pandas as pd, pytest
from src.utils.chart_utils import downsample, _cleanse_daily_returns, _fmt, _fmt_cell, _sig, _rsi_c, _boll_c, _atr_c

class TestDownsample:
    def test_no_change(self):
        df = pd.DataFrame({"date": range(10), "v": range(10)})
        assert len(downsample(df, "date", 100)) == 10
    def test_reduces(self):
        df = pd.DataFrame({"date": range(1000), "v": range(1000)})
        r = downsample(df, "date", 50)
        assert len(r) <= 52
    def test_preserves_bounds(self):
        df = pd.DataFrame({"date": range(500), "v": range(500)})
        r = downsample(df, "date", 50)
        assert r.iloc[0]["v"] == 0 and r.iloc[-1]["v"] == 499
    def test_single_row(self):
        df = pd.DataFrame({"date": [1], "v": [42]})
        assert len(downsample(df, "date", 10)) == 1

class TestCleanse:
    def test_no_outliers(self):
        df = pd.DataFrame({"daily_return": [0.1, -0.2, 0.3]})
        r, s = _cleanse_daily_returns(df)
        assert len(r) == 3 and s["filtered"] == 0
    def test_filters(self):
        df = pd.DataFrame({"daily_return": [0.1, 99.0, -50.0, 0.2]})
        r, s = _cleanse_daily_returns(df, threshold=5.0)
        assert len(r) == 2 and s["filtered"] == 2
    def test_tails(self):
        df = pd.DataFrame({"daily_return": [0.1]*1000})
        r, s = _cleanse_daily_returns(df, max_tail=100)
        assert len(r) == 100 and s["tailed"] == 900
    def test_stats_keys(self):
        df = pd.DataFrame({"daily_return": [0.1]*10})
        _, s = _cleanse_daily_returns(df)
        for k in ("original","after_filter","after_tail","filtered","tailed"): assert k in s

class TestFmt:
    def test_positive(self): assert "#22c55e" in _fmt(5.25, "%")
    def test_negative(self): assert "#ef4444" in _fmt(-3.14, "%")
    def test_zero(self): assert "#c9d1d9" in _fmt(0.0, "%")
    def test_inverted(self): assert "#22c55e" in _fmt(-2.0, "%", inv=True)
    def test_nan(self): assert "--" in _fmt("abc", "%")
    def test_decimals(self): assert "1.2346" in _fmt(1.23456, "", dec=4)

class TestFmtCell:
    def test_positive(self): assert "#22c55e" in _fmt_cell(5.25, "%")
    def test_negative(self): assert "#ef4444" in _fmt_cell(-3.14, "%")
    def test_zero(self): assert "#c9d1d9" in _fmt_cell(0.0, "%")
    def test_near_zero(self): assert "#c9d1d9" in _fmt_cell(0.005, "%")
    def test_invalid(self): assert "--" in _fmt_cell("x", "%")
    def test_inverted(self): assert "#22c55e" in _fmt_cell(-2.0, "", invert_color=True)

class TestSig:
    def test_bull(self): assert "#22c55e" in _sig("金叉", bull=["金叉"], bear=["死叉"])
    def test_bear(self): assert "#ef4444" in _sig("死叉", bull=["金叉"], bear=["死叉"])
    def test_warn(self): assert "#f59e0b" in _sig("注意", bull=["金叉"], bear=["死叉"], warn=["注意"])
    def test_neutral(self): assert "#8b949e" in _sig("中性", bull=["金叉"], bear=["死叉"])

class TestRsi:
    def test_extreme_high(self): assert "#ef4444" in _rsi_c(85)
    def test_high(self): assert "#f59e0b" in _rsi_c(72)
    def test_extreme_low(self): assert "#3b82f6" in _rsi_c(15)
    def test_low(self): assert "#f59e0b" in _rsi_c(28)
    def test_normal(self): assert "#c9d1d9" in _rsi_c(50)

class TestBoll:
    def test_upper_extreme(self): assert "#ef4444" in _boll_c(90)
    def test_upper(self): assert "#22c55e" in _boll_c(65)
    def test_lower_extreme(self): assert "#3b82f6" in _boll_c(10)
    def test_lower(self): assert "#f59e0b" in _boll_c(35)
    def test_mid(self): assert "#c9d1d9" in _boll_c(50)

class TestAtr:
    def test_high(self): assert "#f59e0b" in _atr_c(4.0)
    def test_medium(self): assert "#c9d1d9" in _atr_c(2.5)
    def test_low(self): assert "#22c55e" in _atr_c(1.0)
