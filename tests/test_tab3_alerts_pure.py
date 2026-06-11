"""测试 tabs/tab3_risk_alerts.py 纯数据函数 _detect_realtime_alerts"""

import pandas as pd
import pytest


def _f():
    from tabs.tab3_risk_alerts import _detect_realtime_alerts
    return _detect_realtime_alerts


def _s(**kw):
    d = {"daily_return": 0.0, "max_drawdown": 0.0, "volatility": 10.0, "sharpe_ratio": 1.0}
    d.update(kw); return pd.DataFrame([d])


def _p(rows): return pd.DataFrame(rows)


def _sp():
    return _p([{"name": "A", "pnl_rate": 0.0, "market_value": 25000},
               {"name": "B", "pnl_rate": 0.0, "market_value": 25000},
               {"name": "C", "pnl_rate": 0.0, "market_value": 25000},
               {"name": "D", "pnl_rate": 0.0, "market_value": 25000}])


class TestAlerts:
    def test_empty(self):
        assert _f()(pd.DataFrame(), pd.DataFrame(), "2025-01-01") == []
    def test_no_triggers(self):
        assert len(_f()(_sp(), _s(daily_return=-0.5, max_drawdown=-2.0), "d")) == 0
    def test_day_drop(self):
        a = _f()(_sp(), _s(daily_return=-4.0), "d")
        assert "单日暴跌" in [x["rule"] for x in a] and a[0]["level"] == "error"
    def test_deep_dd(self):
        assert "深度回撤" in [x["rule"] for x in _f()(_sp(), _s(max_drawdown=-18.0), "d")]
    def test_dd_warn(self):
        a = _f()(_sp(), _s(max_drawdown=-12.0), "d")
        assert "回撤预警" in [x["rule"] for x in a] and "深度回撤" not in [x["rule"] for x in a]
    def test_vol_spike(self):
        assert "波动飙升" in [x["rule"] for x in _f()(_sp(), _s(volatility=35.0), "d")]
    def test_neg_sharpe(self):
        assert "夏普异常" in [x["rule"] for x in _f()(_sp(), _s(sharpe_ratio=-0.5), "d")]
    def test_stock_crash(self):
        pos = _p([{"name":"X","pnl_rate":-25.0,"market_value":30000},{"name":"Y","pnl_rate":0.0,"market_value":30000}])
        assert "个股暴跌" in [x["rule"] for x in _f()(pos, _s(), "d")]
    def test_stock_warn(self):
        pos = _p([{"name":"X","pnl_rate":-17.0,"market_value":30000},{"name":"Y","pnl_rate":0.0,"market_value":30000}])
        assert "个股预警" in [x["rule"] for x in _f()(pos, _s(), "d")]
    def test_concentrate(self):
        pos = _p([{"name":"X","pnl_rate":0.0,"market_value":90000},{"name":"Y","pnl_rate":0.0,"market_value":10000}])
        assert "集中度风险" in [x["rule"] for x in _f()(pos, _s(), "d")]
    def test_sorted(self):
        a = _f()(_sp(), _s(daily_return=-4.0, max_drawdown=-18.0, volatility=35.0), "d")
        assert len(a) >= 3
        lv = [x["level"] for x in a]
        for i in range(len(lv)-1): assert lv[i] <= lv[i+1]
    def test_struct(self):
        a = _f()(_sp(), _s(daily_return=-5.0), "d")[0]
        for k in ("level","rule","message","time"): assert k in a
