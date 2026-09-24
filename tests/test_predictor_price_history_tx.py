"""腾讯前复权(qfq)回退源测试 —— 锁定"未复权价绝不再进 etf_price_history"这条口径。

背景：EM 主源被服务端掐断后管线回退到新浪源，而 `fund_etf_hist_sina` 无 adjust 参数（未复权），
导致份额折算日出现约 -50% 的假收益（159220 于 2025-11-10 拆分）。本测试用 mock 响应锁死：
 1. 字段顺序：腾讯每行为 [date, open, close, high, low, volume]，**close 在 index 2**；
 2. 输出与 EM 主源完全同构，且 amount 一律 NULL（接口不提供成交额，禁止伪造）；
 3. qfqday 缺失时返回空表，**绝不**回退到未复权的 `day` 键；
 4. 间歇性断连可被指数退避重试吸收；
 5. backfill 默认回退链已切到 ("em","tx")，不再包含未复权的 "sina"。
全程 mock，不发真实网络请求，不触碰生产库。
"""
import inspect
import re
import sqlite3

import pandas as pd
import pytest
import requests

from src.analysis.predictor import price_history as ph

# 语义化测试数据：六个字段互不相同，能唯一确定解析位置
_ROW_A = ["2025-11-10", "1.111", "2.222", "3.333", "4.444", "5.555"]
_ROW_B = ["2025-11-11", "1.11", "2.22", "3.33", "4.44", "5.55"]


def _payload(rows, code="512010"):
    sym = ph._code6_to_symbol(code)
    return {"code": 0, "msg": "", "data": {sym: {"qfqday": rows}}}


class _Resp:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


@pytest.fixture
def captured(monkeypatch):
    """拦截 requests.Session.get，记录入参并回放预置响应。"""
    state = {"calls": [], "payloads": [], "fail_times": 0}

    def fake_get(self, url, params=None, timeout=None, headers=None):
        state["calls"].append({"url": url, "params": params, "headers": headers})
        if state["fail_times"] > 0:
            state["fail_times"] -= 1
            raise requests.exceptions.ConnectionError("RemoteDisconnected")
        return _Resp(state["payloads"].pop(0))

    monkeypatch.setattr(requests.Session, "get", fake_get)
    monkeypatch.setattr(ph.time, "sleep", lambda *_: None)  # 测试不真等
    return state


def test_tx_close_is_index_2_not_ohlc_order(captured):
    """字段顺序铁律：close 在 index 2，排在 high/low 之前。"""
    captured["payloads"].append(_payload([_ROW_A]))
    df = ph.fetch_etf_ohlcv_tx("512010", start="2025-11-01", end="2025-11-30")
    r = df.iloc[0]
    assert r["open"] == pytest.approx(1.111)
    assert r["close"] == pytest.approx(2.222), "index 2 应为 close"
    assert r["high"] == pytest.approx(3.333)
    assert r["low"] == pytest.approx(4.444)
    assert r["volume"] == pytest.approx(5.555)
    # 若误按 OHLC 顺序解析，high/low 会互换或 close 取错值
    assert not (r["close"] == pytest.approx(3.333))


def test_tx_schema_matches_em_and_amount_null(captured):
    """输出与 EM 主源同构；amount 无数据源 -> NULL，不得用 volume×price 伪造。"""
    captured["payloads"].append(_payload([_ROW_A, _ROW_B]))
    df = ph.fetch_etf_ohlcv_tx("512010", start="2025-11-01", end="2025-11-30")
    # 与 EM 主源列序完全一致（腾讯原始行 close 在 high/low 之前，必须重排）
    assert list(df.columns) == ["date", "open", "high", "low", "close",
                                "volume", "amount", "adj_close", "code", "source"]
    assert list(df.columns) == ph._COLUMNS
    # 列序与 EM fetcher 逐列一致（从源码提取 EM 的 out["..."] 赋值顺序）
    em_order = re.findall(r'out\["(\w+)"\] =', inspect.getsource(ph.fetch_etf_ohlcv_akshare))
    assert em_order == ph._COLUMNS, f"tx 列序 {ph._COLUMNS} 与 EM {em_order} 不同构"
    assert df["amount"].isna().all()
    assert (df["adj_close"] == df["close"]).all()
    assert set(df["source"]) == {ph.SOURCE_TX}
    assert set(df["code"]) == {"512010"}


def test_tx_request_uses_qfq_and_symbol_prefix(captured):
    captured["payloads"].append(_payload([_ROW_A], code="510500"))
    ph.fetch_etf_ohlcv_tx("510500", start="2025-11-01", end="2025-11-30")
    call = captured["calls"][0]
    assert call["url"] == ph._TX_URL
    # 5 开头 = 沪市 sh；param 形如 sh510500,day,<start>,<end>,<count>,qfq
    assert call["params"]["param"] == "sh510500,day,2025-11-01,2025-11-30,320,qfq"

    captured["payloads"].append(_payload([_ROW_A], code="159220"))
    ph.fetch_etf_ohlcv_tx("159220", start="2025-11-01", end="2025-11-30")
    assert captured["calls"][1]["params"]["param"].startswith("sz159220,day,")


def test_tx_missing_qfqday_returns_empty_and_never_falls_back_to_unadjusted_day(captured):
    """只有未复权的 day 键时必须返回空表——宁可今天不更新，也不能写入未复权价。"""
    sym = ph._code6_to_symbol("159220")
    captured["payloads"].append(
        {"code": 0, "data": {sym: {"day": [["2025-11-10", "0.6", "0.31", "0.6", "0.3", "123"]]}}}
    )
    df = ph.fetch_etf_ohlcv_tx("159220", start="2025-11-01", end="2025-11-30")
    assert df.empty


def test_tx_retries_transient_disconnect(captured):
    captured["fail_times"] = 2
    captured["payloads"].append(_payload([_ROW_A]))
    df = ph.fetch_etf_ohlcv_tx("512010", start="2025-11-01", end="2025-11-30")
    assert len(df) == 1
    assert len(captured["calls"]) == 3  # 2 次失败 + 1 次成功


def test_tx_raises_after_retries_exhausted(captured):
    captured["fail_times"] = ph._TX_MAX_RETRIES + 5
    captured["payloads"].append(_payload([_ROW_A]))
    with pytest.raises(requests.exceptions.ConnectionError):
        ph.fetch_etf_ohlcv_tx("512010", start="2025-11-01", end="2025-11-30")
    assert len(captured["calls"]) == ph._TX_MAX_RETRIES


def test_tx_filters_out_of_range_rows(captured):
    captured["payloads"].append(_payload([_ROW_A, _ROW_B]))
    df = ph.fetch_etf_ohlcv_tx("512010", start="2025-11-11", end="2025-11-30")
    assert df["date"].tolist() == ["2025-11-11"]


def test_default_sources_are_qfq_only():
    """默认回退链必须是前复权源；未复权的 sina 不得进入默认路径。"""
    sig = inspect.signature(ph.backfill_etf_price_history)
    default = sig.parameters["sources"].default
    assert tuple(default) == ("em", "tx")
    assert "sina" not in tuple(default)
    assert set(ph.FETCHERS) >= {"em", "tx", "sina"}
    assert ph.FETCHERS["tx"] is ph.fetch_etf_ohlcv_tx
    assert ph.SOURCE_TX.endswith("tx_qfq")


def test_backfill_default_never_writes_unadjusted_rows(captured, monkeypatch):
    """默认路径端到端：写库行的 source 只能是 qfq 源，且 amount 为 NULL。"""
    captured["payloads"].append(_payload([_ROW_A, _ROW_B]))
    conn = sqlite3.connect(":memory:")
    conn.execute("""CREATE TABLE etf_price_history(date TEXT, code TEXT, open REAL, high REAL,
                    low REAL, close REAL, volume REAL, amount REAL, adj_close REAL, source TEXT,
                    PRIMARY KEY(date, code))""")
    res = ph.backfill_etf_price_history(conn, ["512010"], start="20251101", end="20251130",
                                      sources=("tx",), log=lambda *_: None)
    rows = conn.execute("SELECT date, close, amount, adj_close, source "
                        "FROM etf_price_history ORDER BY date").fetchall()
    assert res.rows == 2 and len(rows) == 2
    for _date, close, amount, adj_close, source in rows:
        assert source == ph.SOURCE_TX
        assert amount is None
        assert close == adj_close
