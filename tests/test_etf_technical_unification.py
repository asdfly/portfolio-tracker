"""问题八回归守卫：etf_technical 三套口径统一 + 行日期 = K 线末日。

历史 bug（docs/handover/07_known_data_issues.md 问题八）：
etf_technical 曾被三套口径写入（backfill / 日频 TechnicalAnalyzer / 观察名单），
且日频写入把行日期写成 self.today 而非 K 线末日，导致「行写 D、指标算到 D-1」的
间歇性错位（信息滞后 / 反向未来函数）。

本测试锁定：
1. compute_technical_unified 与 backfill_full_history.rebuild_etf_technical 输出逐字段一致；
2. 日频/观察名单写入的行日期 = 该行所用价格窗口末日（K 线末日），绝不写 self.today。
"""
import os
import sqlite3
import tempfile

import numpy as np

from src.analysis.technical import compute_technical_unified
from src.utils.database import DatabaseManager


SCHEMA = """
CREATE TABLE IF NOT EXISTS portfolio_snapshots (
    code TEXT, name TEXT, date TEXT, current_price REAL
);
CREATE TABLE IF NOT EXISTS etf_technical (
    date TEXT, code TEXT, ma_signal TEXT, macd_signal TEXT,
    rsi_value REAL, rsi_status TEXT, kdj_signal TEXT,
    bollinger_position REAL, atr_pct REAL, trend TEXT
);
"""


def _cleanup_db(path):
    """Best-effort 删除临时 DB 文件。

    Python 3.12+ 对 sqlite3.connect 做了 LRU 缓存：相同参数返回同一连接对象，
    且 close() 不会立即释放底层文件句柄，直到缓存连接被回收。Windows 上这会导致
    teardown 的 os.remove 报 PermissionError。这里强制 gc 并多次重试，仍失败则
    交由系统临时目录清理（不影响断言结果）。
    """
    import gc
    import time

    for _ in range(5):
        try:
            os.remove(path)
            return
        except PermissionError:
            gc.collect()
            time.sleep(0.05)
    try:
        os.remove(path)
    except OSError:
        pass  # 临时文件由系统清理，不阻断测试


def _make_db(prices, dates, code="159732"):
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    conn = sqlite3.connect(path)
    conn.executescript(SCHEMA)
    conn.executemany(
        "INSERT INTO portfolio_snapshots (code, name, date, current_price) VALUES (?,?,?,?)",
        [(code, "测试", d, p) for d, p in zip(dates, prices)],
    )
    conn.commit()
    conn.close()
    return path


def _dates(n, end="2026-09-10"):
    base = __import__("datetime").date.fromisoformat(end)
    out = []
    i = 0
    while len(out) < n:
        d = base - __import__("datetime").timedelta(days=i)
        # 跳过周末，模拟交易日
        if d.weekday() < 5:
            out.append(d.isoformat())
        i += 1
    return list(reversed(out))


def test_compute_technical_unified_sanity():
    # 温和上涨序列：RSI 偏高、MA5>MA20、MACD 多头，KDJ 金叉
    n = 40
    prices = [100 + i * 0.5 + (i % 3) * 0.2 for i in range(n)]
    dates = _dates(n)
    ind = compute_technical_unified(prices, dates, n - 1)

    assert 0 < ind["rsi"]["RSI"] <= 100, f"RSI 越界: {ind['rsi']['RSI']}"
    assert ind["rsi"]["status"] in {
        "严重超买", "超买", "严重超卖", "超卖", "正常"
    }
    assert ind["ma"]["signal"] in {"金叉", "死叉", "多头排列", "空头排列"}
    assert ind["macd"]["signal"] in {"多头", "空头", "金叉", "死叉", "看多", "看空", "中性"}
    assert ind["kdj"]["signal"] in {"金叉", "死叉"}
    assert ind["trend"]["trend"] in {"强势上涨", "温和上涨", "下跌", "震荡整理"}
    assert 0 <= ind["bollinger"]["position"] <= 100


def test_compute_matches_rebuild_on_sample():
    """compute_technical_unified 必须与 backfill 的逐字段实现完全一致。"""
    from scripts.backfill.backfill_full_history import rebuild_etf_technical

    n = 45
    rng = np.random.default_rng(7)
    prices = list(100 + np.cumsum(rng.normal(0, 1, n)) + np.arange(n) * 0.3)
    dates = _dates(n)
    code = "159732"
    path = _make_db(prices, dates, code)
    try:
        rebuild_etf_technical(path)
        conn = sqlite3.connect(path)
        row = conn.execute(
            "SELECT ma_signal, macd_signal, rsi_value, rsi_status, "
            "kdj_signal, bollinger_position, atr_pct, trend "
            "FROM etf_technical WHERE code=? ORDER BY date DESC LIMIT 1",
            (code,),
        ).fetchone()
        conn.close()
        assert row is not None, "rebuild 未写出最新行"
        (ma_s, macd_s, rsi_v, rsi_st, kdj_s, boll_p, atr_p, trend) = row

        ind = compute_technical_unified(prices, dates, n - 1)
        assert ma_s == ind["ma"]["signal"]
        assert macd_s == ind["macd"]["signal"]
        assert abs(rsi_v - ind["rsi"]["RSI"]) < 1e-6
        assert rsi_st == ind["rsi"]["status"]
        assert kdj_s == ind["kdj"]["signal"]
        assert abs(boll_p - ind["bollinger"]["position"]) < 1e-6
        assert abs(atr_p - ind["atr"]["ATR_pct"]) < 1e-6
        assert trend == ind["trend"]["trend"]
    finally:
        _cleanup_db(path)


def test_rebuild_latest_uses_kline_last_day_not_today():
    """日频写入的行日期必须等于快照末日（K 线末日），绝不能写 self.today。

    模拟：快照最后一日是 2026-09-10，而运行日 self.today 是更晚的 2026-09-18——
    写入的行必须落在 2026-09-10，而不是 2026-09-18（老的错位 bug）。
    """
    n = 30
    prices = [100 + i * 0.4 for i in range(n)]
    dates = _dates(n, end="2026-09-10")  # 最后一日 = 2026-09-10
    assert dates[-1] == "2026-09-10"
    code = "159732"
    path = _make_db(prices, dates, code)
    try:
        dm = DatabaseManager(path)
        written = dm.rebuild_latest_technical([code])
        assert written == 1

        conn = sqlite3.connect(path)
        rows = conn.execute(
            "SELECT date, code FROM etf_technical WHERE code=?", (code,)
        ).fetchall()
        conn.close()
        assert len(rows) == 1, f"应只写一行，实际 {len(rows)}"
        assert rows[0][0] == "2026-09-10", (
            f"行日期错位：写了 {rows[0][0]}，应为 K 线末日 2026-09-10"
        )
        # 绝不能出现运行日（self.today）的错位行
        assert all(r[0] != "2026-09-18" for r in rows)
    finally:
        _cleanup_db(path)


def test_backfill_dates_align_to_snapshots():
    """全量 rebuild 后，etf_technical 的每一行日期都来自快照日期（=K 线末日）。"""
    from scripts.backfill.backfill_full_history import rebuild_etf_technical

    n = 60
    prices = list(100 + np.cumsum(np.random.default_rng(3).normal(0, 1, n)))
    dates = _dates(n)
    code = "159732"
    path = _make_db(prices, dates, code)
    try:
        total = rebuild_etf_technical(path)
        conn = sqlite3.connect(path)
        snap_dates = {r[0] for r in conn.execute(
            "SELECT date FROM portfolio_snapshots WHERE code=?", (code,))}
        tech_dates = conn.execute(
            "SELECT date FROM etf_technical WHERE code=?", (code,)).fetchall()
        max_tech = conn.execute(
            "SELECT MAX(date) FROM etf_technical WHERE code=?", (code,)).fetchone()[0]
        max_snap = conn.execute(
            "SELECT MAX(date) FROM portfolio_snapshots WHERE code=?", (code,)).fetchone()[0]
        conn.close()

        assert total == len(tech_dates)
        assert max_tech == max_snap, "etf_technical 末日 != 快照末日（K 线末日错位）"
        assert all(d[0] in snap_dates for d in tech_dates), "存在非快照日期的错行"
    finally:
        _cleanup_db(path)
