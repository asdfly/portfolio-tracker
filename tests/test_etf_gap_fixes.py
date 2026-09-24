"""09-22 etf_price_history 断崖根因修复 A/D/B 的回归测试。

覆盖：
  A. 鲜度闸门：etf_price_history 全局最新日严格落后 portfolio_snapshots 最新日
     ⇒ 系统性 error（抓「全表 0 行 / success 假象」盲区，见报告 17 §1.4）。
  D. 单标的以 snapshot 最新日为锚点的缺失天数升级：缺口跨日累积在下一轮即 error，
     不被冻结的参考日历掩盖。
  B. backfill 隔离名单：连续全源失败达阈值进入隔离期，隔离期跳过重试并单独列示。

全程使用临时库（init_all_tables）+ mock，不触碰生产库、不发真实网络请求。
"""
import datetime as _dt
import sqlite3

import pytest

from src.analysis import price_history_gate as gate
from src.analysis.predictor import price_history as ph
from src.utils.db_schema import init_all_tables

_HELD = ["159992", "515120", "159819", "159949", "159770",
         "515010", "159267", "159796", "561910"]  # 9 只真实缺口标的
_OTHER = ["512010", "588000", "159915"]            # 参照（持续更新的）标的


def _mkdb(tmp_path=None):
    # 用内存库避免 Windows 临时文件锁；同一连接内表与隔离名单均持久可见。
    conn = sqlite3.connect(":memory:")
    init_all_tables(conn)
    return conn


def _snap(conn, date, codes):
    for c in codes:
        conn.execute(
            "INSERT OR REPLACE INTO portfolio_snapshots(date, code, name) VALUES(?,?,?)",
            (date, c, c))
    conn.commit()


def _ohlcv(conn, code, dates):
    for d in dates:
        conn.execute(
            "INSERT OR REPLACE INTO etf_price_history"
            "(date, code, open, high, low, close, volume, amount, adj_close, source)"
            " VALUES(?,?,1,1,1,1,1,1,1,'em')",
            (d, code))
    conn.commit()


def test_A_freshness_detects_systemic_zero_rows():
    """etf_price_history 全局最新日(09-22) < snapshot 最新日(09-23) ⇒ 系统性 error。"""
    import tempfile, pathlib
    with tempfile.TemporaryDirectory() as td:
        conn = _mkdb(pathlib.Path(td))
        # snapshot 前进到 09-23；OHLCV 只到 09-22 ⇒ 当日全量回填失败
        _snap(conn, "2026-09-23", _HELD + _OTHER)
        for c in _HELD + _OTHER:
            _ohlcv(conn, c, ["2026-09-21", "2026-09-22"])
        gaps = gate.detect_etf_price_gaps(conn)
        sys = [g for g in gaps if g["code"] == "(systemic)"]
        assert sys, "应检测到系统性鲜度缺口"
        assert sys[0]["severity"] == "error"
        assert "2026-09-23" in sys[0]["note"]
        conn.close()


def test_D_per_code_escalates_accumulated_gap():
    """9 只缺口标的全局最新日(09-21)落后 snapshot 最新日(09-24) ≥3 天 ⇒ 各自 error。"""
    import tempfile, pathlib
    with tempfile.TemporaryDirectory() as td:
        conn = _mkdb(pathlib.Path(td))
        # 参照标的更新到 09-24（使参考日历包含 09-22/23/24）
        _snap(conn, "2026-09-24", _HELD + _OTHER)
        for c in _OTHER:
            _ohlcv(conn, c, ["2026-09-21", "2026-09-22", "2026-09-23", "2026-09-24"])
        # 9 只缺口标的：有完整历史但停在 09-21（真实场景是滞后而非新纳入）
        for c in _HELD:
            _ohlcv(conn, c, ["2026-09-19", "2026-09-20", "2026-09-21"])
        gaps = gate.detect_etf_price_gaps(conn)
        errs = {g["code"]: g for g in gaps if g["severity"] == "error"}
        for c in _HELD:
            assert c in errs, f"缺口标的 {c} 应升级为 error"
            assert errs[c]["missing_count"] >= 3
        # 参照标的不应被判 error
        for c in _OTHER:
            assert c not in errs, f"参照标的 {c} 不应 error"
        conn.close()


def test_B_quarantine_isolates_persistent_failures(monkeypatch):
    """连续全源失败达阈值 ⇒ 进入隔离期；隔离期跳过重试并单独列示。"""
    import tempfile, pathlib

    def _boom(code6, start="20180101", end=None):
        raise RuntimeError("simulated source down")

    monkeypatch.setitem(ph.FETCHERS, "em", _boom)
    monkeypatch.setitem(ph.FETCHERS, "tx", _boom)

    with tempfile.TemporaryDirectory() as td:
        conn = _mkdb(pathlib.Path(td))
        code = "159992"
        # 连续 3 次全源失败 ⇒ 第 3 次达阈值进入隔离
        for i in range(3):
            res = ph.backfill_etf_price_history(conn, [code], log=lambda *_: None)
            assert res.rows == 0
            assert code in res.failed
        q = conn.execute(
            "SELECT fail_count, quarantined_until FROM etf_backfill_quarantine WHERE code=?",
            (code,)).fetchone()
        assert q is not None and q[0] == 3 and q[1], "隔离计数应达 3 且置 quarantined_until"
        # 隔离期内第 4 次：应跳过重试（在 quarantined 名单），不再计入 failed
        res4 = ph.backfill_etf_price_history(conn, [code], log=lambda *_: None)
        assert res4.rows == 0
        assert code in res4.quarantined
        assert code not in res4.failed
        conn.close()


def test_backfill_result_shape():
    """返回类型应为 BackfillResult 且含预期字段。"""
    import tempfile, pathlib
    with tempfile.TemporaryDirectory() as td:
        conn = _mkdb(pathlib.Path(td))
        res = ph.backfill_etf_price_history(conn, [], log=lambda *_: None)
        assert isinstance(res, ph.BackfillResult)
        assert res.rows == 0 and res.attempted == 0
        assert res.failed == [] and res.quarantined == []
        conn.close()
