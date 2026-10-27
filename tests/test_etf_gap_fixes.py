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
    """etf_price_history 全局最新日(09-22) < snapshot 最新日(09-23) ⇒ 系统性**待回填 warning**。

    ⚠ 契约变更（2026-10-27）：原断言为 `severity == "error"`。改为 warning 的理由是实测
      推翻了原前提「正常 post-close 跑批后两者应相等」——本管线 15:30 跑批，当日日线要到
      收盘后才有、21:45 才兜底回填 ⇒ 15:34 时「当日尚未回填」是常态。实测 2026-10-13~10-26
      每个被判 error 的日期事后全部补齐（无一天真缺），却累计 14 条 error 并拦掉 15/18 次日报。
      现按交易日宽限：滞后 1 个交易日 ⇒ warning；≥2 ⇒ error（见下一条用例）。
      「全表 0 行」这个 A 唯一存在的理由仍成立 —— 真断崖会随时间累积到 ≥2 个交易日。
      本用例保留「日期落在 lookback 窗口之外」的写法，仍用于验证 A 独立于参考日历：

    ⚠ 保留这批"落在 lookback 窗口之外"的绝对日期，是刻意的契约验证（2026-10-14）：
      A 鲜度闸门按设计必须**独立于参考日历**（否则抓不到「全表 0 行 → 日历冻结」
      这个它唯一存在的理由）。旧实现把 A 排在 `if not cal: return []` 之后，
      日历一空就整体提前返回 ⇒ 这里 gaps=[] 假失败。日期越旧，此契约越好使。
      （依赖参考日历的用例见 test_D，那里必须改用相对日期。）
    """
    import tempfile, pathlib
    with tempfile.TemporaryDirectory() as td:
        conn = _mkdb(pathlib.Path(td))
        # snapshot 前进到 09-23；OHLCV 只到 09-22 ⇒ 落后 1 个交易日 = 当日待回填
        _snap(conn, "2026-09-23", _HELD + _OTHER)
        for c in _HELD + _OTHER:
            _ohlcv(conn, c, ["2026-09-21", "2026-09-22"])
        gaps = gate.detect_etf_price_gaps(conn)
        sys = [g for g in gaps if g["code"] == "(systemic)"]
        assert sys, "应检测到系统性鲜度缺口"
        assert sys[0]["severity"] == "warning", "1 个交易日 ⇒ warning（待回填，不拒发）"
        assert sys[0]["missing_count"] == 1, "宽限必须按交易日计，不是自然日"
        assert "2026-09-23" in sys[0]["note"]
        assert "待回填" in sys[0]["note"]
        assert "全量回填失败" not in sys[0]["note"], "待回填不得写成断崖"
        conn.close()


def _systemic_case(snap_date, price_dates, monkeypatch=None, strict=False):
    """构造「snapshot 前进、OHLCV 落后」的最小库，返回 systemic gap。"""
    import tempfile, pathlib
    if strict:
        monkeypatch.setenv("ETF_GAP_STRICT", "1")
    else:
        monkeypatch.delenv("ETF_GAP_STRICT", raising=False)
    with tempfile.TemporaryDirectory() as td:
        conn = _mkdb(pathlib.Path(td))
        _snap(conn, snap_date, _HELD + _OTHER)
        for c in _HELD + _OTHER:
            _ohlcv(conn, c, price_dates)
        gaps = gate.detect_etf_price_gaps(conn)
        sys = [g for g in gaps if g["code"] == "(systemic)"]
        conn.close()
    assert sys, "应产出 systemic 判定"
    return sys[0]


def test_A_systemic_two_trading_days_is_error(monkeypatch):
    """落后 2 个交易日 ⇒ error（09-23 形态仍必须抓到）。"""
    g = _systemic_case("2026-09-24", ["2026-09-21", "2026-09-22"], monkeypatch)
    assert g["severity"] == "error"
    assert g["missing_count"] == 2
    assert "全量回填失败" in g["note"]


def test_A_systemic_weekend_span_counts_one_td(monkeypatch):
    """周五 → 周一：自然日 3 天、交易日 1 天 ⇒ warning。

    这是「按自然日算宽限」会漏掉的分支：实测 2026-10-19（周一）被判 error，
    而它只是「周一当日 OHLCV 未回填」。若宽限按自然日实现，本用例会红。
    """
    g = _systemic_case("2026-10-19", ["2026-10-15", "2026-10-16"], monkeypatch)
    assert g["severity"] == "warning"
    assert g["missing_count"] == 1


def test_A_systemic_holiday_span_counts_one_td(monkeypatch):
    """节前 09-30 → 节后首日 10-08：自然日 8 天、交易日 1 天 ⇒ warning。

    ⚠ 这条同时是「不能用 index_quotes 当交易日轴」的守卫：库内 index_quotes 在
      国庆 10-01~10-07 有历史行（旧采集器缺陷），用它会算出 6 个交易日 ⇒ 误判 error。
    """
    g = _systemic_case("2026-10-08", ["2026-09-29", "2026-09-30"], monkeypatch)
    assert g["severity"] == "warning"
    assert g["missing_count"] == 1


def test_A_strict_env_restores_old_behaviour(monkeypatch):
    """ETF_GAP_STRICT=1 ⇒ 恢复旧行为：滞后 1 个交易日也判 error（逃生阀）。"""
    g = _systemic_case("2026-10-27", ["2026-10-23", "2026-10-26"], monkeypatch, strict=True)
    assert g["severity"] == "error"
    assert "ETF_GAP_STRICT=1" in g["note"]


def test_check_pending_does_not_write_alert_and_is_ok(monkeypatch):
    """待回填 ⇒ ok=True、alert_level=warning、**不写 alerts 表**（不产生告警疲劳）。"""
    import pathlib
    import tempfile
    monkeypatch.delenv("ETF_GAP_STRICT", raising=False)
    with tempfile.TemporaryDirectory() as td:
        db = pathlib.Path(td) / "p.db"
        conn = sqlite3.connect(str(db))
        init_all_tables(conn)
        _snap(conn, "2026-10-27", _HELD + _OTHER)
        for c in _HELD + _OTHER:
            _ohlcv(conn, c, ["2026-10-23", "2026-10-26"])
        conn.close()

        res = gate.check_etf_price_history_gaps(str(db))
        assert res["ok"] is True
        assert res["alert_level"] == "warning"
        assert "待回填（非缺口）" in res["message"]
        conn = sqlite3.connect(str(db))
        n = conn.execute("SELECT COUNT(*) FROM alerts WHERE rule_name='etf_price_gap'").fetchone()[0]
        conn.close()
        assert n == 0, "warning 级不得写 alerts 表"



def test_D_per_code_escalates_accumulated_gap():
    """9 只缺口标的全局最新日落后 snapshot 最新日 ≥3 天 ⇒ 各自 error。

    ⚠ 日期必须相对 `today` 生成（2026-10-14 修复）：本用例依赖参考日历，而
      `_reference_calendar` 只取 `today - GAP_LOOKBACK_DAYS(=15)` 之后的数据。
      原用例写死 2026-09-19~09-24，距当天已超 lookback 窗口 ⇒ 日历为空 ⇒
      单标的判定整段跳过 → 假失败。用相对日期后与时点无关，永久有效。
    """
    import tempfile, pathlib
    ref = _dt.date.today() - _dt.timedelta(days=8)      # 锚点，位于 lookback 窗口内
    d = lambda n: (ref - _dt.timedelta(days=n)).isoformat()   # noqa: E731
    with tempfile.TemporaryDirectory() as td:
        conn = _mkdb(pathlib.Path(td))
        # 参照标的更新到锚点日（使参考日历覆盖 ref-3..ref）
        _snap(conn, d(0), _HELD + _OTHER)
        for c in _OTHER:
            _ohlcv(conn, c, [d(3), d(2), d(1), d(0)])
        # 9 只缺口标的：有完整历史但停在 ref-3（真实场景是滞后而非新纳入）
        for c in _HELD:
            _ohlcv(conn, c, [d(5), d(4), d(3)])
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
