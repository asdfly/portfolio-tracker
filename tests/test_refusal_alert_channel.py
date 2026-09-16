#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""裁定 3 取证：失败/拒绝场景下告警通道与监控账本**确实**被调用（无静默窗口）。

team-lead 的原文问题：
    "是否存在「portfolio_summary 停更（于是闸门拒绝）但 run_status == 'ok'
     （于是不发失败告警）」的组合？如果存在，用户当天会完全静默。"

本文件用 mock 把两条链路各跑一遍并断言实际调用，不依赖推理：

  生产实证（logs/scheduled_run.log 2026-09-15）:
      16435| src.utils.monitor - INFO - 任务执行记录: portfolio_daily_analysis - failed
      16436| src.utils.notification - INFO - 邮件发送成功: [ERROR] 投资组合告警 - Daily Analysis Failed
  即崩溃当天 [ERROR] 告警确实发出；测试 1 把这条链路锁死，防回归。

  测试 2 锁死"非交易日早退"：run_analysis 在创建 RunReporter **之前** return 0
  ⇒ 当天没有 run_report、没有任何告警 —— 这是设计内行为，但会让邮件闸门在周末
  以 rc≠0 退出（假失败）。刻意保留：把周末判成"无需日报"会制造真正的静默窗口
  （例如周五机器没开机、周六补跑时用户将既收不到周五报告也收不到任何提示）。

  测试 3/4 覆盖残留的真静默窗口：进程被 os._exit 强杀（看门狗超时/断电/任务终止）时
  except 与 finally 都不执行，唯一痕迹只能是邮件脚本拒绝时写进监控账本的那条记录。

全程 mock：不发邮件、不跑网络、不触碰生产库（DATABASE_PATH 与 Monitor 均被替换）。
"""
from __future__ import annotations

import importlib.util
import sqlite3
import sys
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TODAY = date.today().isoformat()

_EMAIL_CFG = {
    "email": {
        "enabled": True,
        "username": "sender@example.com",
        "password": "not-a-real-password",
        "recipients": ["asdfl@qq.com"],
        "smtp_server": "smtp.example.invalid",
        "smtp_port": 587,
    }
}


# ============================================================================
# A. run_analysis.main()：阶段一崩溃 ⇒ 必须发 [ERROR] 告警 + 记监控账本
# ============================================================================

@pytest.fixture
def ra_harness(tmp_path, monkeypatch):
    """把 run_analysis.main() 的外部依赖全部替换为记录器，返回 (module, calls)。

    替换点取自真实调用链（run_analysis.py:971-1446）：
      run_database_backup / is_trading_day / install_requests_timeout /
      start_watchdog / cancel_watchdog / run_with_hard_timeout（网络探测）/
      run_stage0_otc_nav / run_stage0b_watchlist / PortfolioAnalyzer /
      RunReporter / Monitor / NotificationManager / get_db_connection
    """
    import run_analysis as ra

    calls = {"alert": [], "ledger": [], "mark_failed": [], "reporter_init": []}

    class _FakeReporter:
        def __init__(self, date_str, mode="daily", **kw):
            calls["reporter_init"].append((date_str, mode))
            self.date = date_str
            self.run_date = date_str
            self.mode = mode
            self.stages = {}

        def stage(self, name, status="ok", note=""):
            self.stages[name] = status

        def record_source(self, *a, **k):
            return None

        def mark_hang_recovered(self):
            return None

        def mark_run_failed(self, reason):
            calls["mark_failed"].append(reason)

        def finalize_and_write(self, **kw):
            return ({"run_status": "failed"}, None)

    class _FakeMonitor:
        def __init__(self, *a, **k):
            pass

        def log_execution(self, task, status, message="", duration=0):
            calls["ledger"].append((task, status, message))

    class _FakeNotifier:
        def __init__(self, *a, **k):
            pass

        def send_alert(self, alert_type, message, level="warning"):
            calls["alert"].append((alert_type, message, level))

    class _FakeAnalyzer:
        def __init__(self):
            self.today = "1970-01-01"

    def _no_db(*a, **k):
        raise RuntimeError("测试内禁止连数据库")

    monkeypatch.setattr(ra, "setup_logging", lambda: None)
    monkeypatch.setattr(ra, "run_database_backup", lambda *a, **k: None)
    monkeypatch.setattr(ra, "is_trading_day", lambda: True)
    monkeypatch.setattr(ra, "install_requests_timeout", lambda **k: None)
    monkeypatch.setattr(ra, "start_watchdog", lambda **k: None)
    monkeypatch.setattr(ra, "cancel_watchdog", lambda: None)
    monkeypatch.setattr(ra, "run_with_hard_timeout", lambda *a, **k: None)
    monkeypatch.setattr(ra, "run_stage0_otc_nav", lambda *a, **k: None)
    monkeypatch.setattr(ra, "run_stage0b_watchlist", lambda *a, **k: None)
    monkeypatch.setattr(ra, "PortfolioAnalyzer", _FakeAnalyzer)
    monkeypatch.setattr(ra, "RunReporter", _FakeReporter)
    monkeypatch.setattr(ra, "Monitor", _FakeMonitor)
    monkeypatch.setattr(ra, "NotificationManager", _FakeNotifier)
    monkeypatch.setattr(ra, "get_db_connection", _no_db)
    monkeypatch.setattr(ra, "DATABASE_PATH", tmp_path / "fake.db")
    monkeypatch.setattr(ra, "MONITOR_CONFIG", {})
    monkeypatch.setattr(ra, "NOTIFICATION_CONFIG", {})
    monkeypatch.setitem(sys.modules, "akshare", SimpleNamespace(
        stock_zh_a_spot_em=lambda: None,
        stock_board_industry_name_em=lambda: None,
        stock_hsgt_hist_em=lambda **k: None,
    ))
    return ra, calls


def test_stage_one_crash_calls_error_alert_and_ledger(ra_harness, monkeypatch):
    """阶段一崩溃（09-15 同款 ValueError）⇒ rc=1 + mark_run_failed + 账本 failed + [ERROR] 告警。"""
    ra, calls = ra_harness

    def _boom(analyzer):
        raise RuntimeError("All arrays must be of the same length")

    monkeypatch.setattr(ra, "run_stage1_basic", _boom)

    rc = ra.main([])

    assert rc == 1
    # 1) 显式失败标记 ⇒ run_report 必为 failed（不再是 09-15 那种 dq_score=100 的伪装）
    assert calls["mark_failed"] == ["RuntimeError: All arrays must be of the same length"]
    # 2) 进入主流程时先记 running，失败时再记 failed（监控账本两条痕迹）
    assert ("portfolio_daily_analysis", "running", "开始每日完整分析") in calls["ledger"]
    assert ("portfolio_daily_analysis", "failed",
            "All arrays must be of the same length") in calls["ledger"]
    # 3) [ERROR] 告警通道确实被调用过（与生产日志 16436 行同一条路径）
    assert calls["alert"] == [
        ("Daily Analysis Failed", "All arrays must be of the same length", "error")
    ]


def test_trading_day_early_return_stays_silent_by_design(ra_harness, monkeypatch):
    """非交易日（周末）早退 ⇒ rc=0、无 report、无告警、无账本（设计内行为）。

    这解释了"周末 rc≠0"的来源：run_analysis 早退后，随后的 send_report_email 仍会
    因 summary != 今天 而拒绝（rc=1）。属"假失败"噪声，刻意不消除 —— 把周末判成
    "无需日报"会掩盖"周五没跑成、周六补跑"这类真缺口。
    """
    ra, calls = ra_harness
    monkeypatch.setattr(ra, "is_trading_day", lambda: False)

    rc = ra.main([])

    assert rc == 0
    assert calls["reporter_init"] == []   # 早退发生在 RunReporter 创建之前 ⇒ 当天无 run_report
    assert calls["alert"] == []
    assert calls["ledger"] == []
    assert calls["mark_failed"] == []


# ============================================================================
# B. send_report_email.main()：拒绝时必须写监控账本（残留静默窗口的唯一痕迹）
# ============================================================================

def _load_email_module():
    spec = importlib.util.spec_from_file_location(
        "sre_alert_probe", str(PROJECT_ROOT / "scripts" / "send_report_email.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _make_db(root: Path, summary_date: str | None) -> Path:
    """造/复用 <root>/data/database/portfolio.db，并把 summary 精确设成 summary_date。"""
    db = root / "data" / "database" / "portfolio.db"
    db.parent.mkdir(parents=True, exist_ok=True)
    (root / "data" / "reports").mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db))
    conn.execute("CREATE TABLE IF NOT EXISTS portfolio_summary "
                 "(date TEXT PRIMARY KEY, total_value REAL)")
    conn.execute("DELETE FROM portfolio_summary")
    if summary_date:
        conn.execute("INSERT INTO portfolio_summary (date, total_value) VALUES (?,?)",
                     (summary_date, 1000000.0))
    conn.commit()
    conn.close()
    return db


def _write_run_report(root: Path, d: str, payload: dict) -> Path:
    import json
    p = root / "data" / "reports" / f"run_report_{d}.json"
    p.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return p


@pytest.fixture
def ledger_recorder(monkeypatch):
    """替换监控账本写入，记录调用并确保测试绝不碰真实库。"""
    from src.utils import monitor as _monitor

    seen: list = []

    def _fake_log_execution(self, task_name, status, message="", duration=0):
        seen.append((task_name, status, message))
        return None

    monkeypatch.setattr(_monitor.Monitor, "log_execution", _fake_log_execution)
    return seen


@pytest.fixture
def email_env(tmp_path, monkeypatch, ledger_recorder):
    sre = _load_email_module()
    monkeypatch.setattr(sre, "ROOT", str(tmp_path))
    monkeypatch.setattr(sre, "NOTIFICATION_CONFIG", _EMAIL_CFG)
    monkeypatch.setattr(sys, "argv", ["send_report_email.py"])
    import config.settings as _cs
    db = _make_db(tmp_path, summary_date=None)
    monkeypatch.setattr(_cs, "DATABASE_PATH", db)
    return sre, tmp_path, ledger_recorder, db


def test_refused_report_is_recorded_in_monitor_ledger(email_env):
    """崩溃日：闸门拒绝 ⇒ rc=1 且账本留下 daily_report_not_sent。"""
    sre, root, ledger, db = email_env
    _make_db(root, summary_date="2020-01-02")   # summary 停更
    _write_run_report(root, TODAY, {"date": TODAY, "run_status": "failed"})

    rc = sre.main()

    assert rc == 1
    assert [t[0] for t in ledger] == [sre._NOT_SENT_LEDGER_TASK]
    task, status, message = ledger[0]
    assert status == "failed"
    assert "拒绝发送" in message and TODAY in message


def test_stale_summary_is_refused_even_when_run_status_ok(email_env):
    """run_status="ok" 不能绕过数据日期闸门 —— 两个判据相互独立。"""
    sre, root, ledger, db = email_env
    _make_db(root, summary_date="2020-01-02")
    _write_run_report(root, TODAY, {"date": TODAY, "run_status": "ok"})

    rc = sre.main()

    assert rc == 1
    assert len(ledger) == 1


def test_legacy_run_report_without_run_status_still_gated_by_summary_date(email_env):
    """兼容性：旧 run_report 无 run_status 字段时，仍由 summary 日期判定拒绝。"""
    sre, root, ledger, db = email_env
    _make_db(root, summary_date="2020-01-02")
    # 09-15 及以前的真实文件形态：没有 run_status 键
    _write_run_report(root, TODAY, {"date": TODAY, "dq_score": 100, "alerts": []})

    assert sre.read_run_status(TODAY) is None
    assert sre.main() == 1
    assert len(ledger) == 1


def test_ledger_failure_never_breaks_the_refusal(email_env, monkeypatch):
    """账本不可用（表缺失/库锁死）时必须保持"拒绝"结论不变。"""
    sre, root, ledger, db = email_env
    _make_db(root, summary_date="2020-01-02")

    from src.utils import monitor as _monitor

    def _explode(self, *a, **k):
        raise sqlite3.OperationalError("no such table: execution_logs")

    monkeypatch.setattr(_monitor.Monitor, "log_execution", _explode)

    assert sre.main() == 1


def test_smtp_failure_is_recorded_in_ledger(email_env, monkeypatch):
    """SMTP 发送失败同样是"用户当天收不到报告"，必须留下账本痕迹。"""
    sre, root, ledger, db = email_env
    _make_db(root, summary_date=TODAY)
    html = root / "data" / "reports" / f"enhanced_report_{TODAY.replace('-', '')}.html"
    html.write_text("<html>ok</html>", encoding="utf-8")
    monkeypatch.setattr(
        sre, "_resolve_report",
        lambda today_str: {"html_path": str(html), "md_path": None, "report_date": today_str},
    )

    from src.utils import enhanced_report as _er
    monkeypatch.setattr(
        _er.EnhancedReportBuilder, "build_full_report",
        lambda self, *a, **k: "<html><body>干净正文</body></html>",
    )

    class _DeadSMTP:
        def __init__(self, *a, **k):
            pass

        def starttls(self):
            return None

        def login(self, *a, **k):
            return None

        def sendmail(self, *a, **k):
            raise OSError("smtp unreachable")

        def quit(self):
            return None

    monkeypatch.setattr(sre, "smtplib", SimpleNamespace(SMTP=_DeadSMTP, SMTP_SSL=_DeadSMTP))

    rc = sre.main()

    assert rc == 1
    assert [t[0] for t in ledger] == [sre._NOT_SENT_LEDGER_TASK]
    assert "未送达" in ledger[0][2]
