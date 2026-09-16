#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""P0: 报告数据日期一致性 / 邮件数据就绪闸门 回归测试。

背景（2026-09-15 事故）：
  15:30 管线在阶段一崩溃(rc=1)，portfolio_summary 未写入 09-15（停在 09-14），
  但 scripts/send_report_email.py 的 stale 守卫仍"现场重生"了一份今日报告并推送。
  那份报告页头指标逐项等于 09-14 的 portfolio_summary，持仓表却是 09-15 的
  portfolio_snapshots（合计差 6,735 元），页头日期又用 datetime.now() —— 读者
  肉眼完全看不出这是跨日期拼接报告。

  ⇒ 本文件锁死两条硬约束：
     (1) 任何调用方都不可能在不带可见标记的情况下拿到拼接报告；
     (2) 邮件路径在数据未就绪时拒绝发送（宁可当天不发，也不要发错日期的报告）。

本测试全程使用 tmp_path 现造 SQLite + mock SMTP：不触碰生产库、不真发邮件。
"""
from __future__ import annotations

import importlib.util
import json
import sqlite3
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.enhanced_report import (  # noqa: E402
    DEGRADED_MARKER,
    EnhancedReportBuilder,
    ReportDataInconsistentError,
)

TODAY = date.today().isoformat()
YESTERDAY = (date.today() - timedelta(days=1)).isoformat()


def _cn(d: str) -> str:
    """'2026-09-14' -> '2026年09月14日'（报告页头的中文日期写法）。"""
    return datetime.strptime(d, "%Y-%m-%d").strftime("%Y年%m月%d日")


# ============================================================================
# 造库：结构与生产库一致（只保留本报告用到的列）
# ============================================================================

_SCHEMA = (
    """CREATE TABLE portfolio_summary (
           date TEXT PRIMARY KEY, total_value REAL, total_cost REAL, total_pnl REAL,
           daily_pnl REAL, daily_return REAL, vs_hs300 REAL, profit_count INTEGER,
           loss_count INTEGER, sharpe_ratio REAL, max_drawdown REAL, volatility REAL,
           snapshot_type TEXT DEFAULT 'daily')""",
    """CREATE TABLE portfolio_snapshots (
           id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT NOT NULL, code TEXT NOT NULL,
           name TEXT, quantity REAL, cost_price REAL, current_price REAL,
           market_value REAL, pnl REAL, pnl_rate REAL, ytd_return REAL, beta REAL,
           UNIQUE(date, code))""",
    """CREATE TABLE alerts (
           id INTEGER PRIMARY KEY AUTOINCREMENT, rule_name TEXT, level TEXT, message TEXT,
           created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, acknowledged BOOLEAN DEFAULT 0)""",
    """CREATE TABLE index_quotes (
           date TEXT, code TEXT, name TEXT, close REAL, change_pct REAL)""",
    """CREATE TABLE etf_technical (
           date TEXT, code TEXT, ma_signal TEXT, macd_signal TEXT, rsi_value REAL,
           rsi_status TEXT, kdj_signal TEXT, bollinger_position REAL, atr_pct REAL,
           trend TEXT)""",
)


def _build_db(root: Path) -> Path:
    """在 <root>/data/database/portfolio.db 造一个空库。

    报告构建器把报告目录解析为 <db>/../../..//data/reports，因此 root 必须
    同时容纳 database 与 reports 两个子目录。
    """
    db_path = root / "data" / "database" / "portfolio.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    (root / "data" / "reports").mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    for ddl in _SCHEMA:
        conn.execute(ddl)
    conn.commit()
    conn.close()
    return db_path


def _add_summary(db_path: Path, d: str, total_value=1000000.0) -> None:
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "INSERT OR REPLACE INTO portfolio_summary "
        "(date, total_value, total_cost, total_pnl, daily_pnl, daily_return, "
        " profit_count, loss_count, sharpe_ratio, max_drawdown, volatility) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (d, total_value, 900000.0, 100000.0, -1000.0, -0.5, 3, 2, 0.8, 6.7, 22.0),
    )
    conn.commit()
    conn.close()


def _add_snapshot(db_path: Path, d: str, code: str, name: str, mv=100000.0) -> None:
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "INSERT OR REPLACE INTO portfolio_snapshots "
        "(date, code, name, quantity, cost_price, current_price, market_value, pnl, pnl_rate) "
        "VALUES (?,?,?,?,?,?,?,?,?)",
        (d, code, name, 10000.0, 1.0, 10.0, mv, 500.0, 1.5),
    )
    conn.commit()
    conn.close()


def _add_alert(db_path: Path, created_at: str, message: str, level="warning") -> None:
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "INSERT INTO alerts (rule_name, level, message, created_at) VALUES (?,?,?,?)",
        ("rule_x", level, message, created_at),
    )
    conn.commit()
    conn.close()


def _write_smart_report(root: Path, d: str, gen_time: str = "15:34") -> Path:
    """在 <root>/data/reports 写一份 smart_report_<d>.md。"""
    reports = root / "data" / "reports"
    reports.mkdir(parents=True, exist_ok=True)
    path = reports / ("smart_report_" + d.replace("-", "") + ".md")
    path.write_text(
        "# 投资组合智能分析报告\n"
        f"**生成时间**: {d} {gen_time}\n"
        f"**报告周期**: {d}\n\n"
        "### 1. [中] 测试建议\n",
        encoding="utf-8",
    )
    return path


def _write_run_report(root: Path, d: str, payload: dict) -> Path:
    reports = root / "data" / "reports"
    reports.mkdir(parents=True, exist_ok=True)
    path = reports / f"run_report_{d}.json"
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


@pytest.fixture
def inconsistent_env(tmp_path):
    """summary 停在 YESTERDAY、snapshots 已到 TODAY —— 复刻 09-15 事故现场。"""
    db = _build_db(tmp_path)
    _add_summary(db, YESTERDAY)
    _add_snapshot(db, YESTERDAY, "AAAAAA", "旧持仓", mv=800000.0)
    _add_snapshot(db, TODAY, "BBBBBB", "新持仓", mv=900000.0)
    _add_alert(db, f"{YESTERDAY}T15:31:14.641471", "OLD_ALERT_0914")
    _add_alert(db, f"{TODAY}T15:31:14.641471", "NEW_ALERT_0916")
    return tmp_path, db


@pytest.fixture
def consistent_env(tmp_path):
    """summary 与 snapshots 同为 TODAY —— 正常日。"""
    db = _build_db(tmp_path)
    _add_summary(db, TODAY)
    _add_snapshot(db, TODAY, "CCCCCC", "今日持仓", mv=1000000.0)
    _add_alert(db, f"{TODAY}T15:31:14.641471", "TODAY_ALERT")
    return tmp_path, db


# ============================================================================
# 1. 报告层：数据日期一致性
# ============================================================================

def test_inconsistent_dates_report_is_visibly_marked(inconsistent_env):
    """日期不一致时，报告必须带降级标记，且各区块统一按 summary 数据日期取值。"""
    _root, db = inconsistent_env
    html = EnhancedReportBuilder(str(db)).build_full_report()

    # (a) 机器可读标记 + 人眼可见的红色降级横幅
    assert DEGRADED_MARKER in html
    assert "数据降级" in html
    assert "不可作为决策依据" in html
    assert "fdecea" in html  # 红色横幅底色

    # (b) 页头数据日期 = summary 的数据日期，且**不是**今天(不能用 datetime.now() 伪装)
    assert "数据日期: " + _cn(YESTERDAY) in html
    assert _cn(TODAY) not in html

    # (c) 数据日期与生成时间分开显示
    assert "生成时间: " in html

    # (d) 持仓表按 summary 数据日期取，不出现 TODAY 快照的那只
    assert "AAAAAA" in html
    assert "BBBBBB" not in html

    # (e) 告警只渲染数据日期当天的
    assert "OLD_ALERT_0914" in html
    assert "NEW_ALERT_0916" not in html


def test_strict_mode_raises_instead_of_splicing(inconsistent_env):
    """strict=True 时抛专用异常 —— 邮件等调用方据此拒绝发送。"""
    _root, db = inconsistent_env
    with pytest.raises(ReportDataInconsistentError) as ei:
        EnhancedReportBuilder(str(db)).build_full_report(strict=True)
    assert ei.value.summary_date == YESTERDAY
    assert ei.value.snapshot_date == TODAY


def test_consistent_dates_report_is_clean(consistent_env):
    """日期一致（正常日）时正常出报告，不得出现降级标记。"""
    _root, db = consistent_env
    html = EnhancedReportBuilder(str(db)).build_full_report()

    assert DEGRADED_MARKER not in html
    assert "数据降级" not in html
    assert "数据日期: " + _cn(TODAY) in html
    assert "CCCCCC" in html
    assert "TODAY_ALERT" in html


def test_load_alerts_filters_by_report_date(inconsistent_env):
    """_load_alerts 只返回与报告数据日期相同的告警（原来取"最近 5 条"）。"""
    _root, db = inconsistent_env
    builder = EnhancedReportBuilder(str(db))

    old = builder._load_alerts(YESTERDAY)
    new = builder._load_alerts(TODAY)
    assert [a["message"] for a in old] == ["OLD_ALERT_0914"]
    assert [a["message"] for a in new] == ["NEW_ALERT_0916"]


def test_load_alerts_without_date_falls_back_to_summary_date(inconsistent_env):
    """不传日期时以 portfolio_summary 的数据日期为准，绝不取"最新"告警。"""
    _root, db = inconsistent_env
    rows = EnhancedReportBuilder(str(db))._load_alerts()
    assert [a["message"] for a in rows] == ["OLD_ALERT_0914"]


# ============================================================================
# 2. 邮件层：数据就绪闸门
# ============================================================================

def _load_email_module():
    """按文件路径加载 scripts/send_report_email.py（scripts 不是包）。"""
    spec = importlib.util.spec_from_file_location(
        "send_report_email_under_test", PROJECT_ROOT / "scripts" / "send_report_email.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def sre():
    return _load_email_module()


def test_gate_refuses_when_summary_is_stale(sre, inconsistent_env):
    root, db = inconsistent_env
    ok, reason, summary_date = sre.data_readiness_gate(str(db), TODAY, root / "data" / "reports")
    assert ok is False
    assert summary_date == YESTERDAY
    assert YESTERDAY in reason


def test_gate_passes_when_summary_is_today(sre, consistent_env):
    """run_report 尚无 run_status 字段（旧文件）时不得因此拒绝。"""
    root, db = consistent_env
    _write_run_report(root, TODAY, {"date": TODAY, "dq_score": 100})  # 无 run_status
    ok, reason, summary_date = sre.data_readiness_gate(str(db), TODAY, root / "data" / "reports")
    assert ok is True, reason
    assert summary_date == TODAY


def test_gate_passes_when_run_report_missing(sre, consistent_env):
    """run_report 文件缺失 = 状态未知，不得因此拒绝。"""
    root, db = consistent_env
    ok, _reason, _d = sre.data_readiness_gate(str(db), TODAY, root / "data" / "reports")
    assert ok is True


@pytest.mark.parametrize("status", ["partial", "failed"])
def test_gate_blocks_incomplete_run_status(sre, consistent_env, status):
    root, db = consistent_env
    _write_run_report(root, TODAY, {"date": TODAY, "run_status": status})
    ok, reason, _d = sre.data_readiness_gate(str(db), TODAY, root / "data" / "reports")
    assert ok is False
    assert status in reason


def test_gate_tolerates_corrupt_or_missing_run_status_key(sre, consistent_env):
    """JSON 损坏 / run_status 为 null 都视为"未知"，不拒绝。"""
    root, db = consistent_env
    reports = root / "data" / "reports"
    reports.mkdir(parents=True, exist_ok=True)

    (reports / f"run_report_{TODAY}.json").write_text("{not json", encoding="utf-8")
    assert sre.data_readiness_gate(str(db), TODAY, reports)[0] is True

    (reports / f"run_report_{TODAY}.json").write_text(
        json.dumps({"date": TODAY, "run_status": None}), encoding="utf-8")
    assert sre.data_readiness_gate(str(db), TODAY, reports)[0] is True


def test_read_run_status_returns_none_for_legacy_file(sre, tmp_path):
    """2026-09-15 及以前的 run_report 没有 run_status 字段 → None（未知）。"""
    reports = tmp_path / "reports"
    reports.mkdir()
    (reports / "run_report_2026-09-15.json").write_text(
        json.dumps({"date": "2026-09-15", "dq_score": 100}), encoding="utf-8")
    assert sre.read_run_status("2026-09-15", reports) is None
    assert sre.read_run_status("2026-09-01", reports) is None  # 文件不存在


# ============================================================================
# 3. 邮件层：md 摘要校验
# ============================================================================

def test_validate_summary_md_rejects_other_date(sre, tmp_path):
    md = _write_smart_report(tmp_path, YESTERDAY)
    ok, reason = sre.validate_summary_md(str(md), TODAY)
    assert ok is False
    assert YESTERDAY in reason


def test_validate_summary_md_rejects_intraday_draft(sre, tmp_path):
    """当天 09:18 的盘中稿不得当作收盘稿附上。"""
    md = _write_smart_report(tmp_path, TODAY, gen_time="09:18")
    ok, reason = sre.validate_summary_md(str(md), TODAY)
    assert ok is False
    assert "盘中稿" in reason


def test_validate_summary_md_accepts_closing_draft(sre, tmp_path):
    md = _write_smart_report(tmp_path, TODAY, gen_time="15:34")
    ok, reason = sre.validate_summary_md(str(md), TODAY)
    assert ok is True, reason


def test_validate_summary_md_without_md(sre):
    ok, _reason = sre.validate_summary_md(None, TODAY)
    assert ok is False


# ============================================================================
# 4. 邮件层：main() 端到端（SMTP 全程 mock，绝不真发邮件）
# ============================================================================

class _FakeSMTP:
    instances: list = []

    def __init__(self, server, port, timeout=None):
        self.server = server
        self.port = port
        self.sent = []
        _FakeSMTP.instances.append(self)

    def starttls(self):
        return None

    def login(self, user, password):
        return None

    def sendmail(self, sender, recipients, body):
        self.sent.append((sender, recipients, body))

    def quit(self):
        return None


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


@pytest.fixture
def email_harness(sre, tmp_path, monkeypatch):
    """把 ROOT / DATABASE_PATH / SMTP / argv 全部指向沙箱，返回 (module, root)。"""
    _FakeSMTP.instances = []
    monkeypatch.setattr(sre, "ROOT", str(tmp_path))
    monkeypatch.setattr(sre, "NOTIFICATION_CONFIG", _EMAIL_CFG)
    monkeypatch.setattr(
        sre, "smtplib",
        SimpleNamespace(SMTP=_FakeSMTP, SMTP_SSL=_FakeSMTP),
    )
    monkeypatch.setattr(sys, "argv", ["send_report_email.py"])
    import config.settings as _cs
    db = tmp_path / "data" / "database" / "portfolio.db"
    monkeypatch.setattr(_cs, "DATABASE_PATH", db)
    return sre, tmp_path


def test_main_refuses_and_never_touches_smtp_when_summary_stale(email_harness, inconsistent_env):
    """崩溃日（summary 停在昨天）：rc != 0 且一次都没连 SMTP。"""
    sre, root = email_harness
    src_db = inconsistent_env[1]
    (root / "data" / "database").mkdir(parents=True, exist_ok=True)
    (root / "data" / "database" / "portfolio.db").write_bytes(src_db.read_bytes())

    rc = sre.main()

    assert rc != 0
    assert _FakeSMTP.instances == []


def test_main_sends_when_data_ready(email_harness, consistent_env):
    """正常日：数据齐备 → rc == 0 且确实调用了一次 sendmail（SMTP 为 mock）。"""
    sre, root = email_harness
    src_db = consistent_env[1]
    (root / "data" / "database").mkdir(parents=True, exist_ok=True)
    (root / "data" / "database" / "portfolio.db").write_bytes(src_db.read_bytes())

    _write_run_report(root, TODAY, {"date": TODAY, "run_status": "ok"})
    md = _write_smart_report(root, TODAY, gen_time="15:34")

    rc = sre.main()

    assert rc == 0
    assert len(_FakeSMTP.instances) == 1
    _sender, recipients, raw = _FakeSMTP.instances[0].sent[0]
    assert recipients == ["asdfl@qq.com"]
    assert DEGRADED_MARKER not in raw

    import email as _email
    parsed = _email.message_from_string(raw)
    decoded = b"".join(
        (p.get_payload(decode=True) or b"")
        for p in parsed.walk() if p.get_content_maintype() == "text"
    ).decode("utf-8", "replace")
    assert "投资组合智能分析报告" in decoded          # 正文/纯文本兜底
    assert "数据日期: " + _cn(TODAY) in decoded        # 页头数据日期来自数据
    # 收盘稿摘要作为附件带上（md 校验通过）
    assert md.name in raw


def test_main_refuses_degraded_body(email_harness, consistent_env, monkeypatch):
    """兜底：正文只要带降级标记就拒绝发送 —— 不存在"静默降级"这条路。"""
    sre, root = email_harness
    src_db = consistent_env[1]
    (root / "data" / "database").mkdir(parents=True, exist_ok=True)
    (root / "data" / "database" / "portfolio.db").write_bytes(src_db.read_bytes())
    _write_run_report(root, TODAY, {"date": TODAY, "run_status": "ok"})

    from src.utils import enhanced_report as er
    monkeypatch.setattr(
        er.EnhancedReportBuilder, "build_full_report",
        lambda self, *a, **k: f"<html><!-- {DEGRADED_MARKER} -->降级</html>",
    )

    rc = sre.main()

    assert rc != 0
    assert _FakeSMTP.instances == []
