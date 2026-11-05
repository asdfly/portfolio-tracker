#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""治本（报告库漂移 #140 哨兵值同形）回归：回填/重算 portfolio_summary 前的「可写」闸门 + 重渲染队列。

根因（2026-10-26）：当日快照仅 12 行（基线 34/35），回填据此写出
total_value=510,118.64 并由报告固化；后续快照补齐至 35 行后重算又把同一行静默改写
为 1,416,039.63 ⇒ 静态 HTML 报告无法跟随 ⇒ 哨兵 MATCH→DIFFER。根因 = 摘要写入方
未复用「快照基线闸门」而直接 INSERT OR REPLACE。

修复（`src/analysis/snapshot_gate.py` 新增 `is_summary_writable` + `queue_report_rerender_if_drifted`，
并由 `src/utils/backfill.py` 与 `scripts/backfill/backfill_full_history.py` 在落库前调用）：
  - 缺行快照 ⇒ 拒绝写 portfolio_summary（与日频路径 check_snapshot_baseline 同口径）；
  - 已发布报告与本次算出值不一致 ⇒ 记入 audit/report_rerender_queue.jsonl（可见、可动作），
    把静默漂移转为可消费队列，而非让哨兵无差别每轮告警。

本文件反证：
  W1)  缺行快照（12 行 vs 基线 34）⇒ is_summary_writable 返回 False；
  W2)  完整 34 行 ⇒ True；
  W3)  历史稀疏日（基线不可判定）⇒ True（不误伤古早日/全新库）；
  W4)  周五 35 行 ⇒ True；
  W5)  重渲染队列：报告内嵌值与算出值相差 > 阈值 ⇒ 追加队列项；
  W6)  报告内嵌值≈算出值（< 阈值）⇒ 不追加；
  W7)  无报告文件 ⇒ 不追加（静默 noop）；
  W8)  幂等：同日同算出值重复调用 ⇒ 队列不新增同日期项；
  W9)  队列超上限 ⇒ 裁剪到末尾 N 行（不无限增长）；
  W10) 源码顺序：两处 summary 写入方均在 INSERT/REPLACE 之前调用 is_summary_writable，
       且都挂了重渲染队列调用。

全程只在 tmp_path 造库 + monkeypatch 队列路径，绝不触碰 data/database/portfolio.db 与真实 audit/。
"""
from __future__ import annotations

import datetime as dt
import json
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.analysis import snapshot_gate as sg  # noqa: E402

# 标的域（与 test_snapshot_baseline_gate 同口径，独立拷贝避免跨文件耦合）
ETF_CODES = ["159220", "159267", "159300", "159650", "159770", "159796",
             "159819", "159949", "159992", "510300", "510500", "511380",
             "511520", "512010", "512100", "512810", "515010", "515120",
             "516160", "561910", "563020", "588000"]
OTC_CODES = ["001194", "001323", "001407", "001437", "001765", "002152",
             "007994", "008269", "100032", "166301", "519770", "880013"]
FRI_EXTRA = ["027293"]
BASE_CODES = ETF_CODES + OTC_CODES            # 34
TARGET_WED = "2026-10-26"                     # 真实漂移日，做端到端反证

# W4 需要一个真正落在周五的目标日：从漂移日向后找首个周五
_d = dt.date.fromisoformat(TARGET_WED)
while _d.weekday() != 4:
    _d += dt.timedelta(days=1)
TARGET_FRI = _d.isoformat()


def _weekdays_back(end_date: str, n: int):
    """返回 end_date 之前（不含）最近的 n 个工作日，升序。"""
    d = dt.date.fromisoformat(end_date)
    out = []
    while len(out) < n:
        d -= dt.timedelta(days=1)
        if d.weekday() < 5:
            out.append(d.isoformat())
    return sorted(out)


def _make_db(tmp_path: Path, target: str, history_n: int = 30) -> Path:
    import sqlite3
    db = tmp_path / "data" / "database" / "portfolio.db"
    db.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db))
    conn.execute("""CREATE TABLE portfolio_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT NOT NULL, code TEXT NOT NULL,
        name TEXT, quantity REAL, cost_price REAL, current_price REAL,
        market_value REAL, pnl REAL, pnl_rate REAL, ytd_return REAL, beta REAL,
        UNIQUE(date, code))""")
    for d in _weekdays_back(target, history_n):
        codes = list(BASE_CODES) + (FRI_EXTRA if dt.date.fromisoformat(d).weekday() == 4 else [])
        for c in codes:
            conn.execute("INSERT INTO portfolio_snapshots "
                         "(date, code, quantity, cost_price, current_price, market_value) "
                         "VALUES (?,?,100.0,1.0,1.0,100.0)", (d, c))
    conn.commit()
    conn.close()
    return db


def _write_rows(db: Path, date_str: str, codes) -> None:
    import sqlite3
    conn = sqlite3.connect(str(db))
    for c in codes:
        conn.execute("INSERT OR REPLACE INTO portfolio_snapshots "
                     "(date, code, quantity, cost_price, current_price, market_value) "
                     "VALUES (?,?,100.0,1.0,1.0,100.0)", (date_str, c))
    conn.commit()
    conn.close()


def _report_html(root: Path, date_str: str, embedded_value: float) -> None:
    """写出 enhanced_report_YYYYMMDD.html 头部，内嵌 总市值 = embedded_value。"""
    reports = root / "data" / "reports"
    reports.mkdir(parents=True, exist_ok=True)
    stamp = date_str.replace("-", "")
    html = ('<html><head></head><body>'
            '<div class="kpi">总市值</div><div class="v">¥%s</div>'
            '</body></html>') % format(embedded_value, ",.2f")
    (reports / ("enhanced_report_%s.html" % stamp)).write_text(html, encoding="utf-8")


@pytest.fixture
def queue_path(tmp_path, monkeypatch):
    qp = tmp_path / "report_rerender_queue.jsonl"
    monkeypatch.setattr(sg, "_RERENDER_QUEUE", str(qp))
    return qp


# ----------------------------------------------- W1 / W2: 缺行 vs 完整 ----
def test_refuses_incomplete_snapshot_12_of_34(tmp_path):
    """W1) 2026-10-26 实测形态：12 行 vs 基线 34 ⇒ 拒绝。"""
    db = _make_db(tmp_path, TARGET_WED)
    _write_rows(db, TARGET_WED, BASE_CODES[:12])           # 仅 12 只
    ok, why = sg.is_summary_writable(str(db), TARGET_WED)
    assert ok is False
    assert "12" in why and "34" in why


def test_allows_complete_34_row_day(tmp_path):
    """W2) 完整 34 行 ⇒ 放行。"""
    db = _make_db(tmp_path, TARGET_WED)
    _write_rows(db, TARGET_WED, BASE_CODES)
    ok, why = sg.is_summary_writable(str(db), TARGET_WED)
    assert ok is True
    assert why == ""


# ----------------------------------------------- W3: 历史稀疏日放行 ----
def test_tolerates_historically_sparse_dates(tmp_path):
    """W3) 历史快照日不足 BASELINE_MIN_DATES(5) ⇒ 基线不可判定 ⇒ 放行（不误伤古早日/新库）。"""
    db = _make_db(tmp_path, TARGET_WED, history_n=2)      # 仅 2 个历史日 < 5
    _write_rows(db, TARGET_WED, BASE_CODES[:3])            # 当天也稀疏
    ok, why = sg.is_summary_writable(str(db), TARGET_WED)
    assert ok is True
    assert "baseline_unavailable" in why


# ----------------------------------------------- W4: 周五 35 行放行 ----
def test_friday_35_rows_allowed(tmp_path):
    """W4) 周五 35 行（含 027293）⇒ 放行。"""
    db = _make_db(tmp_path, TARGET_FRI)
    _write_rows(db, TARGET_FRI, BASE_CODES + FRI_EXTRA)
    ok, why = sg.is_summary_writable(str(db), TARGET_FRI)
    assert ok is True


# --------------------------------- W5/W6/W7/W8/W9: 重渲染队列 ----
def test_queue_appends_on_divergence(tmp_path, queue_path):
    """W5) 报告内嵌 510,118.64 与算出 1,416,039.63 相差巨大 ⇒ 追加队列项（含两值留证）。"""
    db = _make_db(tmp_path, TARGET_WED)
    _report_html(tmp_path, TARGET_WED, 510118.64)
    sg.queue_report_rerender_if_drifted(str(db), TARGET_WED, 1416039.63)
    lines = [l for l in queue_path.read_text(encoding="utf-8").splitlines() if l.strip()]
    assert len(lines) == 1
    e = json.loads(lines[0])
    assert e["date"] == "2026-10-26"
    assert abs(e["report_embedded_total_value"] - 510118.64) < 1e-6
    assert abs(e["computed_total_value"] - 1416039.63) < 1e-6


def test_queue_noop_when_near_equal(tmp_path, queue_path):
    """W6) 相差 < 阈值(0.51) ⇒ 不追加。"""
    db = _make_db(tmp_path, TARGET_WED)
    _report_html(tmp_path, TARGET_WED, 1416039.63)
    sg.queue_report_rerender_if_drifted(str(db), TARGET_WED, 1416039.63 + 0.30)
    assert not queue_path.exists() or not queue_path.read_text(encoding="utf-8").strip()


def test_queue_noop_when_no_report(tmp_path, queue_path):
    """W7) 无报告文件 ⇒ 静默 noop（不创建队列）。"""
    db = _make_db(tmp_path, TARGET_WED)
    sg.queue_report_rerender_if_drifted(str(db), TARGET_WED, 1416039.63)
    assert not queue_path.exists()


def test_queue_idempotent_on_same_computed(tmp_path, queue_path):
    """W8) 同日同算出值重复调用 ⇒ 仅 1 条队列项（夜间自动回填不重复刷屏）。"""
    db = _make_db(tmp_path, TARGET_WED)
    _report_html(tmp_path, TARGET_WED, 510118.64)
    sg.queue_report_rerender_if_drifted(str(db), TARGET_WED, 1416039.63)
    sg.queue_report_rerender_if_drifted(str(db), TARGET_WED, 1416039.63)
    lines = [l for l in queue_path.read_text(encoding="utf-8").splitlines() if l.strip()]
    assert len(lines) == 1


def test_queue_caps_at_max_lines(tmp_path, queue_path, monkeypatch):
    """W9) 超过上限 ⇒ 裁剪到末尾 N 行（防夜间自动回填无限增长）。"""
    monkeypatch.setattr(sg, "_RERENDER_MAX_LINES", 50)
    db = _make_db(tmp_path, TARGET_WED)
    base = dt.date(2026, 1, 1)
    for i in range(200):
        d = (base + dt.timedelta(days=i)).isoformat()
        _report_html(tmp_path, d, 1.0)
        sg.queue_report_rerender_if_drifted(str(db), d, 2.0 + i)
    lines = [l for l in queue_path.read_text(encoding="utf-8").splitlines() if l.strip()]
    assert len(lines) == 50


# --------------------------------- W10: 源码顺序（闸门在落库之前） ----
def test_gate_wired_before_summary_write_in_backfill_py():
    """W10a) src/utils/backfill.py 必须在 INSERT OR REPLACE 之前调用 is_summary_writable。"""
    src = (PROJECT_ROOT / "src" / "utils" / "backfill.py").read_text(encoding="utf-8")
    gate_at = src.index("is_summary_writable(self.db_path")
    insert_at = src.index("INSERT OR REPLACE INTO portfolio_summary")
    assert gate_at < insert_at


def test_gate_wired_before_summary_write_in_backfill_full_history():
    """W10b) backfill_full_history.py 必须在 INSERT OR REPLACE 之前调用 is_summary_writable。"""
    src = (PROJECT_ROOT / "scripts" / "backfill" / "backfill_full_history.py").read_text(encoding="utf-8")
    gate_at = src.index("is_summary_writable(db_path")
    insert_at = src.index("INSERT OR REPLACE INTO portfolio_summary")
    assert gate_at < insert_at


def test_queue_wired_into_both_writers():
    """W10c) 两处写入方都挂了重渲染队列调用。"""
    for p in ["src/utils/backfill.py", "scripts/backfill/backfill_full_history.py"]:
        src = (PROJECT_ROOT / p).read_text(encoding="utf-8")
        assert "queue_report_rerender_if_drifted(" in src
