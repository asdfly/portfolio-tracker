#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""#115 持仓合并（场内文件 ∪ 场外库内快照）回归用例。

事故（#115 / #118 合并裁定）::

    通达信导出的持仓文件**只含场内 22 只**；12 只场外基金结构性不在导出里
    （它们只存在于 portfolio_snapshots，由 scripts/fetch_otc_fund_nav.py 维护）。
    run_daily_analysis 的"读文件"分支却把它当成**全组合的权威源**整体替换：

        positions = self.position_reader.read_positions()   # 22 只
        ...
        total_value = Σ market_value                        # ⇒ 恒为场内-only

    实证：09-01/02/04/07/08/09/10/11/14 与 09-16/17 的管线自报总市值全部落在
    933k~952k（场内-only 量级），而库内当日快照真实合计约 1.51M —— 恒少 38%。

修正方向：**合并（并集）**而非替换。文件侧 code 一律以文件为准（真实调仓不得被吞），
文件中没有的 code 取库内该 code 最近一行的现值（前向填充，**不伪造当日净值**）。

本文件钉住的四件事：
  (a) 文件 22 / DB 34 ⇒ 合并后 34，且文件侧 code 以文件为准；
  (b) 文件里某 code 的 quantity 变了 ⇒ 仍以文件为准（真实调仓不能被吞）；
  (c) 文件 34 / DB 34 ⇒ 不重复、不丢行，且不产生任何填充；
  (d) `snapshot_gate` 在合并后必须 `ok=True`（#115 与 #116 的接缝）；
  (e) 库内陈旧行（> FORWARD_FILL_MAX_STALENESS_DAYS）不得被"复活"；
  (f) 合并必须落显式日志（哪些 code、取自哪一天），并写进 results['position_merge']。

全程只在 tmp_path 造库；`_merge_positions` 只读 `self.db.db_path` 与 `self.today`，
故用轻量 stub 替代 DatabaseManager，不触碰任何真实库。
"""
from __future__ import annotations

import datetime as dt
import logging
import sqlite3
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.analysis.snapshot_gate import check_snapshot_baseline  # noqa: E402

# 2026-09-16 实况的标的域：场内 22 + 场外 12 = 34；027293 只在周五多 1 行。
ETF_CODES = ["159220", "159267", "159300", "159650", "159770", "159796",
             "159819", "159949", "159992", "510300", "510500", "511380",
             "511520", "512010", "512100", "512810", "515010", "515120",
             "516160", "561910", "563020", "588000"]
OTC_CODES = ["001194", "001323", "001407", "001437", "001765", "002152",
             "007994", "008269", "100032", "166301", "519770", "880013"]
BASE_CODES = ETF_CODES + OTC_CODES            # 34
FRI_EXTRA = ["027293"]                        # 只在周五出现的份额
TARGET_WED = "2026-09-16"                     # 周三
TARGET_THU = "2026-09-17"                     # 周四（09-16 的次一交易日）
TARGET_FRI = "2026-09-18"                     # 周五
EXITED_CODE = "159732"                        # 2026-07-30 起停更（已清仓，不得复活）

# 场内-only 与含场外的真实量级（用于 (a) 的量纲对照）
MV_FILE_ONLY = 933195.10                      # 22 只场内合计（09-16 现场）
MV_ALL = 1527928.85                           # 34 只合计（09-16 真实）


class _DbStub:
    """只提供 .db_path：_merge_positions / _load_snapshot_rows_per_code 只用到它。

    刻意不构造 DatabaseManager —— 那会连带建表并（在真实场景下）绑定 DATABASE_PATH，
    与本用例要验证的合并语义无关。
    """

    def __init__(self, path: Path):
        self.db_path = str(path)


def _analyzer(db: Path, today: str):
    from src.analysis.portfolio import PortfolioAnalyzer

    a = PortfolioAnalyzer.__new__(PortfolioAnalyzer)   # 不跑 __init__（会连数据源）
    a.db = _DbStub(db)
    a.today = today
    return a


def _weekdays_back(end_date: str, n: int):
    """end_date 之前（不含）最近的 n 个工作日，升序。"""
    d = dt.date.fromisoformat(end_date)
    out = []
    while len(out) < n:
        d -= dt.timedelta(days=1)
        if d.weekday() < 5:
            out.append(d.isoformat())
    return sorted(out)


def _make_db(tmp_path: Path, target: str, history_n: int = 30) -> Path:
    """造库：target 之前 history_n 个工作日各 34 只（不含 target 当日）。"""
    db = tmp_path / "data" / "database" / "portfolio.db"
    db.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db))
    conn.execute("""CREATE TABLE portfolio_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT NOT NULL, code TEXT NOT NULL,
        name TEXT, quantity REAL, cost_price REAL, current_price REAL,
        market_value REAL, pnl REAL, pnl_rate REAL, ytd_return REAL, beta REAL,
        UNIQUE(date, code))""")
    conn.execute("""CREATE TABLE alerts (
        id INTEGER PRIMARY KEY AUTOINCREMENT, rule_name TEXT NOT NULL,
        level TEXT NOT NULL, message TEXT, created_at TEXT,
        acknowledged INTEGER DEFAULT 0)""")
    for d in _weekdays_back(target, history_n):
        codes = list(BASE_CODES) + (FRI_EXTRA if dt.date.fromisoformat(d).weekday() == 4 else [])
        for c in codes:
            conn.execute("INSERT INTO portfolio_snapshots "
                         "(date, code, name, quantity, cost_price, current_price, market_value) "
                         "VALUES (?,?,?,?,?,?,?)",
                         (d, c, f"标的{c}", 100.0, 1.0, 1.0, 100.0))
    conn.commit()
    conn.close()
    return db


def _write_rows(db: Path, date_str: str, rows):
    """rows: [(code, quantity, market_value)] 或 [code, ...]"""
    conn = sqlite3.connect(str(db))
    for r in rows:
        if isinstance(r, str):
            code, qty, mv = r, 100.0, 100.0
        else:
            code, qty, mv = r
        conn.execute(
            "INSERT OR REPLACE INTO portfolio_snapshots "
            "(date, code, name, quantity, cost_price, current_price, market_value) "
            "VALUES (?,?,?,?,?,?,?)",
            (date_str, code, f"标的{code}", qty, 1.0, 1.0, mv))
    conn.commit()
    conn.close()


def _snapshot_rows(db: Path, date_str: str):
    conn = sqlite3.connect(str(db))
    try:
        return conn.execute(
            "SELECT code, quantity, market_value FROM portfolio_snapshots "
            "WHERE date = ? ORDER BY code", (date_str,)).fetchall()
    finally:
        conn.close()


def _file_positions(codes, quantity: float = 100.0, market_value: float = 100.0):
    """模拟 position_reader.read_positions() 的输出（场内台账，11 列口径）。"""
    return [{"code": c, "name": f"标的{c}", "quantity": quantity,
             "cost_price": 1.0, "current_price": 1.0, "market_value": market_value,
             "pnl": 0.0, "pnl_rate": 0.0, "ytd_return": 0.0, "beta": 1.0}
            for c in codes]


def _alerts(db: Path):
    conn = sqlite3.connect(str(db))
    try:
        return conn.execute(
            "SELECT rule_name, level FROM alerts ORDER BY id").fetchall()
    finally:
        conn.close()


# ---------------------------------------- (a) 文件 22 / DB 34 ⇒ 合并 34 ---

def test_file_22_db_34_merges_to_34_file_side_wins(tmp_path):
    """(a) 核心修复：场内 22 + 场外 12 = 34，文件侧 code 的字段一律取文件值。"""
    db = _make_db(tmp_path, TARGET_THU)
    _write_rows(db, TARGET_WED, [(c, 100.0, 100.0) for c in BASE_CODES])
    a = _analyzer(db, TARGET_THU)

    file_rows = _file_positions(ETF_CODES, quantity=250.0, market_value=250.0)
    merged, info = a._merge_positions(file_rows)

    assert len(merged) == 34
    assert {p["code"] for p in merged} == set(BASE_CODES)
    assert info["file_n"] == 22
    assert info["universe"]["recent_n"] == 34     # 最近快照日（09-16）就是 34 只
    assert info["carried_n"] == 12 and info["merged_n"] == 34
    assert sorted(info["source_dates"]) == [TARGET_WED]
    assert sorted(info["source_dates"][TARGET_WED]) == sorted(OTC_CODES)

    by_code = {p["code"]: p for p in merged}
    for c in ETF_CODES:                       # 文件侧：以文件为准（250，而非库内 100）
        assert by_code[c]["quantity"] == 250.0
        assert by_code[c]["market_value"] == 250.0
    for c in OTC_CODES:                       # 场外侧：取库内该 code 最近一行现值
        assert by_code[c]["quantity"] == 100.0
        assert by_code[c]["market_value"] == 100.0
        assert by_code[c]["date"] == TARGET_WED   # 价格取自哪一天，可查

    # (a) 量纲对照：合并前后 total_value 的差距正是 38% 那一档
    file_only = sum(p["market_value"] for p in file_rows)
    assert file_only == 22 * 250.0
    assert sum(p["market_value"] for p in merged) > file_only


def test_total_value_scale_22_vs_34(tmp_path):
    """(a') 真实量级的量纲反证（合成数）：933,195.10 ⇒ 1,527,928.85。

    库副本上的真库反证见 tools/audit/_r17_merge_proof.py；这里是同口径的迷你版，
    让"少 38%"这件事有一条恒定的数字断言。
    """
    db = _make_db(tmp_path, TARGET_THU)
    etf_mv = MV_FILE_ONLY / len(ETF_CODES)
    otc_mv = (MV_ALL - MV_FILE_ONLY) / len(OTC_CODES)
    _write_rows(db, TARGET_WED,
                [(c, 1.0, etf_mv) for c in ETF_CODES]
                + [(c, 1.0, otc_mv) for c in OTC_CODES])
    a = _analyzer(db, TARGET_THU)

    file_rows = _file_positions(ETF_CODES, quantity=1.0, market_value=etf_mv)
    merged, _ = a._merge_positions(file_rows)

    file_only = sum(p["market_value"] for p in file_rows)
    total = sum(p["market_value"] for p in merged)

    assert len(merged) == 34
    assert file_only == pytest.approx(MV_FILE_ONLY, abs=0.01)
    assert total == pytest.approx(MV_ALL, abs=0.01)
    assert file_only / total == pytest.approx(0.6107, abs=0.001)   # 场内占比 ~61%


# ------------------------- (b) 文件里 quantity 变了 ⇒ 仍以文件为准 ---

def test_file_quantity_change_is_not_swallowed(tmp_path):
    """(b) 真实调仓（加仓/减仓）不得被库内旧行吞掉。"""
    db = _make_db(tmp_path, TARGET_THU)
    # 库内 09-16：512100 已按 777 记过；09-17 文件显示调仓到 1234
    _write_rows(db, TARGET_WED,
                [(c, 777.0 if c == "512100" else 100.0, 100.0) for c in BASE_CODES])
    a = _analyzer(db, TARGET_THU)

    file_rows = _file_positions(ETF_CODES, quantity=100.0)
    for p in file_rows:
        if p["code"] == "512100":
            p["quantity"] = 1234.0

    merged, info = a._merge_positions(file_rows)
    by_code = {p["code"]: p for p in merged}

    assert len(merged) == 34
    assert by_code["512100"]["quantity"] == 1234.0        # 文件赢
    assert "512100" not in [c for d in info["source_dates"].values() for c in d]


# --------------------------------- (c) 文件 34 / DB 34 ⇒ 不重不漏 ---

def test_file_34_db_34_no_dup_no_loss(tmp_path):
    """(c) 两边都是 34 ⇒ 合并恒等：不重复、不丢行、不产生前向填充。"""
    db = _make_db(tmp_path, TARGET_THU)
    _write_rows(db, TARGET_WED, [(c, 100.0, 100.0) for c in BASE_CODES])
    a = _analyzer(db, TARGET_THU)

    file_rows = _file_positions(BASE_CODES, quantity=555.0)
    merged, info = a._merge_positions(file_rows)

    assert len(merged) == 34
    assert len({p["code"] for p in merged}) == 34         # 无重复
    assert {p["code"] for p in merged} == set(BASE_CODES)  # 无丢失
    assert info["carried_n"] == 0 and info["source_dates"] == {}
    assert all(p["quantity"] == 555.0 for p in merged)     # 文件为准


# ------------------- (d) 接缝：#115 合并后 snapshot_gate 必须 ok=True ---

def test_snapshot_gate_ok_after_merge(tmp_path):
    """(d) 改前拒绝、改后放行 —— 这条钉住 #115 与 #116 的接缝。

    改前：文件 22 只整体替换 ⇒ 当日快照只写 22 行 ⇒ 闸门 missing=12、拒绝生成 summary。
    改后：合并出 34 只 ⇒ 当日快照 34 行 + positions 34 只 ⇒ 闸门 ok=True。
    """
    db = _make_db(tmp_path, TARGET_THU)
    a = _analyzer(db, TARGET_THU)

    # --- 改前：整体替换 ---
    _write_rows(db, TARGET_THU, ETF_CODES)
    before = check_snapshot_baseline(db, TARGET_THU, _file_positions(ETF_CODES))
    assert before["ok"] is False
    assert before["expected_n"] == 34 and before["actual_n"] == 22
    assert sorted(before["missing"]) == sorted(OTC_CODES)
    assert before["alert_written"] is True     # 改前那次拒绝必须落了 error 告警
    assert [r[1] for r in _alerts(db)] == ["error"]

    # --- 改后：合并 ---
    merged, _ = a._merge_positions(_file_positions(ETF_CODES))
    _write_rows(db, TARGET_THU,
                [(p["code"], p["quantity"], p["market_value"]) for p in merged])
    after = check_snapshot_baseline(db, TARGET_THU, merged)

    assert len(merged) == 34
    assert after["ok"] is True, after["reason"]
    assert after["missing"] == [] and after["not_summarized"] == []
    assert after["actual_n"] == 34


# --------------------------- (e) 陈旧行不得被"复活" ---

def test_stale_row_is_not_resurrected(tmp_path):
    """(e) 库内停在 2026-07-30 的 159732 距目标日 >> 10 天 ⇒ 不并入当日组合。"""
    db = _make_db(tmp_path, TARGET_THU)
    _write_rows(db, TARGET_WED, BASE_CODES)
    _write_rows(db, "2026-07-30", [EXITED_CODE])
    a = _analyzer(db, TARGET_THU)

    merged, info = a._merge_positions(_file_positions(ETF_CODES))
    codes = {p["code"] for p in merged}

    assert EXITED_CODE not in codes
    assert len(merged) == 34
    assert [s["code"] for s in info["stale_skipped"]] == [EXITED_CODE]
    assert info["stale_skipped"][0]["age_days"] > 10


# --------------------- (h) 域外标的（027293 仅周五）不得被填进来 ---

def test_friday_only_code_is_not_backfilled_on_weekday(tmp_path, caplog):
    """(h) 027293 是"周五份额"：周三合并不得把它从上周五前向填充进来。

    库副本实测：不过滤时 35 行 ⇒ 1,557,293.17；过滤后 34 行 ⇒ 1,527,928.85。
    差额 +29,364.32 全部来自 027293 —— 与权威快照（非周五不含它）直接冲突。
    """
    db = _make_db(tmp_path, TARGET_THU)
    _write_rows(db, TARGET_WED, BASE_CODES)
    _write_rows(db, "2026-09-11", ["027293"])      # 上周五：库内确实有这个 code
    a = _analyzer(db, TARGET_THU)

    with caplog.at_level(logging.INFO, logger="src.analysis.portfolio"):
        merged, info = a._merge_positions(_file_positions(ETF_CODES))

    codes = {p["code"] for p in merged}
    assert "027293" not in codes
    assert len(merged) == 34
    assert [x["code"] for x in info["outside_universe"]] == ["027293"]
    assert info["universe"]["latest_day"] == TARGET_WED

    text = "\n".join(r.getMessage() for r in caplog.records)
    assert "不并入 027293" in text                     # 域外必须告警，不得静默


def test_new_holding_in_latest_day_is_kept(tmp_path):
    """(h') 反向保护：基线交集之外的真实新持仓（最近一天才有）不能被丢掉。"""
    db = _make_db(tmp_path, TARGET_THU)
    _write_rows(db, TARGET_WED, BASE_CODES + ["019999"])   # 3 天前才建的仓
    a = _analyzer(db, TARGET_THU)

    merged, info = a._merge_positions(_file_positions(ETF_CODES))
    codes = {p["code"] for p in merged}

    assert "019999" in codes                          # 在最近快照日 ⇒ 保留
    assert len(merged) == 35
    # 新持仓自身绝不能被判为"域外"（域外的只会是 027293 这类只在周五出现的份额）
    assert "019999" not in [x["code"] for x in info["outside_universe"]]


def test_friday_merge_keeps_027293_and_gate_passes(tmp_path):
    """(h'') 周五的反向：027293 属当日基线域 ⇒ **必须**并入，合并后 35 行且闸门放行。

    这条与 (h) 成对：(h) 证明非周五不得填 027293，(h'') 证明周五不得丢 027293。
    丢了的后果同样是闸门拒绝（expected 35 vs actual 34），但根因完全不同。
    """
    db = _make_db(tmp_path, TARGET_FRI)
    _write_rows(db, TARGET_THU, BASE_CODES)           # 周四权威 34 行
    a = _analyzer(db, TARGET_FRI)

    merged, info = a._merge_positions(_file_positions(ETF_CODES))

    codes = {p["code"] for p in merged}
    assert FRI_EXTRA[0] in codes
    assert len(merged) == 35
    assert info["universe"]["expected_n"] == 35
    assert info["outside_universe"] == []

    _write_rows(db, TARGET_FRI,
                [(p["code"], p["quantity"], p["market_value"]) for p in merged])
    gate = check_snapshot_baseline(db, TARGET_FRI, merged)

    assert gate["ok"] is True, gate["reason"]
    assert gate["expected_n"] == 35 and gate["actual_n"] == 35


# ------------------- (i) T+1 回填：当日已有行 ⇒ 取当日（而非前一日）---

def test_backfill_takes_today_rows_when_present(tmp_path):
    """(i) 当日快照已有场外行（T+1 补写）⇒ 填充取**当日**行，反证 B 的语义。

    库副本实测：09-16 已有 34 行（含 09-16 当日场外净值）时，合并结果
    total_value = 1,527,928.85，与该库自身 34 行 Σ **分毫不差**（差 0.00）。
    """
    db = _make_db(tmp_path, TARGET_THU)
    _write_rows(db, TARGET_WED, BASE_CODES)
    _write_rows(db, TARGET_THU,
                [(c, 100.0, 42417.955) for c in ETF_CODES]
                + [(c, 100.0, 49561.146) for c in OTC_CODES])
    a = _analyzer(db, TARGET_THU)

    merged, info = a._merge_positions(_file_positions(ETF_CODES))

    assert len(merged) == 34
    assert info["carried_n"] == 12
    assert sorted(info["source_dates"]) == [TARGET_THU]      # 取当日，不是前一日
    assert sorted(info["source_dates"][TARGET_THU]) == sorted(OTC_CODES)
    assert sum(p["market_value"] for p in merged) == pytest.approx(
        len(ETF_CODES) * 100.0 + 12 * 49561.146, abs=0.01)


# ------------------------------- (f) 显式日志：不许静默 fill-forward ---

def test_merge_logs_which_codes_from_which_day(tmp_path, caplog):
    """(f) 日志必须能读出"共 12 只取自 2026-09-16"与逐 code 清单（#115 硬要求）。"""
    db = _make_db(tmp_path, TARGET_THU)
    _write_rows(db, TARGET_WED, BASE_CODES)
    a = _analyzer(db, TARGET_THU)

    with caplog.at_level(logging.INFO, logger="src.analysis.portfolio"):
        merged, _ = a._merge_positions(_file_positions(ETF_CODES))

    text = "\n".join(r.getMessage() for r in caplog.records)
    assert len(merged) == 34
    assert "持仓文件 22 只" in text and "库内前向填充 12 只" in text
    assert f"共 12 只取自 {TARGET_WED}" in text
    for c in OTC_CODES:                       # 逐 code 可见
        assert c in text
    assert "不伪造当日净值" in text


def test_daily_analysis_merges_before_saving_snapshot():
    """结构断言：读文件之后必须先合并、再落快照，且 results['positions'] 是合并结果。

    否则闸门读到的是合并前的 22 只 —— 修了快照却仍拒绝写 summary。
    """
    src = (PROJECT_ROOT / "src" / "analysis" / "portfolio.py").read_text(encoding="utf-8")

    read_at = src.index("file_positions = self.position_reader.read_positions()")
    merge_at = src.index("positions, merge_info = self._merge_positions(file_positions)")
    assign_at = src.index("results['positions'] = positions")
    save_at = src.index("self.db.save_portfolio_snapshot(self.today, positions)")
    gate_at = src.index("check_snapshot_baseline(self.db.db_path")

    assert read_at < merge_at < assign_at < save_at < gate_at


# ------------------- (g) §6.4 死路径：绕过闸门的写入口已删除 ---

def test_gate_bypassing_save_helper_is_gone():
    """(g) `_save_to_database` 零调用且能绕过 #116 闸门 ⇒ 必须删除而不是留后门。"""
    src = (PROJECT_ROOT / "src" / "analysis" / "portfolio.py").read_text(encoding="utf-8")
    assert "def _save_to_database" not in src
    # 且全仓不再有调用点
    for py in (PROJECT_ROOT / "src").rglob("*.py"):
        assert "_save_to_database(" not in py.read_text(encoding="utf-8"), py
