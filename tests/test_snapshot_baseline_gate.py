#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""#116 契约 1/2/3 的反证用例：闸门**确实**拒绝，且拒绝**确实**落了 error 告警。

现场（2026-09-16 15:30，日志 logs/portfolio_20260916.log）::

    [001194] 最新净值 2026-09-15 | 待插入 0 行      ← 13 只逐一同形
    场外净值: 成功 13 只, 失败 0 只, 跳过(无净值源) 0 只, 新增 0 行
    保存持仓快照: 2026-09-16, 22条记录
    持仓数量: 22 / 总市值: 933,195.10 / 当日收益: -0.07%

即：场外源当日只到 D-1（T+1 披露）+ 库内 D-1 行已存在 ⇒ 采集器判「待插入 0 行」
⇒ 整篮子静默跳过 ⇒ 当日快照只有场内 22 行 ⇒ total_value 只剩 933,195.10
（真实 1,527,928.85，少 38.1%），而 run_status 仍是 "ok"。

本文件构造该场景（以及 09-17 08:59 的"覆盖"变体），证明：
  A) 行数 22 != 基线 34 ⇒ 拒绝生成 summary + alerts 表落 error 级告警；
  B) 行数虽为 34、但本次 positions 只覆盖 22 行 ⇒ 同样拒绝
     （**单看行数会放行**——2026-09-17 08:59 的无人值守回填运行就是这样把
      08:5x 刚修好的 09-16 summary 打回 933,195.10 的）；
  C) 周五 35 行（34 + 027293）**不得**被判为异常；
  D) 场外当日无净值 **且当日快照缺这些 code**（09-16 事故形态）⇒ uncovered ⇒ error 告警；
  E) error 级告警 ⇒ run_status 降级为 degraded，且该状态被邮件闸门拦下；
  F) 场外当日无净值 **但当日快照已含这 12 行**（并集合并路径已用上一可用净值补位，
     即 T+1 披露的结构性常态）⇒ filled ⇒ warning、**不写 alerts 表、不降级**、
    日报闸门放行。否则 2026-09-17 15:30 起，这条误报会**每一个交易日**都把
    日报邮件拦死（logs/scheduled_run.log 原文锚：
    `[EMAIL] [CRITICAL] 数据未就绪，拒绝生成/发送今日…日报：… run_status=degraded`
    + `report email send FAILED, rc=1`。**不写行号**：该日志是长跑追加文件，行号会漂）。

G) 时序守卫（`test_contract1_check_must_run_after_snapshot_write`）：上面 F 的两条用例
   都**预置了目标日的快照行**，因此只证明「函数在给定输入下的行为」，对**这个检查在
   管线里的评估时刻**完全失明 —— 上一轮正是如此：用例全绿，而生产里 `filled` 分支
   **永不可达**（检查排在阶段一之前 ⇒ 当日快照尚未落库 ⇒ 必然 uncovered/error）。
   故新增一条读源码做顺序断言的用例，把"评估点必须在当日快照落库之后"钉死。

全程只在 tmp_path 造库，绝不触碰 data/database/portfolio.db。
"""
from __future__ import annotations

import datetime as dt
import sqlite3
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.analysis.snapshot_gate import (          # noqa: E402
    OTC_NAV_MISSING_KIND,
    SUMMARY_REFUSED_KIND,
    check_otc_nav_coverage,
    check_snapshot_baseline,
    expected_universe,
)

# 2026-09-16 实况的标的域：场内 22 + 场外 12 = 34；027293 只在周五多 1 行。
ETF_CODES = ["159220", "159267", "159300", "159650", "159770", "159796",
             "159819", "159949", "159992", "510300", "510500", "511380",
             "511520", "512010", "512100", "512810", "515010", "515120",
             "516160", "561910", "563020", "588000"]
OTC_CODES = ["001194", "001323", "001407", "001437", "001765", "002152",
             "007994", "008269", "100032", "166301", "519770", "880013"]
FRI_EXTRA = ["027293"]
BASE_CODES = ETF_CODES + OTC_CODES            # 34
TARGET_WED = "2026-09-16"                     # 周三
TARGET_FRI = "2026-09-18"                     # 周五


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
    conn = sqlite3.connect(str(db))
    for c in codes:
        conn.execute("INSERT OR REPLACE INTO portfolio_snapshots "
                     "(date, code, quantity, cost_price, current_price, market_value) "
                     "VALUES (?,?,100.0,1.0,1.0,100.0)", (date_str, c))
    conn.commit()
    conn.close()


def _positions(codes):
    return [{"code": c, "quantity": 100.0, "cost_price": 1.0,
             "current_price": 1.0, "market_value": 100.0} for c in codes]


def _alerts(db: Path):
    conn = sqlite3.connect(str(db))
    try:
        return conn.execute(
            "SELECT rule_name, level, message FROM alerts ORDER BY id").fetchall()
    except sqlite3.OperationalError:
        return []
    finally:
        conn.close()


# ---------------------------------------------------------------- 基线口径 ---

def test_baseline_is_34_weekday_and_35_friday(tmp_path):
    """基线必须数据驱动：非周五 34、周五 35，且 027293 归入 friday_extra。"""
    db = _make_db(tmp_path, TARGET_WED)
    conn = sqlite3.connect(str(db))
    try:
        wed = expected_universe(conn, TARGET_WED)
        fri = expected_universe(conn, TARGET_FRI)
    finally:
        conn.close()

    assert len(wed["base"]) == 34 and wed["friday_extra"] == set(FRI_EXTRA)
    assert len(wed["expected_codes"]) == 34          # 周三 = 34
    assert len(fri["expected_codes"]) == 35          # 周五 = 34 + 027293
    assert wed["n_dates"] == 30


# ---------------------------------------------- A. 反证：场外整篮子被跳过 ---

def test_otc_basket_dropped_is_refused_and_alerts(tmp_path):
    """A) 当日只落场内 22 行（09-16 现场）⇒ ok=False + 缺失 12 只 + error 告警。"""
    db = _make_db(tmp_path, TARGET_WED)
    _write_rows(db, TARGET_WED, ETF_CODES)           # 场外整篮子被跳过

    res = check_snapshot_baseline(db, TARGET_WED, _positions(ETF_CODES),
                                 computed_total_value=933195.10)

    assert res["ok"] is False
    assert res["expected_n"] == 34 and res["actual_n"] == 22
    assert sorted(res["missing"]) == sorted(OTC_CODES)
    assert res["alert_written"] is True

    rows = _alerts(db)
    assert len(rows) == 1
    rule, level, msg = rows[0]
    assert rule == SUMMARY_REFUSED_KIND and level == "error"
    assert "行数 22 != 基线 34" in msg
    for c in OTC_CODES:                              # 缺失 code 清单必须落进告警
        assert c in msg
    assert "933195.10" in msg                        # 残缺 total_value 一并留证


# ------------------------------------- B. 反证：行数够但 positions 不够 ---

def test_row_count_alone_cannot_pass_when_positions_is_narrower(tmp_path):
    """B) 09-17 08:59 的覆盖场景：库内 34 行齐、positions 只 22 只 ⇒ 仍拒绝。

    这是"只校验行数"的漏网口：当日 12 行场外曾被别的路径补过，行数等于基线，
    但本次真正参与汇总的 positions 仍只有 22 只 ⇒ total_value 又是 933,195.10。
    """
    db = _make_db(tmp_path, TARGET_WED)
    _write_rows(db, TARGET_WED, BASE_CODES)          # 34 行齐（曾被补过）

    res = check_snapshot_baseline(db, TARGET_WED, _positions(ETF_CODES),
                                 computed_total_value=933195.10)

    assert res["ok"] is False
    assert res["missing"] == []                      # 行数判据此处**看不见**问题
    assert res["actual_n"] == 34
    assert sorted(res["not_summarized"]) == sorted(OTC_CODES)
    assert "本次 positions 未覆盖当日快照行 12 只" in res["reason"]
    assert _alerts(db)[0][1] == "error"


# ------------------------------------------- C. 反证：周五 35 行不得误判 ---

def test_friday_35_rows_is_not_an_anomaly(tmp_path):
    """C) 周五 35 行（含 027293）必须放行。"""
    db = _make_db(tmp_path, TARGET_FRI)
    _write_rows(db, TARGET_FRI, BASE_CODES + FRI_EXTRA)

    res = check_snapshot_baseline(db, TARGET_FRI, _positions(BASE_CODES + FRI_EXTRA))

    assert res["ok"] is True, res["reason"]
    assert res["expected_n"] == 35 and res["actual_n"] == 35
    assert _alerts(db) == []


def test_clean_34_row_day_passes(tmp_path):
    """D) 正常日（34 行 + 34 只 positions）放行，且不写任何告警。"""
    db = _make_db(tmp_path, TARGET_WED)
    _write_rows(db, TARGET_WED, BASE_CODES)

    res = check_snapshot_baseline(db, TARGET_WED, _positions(BASE_CODES))

    assert res["ok"] is True, res["reason"]
    assert res["missing"] == [] and res["not_summarized"] == []
    assert _alerts(db) == []


def test_baseline_unavailable_does_not_block(tmp_path):
    """新库（历史不足 5 天）基线不可判定 ⇒ 不拦，但显式标注 baseline_available=False。"""
    db = _make_db(tmp_path, TARGET_WED, history_n=3)
    _write_rows(db, TARGET_WED, ETF_CODES)

    res = check_snapshot_baseline(db, TARGET_WED, _positions(ETF_CODES))

    assert res["ok"] is True and res["baseline_available"] is False
    assert _alerts(db) == []


# ------------------------------------------------- 契约1：场外净值缺失 ---

def _per_code(rows):
    return [{"code": c, "status": "OK", "nav_latest_date": d} for c, d in rows]


def test_otc_nav_missing_raises_error_alert(tmp_path):
    """D) 场外源当日只到 D-1 ⇒ 12 只基线标的缺失、027293 记为延后披露（不拦）。"""
    db = _make_db(tmp_path, TARGET_WED)
    per_code = _per_code([(c, "2026-09-15") for c in OTC_CODES])
    per_code.append({"code": FRI_EXTRA[0], "status": "OK",
                     "nav_latest_date": "2026-09-11"})

    res = check_otc_nav_coverage(db, TARGET_WED, per_code)

    assert res["ok"] is False
    assert sorted(r["code"] for r in res["missing"]) == sorted(OTC_CODES)
    assert [r["code"] for r in res["deferred"]] == FRI_EXTRA   # 非当日基线标的
    assert res["missing"][0]["lag_days"] == 1                  # 滞后 1 个自然日
    assert OTC_NAV_MISSING_KIND in res["message"]
    assert "001194" in res["message"] and "027293" not in res["missing"][0]["code"]


def test_otc_nav_present_is_silent(tmp_path):
    """场外当日净值齐 ⇒ 不告警。"""
    db = _make_db(tmp_path, TARGET_WED)
    per_code = _per_code([(c, TARGET_WED) for c in OTC_CODES])

    res = check_otc_nav_coverage(db, TARGET_WED, per_code)

    assert res["ok"] is True and res["missing"] == [] and res["message"] == ""


# ------------------------------------------- 契约3：error 告警 ⇒ 降级 ---

def test_error_alert_degrades_run_status(tmp_path):
    """E) 有 error 级告警 ⇒ run_status=degraded（不再是 ok）。"""
    from src.data_sources.collect_core import (
        RUN_STATUS_DEGRADED, RUN_STATUS_OK, RunReporter)

    rep = RunReporter("2026-09-16", reports_dir=str(tmp_path))
    for s in ("basic", "risk", "monitor", "dq_check"):
        rep.stage(s, "ok")

    assert rep.evaluate_run_status()[0] == RUN_STATUS_OK

    rep.alert("error", SUMMARY_REFUSED_KIND, "快照不完整，已拒绝生成 summary")
    report, _ = rep.finalize_and_write(reports_dir=str(tmp_path))

    assert report["run_status"] == RUN_STATUS_DEGRADED
    assert report["dq_score"] is None            # 不可信运行不得拿漂亮 dq_score
    assert any(a["kind"] == SUMMARY_REFUSED_KIND for a in report["alerts"])


def test_degraded_is_blocked_by_email_gate(tmp_path):
    """E) degraded 必须与 partial/failed 同列拒发（否则残缺值仍会进日报）。"""
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "sre_gate_probe", str(PROJECT_ROOT / "scripts" / "send_report_email.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    assert "degraded" in mod._RUN_STATUS_BLOCKING


# ------------------------------- 契约2：闸门确实在写库之前被调用 ---

def test_gate_runs_before_saving_summary():
    """源码顺序断言：拒绝判据必须位于 save_portfolio_summary 之前且在其前 return。"""
    src = (PROJECT_ROOT / "src" / "analysis" / "portfolio.py").read_text(encoding="utf-8")
    gate_at = src.index("check_snapshot_baseline(self.db.db_path")
    save_at = src.index("self.db.save_portfolio_summary(self.today")
    assert gate_at < save_at
    # 拒绝分支必须在保存之前 return，而不是"先写再校验"
    between = src[gate_at:save_at]
    assert 'if not gate["ok"]:' in between
    assert "return results" in between


# ------------------------- F. 契约1 的两种形态：常态(warning) / 事故(error) ---

def _add_summary_table(db: Path, dates) -> None:
    """把 portfolio_summary 写到目标日（data_readiness_gate 的判据 1）。"""
    conn = sqlite3.connect(str(db))
    conn.execute("CREATE TABLE IF NOT EXISTS portfolio_summary "
                 "(date TEXT, total_value REAL)")
    for d in dates:
        conn.execute("INSERT INTO portfolio_summary (date, total_value) VALUES (?,?)",
                     (d, 100.0))
    conn.commit()
    conn.close()


def _load_send_report_email():
    """按 main() 的同一方式加载 scripts/send_report_email.py（不执行 main / 不发信）。"""
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "sre_gate_probe", str(PROJECT_ROOT / "scripts" / "send_report_email.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _run_status_after_coverage_alert(reports_dir: Path, target: str,
                                    alert_level: str, message: str) -> str:
    """复刻 run_analysis.py 里契约1 调用点的 alert_level 分流，返回本次运行的 run_status。

    这里是"被测语义"而非"被测实现"的镜像：它只断言
    「error 告警 ⇒ 降级 / warning 或空 ⇒ 不降级」这一 collect_core 的既有规则
    （该规则本次**未改动**），用来证明分流本身不会再误伤日报。
    """
    from src.data_sources.collect_core import RunReporter

    rep = RunReporter(target, reports_dir=str(reports_dir))
    for s in ("basic", "risk", "monitor", "dq_check"):
        rep.stage(s, "ok")
    if alert_level:
        rep.alert(alert_level, OTC_NAV_MISSING_KIND, message)
    report, _ = rep.finalize_and_write(reports_dir=str(reports_dir))
    return report["run_status"]


def test_otc_nav_filled_is_warning_and_does_not_degrade(tmp_path):
    """F) 12 只无 D 净值、但当日快照已含这 12 行（合并路径补位）⇒ warning，不降级。

    2026-09-17 15:30 实况：日志里「这些标的本日不落快照行(禁止静默跳过)」那句与
    「保存持仓快照: 2026-09-17, 34条记录」直接矛盾 ⇒ 应是常态告警。
    ⚠️ 本用例预置了当日快照行，只测函数行为；评估时刻由 G 的源码顺序用例守。
    """
    from src.data_sources.collect_core import RUN_STATUS_OK

    db = _make_db(tmp_path, TARGET_WED)
    _write_rows(db, TARGET_WED, BASE_CODES)          # 合并路径已补位并落当日行
    per_code = _per_code([(c, "2026-09-15") for c in OTC_CODES])
    per_code.append({"code": FRI_EXTRA[0], "status": "OK",
                     "nav_latest_date": "2026-09-11"})

    res = check_otc_nav_coverage(db, TARGET_WED, per_code)

    assert res["ok"] is True
    assert res["alert_level"] == "warning"
    assert res["uncovered"] == []
    assert sorted(r["code"] for r in res["filled"]) == sorted(OTC_CODES)
    assert sorted(r["code"] for r in res["missing"]) == sorted(OTC_CODES)
    assert [r["code"] for r in res["deferred"]] == FRI_EXTRA
    # 旧的假陈述必须消失，且 lag 现算为 1（不得硬编码）
    assert "不落快照行" not in res["message"]
    assert "T+1" in res["message"] and "滞后 1 自然日" in res["message"]
    assert "2026-09-15" in res["message"]
    assert _alerts(db) == []                          # warning 形态不写 alerts 表

    reports = tmp_path / "reports"
    reports.mkdir()
    assert _run_status_after_coverage_alert(
        reports, TARGET_WED, res["alert_level"], res["message"]) == RUN_STATUS_OK


def test_otc_nav_uncovered_is_error_and_degrades(tmp_path):
    """F) 当日快照**缺**这些行（09-16 事故形态）⇒ uncovered ⇒ error ⇒ 降级 + 被邮件闸门拦下。"""
    from src.data_sources.collect_core import RUN_STATUS_DEGRADED

    db = _make_db(tmp_path, TARGET_WED)              # 当日快照一行都没有
    per_code = _per_code([(c, "2026-09-15") for c in OTC_CODES])

    res = check_otc_nav_coverage(db, TARGET_WED, per_code)

    assert res["ok"] is False
    assert res["alert_level"] == "error"
    assert res["filled"] == []
    assert sorted(r["code"] for r in res["uncovered"]) == sorted(OTC_CODES)
    assert "缺行" in res["message"] and "整篮子未落库" in res["message"]
    assert "不落快照行" not in res["message"]
    assert _alerts(db) == []                          # 本模块不写库，写库由调用方负责

    _add_summary_table(db, [TARGET_WED])
    reports = tmp_path / "reports"
    reports.mkdir()
    status = _run_status_after_coverage_alert(
        reports, TARGET_WED, res["alert_level"], res["message"])
    assert status == RUN_STATUS_DEGRADED

    gate = _load_send_report_email()
    ok, reason, _summary_date = gate.data_readiness_gate(
        str(db), TARGET_WED, reports_dir=str(reports))
    assert ok is False and "degraded" in reason


def test_otc_two_forms_are_distinguished_end_to_end(tmp_path):
    """F) 端到端区分：同一份 per_code，仅"当日快照是否已覆盖"不同 ⇒ 日报放行/拦截必须相反。

    两种形态各自独立造数据（同一实现下结果若相同，就只能得出"无法判定"）。
    """
    gate = _load_send_report_email()
    per_code = _per_code([(c, "2026-09-15") for c in OTC_CODES])

    db_filled = _make_db(tmp_path / "filled", TARGET_WED)
    _write_rows(db_filled, TARGET_WED, BASE_CODES)    # 形态一：已补位落行
    db_uncovered = _make_db(tmp_path / "uncovered", TARGET_WED)  # 形态二：整篮子缺行

    observed = {}
    for tag, db in (("filled", db_filled), ("uncovered", db_uncovered)):
        cov = check_otc_nav_coverage(db, TARGET_WED, per_code)
        _add_summary_table(db, [TARGET_WED])
        reports = tmp_path / tag / "reports"
        reports.mkdir(parents=True, exist_ok=True)
        status = _run_status_after_coverage_alert(
            reports, TARGET_WED, cov["alert_level"], cov["message"])
        ok, reason, _summary_date = gate.data_readiness_gate(
            str(db), TARGET_WED, reports_dir=str(reports))
        observed[tag] = (cov["alert_level"], status, ok, reason[:40])

    assert observed["filled"][:3] == ("warning", "ok", True), observed
    assert observed["uncovered"][:3] == ("error", "degraded", False), observed


# ------------------- G. 契约1 的**评估时刻**：必须晚于当日快照落库 ---

def test_contract1_check_must_run_after_snapshot_write():
    """源码顺序断言：契约1 必须在阶段一（= 写当日快照的那一步）**之后**评估。

    这条断言防的是「检查点放得太早」。上面 F 那三条用例**抓不到**它：它们都预置了
    目标日的快照行，只证明"函数在给定输入下的行为"，对**这个检查在管线里的评估时刻**
    完全失明 —— 上一轮就是这样：用例全绿，生产里 `filled` 分支却**永不可达**。

    为什么早一步必然误报（2026-09-17 15:30 日频首跑实测）：
      * 当日 `portfolio_snapshots` 行是**阶段一**写进去的：
        `run_stage1_basic` → `src/analysis/portfolio.py` 的持仓合并路径，
        日志留痕「保存持仓快照: <今天>, <N>条记录」；
      * 契约1 原先紧跟阶段0（`run_stage0_otc_nav` 之后、`run_stage1_basic` 之前），
        那一刻当日快照一行都没有 ⇒ filled 恒为 0、uncovered = 全部
        ⇒ alert_level='error' ⇒ run_status=degraded ⇒ 日报**每个交易日**都被拦死；
      * 实测时间戳：契约1 告警 15:30:43,930 早于「保存持仓快照: 2026-09-17, 34条记录」
        的 15:30:45,386。
    """
    src = (PROJECT_ROOT / "run_analysis.py").read_text(encoding="utf-8")

    stage0_anchor = "run_stage0_otc_nav(backfill_date)"
    stage1_anchor = "results = run_stage1_basic(analyzer)"
    cov_anchor = "check_otc_nav_coverage(DATABASE_PATH"
    for anchor in (stage0_anchor, stage1_anchor, cov_anchor):
        assert src.count(anchor) == 1, f"锚点不唯一，顺序断言会失真: {anchor!r}"

    stage0_at = src.index(stage0_anchor)
    stage1_at = src.index(stage1_anchor)
    cov_at = src.index(cov_anchor)

    assert stage1_at < cov_at, (
        "契约1 的评估点必须晚于阶段一：当日 portfolio_snapshots 行由 run_stage1_basic "
        "写入；早于它评估则当日快照尚不存在 ⇒ filled 永远为空 ⇒ 每个交易日都误报 "
        "error ⇒ 日报被 run_status=degraded 拦死")
    # 兜底：也不得被挪到阶段0 之前或阶段0/阶段一之间
    assert stage0_at < stage1_at, "阶段0 必须早于阶段一（阶段0 的值是后续分析的基础）"
