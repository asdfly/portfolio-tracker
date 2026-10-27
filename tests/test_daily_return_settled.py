#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""P1#12 回归用例：`daily_return_settled` 必须区分「真实平价」与「未结算」（2026-10-27）。

背景
----
台账 2026-10-16：真实交易日 18:02 报告时点 `daily_return` 仍 =0.0，而当日快照已写、
`vs_hs300` 已算。这个 0.0 与「今天真的不涨不跌」在库内**完全同形**，下游没有任何
判据能区分「已结算的平价」与「价格没更新」。

裁定：`daily_return` 的数值语义不动（它是 nav_engine 的主口径输入，写 NULL 会让
`cumprod` 把 NaN 传播成整条 NAV 失真），另外落一列 `daily_return_settled`：
  1 = 该值由当日新价结算（含合法的 0.0 平价日）
  0 = 该值不是收益证据（无当日新价 / 无前日基准）

本文件钉住四件事
----------------
  (A) 🔴 反证：有新鲜价 + 净零 ⇒ `settled=1`。若判据写成「daily_return==0 即未结算」，
      本用例会红 —— 它守的正是「别把平价日批量标成不可信」。
  (B) 全陈旧且 total_value 无变化 ⇒ `settled=0` + 落 `daily_return_unsettled` 告警；
  (C) 全陈旧但 total_value 确实动了 ⇒ `settled=1`（回退口径有信息量，只是口径不同）；
  (D) 无前日 summary（prev_value 不可得）⇒ `settled=0`（分母都没有，谈不上结算）。

全程只在 tmp_path 造库；不触碰任何真实库。
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from tests.test_daily_return_caliber_b import (  # noqa: E402
    ETF_CODES, OTC_CODES, TARGET_THU, _analyzer, _file_positions, _make_db, _run,
)


def _alert_names(db: Path):
    conn = sqlite3.connect(str(db))
    try:
        return [r[0] for r in conn.execute("SELECT rule_name FROM alerts ORDER BY id")]
    finally:
        conn.close()


# ------------------------- (A) 🔴 反证：平价日必须判「已结算」 ---

def test_flat_day_with_fresh_prices_is_settled(tmp_path):
    """有新鲜篮子且净零 ⇒ settled=1。判据不得用 `daily_return == 0` 当哨兵。

    反证能力：本用例的 included_n=22（有新鲜价），若把「值=0」当作未结算，
    `settled` 会变 0，断言立刻失败 —— 这正是要防的那种「修一个坏十个」。
    """
    db = _make_db(tmp_path, prev_total_value=3400.0)
    a = _analyzer(db, TARGET_THU)
    merged, info, s = _run(a, _file_positions(ETF_CODES, price=1.0))

    assert s["daily_return"] == pytest.approx(0.0, abs=1e-9)
    assert s["daily_return_coverage"]["included_n"] == 22
    assert s["daily_return_settled"] == 1, "有当日新价的平价日是合法值，必须判已结算"
    assert s["daily_return_unsettled_reason"] is None
    assert "daily_return_unsettled" not in _alert_names(db)


# ------------------------- (B) 全陈旧 + 总值不动 ⇒ 未结算 ---

def test_all_stale_and_flat_total_is_unsettled(tmp_path):
    """无任何新鲜价、total_value 与昨日完全相同 ⇒ settled=0 + 告警。

    构造：前日 total_value = 当日 total_value = 3400（100×34），且当日文件为空
    ⇒ 合并后全是 age=1 的场外，回退口径算出来正好 0.0 —— 这就是 10-16 的形态。
    """
    db = _make_db(tmp_path, prev_total_value=3400.0)
    a = _analyzer(db, TARGET_THU)
    merged, info, s = _run(a, [])

    assert s["daily_return_coverage"]["included_n"] == 0
    assert s["daily_return"] == pytest.approx(0.0, abs=1e-9)
    assert s["daily_return_settled"] == 0, "旧价对旧价算出的 0 不是收益证据"
    assert s["daily_return_unsettled_reason"], "未结算必须给出可核的原因串"
    assert s["daily_return_coverage"]["settled"] is False
    names = _alert_names(db)
    assert "daily_return_unsettled" in names, "未结算必须落告警，不许静默"
    # 回退口径的告警同时存在（两者是不同事实，不互相替代）
    assert "daily_return_caliber_fallback" in names


# ------------------------- (C) 全陈旧但总值确实动了 ⇒ 已结算 ---

def test_all_stale_but_total_value_moved_is_settled(tmp_path):
    """无新鲜价但 total_value 有真实变化（如场外 NAV 补更新）⇒ settled=1。

    这是「不能一刀切把 included_n==0 全判未结算」的反证：回退口径给出的非 0 值
    仍有信息量，差别只在**口径**（caliber='total_value_fallback' 已单独标注）。
    """
    # 前日 total_value 明显低于当日（当日 total_value = 34×100 = 3400）
    db = _make_db(tmp_path, prev_total_value=3000.0)
    a = _analyzer(db, TARGET_THU)
    merged, info, s = _run(a, [])

    assert s["daily_return_coverage"]["included_n"] == 0
    # 返回值按 2 位小数落库口径，故与理论值 13.333… 比对时按 0.01 容差
    assert s["daily_return"] == pytest.approx((3400.0 - 3000.0) / 3000.0 * 100, abs=0.01)
    assert s["daily_return"] != 0
    assert s["daily_return_settled"] == 1, "非 0 的回退值有信息量，应判已结算"
    assert s["daily_return_unsettled_reason"] is None


# ------------------------- (D) 无前日基准 ⇒ 未结算 ---

def test_no_prev_summary_is_unsettled(tmp_path):
    """没有前一交易日 summary ⇒ 分母不可得，daily_return 从未被计算 ⇒ settled=0。

    这条与 (B) 的区别是**原因不同**：不是「价没更新」，而是「根本没有基准」。
    两种都要拒绝，但拒绝理由必须可区分，否则事后无法定位。
    """
    db = _make_db(tmp_path, with_summary=False)
    a = _analyzer(db, TARGET_THU)
    merged, info, s = _run(a, _file_positions(ETF_CODES, price=1.02))

    assert s["daily_return_settled"] == 0
    reason = s["daily_return_unsettled_reason"]
    assert reason and "prev_value=0" in reason, f"原因串须含实测的 prev_value，实际: {reason!r}"
