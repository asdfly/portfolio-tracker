#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""P1#11 回归用例：休市日「分析照跑、不写日频表」（2026-10-27）。

背景
----
采集器此前只认周末、不认法定节假日（`run_analysis.is_trading_day` 仅判 weekday）。
国庆 10-01~10-07 连续 7 天写入 `total_value` 逐字相同的冻结行 + `daily_return=0.0`，
`MAX(date)` 被顶到休市日，而 `MAX(date)` 在下游就是「最近交易日」的同义词
（报告 DATA_DATE、NAV 结算、补采基准、风险窗口），于是报告逐日把休市日当交易日讲。

裁定：休市日**仍然跑分析**（读者要看最近交易日复盘），但把写库这一步关掉。
台账为此建过 5 条独立待办（#67/#68/#75/#29/#60），全部指向同一根因。

本文件钉住三件事
----------------
  (A) 节假日/周末判 `persist=False`，交易日判 `True`（含国庆首尾两端）；
  (B) 🔴 源码守卫：`run_daily_analysis` 里**每一处** `self.db.save_*` 都必须落在
      `persist` 门控块内。新增写库调用却忘了门控 = 休市日照样污染 —— 这是
      「修一处漏一处」的典型形态，靠源码结构断言守住；
  (C) `PT_FORCE_PERSIST=1` 逃生阀可用（排查/回填需要时强制写库）。

全程不连库、不发网络请求。
"""
from __future__ import annotations

import ast
import os
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

PORTFOLIO_PY = PROJECT_ROOT / "src" / "analysis" / "portfolio.py"


def _bare_analyzer(today: str):
    """绕开 __init__（会连数据源/建库），只测纯判定逻辑。"""
    from src.analysis.portfolio import PortfolioAnalyzer

    a = PortfolioAnalyzer.__new__(PortfolioAnalyzer)
    a.today = today
    return a


# --------------------------------------- (A) 交易日判定 ---

@pytest.mark.parametrize("day,expected,why", [
    ("2026-10-27", True, "周二交易日"),
    ("2026-10-23", True, "周五交易日"),
    ("2026-10-24", False, "周六"),
    ("2026-10-25", False, "周日"),
    ("2026-10-01", False, "国庆首日（周四，仅周末规则会误判为交易日）"),
    ("2026-10-05", False, "国庆中段（周一，同上）"),
    ("2026-10-07", False, "国庆末日（周三）"),
    ("2026-10-08", True, "国庆后首个交易日"),
    ("2026-02-18", False, "春节休市"),
    ("2026-02-24", True, "春节后开市"),
])
def test_persist_disabled_on_non_trading_days(day, expected, why):
    got = _bare_analyzer(day)._compute_persist_enabled()
    assert got is expected, f"{day} 应判 persist={expected}（{why}），实际 {got}"


# --------------------------------------- (C) 逃生阀 ---

def test_force_persist_env_overrides_calendar(monkeypatch):
    monkeypatch.setenv("PT_FORCE_PERSIST", "1")
    assert _bare_analyzer("2026-10-05")._compute_persist_enabled() is True


def test_force_persist_absent_by_default(monkeypatch):
    monkeypatch.delenv("PT_FORCE_PERSIST", raising=False)
    assert _bare_analyzer("2026-10-05")._compute_persist_enabled() is False


# ------------------------------- (B) 🔴 源码守卫：写库点必须全部门控 ---

def test_every_save_call_in_run_daily_analysis_is_persist_gated():
    """所有 `self.db.save_*` 必须在 `if persist` / `if not persist` 代码块内。

    实现：解析 AST，取 `run_daily_analysis` 的函数体，收集全部 `self.db.save_*`
    调用的行号，以及全部 `persist` 相关 `If` 语句的 body 行号区间，逐一对齐。
    """
    src = PORTFOLIO_PY.read_text(encoding="utf-8")
    tree = ast.parse(src)
    func = next((n for n in ast.walk(tree)
                 if isinstance(n, ast.FunctionDef) and n.name == "run_daily_analysis"),
                None)
    assert func is not None, "portfolio.py 里找不到 run_daily_analysis"

    save_lines = []
    for node in ast.walk(func):
        if not isinstance(node, ast.Call):
            continue
        f = node.func
        if (isinstance(f, ast.Attribute) and isinstance(f.value, ast.Attribute)
                and f.value.attr == "db" and f.attr.startswith("save_")):
            save_lines.append((f.attr, node.lineno))
    assert save_lines, "未找到任何 self.db.save_* 调用，守卫失效（可能重命名了）"

    gated_lines = set()
    # 早退模式：`if not persist: … return` 之后的所有语句同样受保护
    # （portfolio.py 用早退而非整段缩进，两种写法语义等价，守卫都要认）
    early_return_end = []
    for node in ast.walk(func):
        if not isinstance(node, ast.If):
            continue
        names = {n.id for n in ast.walk(node.test) if isinstance(n, ast.Name)}
        if "persist" not in names:
            continue
        has_return = any(isinstance(s, ast.Return) for s in ast.walk(node))
        if has_return:
            early_return_end.append(node.end_lineno)
        for stmt in node.body:
            for sub in ast.walk(stmt):
                if hasattr(sub, "lineno"):
                    gated_lines.add(sub.lineno)

    def _is_gated(ln: int) -> bool:
        return ln in gated_lines or any(ln > e for e in early_return_end)

    unguarded = [(n, ln) for n, ln in save_lines if not _is_gated(ln)]
    assert not unguarded, (
        "以下写库调用不在 persist 门控内（休市日会污染日频表）: "
        + ", ".join(f"{n}@line{ln}" for n, ln in unguarded)
    )
    print(f"[P1#11] 门控检查通过：{len(save_lines)} 处写库调用全部在 persist 块内")
