#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""#130 fund_flows「全空指标覆盖」回归用例：上游守卫 + 读取端 NULL 防御。

事故（同一缺陷已发生两次）::

    2026-09-16 `fund_flows` 的 20 行（category='etf'，id 30796–30815）
    **12 个指标列全部 NULL**，却仍带着 `source=em_spot / is_estimated=0 /
    confidence=1.0` —— 元数据宣称"这是完全可信的真值"，而值已被清零。

    机制：`save_fund_flows` 的 UPDATE 分支原先**无条件覆盖** net_inflow/
    buy_amount/sell_amount 与各扩展列，df 为 NaN 时 `_float()` 得到 None 就直接
    写进去 ⇒ 用 NULL 把 18:02 还是好的真值原地清零；且 UPDATE 不刷新 created_at
    ⇒ 这次清零在时间戳上完全看不见。
    09-16 的行 id 与 18:02 备份一致、而 08:51/08:55/08:59 备份的 NULL 行数都是 0
    ⇒ 覆盖发生在 09-17 08:59 之后，是**同缺陷的第二次发生**。

    下游炸点：`pre_post_market.py` 的 `float(flow_df.iloc[0]["net_inflow"])`
    → TypeError，而该异常原先不在 except 白名单内 ⇒ 打断盘前报告
    （全量回归里唯一 1 failed 就是它）。

本文件钉住的六件事：
  (a) 指标全空的行既不 INSERT 也不 UPDATE，并落 error 级告警；
  (b) NULL 不得覆盖非 NULL（部分为空的 payload 只能更新它真的带来的那些列）；
  (c) 正常 payload 的 upsert 行为不变（不含回归副作用）；
  (d) 盘前读取端遇到 NULL 行**回落到上一可用日**并标出 as-of，不抛异常；
  (e) 全库只有 NULL 行时字段留空（0.0/""），不抛异常、不伪造 0 含义；
  (f) `data_loader` 两个同族读取函数也不抛异常（含 `'etf_flow'` 字面量修正后
      不得引入同类崩溃）。

全程只在 tmp_path 造库。
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import data_loader                                       # noqa: E402
from src.analysis.pre_post_market import (               # noqa: E402
    _load_etf_signal_previews,
    _load_fund_flow_changes,
)
from src.data_sources.fund_flow import (                 # noqa: E402
    FUND_FLOW_EMPTY_KIND,
    save_fund_flows,
)

DDL = """
CREATE TABLE fund_flows (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL, code TEXT NOT NULL, name TEXT,
    net_inflow REAL, buy_amount REAL, sell_amount REAL,
    category TEXT NOT NULL DEFAULT '', created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    net_inflow_pct REAL, super_large_inflow REAL, super_large_pct REAL,
    large_inflow REAL, large_pct REAL, medium_inflow REAL, medium_pct REAL,
    small_inflow REAL, small_pct REAL,
    source TEXT, is_estimated INTEGER DEFAULT 0, confidence REAL
);
CREATE TABLE alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT, rule_name TEXT NOT NULL,
    level TEXT NOT NULL, message TEXT, created_at TEXT, acknowledged INTEGER DEFAULT 0
);
CREATE TABLE portfolio_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT NOT NULL, code TEXT NOT NULL,
    name TEXT, quantity REAL, cost_price REAL, current_price REAL,
    market_value REAL, pnl REAL, pnl_rate REAL, ytd_return REAL, beta REAL,
    UNIQUE(date, code)
);
CREATE TABLE etf_technical (
    id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT NOT NULL, code TEXT NOT NULL,
    ma_signal TEXT, macd_signal TEXT, rsi_value REAL, rsi_status TEXT, trend TEXT,
    UNIQUE(date, code)
);
"""


def _db(tmp_path: Path) -> Path:
    p = tmp_path / "portfolio.db"
    c = sqlite3.connect(str(p))
    c.executescript(DDL)
    c.commit()
    c.close()
    return p


def _conn(p: Path) -> sqlite3.Connection:
    c = sqlite3.connect(str(p))
    c.row_factory = sqlite3.Row
    return c


def _seed(p: Path, date: str, code: str, net_inflow, name="测试ETF",
          buy=None, sell=None, category="etf", created_at=None):
    c = sqlite3.connect(str(p))
    c.execute(
        "INSERT INTO fund_flows (date, code, name, net_inflow, buy_amount, sell_amount, "
        "category, created_at, source, is_estimated, confidence) "
        "VALUES (?,?,?,?,?,?,?,COALESCE(?,CURRENT_TIMESTAMP),'em_spot',0,1.0)",
        (date, code, name, net_inflow, buy, sell, category, created_at))
    c.commit()
    c.close()


def _rows(p: Path, date: str, code: str):
    c = sqlite3.connect(str(p))
    c.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in c.execute(
            "SELECT * FROM fund_flows WHERE date=? AND code=? AND category='etf'",
            (date, code))]
    finally:
        c.close()


def _alerts(p: Path):
    c = sqlite3.connect(str(p))
    try:
        return c.execute("SELECT rule_name, level, message FROM alerts ORDER BY id").fetchall()
    finally:
        c.close()


def _df(rows):
    return pd.DataFrame(rows)


NAN = float("nan")


# ============================================ (a) 全空行拒绝写入 + 告警 ---

def test_all_empty_row_is_not_inserted_and_raises_error_alert(tmp_path):
    """(a) 一行指标全空 ⇒ 不落库，且必须落 error 级告警（不许静默）。"""
    p = _db(tmp_path)
    conn = _conn(p)
    try:
        n = save_fund_flows(conn, _df([
            {"date": "2026-09-16", "code": "510300", "category": "etf",
             "name": "沪深300ETF", "net_inflow": NAN},
        ]))
    finally:
        conn.close()

    assert n == 0
    assert _rows(p, "2026-09-16", "510300") == []      # 没有留下自称可信的空行
    alerts = _alerts(p)
    assert len(alerts) == 1
    rule, level, msg = alerts[0]
    assert rule == FUND_FLOW_EMPTY_KIND and level == "error"
    assert "510300" in msg and "拒绝" in msg


def test_all_empty_payload_does_not_clobber_existing_value(tmp_path):
    """(b) 覆盖场景（09-17 二次事故）：全空 payload 绝不能清零既有真值。"""
    p = _db(tmp_path)
    _seed(p, "2026-09-16", "512100", 12345.6, buy=8000.0, created_at="2026-09-16 15:31:35")

    conn = _conn(p)
    try:
        n = save_fund_flows(conn, _df([
            {"date": "2026-09-16", "code": "512100", "category": "etf",
             "name": "中证1000ETF", "net_inflow": NAN, "buy_amount": NAN},
        ]))
    finally:
        conn.close()

    row = _rows(p, "2026-09-16", "512100")[0]
    assert n == 0
    assert row["net_inflow"] == pytest.approx(12345.6)   # 真值保住
    assert row["buy_amount"] == pytest.approx(8000.0)
    assert row["created_at"] == "2026-09-16 15:31:35"    # 也没有被"更新"痕迹
    assert _alerts(p)[0][0] == FUND_FLOW_EMPTY_KIND
    assert _alerts(p)[0][1] == "error"


def test_partial_null_does_not_clobber_non_null_columns(tmp_path):
    """(b) 部分为空：只更新它真的带来的那一列，其余保持原值。"""
    p = _db(tmp_path)
    _seed(p, "2026-09-16", "510500", 1000.0, buy=600.0, sell=400.0)

    conn = _conn(p)
    try:
        n = save_fund_flows(conn, _df([
            {"date": "2026-09-16", "code": "510500", "category": "etf",
             "name": "中证500ETF", "net_inflow": NAN, "buy_amount": 900.0},
        ]))
    finally:
        conn.close()

    row = _rows(p, "2026-09-16", "510500")[0]
    assert n == 1
    assert row["net_inflow"] == pytest.approx(1000.0)    # NULL 没有覆盖非 NULL
    assert row["buy_amount"] == pytest.approx(900.0)     # 真带来的值照常更新
    assert row["sell_amount"] == pytest.approx(400.0)
    assert _alerts(p) == []                              # 非全空 ⇒ 不告警


# ====================================== (c) 正常 upsert 行为不变 ---

def test_normal_payload_still_upserts(tmp_path):
    """(c) 回归副作用检查：有值的 INSERT / UPDATE 行为与守卫前一致。"""
    p = _db(tmp_path)
    conn = _conn(p)
    try:
        n1 = save_fund_flows(conn, _df([
            {"date": "2026-09-16", "code": "588000", "category": "etf",
             "name": "科创50ETF", "net_inflow": 500.0},
        ]))
        n2 = save_fund_flows(conn, _df([
            {"date": "2026-09-16", "code": "588000", "category": "etf",
             "name": "科创50ETF", "net_inflow": 700.0},
        ]))
    finally:
        conn.close()

    rows = _rows(p, "2026-09-16", "588000")
    assert n1 == 1 and n2 == 1
    assert len(rows) == 1                                # upsert 不产生重复行
    assert rows[0]["net_inflow"] == pytest.approx(700.0)
    assert _alerts(p) == []


# ============================ (d)(e) 盘前读取端：回落 + as-of + 不抛 ---

def _seed_preview_db(p: Path, flows):
    """flows: [(date, code, net_inflow)]，None 表示空值；另建一只持仓标的。"""
    c = sqlite3.connect(str(p))
    c.execute("INSERT INTO portfolio_snapshots (date, code, name, quantity, cost_price, "
              "current_price, market_value) VALUES ('2026-09-17','510300','沪深300ETF',"
              "100,1.0,1.0,100)")
    c.commit()
    c.close()
    for d, code, v in flows:
        _seed(p, d, code, v)


def test_pre_market_falls_back_to_previous_available_day(tmp_path, monkeypatch):
    """(d) 最近一行是 NULL ⇒ 回落到上一可用日，并标出 as-of 日期。"""
    p = _db(tmp_path)
    monkeypatch.setattr(data_loader, "DATABASE_PATH", str(p))
    _seed_preview_db(p, [("2026-09-16", "510300", None),
                         ("2026-09-14", "510300", 12345.0)])

    conn = _conn(p)
    try:
        previews = _load_etf_signal_previews(conn)      # 不得抛 TypeError
    finally:
        conn.close()

    assert len(previews) == 1
    es = previews[0]
    assert es.fund_flow_net == pytest.approx(12345.0 / 1e4)
    assert es.fund_flow_asof == "2026-09-14"            # 是哪一天的钱，看得见


def test_pre_market_leaves_field_empty_when_all_rows_null(tmp_path, monkeypatch):
    """(e) 全是 NULL ⇒ 字段留空（0.0/""），不抛异常，也不把"缺失"说成 0 值。"""
    p = _db(tmp_path)
    monkeypatch.setattr(data_loader, "DATABASE_PATH", str(p))
    _seed_preview_db(p, [("2026-09-16", "510300", None)])

    conn = _conn(p)
    try:
        previews = _load_etf_signal_previews(conn)
    finally:
        conn.close()

    es = previews[0]
    assert es.fund_flow_net == 0.0
    assert es.fund_flow_asof == ""


# ================== (f) 盘后 & data_loader 同族读取端：不抛异常 ---

def test_post_market_fund_flow_changes_handles_etf_and_null(tmp_path):
    """(f) 盘后：`'etf_flow'` 字面量修正后必须同时吃得下 NULL 行。"""
    p = _db(tmp_path)
    _seed(p, "2026-09-16", "510300", None)              # 最新一行空
    _seed(p, "2026-09-14", "510300", 900.0)             # 上一可用日有值
    _seed(p, "2026-09-16", "半导体", 5000.0, category="sector")

    conn = _conn(p)
    try:
        changes = _load_fund_flow_changes(conn)          # 不得抛 TypeError
    finally:
        conn.close()

    codes = {c["code"] for c in changes}
    assert "510300" in codes                            # 'etf' 口径确实被修对了
    assert "半导体" in codes
    etf = [c for c in changes if c["code"] == "510300"][0]
    assert etf["today_flow"] == pytest.approx(900.0 / 1e4)   # 取的是有值那行


def test_load_etf_fund_flow_drops_null_rows(tmp_path, monkeypatch):
    """(f) `load_etf_fund_flow` 不得把 NULL 行交给调用方。"""
    p = _db(tmp_path)
    monkeypatch.setattr(data_loader, "DATABASE_PATH", str(p))
    _seed(p, "2026-09-16", "510300", None)
    _seed(p, "2026-09-14", "510300", 777.0)

    df = data_loader.load_etf_fund_flow("510300", days=30)

    assert len(df) == 1
    assert df.iloc[0]["date"] == "2026-09-14"
    assert df["net_inflow"].notna().all()


def test_load_etf_fund_flow_alerts_does_not_raise_on_null_latest(tmp_path, monkeypatch):
    """(f) `load_etf_fund_flow_alerts` 在最新行为 NULL 时不得抛 TypeError。"""
    p = _db(tmp_path)
    monkeypatch.setattr(data_loader, "DATABASE_PATH", str(p))
    _seed(p, "2026-09-16", "510300", None)
    _seed(p, "2026-09-14", "510300", 100.0)

    df = data_loader.load_etf_fund_flow_alerts(threshold_pct=200)   # 不得抛

    assert isinstance(df, pd.DataFrame)


def test_etf_flow_typo_literal_is_gone_from_source():
    """(f) 源码断言：SQL 里的 `'etf_flow'`（库内一行都没有）不得再出现。

    注：只断言**查询字面量**（`WHERE category IN ('etf_flow'` 这一形态），
    不断言"全文不含 etf_flow 这串字符" —— 讲解这段陷阱的注释里会引用它，
    断言全文会逼着后人删掉注释。
    """
    src = (PROJECT_ROOT / "src" / "analysis" / "pre_post_market.py").read_text(encoding="utf-8")
    assert "WHERE category IN ('etf_flow'" not in src
    # 且修口径的同时必须带 NULL 过滤，否则改对字面量会立刻引入同类崩溃
    for marker in ("category IN ('etf','sector') AND net_inflow IS NOT NULL",
                   "preview.fund_flow_asof",
                   "TypeError"):
        assert marker in src, marker


# ============ (g) 守卫三：拒绝「关键列 net_inflow 为空」的 INSERT ---
#
# 本节是对上面 (a)~(f) 的**追加**（原本 module docstring 只列了 (a)~(f)，未改），
# 来源 = `audit/_guard3_verify.py` 的探针五例。
# 🔑 判别力证据有两级，**以用例级为准**：
#   · **用例级**（最强）：`audit/_g3_pytest_falsify.py` → `.txt` —— 拆掉守卫三后
#     **直接调用下面这四个用例函数本身**（`module.save_fund_flows = broken`，
#     不改源码）⇒ A / E 红、B / C 绿。
#     ⚠️ 为什么必须做用例级：把 `src/` 换成坏版再跑 pytest 在本机走不通
#     （真实代码库是 grep 出来的、不是 import 包），所以「跑不了就只做探针」会让
#     「自称反证」的用例**从未被真正证伪过**。
#   · **探针级**（较弱）：`audit/_guard3_falsify.py` → `.txt` —— 自己复刻判据逻辑后摘掉
#     守卫三 4 行。探针可能与用例不一致（判据复刻走样、夹具差异），**故只作旁证**。
#
# 判据 1 **已有等价覆盖**，故本节**不加**「12 列全空」那一例：
#   `test_all_empty_row_is_not_inserted_and_raises_error_alert`（文件开头，payload 只给
#   `net_inflow=NAN`）与「12 列全给 NaN」**落到同一条拒绝路径**。源码依据：
#   `save_fund_flows` 的 `metric_names` 只收 `row.index` 里存在的列，可 grep 原文：
#       metric_names = [c for c in (['net_inflow','buy_amount','sell_amount'] + extra_cols)
#                       if c in row.index]
#       if not metric_names or all(v is None for v in metric_vals.values()): ...
#   （取证：2026-09-17，`save_fund_flows` 含守卫三（该版已随 `43d06ce` 入库）。
#     🔑 上面引的原文在 `src/data_sources/fund_flow.py` 里**唯一**（函数体里那一处）——
#     `src/` 侧的 docstring 已改为**只作文字描述、不复制代码行**，就是为了保证这一点。
#     🔴 **不给行号**：实测同一段在本轮内漂了两次（`:544`→`:549`→`:554`），两次都是
#     `src/` 侧 docstring 自己的增删造成的 ⇒ 引用别人文件时行号也不可靠。）
#     ⚠️ 反向教训（我踩过两次）：**在引用对象所在的文件里复制它的原文，会让
#     `grep` 命中两处**（引文 + 真码），读者可能停在引文上 = 「自指空锚」；
#     连「用某条注释做唯一锚点」这种写法都会因为把该注释抄进去而自我失效（实测 3 处命中）。
#   ⇒ 判据 1 的真实语义是「**payload 里给出的指标列全为空**」，而不是字面的「表里 12 列全空」。
#   所以 A 例（net_inflow 空 + buy 有值）才会被判据 1 放行、必须由守卫三兜住。
#
# ⚠️ 本节的判别力**不对称**，不要一视同仁：
#   A / E —— **守卫三的反证**：把守卫三那 4 行拆掉 ⇒ 本两例会红。
#            **用例级实测**见 `audit/_g3_pytest_falsify.txt`（探针级见 `_guard3_falsify.txt`）。
#   B / C —— **「不误伤」型**：拆掉守卫三后**仍然通过**。它们防的是守卫三被写成过宽判据
#            （例如按 payload 里是否**出现** `net_inflow` 键来判断），那会把
#            「已有行的部分更新」和「正常新行」一起拒掉。**不得把它们当成反证引用。**

def test_guard3_rejects_insert_when_net_inflow_is_null(tmp_path):
    """(g) 守卫三的核心：`net_inflow` 空而别的列有值 ⇒ 判据 1 放行，INSERT 仍须拒绝。

    这是 `#135` §3.1 的洞：判据 1 只挡「payload 里给出的指标列**全**空」。当
    `net_inflow` 为空、`buy_amount` 有值时判据 1 放行，于是仍会落一行
    `net_inflow IS NULL` + 缺省 `confidence=1.0` 的「自称完全可信的空值行」——
    与 09-16 那次**同型**（元数据宣称真值、关键值为空），只是这次靠读取端的
    NULL 过滤兜住。守卫三把这条路从"靠下游兜"改成"源头拒绝"。
    """
    p = _db(tmp_path)
    conn = _conn(p)
    try:
        n = save_fund_flows(conn, _df([
            {"date": "2026-09-16", "code": "NEW001", "category": "etf",
             "name": "探针A", "net_inflow": NAN, "buy_amount": 1000.0},
        ]))
    finally:
        conn.close()

    assert n == 0
    assert _rows(p, "2026-09-16", "NEW001") == []      # 没有留下自称可信的空值行
    alerts = _alerts(p)
    assert len(alerts) == 1
    rule, level, msg = alerts[0]
    assert rule == FUND_FLOW_EMPTY_KIND and level == "error"
    assert "NEW001" in msg and "拒绝" in msg
    assert "net_inflow为空" in msg                     # 守卫三自己的理由标记，可与判据 1 区分


def test_guard3_rejects_labeled_but_null_net_inflow(tmp_path):
    """(g) 守卫三：带 `source/is_estimated/confidence` 标签也不能放行空的 `net_inflow`。

    标签是"自称可信"，不是"有值"。若因带标签就放行，等于重新造出 09-16 那种
    「元数据宣称真值、关键值为空」的行 —— 正是本次事故的形态。
    """
    p = _db(tmp_path)
    conn = _conn(p)
    try:
        n = save_fund_flows(conn, _df([
            {"date": "2026-09-16", "code": "NEW004", "category": "etf", "name": "探针E",
             "net_inflow": NAN, "buy_amount": 1.0,
             "source": "em_spot", "is_estimated": 0, "confidence": 1.0},
        ]))
    finally:
        conn.close()

    assert n == 0
    assert _rows(p, "2026-09-16", "NEW004") == []
    alerts = _alerts(p)
    assert len(alerts) == 1
    assert alerts[0][0] == FUND_FLOW_EMPTY_KIND and alerts[0][1] == "error"
    assert "NEW004" in alerts[0][2]


def test_guard3_does_not_shadow_guard2_update_path(tmp_path):
    """(g) 守卫三**不误伤**判据 2：已有行的部分为空仍走 UPDATE，既有真值不被拒掉。

    ⚠️ 本例证的是「**不误伤**」，**不是**「守卫三存在」——把守卫三整段拆掉后，
    本例**仍然通过**（**用例级**实测见 `audit/_g3_pytest_falsify.txt`，探针级见
    `_guard3_falsify.txt`）。它防的是守卫三被写成
    过宽判据（例如按 payload 里是否**出现** `net_inflow` 键来判断），那样会把
    「已有行的部分更新」也一并拒绝，把一次正常更新变成整行丢弃。
    守卫三只作用于 INSERT 分支，已有行继续走判据 2。
    """
    p = _db(tmp_path)
    _seed(p, "2026-09-15", "EX0001", 1000.0, buy=600.0, sell=400.0)

    conn = _conn(p)
    try:
        n = save_fund_flows(conn, _df([
            {"date": "2026-09-15", "code": "EX0001", "category": "etf",
             "name": "既有行", "net_inflow": NAN, "buy_amount": 900.0},
        ]))
    finally:
        conn.close()

    row = _rows(p, "2026-09-15", "EX0001")[0]
    assert n == 1
    assert row["net_inflow"] == pytest.approx(1000.0)   # 既有真值没有被守卫三一起拒掉
    assert row["buy_amount"] == pytest.approx(900.0)    # 真带来的值照常更新
    assert row["sell_amount"] == pytest.approx(400.0)   # NULL 未覆盖非 NULL（判据 2）
    assert _alerts(p) == []                             # 非全空 ⇒ 不告警


def test_guard3_allows_normal_insert(tmp_path):
    """(g) 守卫三**不过度拦截**：`net_inflow` 有值的新行照常 INSERT。

    ⚠️ 同上一例：证的是「**不过度拦截**」，**不是**「守卫三存在」
    （拆掉守卫三本例仍通过）。防的是守卫三把正常写入也拦掉。
    """
    p = _db(tmp_path)
    conn = _conn(p)
    try:
        n = save_fund_flows(conn, _df([
            {"date": "2026-09-16", "code": "NEW002", "category": "etf",
             "name": "探针C", "net_inflow": 500.0, "buy_amount": 300.0},
        ]))
    finally:
        conn.close()

    row = _rows(p, "2026-09-16", "NEW002")[0]
    assert n == 1
    assert row["net_inflow"] == pytest.approx(500.0)
    assert _alerts(p) == []                             # 正常数据不得告警
