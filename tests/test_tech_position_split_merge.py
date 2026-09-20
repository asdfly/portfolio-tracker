"""回归：etf_features 位置类特征（ma/macd/boll 及相对量 macd/s、boll_pctb）
须用「qfq 优先、快照兜底」合并价，避免拆分日 current_price 合法跳变造成的
假台阶（问题十二 C 线 / 合并而非替换）。

见 docs/handover/07_known_data_issues.md 问题十二。
- 有 qfq 覆盖：boll_pctb / macd 在拆分日保持连续且在常态区间（无尖刺）。
- 无 qfq 覆盖：拆分日 boll_pctb 跌出 [0,1] 区间（复现修复前假台阶，证明守卫有效）。

与 test_etf_features_qfq_split.py 互补：那个只验 return/momentum/volatility 8 列，
本测试专验位置类特征在合并价下的连续性。
"""
import os
import gc
import sqlite3
import tempfile

import numpy as np
import pandas as pd

from src.utils.db_schema import init_all_tables
from src.analysis.predictor.features import build_feature_matrix


def _cleanup_db(path):
    for _ in range(5):
        try:
            os.remove(path)
            return
        except PermissionError:
            gc.collect()
    try:
        os.remove(path)
    except OSError:
        pass


def _seed(conn, n=40, split_at=21):
    init_all_tables(conn)
    cur = conn.cursor()
    dates = pd.bdate_range("2024-01-01", periods=n).strftime("%Y-%m-%d").tolist()
    # 连续前复权价（无台阶）：1.00 起缓慢上行
    qfq = [1.00 + 0.005 * i for i in range(n)]
    # 快照价：split_at 之前 2x、之后 1x（模拟 1:2 拆分，current_price 合法减半）
    snap = [round(2.0 * qfq[i] if i < split_at else 1.0 * qfq[i], 4) for i in range(n)]

    # TEST_A：快照含拆分台阶，但提供连续 qfq -> 合并价应消除台阶
    cur.executemany(
        "INSERT INTO portfolio_snapshots(date,code,name,quantity,cost_price,current_price,market_value) "
        "VALUES(?,?,?,?,?,?,?)",
        [(dates[i], "TESTA", "TESTA", 100, snap[i], snap[i], snap[i] * 100) for i in range(n)],
    )
    cur.executemany(
        "INSERT INTO etf_price_history(date,code,open,high,low,close,volume,amount,adj_close,source) "
        "VALUES(?,?,?,?,?,?,?,?,?,?)",
        [(dates[i], "TESTA", qfq[i], qfq[i], qfq[i], qfq[i], 0, 0.0, qfq[i], "unit") for i in range(n)],
    )
    # TEST_B：同样快照台阶，但无 etf_price_history -> 合并价回退 raw snapshot（应保留台阶）
    cur.executemany(
        "INSERT INTO portfolio_snapshots(date,code,name,quantity,cost_price,current_price,market_value) "
        "VALUES(?,?,?,?,?,?,?)",
        [(dates[i], "TESTB", "TESTB", 100, snap[i], snap[i], snap[i] * 100) for i in range(n)],
    )
    conn.commit()


def test_position_features_continuous_across_split_with_qfq():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    try:
        conn = sqlite3.connect(path)
        _seed(conn)
        conn.close()

        conn = sqlite3.connect(path)
        feat = build_feature_matrix(conn, ["TESTA", "TESTB"])
        conn.close()

        split_at = 21  # 必须与 _seed 默认 split_at 一致
        dates = pd.bdate_range("2024-01-01", periods=40).strftime("%Y-%m-%d").tolist()
        split_date = dates[split_at]       # 首个减半日（i>=split_at 才减半）
        prev_date = dates[split_at - 1]
        next_date = dates[split_at + 1]

        def row(code, d):
            r = feat[(feat.code == code) & (feat.date == d)]
            assert not r.empty, f"{code} {d} 缺失"
            return r.iloc[0]

        # --- TESTA（有 qfq）：拆分层位置特征应连续且常态 ---
        a_prev = row("TESTA", prev_date)
        a_split = row("TESTA", split_date)
        a_next = row("TESTA", next_date)
        # boll_pctb 常态落在 [0,1]
        assert 0.0 <= a_split["boll_pctb"] <= 1.0, f"TESTA boll_pctb 应常态, got {a_split['boll_pctb']}"
        # 拆分日前后 boll_pctb 连续（无 >0.3 的尖刺）
        assert abs(a_split["boll_pctb"] - a_prev["boll_pctb"]) < 0.3, \
            f"TESTA boll_pctb 跨拆分不应跳变, {a_prev['boll_pctb']}->{a_split['boll_pctb']}"
        assert abs(a_next["boll_pctb"] - a_split["boll_pctb"]) < 0.3, \
            f"TESTA boll_pctb 拆分后不应跳变, {a_split['boll_pctb']}->{a_next['boll_pctb']}"
        # macd 相对量跨拆分连续
        assert abs(a_split["macd"] - a_prev["macd"]) < 0.05, \
            f"TESTA macd 跨拆分不应尖刺, {a_prev['macd']}->{a_split['macd']}"

        # --- TESTB（无 qfq）：拆分日 boll_pctb 应跌出 [0,1]（复现修复前假台阶）---
        b_split = row("TESTB", split_date)
        assert b_split["boll_pctb"] < 0.0 or b_split["boll_pctb"] > 1.0, \
            f"TESTB 无 qfq 应保留拆分假台阶(boll_pctb 越界), got {b_split['boll_pctb']}"
    finally:
        _cleanup_db(path)
