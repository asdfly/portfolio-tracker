"""数据问题十二 · 特征侧(etf_features 37 维)闸门单元测试

验证 build_feature_matrix 在「qfq 缺失的拆分/合并伪收益日」（典型 pre-2018 拆分，
如 510500 2015，etf_price_history 无覆盖 → close 回退到未复权快照价，含台阶）会把
当天位置/收益类特征置 NaN 并打 is_split_merge=1，避免拆分台阶被 predictor 当成真实
价格信号静默吸收。

- 仅当 qfq 缺失（close 含台阶）才置 NaN；qfq 已覆盖的拆分日保持原值（不丢数据）。
- 普通日 / 普通标的：is_split_merge=0，特征保持非 NaN。
不写生产库。
"""
import sqlite3

import pandas as pd
import pytest

from src.analysis.predictor.features import build_feature_matrix


def _make_etf_db(path: str):
    """构造含「X=无 qfq 的拆分日」「Y=普通」的临时库（仅测试用，非生产库）。"""
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    cur.execute(
        "CREATE TABLE portfolio_snapshots ("
        "id INTEGER PRIMARY KEY, date TEXT, code TEXT, name TEXT, "
        "quantity REAL, cost_price REAL, current_price REAL, market_value REAL, "
        "pnl REAL, pnl_rate REAL, ytd_return REAL, beta REAL)")
    # X：01-04 拆分（qty 0.5、mv 连续 ⇒ 触发判别），无 etf_price_history ⇒ close 回退快照价含台阶
    x_rows = [
        (1, "2024-01-01", "X", "X", 100.0, 10.0, 10.0, 1000.0, 0, 0, 0, 0),
        (2, "2024-01-02", "X", "X", 100.0, 10.0, 10.0, 1000.0, 0, 0, 0, 0),
        (3, "2024-01-03", "X", "X", 100.0, 10.0, 10.0, 1000.0, 0, 0, 0, 0),
        (4, "2024-01-04", "X", "X", 50.0, 10.0, 20.0, 1000.0, 0, 0, 0, 0),
    ]
    # Y：全程普通（无拆分）
    y_rows = [
        (5, "2024-01-01", "Y", "Y", 100.0, 10.0, 10.0, 1000.0, 0, 0, 0, 0),
        (6, "2024-01-02", "Y", "Y", 100.0, 10.0, 10.0, 1000.0, 0, 0, 0, 0),
        (7, "2024-01-03", "Y", "Y", 100.0, 10.0, 10.0, 1000.0, 0, 0, 0, 0),
        (8, "2024-01-04", "Y", "Y", 100.0, 10.0, 10.0, 1000.0, 0, 0, 0, 0),
    ]
    cur.executemany(
        "INSERT INTO portfolio_snapshots VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        x_rows + y_rows)
    # build_feature_matrix 依赖的辅助表（空表即可，确保可读、不崩）
    cur.execute("CREATE TABLE index_quotes (date TEXT, code TEXT, name TEXT, "
                "close REAL, change_pct REAL)")
    cur.execute("CREATE TABLE fund_flows (date TEXT, code TEXT, category TEXT, "
                "net_inflow REAL, super_large_inflow REAL, large_inflow REAL)")
    # etf_features 落库表（含 is_split_merge 列）
    cur.execute(
        "CREATE TABLE etf_features ("
        "date TEXT NOT NULL, code TEXT NOT NULL, feat_version TEXT, "
        "ma5 REAL, ma10 REAL, ma20 REAL, ma60 REAL, "
        "macd REAL, macd_signal REAL, macd_hist REAL, rsi_14 REAL, "
        "boll_mid REAL, boll_upper REAL, boll_lower REAL, boll_pctb REAL, "
        "kdj_k REAL, kdj_d REAL, kdj_j REAL, atr_pct REAL, "
        "ret_1d REAL, ret_5d REAL, vol_20d REAL, mom_20d REAL, "
        "vol_5d REAL, vol_60d REAL, vol_ratio_5_20 REAL, ret_60d REAL, "
        "mom_5d REAL, range_20d REAL, parkinson_vol_20d REAL, hl_range_20d REAL, "
        "volume_zscore_20d REAL, hs300_ret_20d REAL, hs300_vol_20d REAL, "
        "is_split_merge BOOLEAN DEFAULT 0, PRIMARY KEY (date, code))")
    # 故意不建 etf_price_history ⇒ load_ohlc 返回 {}，X 的 close 回退快照价含台阶
    conn.commit()
    return conn


def test_etf_features_nulls_and_flags_split_day(tmp_path):
    db = tmp_path / "etf_test.db"
    conn = _make_etf_db(str(db))
    try:
        feat = build_feature_matrix(conn, ["X", "Y"])
        assert not feat.empty
        # --- X 的拆分日 2024-01-04：特征应被置 NaN，is_split_merge=1 ---
        x_split = feat[(feat["code"] == "X") & (feat["date"] == "2024-01-04")]
        assert len(x_split) == 1, "X 拆分日应有且仅有一行"
        row = x_split.iloc[0]
        assert int(row["is_split_merge"]) == 1, "X 拆分日应打 is_split_merge=1"
        assert pd.isna(row["ma5"]), "X 拆分日 ma5 应被置 NaN（消除台阶）"
        assert pd.isna(row["ret_1d"]), "X 拆分日 ret_1d 应被置 NaN"
        # --- X 的普通日：is_split_merge=0；窗口已预热的正常日 ma5 非 NaN ---
        x_norm = feat[(feat["code"] == "X") & (feat["date"] != "2024-01-04")]
        assert (x_norm["is_split_merge"] == 0).all()
        # 01-01/01-02 的 ma5 为 NaN 是 rolling(5,min_periods=3) 预热所致（正常），
        # 仅 01-03（已有 3 点）应非 NaN —— 证明本闸门只清了拆分日、未误伤正常日。
        x_warm = feat[(feat["code"] == "X") & (feat["date"] == "2024-01-03")]
        assert x_warm["ma5"].notna().all(), "X 已预热的正常日 ma5 应保持非 NaN"
        # --- Y 全程普通：无拆分标记；已预热的正常日 ma5 非 NaN（未被误清）---
        y = feat[feat["code"] == "Y"]
        assert (y["is_split_merge"] == 0).all(), "普通标的 Y 不应有拆分标记"
        y_warm = y[y["date"].isin(["2024-01-03", "2024-01-04"])]
        assert y_warm["ma5"].notna().all(), "Y 已预热的普通日 ma5 不应被误置 NaN"
    finally:
        conn.close()


def test_etf_features_flag_column_present(tmp_path):
    """etf_features 必须带 is_split_merge 列（落库 / 下游可消费）。"""
    db = tmp_path / "etf_test.db"
    conn = _make_etf_db(str(db))
    try:
        feat = build_feature_matrix(conn, ["X", "Y"])
        assert "is_split_merge" in feat.columns
        # upsert 后列存在且 ALTER 幂等不报错
        from src.analysis.predictor.features import upsert_features
        n = upsert_features(conn, feat)
        assert n == 8
        cur = conn.cursor()
        cols = [r[1] for r in cur.execute("PRAGMA table_info(etf_features)")]
        assert "is_split_merge" in cols
    finally:
        conn.close()
