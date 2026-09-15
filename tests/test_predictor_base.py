"""Phase 0 预测底座测试（不触碰生产库，全部基于内存 sqlite + 合成数据）。

覆盖：
 1. 前瞻收益标签口径对齐 signal_backtest（fwd_ret_n = close[t+n]/close[t]-1）
 2. 标签无未来函数（注入未来尖峰仅影响对应未来的标签，绝不影响特征）
 3. 技术特征无未来泄漏（ma20[t] 仅用 close[t-19..t]）
 4. build_feature_matrix 集成（技术/资金流/市场因子拼接、后期无 NaN）
 5. build_labels 集成（方向标签与收益一致）
"""
import numpy as np
import pandas as pd
import pytest
import sqlite3

from src.analysis.predictor.features import compute_technical_from_close, build_feature_matrix
from src.analysis.predictor.labels import compute_forward_returns, build_labels
from src.utils.db_schema import init_all_tables


@pytest.fixture
def memdb():
    conn = sqlite3.connect(":memory:")
    init_all_tables(conn)  # 同时验证三张新表已注册
    dates = pd.date_range("2024-01-01", periods=120, freq="B").strftime("%Y-%m-%d").tolist()
    rng = np.random.default_rng(0)
    rows = []
    for code in ("510300", "512010"):
        price = 1.0
        for d in dates:
            price *= (1 + rng.normal(0, 0.01))
            rows.append((d, code, round(price, 4)))
    conn.executemany(
        "INSERT INTO portfolio_snapshots(date, code, current_price) VALUES(?,?,?)", rows
    )
    ff = [(d, "510300", 100.0, 50.0, 30.0, "etf") for d in dates]
    conn.executemany(
        "INSERT INTO fund_flows(date, code, net_inflow, super_large_inflow, large_inflow, category) "
        "VALUES(?,?,?,?,?,?)", ff
    )
    idx = [(d, "sh000300", "沪深300", 3000.0) for d in dates]
    conn.executemany(
        "INSERT INTO index_quotes(date, code, name, close) VALUES(?,?,?,?)", idx
    )
    conn.commit()
    return conn


def test_forward_returns_formula():
    close = pd.Series([100, 101, 102, 104, 103, 105],
                      index=[f"2024-01-{i:02d}" for i in range(1, 7)])
    lab = compute_forward_returns(close, windows=(2,))
    assert abs(lab["fwd_ret_2"].iloc[0] - (102 / 100 - 1)) < 1e-9
    # iloc[3] 能看到 iloc[5]（105/104-1）；iloc[4]/iloc[5] 无足够未来数据 -> NaN
    assert abs(lab["fwd_ret_2"].iloc[3] - (105 / 104 - 1)) < 1e-9
    assert pd.isna(lab["fwd_ret_2"].iloc[5])  # 无未来数据 -> NaN
    assert bool(lab["is_up_2"].iloc[0]) == (lab["fwd_ret_2"].iloc[0] > 0)


def test_forward_returns_no_leakage():
    dates = pd.date_range("2024-01-01", periods=120, freq="B").strftime("%Y-%m-%d").tolist()
    close = pd.Series(np.linspace(1, 2, 120), index=dates)
    close.iloc[-1] = 9999.0  # 未来尖峰
    lab = compute_forward_returns(close, windows=(5,))
    # 尖峰仅影响能"看到"它的前瞻标签（t=114 能看到 t=119）
    assert lab["fwd_ret_5"].iloc[114] > 100
    # 最后 5 行无足够未来数据 -> NaN（绝不回填/前视）
    assert pd.isna(lab["fwd_ret_5"].iloc[119])
    assert pd.isna(lab["fwd_ret_5"].iloc[118])


def test_technical_no_future_leakage():
    n = 200
    idx = pd.date_range("2020-01-01", periods=n, freq="B")
    rng = np.random.default_rng(1)
    close = pd.Series(np.cumprod(1 + rng.normal(0, 0.01, n)), index=idx)
    tech = compute_technical_from_close(close)
    for t in (50, 100, 150):
        # P1-6 R3：ma20 现为相对量 close/ma-1（仍仅用 close[t-19..t]，无未来泄漏）
        expected = close.iloc[t] / close.iloc[t - 19:t + 1].mean() - 1.0
        assert abs(tech["ma20"].iloc[t] - expected) < 1e-9
    assert pd.isna(tech["ma20"].iloc[0])  # 窗口不足 -> NaN


def test_build_feature_matrix_integrates(memdb):
    codes = ("510300", "512010")
    feat = build_feature_matrix(memdb, codes)
    assert not feat.empty
    assert set(feat["code"].unique()) <= set(codes)
    for col in ("ma20", "vol_20d", "hs300_ret_20d", "feat_version"):
        assert col in feat.columns
    late = feat[feat["date"] >= "2024-06-01"]
    assert late["ma20"].notna().all()
    assert (feat["feat_version"] == "v3").all()
    # 后期技术特征也应无 NaN（合成快照已为两标的提供完整收盘价序列）
    late_300 = late[late["code"] == "510300"]
    assert late_300["vol_20d"].notna().all()


def test_build_labels_integrates(memdb):
    codes = ("510300", "512010")
    lab = build_labels(memdb, codes)
    assert not lab.empty
    for col in ("fwd_ret_5", "fwd_ret_20", "fwd_ret_60", "is_up_5", "is_up_20", "is_up_60"):
        assert col in lab.columns
    mask = lab["fwd_ret_5"].notna()
    expected_up = (lab.loc[mask, "fwd_ret_5"] > 0).astype("Int64")
    assert (lab.loc[mask, "is_up_5"] == expected_up).all()


# ==================== P1-6 量纲回归护栏（2026-09-15 事故后加固）====================
# 事故：ma*/macd*/boll_* 由绝对价改相对量后未做全表重算，导致同一列混两套尺度。
# 根因：etf_features PK=(date, code)，feat_version 不在键里 -> 升版本号无法隔离。
_ABS_TOL = 1e-6  # 同代码同源应精确复现；容差仅吸收浮点噪声


def test_stored_features_scale_matches_recompute():
    """护栏：库内 etf_features 存量值必须与【当前代码】现算值同尺度。

    任何「改量纲/语义却不全表重算」都会让存量(旧尺度) 与现算(新尺度) 不一致而立即失败：
    例如把 ma20 由绝对价改相对量却不重算，存量≈4.6、现算≈-0.02，断言直接失败。
    测试期 DATABASE_PATH 已被 conftest 改道到「生产库副本」——只读比对，零污染。
    """
    import os
    from config.settings import DATABASE_PATH
    p = str(DATABASE_PATH)
    if not os.path.exists(p):
        pytest.skip("无可用数据库（CI 下 DATABASE_PATH=:memory:）")
    conn = sqlite3.connect(p)
    try:
        codes = [r[0] for r in conn.execute(
            "SELECT code FROM etf_features GROUP BY code HAVING COUNT(*) >= 60 "
            "ORDER BY code LIMIT 3").fetchall()]
        if not codes:
            pytest.skip("etf_features 无足够历史行")
        mx = conn.execute("SELECT MAX(date) FROM etf_features").fetchone()[0]
        checks = ["ma20", "macd", "boll_mid", "rsi_14", "mom_20d", "vol_20d"]
        stored = pd.read_sql_query(
            f"SELECT date, code, {', '.join(checks)} FROM etf_features "
            f"WHERE code IN ({', '.join('?' * len(codes))})", conn, params=codes)
        fresh = build_feature_matrix(conn, codes, as_of=mx)
        merged = fresh[["date", "code"] + checks].merge(
            stored, on=["date", "code"], suffixes=("_fresh", "_db"))
        assert len(merged) >= 30, f"可比对行数过少: {len(merged)}"
        for col in checks:
            a = pd.to_numeric(merged[col + "_fresh"], errors="coerce")
            b = pd.to_numeric(merged[col + "_db"], errors="coerce")
            ok = a.notna() & b.notna()
            assert int(ok.sum()) >= 30, f"{col} 可比对非空行过少: {int(ok.sum())}"
            md = float((a[ok] - b[ok]).abs().max())
            assert md < _ABS_TOL, (
                f"{col} 存量与现算不一致(max|Δ|={md:.6g})：疑全表未重算或量纲漂移")
    finally:
        conn.close()
