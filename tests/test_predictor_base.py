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
        expected = close.iloc[t - 19:t + 1].mean()
        assert abs(tech["ma20"].iloc[t] - expected) < 1e-9
    assert pd.isna(tech["ma20"].iloc[0])  # 窗口不足 -> NaN


def test_build_feature_matrix_integrates(memdb):
    codes = ("510300", "512010")
    feat = build_feature_matrix(memdb, codes)
    assert not feat.empty
    assert set(feat["code"].unique()) <= set(codes)
    for col in ("ma20", "ff_net_inflow_5d", "hs300_ret_20d", "feat_version"):
        assert col in feat.columns
    late = feat[feat["date"] >= "2024-06-01"]
    assert late["ma20"].notna().all()
    assert (feat["feat_version"] == "v2").all()
    # 后期资金流特征也应有值（合成数据已为 510300 提供 fund_flows）
    late_300 = late[late["code"] == "510300"]
    assert late_300["ff_net_inflow_5d"].notna().all()


def test_build_labels_integrates(memdb):
    codes = ("510300", "512010")
    lab = build_labels(memdb, codes)
    assert not lab.empty
    for col in ("fwd_ret_5", "fwd_ret_20", "fwd_ret_60", "is_up_5", "is_up_20", "is_up_60"):
        assert col in lab.columns
    mask = lab["fwd_ret_5"].notna()
    expected_up = (lab.loc[mask, "fwd_ret_5"] > 0).astype("Int64")
    assert (lab.loc[mask, "is_up_5"] == expected_up).all()
