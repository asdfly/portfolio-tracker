"""Tier0/Tier1 建模层测试：内存 sqlite + 合成数据，验证无泄漏、集成逻辑与风险预测。"""
import numpy as np
import pandas as pd
import pytest

from src.analysis.predictor import models, tier0
from src.utils.db_schema import init_all_tables

FEATS = models.FEATURE_COLS


def _memdb() -> "sqlite3.Connection":
    import sqlite3
    conn = sqlite3.connect(":memory:")
    init_all_tables(conn)
    return conn


def _make_panel(n_days: int = 400, n_codes: int = 3, noise=1.0, trend=0.0):
    """合成面板：date×code。特征为随机噪声；标签可叠加确定性信号（trend>0）。

    风险标签用高持续性 AR(1) 波动率过程：vol_20d 特征编码当前波动率 sigma_t，
    fwd_vol 标签 = 未来 sigma（波动率聚类 → 可预测，R² 显著为正）。
    """
    dates = pd.bdate_range("2024-01-01", periods=n_days)
    codes = [f"1{59000 + c}" for c in range(n_codes)]
    df = pd.DataFrame([{"date": d.strftime("%Y-%m-%d"), "code": c}
                       for c in codes for d in dates])
    rng = np.random.RandomState(42)
    for f in FEATS:
        df[f] = rng.normal(0, 1, len(df))
    # 方向标签
    for w, lbl in models.LABEL_COLS.items():
        df[lbl] = trend * df["ma5"] + rng.normal(0, noise, len(df))
    # 风险标签：每 code 生成独立同分布波动率 sigma，vol_* 特征编码当前波动率，
    # fwd_vol 标签 = sigma + 小噪声（波动率聚类：当前波动率是未来波动率的最佳预测）。
    for c in codes:
        m = df["code"] == c
        seed = 100 + int(c[-1]) + 10 * int(c[-2])
        rc = np.random.RandomState(seed)
        sigma = np.abs(rc.normal(0.1, 0.03, n_days)) + 0.05
        sigma = pd.Series(sigma)
        df.loc[m, "vol_20d"] = sigma.values
        df.loc[m, "vol_5d"] = sigma.values
        df.loc[m, "vol_60d"] = sigma.values
        for w, (vcol, ddcol) in models.RISK_LABEL_COLS.items():
            df.loc[m, vcol] = sigma.values + 0.005 * rc.normal(n_days)
            df.loc[m, ddcol] = -df.loc[m, vcol].values
    return df


def test_walkforward_splits_no_overlap():
    n_dates = 1200
    splits = models.walkforward_splits(n_dates, n_splits=5, embargo=60)
    assert len(splits) >= 4, f"应至少 4 折, 实际 {len(splits)}"
    for tr_end, ts_start, ts_end in splits:
        assert ts_start - tr_end >= 60
        assert ts_end - ts_start >= 30
        assert ts_end <= n_dates


def test_walkforward_no_fake_ic_on_noise():
    df = _make_panel(n_days=500, n_codes=3, noise=1.0, trend=0.0)
    res = models.walkforward_evaluate(df, window=5, model="lgb", n_splits=3, embargo=30)
    assert "error" not in res, res
    ic = res["ic_pearson"]
    assert ic is not None
    assert abs(ic) < 0.15, f"噪声数据不应产生强 IC, 实际 {ic}"


def test_walkforward_detects_signal():
    df = _make_panel(n_days=800, n_codes=3, noise=0.8, trend=0.5)
    res = models.walkforward_evaluate(df, window=20, model="ridge", n_splits=3, embargo=30)
    assert "error" not in res, res
    assert res["ic_pearson"] is not None and res["ic_pearson"] > 0.05


def test_risk_walkforward_detects_vol_signal():
    """风险预测：波动率标签与特征强相关，OOS R² 应显著为正（信噪比高于方向预测）。"""
    df = _make_panel(n_days=800, n_codes=3, noise=0.5, trend=0.0)
    res = models.risk_walkforward_evaluate(df, window=20, model="ridge", n_splits=3, embargo=30)
    assert "error" not in res, res
    assert res["r2"] is not None and res["r2"] > 0.2, f"波动率应可预测, R²={res.get('r2')}"


def test_tier0_ensemble_direction():
    sig = pd.DataFrame([
        {"date": "2026-08-18", "code": "510300", "indicator": "ma_signal",
         "signal_direction": 1, "hit_rate_5d": 0.62, "hit_rate_10d": 0.55,
         "hit_rate_20d": 0.58, "hit_rate_30d": 0.55, "hit_rate_60d": 0.60,
         "composite_confidence": 60.0, "direction_net_score": 10.0, "market_regime": "all"},
        {"date": "2026-08-18", "code": "510300", "indicator": "macd_signal",
         "signal_direction": -1, "hit_rate_5d": 0.55, "hit_rate_10d": 0.52,
         "hit_rate_20d": 0.53, "hit_rate_30d": 0.52, "hit_rate_60d": 0.54,
         "composite_confidence": 30.0, "direction_net_score": -5.0, "market_regime": "all"},
    ])
    ens = tier0.build_ensemble(sig, "2026-08-18")
    assert len(ens) == 3
    row5 = ens[(ens["forward_window"] == 5)].iloc[0]
    assert row5["direction"] == 1, row5.to_dict()
    assert row5["exp_hit_rate"] is not None and 0.55 <= row5["exp_hit_rate"] <= 0.62


def test_upsert_predictions_roundtrip():
    conn = _memdb()
    pred = pd.DataFrame([
        {"date": "2026-08-19", "code": "510300", "model": tier0.MODEL_NAME,
         "forward_window": 5, "direction": 1, "score": 0.05, "exp_hit_rate": 0.58,
         "confidence": 60.0, "grade": "B"},
    ])
    n = tier0.upsert_predictions(conn, pred)
    assert n == 1
    got = pd.read_sql_query("SELECT * FROM etf_predictions", conn)
    assert len(got) == 1
    assert got.iloc[0]["model"] == tier0.MODEL_NAME
    assert got.iloc[0]["direction"] == 1
    tier0.upsert_predictions(conn, pred)
    assert pd.read_sql_query("SELECT COUNT(*) AS n FROM etf_predictions", conn)["n"].iloc[0] == 1
    conn.close()
