"""Tier1 轻量 ML：etf_features → LightGBM/Ridge 预测前瞻收益（5/20/60 日）。

对齐可行性报告「阶段2 Tier1 ML」+ QA 护栏：
 - 模型：LightGBM（主，快/低内存/适小样本）+ Ridge 线性回归（对照，保可解释）。
 - 验证：walk-forward 滚动（训练只用过去）+ embargo（默认 60 交易日）防标签重叠泄漏；
   禁随机切分（会引入时间/截面泄漏）。
 - 指标：OOS IC（Pearson/Spearman，各折 + 合并）+ HAC 稳健 t（stats_utils.newey_west_tstat，
   对日 IC 序列）。
 - 门槛：目标 1周≥0.03 / 1月≥0.05 / 1季≥0.06；否决 IC<0.02 或 |t|<2（任一触发不许上线）。
 - 护栏：特征数 ≤ N_train/10（30 特征 << 训练样本，自动满足）；pooled 跨代码训练缓解
   小样本（弃 per-ETF 单标的模型）。
 - 红线：仅增强参考，不自动调仓、不做点位承诺。

运行（从项目根）：
    python -m src.analysis.predictor.models
"""
import datetime as dt
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

# 数值特征（对应 etf_features，除 date/code/feat_version）
FEATURE_COLS = [
    "ma5", "ma10", "ma20", "ma60", "macd", "macd_signal", "macd_hist",
    "rsi_14", "boll_mid", "boll_upper", "boll_lower", "boll_pctb",
    "kdj_k", "kdj_d", "kdj_j", "atr_14", "atr_pct",
    "ret_1d", "ret_5d", "ret_20d", "vol_20d", "mom_20d",
    # v2 新增：波动率结构 + 量价 + 多周期动量
    "vol_5d", "vol_60d", "vol_ratio_5_20", "ret_60d", "mom_5d", "range_20d",
    "parkinson_vol_20d", "hl_range_20d", "volume_zscore_20d",
    "ff_net_inflow_5d", "ff_net_inflow_20d", "ff_super_net_5d", "ff_large_net_5d",
    "hs300_ret_20d", "hs300_vol_20d",
]
WINDOWS = (5, 20, 60)
LABEL_COLS = {w: f"fwd_ret_{w}" for w in WINDOWS}

# 风险标签（未来已实现波动率 / 最大回撤）
RISK_LABEL_COLS = {w: (f"fwd_vol_{w}", f"fwd_max_dd_{w}") for w in WINDOWS}

# QA 门槛
TARGET_IC = {5: 0.03, 20: 0.05, 60: 0.06}   # 1周/1月/1季
VETO_IC = 0.02
VETO_T = 2.0
EMBARGO_DAYS = 60
N_SPLITS = 5

# LightGBM 参数（小样本强正则）
LGB_PARAMS = dict(
    objective="regression",
    num_leaves=15,
    max_depth=4,
    learning_rate=0.05,
    n_estimators=200,
    min_child_samples=50,
    subsample=0.8,
    subsample_freq=1,
    colsample_bytree=0.8,
    reg_lambda=1.0,
    random_state=42,
    verbose=-1,
)
RIDGE_ALPHA = 10.0


def load_panel(conn) -> pd.DataFrame:
    """读 etf_features + etf_forward_returns，merge 成 (date, code, features..., labels)。"""
    feat = pd.read_sql_query(
        f"SELECT date, code, {', '.join(FEATURE_COLS)} FROM etf_features", conn)
    lab_cols = [LABEL_COLS[w] for w in WINDOWS]
    risk_cols = [c for w in WINDOWS for c in RISK_LABEL_COLS[w]]
    lab = pd.read_sql_query(
        f"SELECT date, code, {', '.join(lab_cols + risk_cols)} FROM etf_forward_returns", conn)
    df = feat.merge(lab, on=["date", "code"], how="inner")
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values(["date", "code"]).reset_index(drop=True)


def walkforward_splits(n_dates: int, n_splits: int = N_SPLITS,
                       embargo: int = EMBARGO_DAYS) -> List[Tuple[int, int, int]]:
    """生成 walk-forward 折 [(train_end_excl, test_start, test_end_excl)]。"""
    first = int(n_dates * 0.40)
    avail = n_dates - first - embargo
    if avail < 2 * n_splits:
        return []
    step = max(30, avail // (n_splits + 1))
    splits = []
    for i in range(n_splits):
        tr_end = first + i * step
        ts_start = tr_end + embargo
        ts_end = min(n_dates, ts_start + step)
        if ts_end - ts_start >= 30:
            splits.append((tr_end, ts_start, ts_end))
    return splits


def _fit_lgb(X_tr: pd.DataFrame, y_tr: np.ndarray):
    import lightgbm as lgb
    return lgb.LGBMRegressor(**LGB_PARAMS).fit(X_tr, y_tr)


def _fit_ridge(X_tr: pd.DataFrame, y_tr: np.ndarray):
    from sklearn.linear_model import Ridge
    from sklearn.preprocessing import StandardScaler
    sc = StandardScaler().fit(X_tr)
    mdl = Ridge(alpha=RIDGE_ALPHA).fit(sc.transform(X_tr), y_tr)
    return sc, mdl


def _pred_ridge(sc, mdl, X):
    return mdl.predict(sc.transform(X))


def _ic(y_true: np.ndarray, y_pred: np.ndarray) -> Tuple[float, float]:
    from scipy.stats import pearsonr, spearmanr
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)
    ok = np.isfinite(y_true) & np.isfinite(y_pred)
    if ok.sum() < 10:
        return np.nan, np.nan
    p = pearsonr(y_true[ok], y_pred[ok])[0]
    s = spearmanr(y_true[ok], y_pred[ok])[0]
    return (float(p) if np.isfinite(p) else np.nan,
            float(s) if np.isfinite(s) else np.nan)


def walkforward_evaluate(df: pd.DataFrame, window: int, model: str = "lgb",
                         n_splits: int = N_SPLITS,
                         embargo: int = EMBARGO_DAYS) -> dict:
    """对单窗口跑 walk-forward，返回 {fold_ics, ic_pearson, ic_spearman, t_stat, n_test}。"""
    y_col = LABEL_COLS[window]
    panel = df.dropna(subset=[y_col]).copy()
    if panel.empty:
        return {"error": "no labeled rows"}
    dates = sorted(panel["date"].unique())
    pos = {d: i for i, d in enumerate(dates)}
    panel["_pos"] = panel["date"].map(pos)
    n_dates = len(dates)
    splits = walkforward_splits(n_dates, n_splits=n_splits, embargo=embargo)
    if not splits:
        return {"error": f"insufficient history ({n_dates} days)"}

    X_all = panel[FEATURE_COLS].fillna(0.0)
    y_all = panel[y_col].astype(float)
    folds = []
    ys, ps = [], []
    daily_records = []
    for tr_end, ts_start, ts_end in splits:
        tr_mask = panel["_pos"] < tr_end
        te_mask = (panel["_pos"] >= ts_start) & (panel["_pos"] < ts_end)
        if tr_mask.sum() < 300 or te_mask.sum() < 30:
            continue
        X_tr, y_tr = X_all[tr_mask], y_all[tr_mask]
        X_te, y_te = X_all[te_mask], y_all[te_mask]
        if model == "lgb":
            mdl = _fit_lgb(X_tr, y_tr)
            p = mdl.predict(X_te)
        else:
            sc, mdl = _fit_ridge(X_tr, y_tr)
            p = _pred_ridge(sc, mdl, X_te)
        ic_p, ic_s = _ic(y_te.values, p)
        if not np.isnan(ic_p):
            folds.append(ic_p)
            ys.extend(y_te.tolist())
            ps.extend(p.tolist())
            sub = panel[te_mask][["date"]].copy()
            sub["_y"] = y_te.values
            sub["_p"] = p
            for d, g in sub.groupby("date"):
                ip, _ = _ic(g["_y"].values, g["_p"].values)
                if not np.isnan(ip):
                    daily_records.append(ip)

    if not folds:
        return {"error": "no valid folds"}
    ic_p_all, ic_s_all = _ic(np.asarray(ys), np.asarray(ps))
    t_stat = float("nan")
    if len(daily_records) >= 20:
        from src.analysis.stats_utils import newey_west_tstat
        t_stat = float(newey_west_tstat(np.asarray(daily_records)))
    return {
        "fold_ics": [round(f, 4) for f in folds],
        "ic_pearson": round(float(ic_p_all), 4) if np.isfinite(ic_p_all) else None,
        "ic_spearman": round(float(ic_s_all), 4) if np.isfinite(ic_s_all) else None,
        "t_stat": round(t_stat, 3) if np.isfinite(t_stat) else None,
        "n_test": len(ys),
        "n_folds": len(folds),
    }


def _verdict(res: dict) -> str:
    ic = res.get("ic_pearson")
    t = res.get("t_stat")
    if ic is None:
        return "NA"
    if ic < VETO_IC or (t is not None and abs(t) < VETO_T):
        return "VETO"
    w = res.get("window")
    target = TARGET_IC.get(w, 0.03)
    return "PASS" if ic >= target else "BELOW_TARGET"


def predict_latest(conn, model: str = "lgb", as_of: Optional[str] = None) -> pd.DataFrame:
    """用全量历史重训模型，预测最新特征日各 ETF 的 5/20/60 前瞻方向 → DataFrame。"""
    df = load_panel(conn)
    if as_of is None:
        as_of = df["date"].max().strftime("%Y-%m-%d")
    latest = df[df["date"] == pd.to_datetime(as_of)]
    if latest.empty:
        raise ValueError(f"as_of={as_of} 无特征数据")
    rows = []
    for w in WINDOWS:
        y_col = LABEL_COLS[w]
        panel = df.dropna(subset=[y_col]).copy()
        X_all = panel[FEATURE_COLS].fillna(0.0)
        y_all = panel[y_col].astype(float)
        if model == "lgb":
            mdl = _fit_lgb(X_all, y_all)
        else:
            sc, mdl = _fit_ridge(X_all, y_all)
        X_new = latest[FEATURE_COLS].fillna(0.0)
        pred = mdl.predict(X_new) if model == "lgb" else _pred_ridge(sc, mdl, X_new)
        for r, p in zip(latest.itertuples(index=False), pred):
            d = 1 if p > 0 else (-1 if p < 0 else 0)
            rows.append((as_of, r.code, w, d, round(float(p), 6), model))
    return pd.DataFrame(rows, columns=["date", "code", "forward_window", "direction",
                                       "score", "model"])


def run_tier1(conn, log=print) -> dict:
    """Tier1 主流程：walk-forward 验证 lgb/ridge × 三窗口 → 最新预测落 etf_predictions。"""
    df = load_panel(conn)
    if df.empty:
        log("[Tier1] 面板数据为空")
        return {"error": "empty panel"}
    log(f"[Tier1] 面板 {len(df)} 行, {df['code'].nunique()} 只, 特征 {len(FEATURE_COLS)} 维")

    results = {}
    for w in WINDOWS:
        results[w] = {}
        for m in ("lgb", "ridge"):
            res = walkforward_evaluate(df, w, model=m)
            res["window"] = w
            res["model"] = m
            res["verdict"] = _verdict(res)
            results[w][m] = res
            log(f"[Tier1] w={w} {m}: IC={res.get('ic_pearson')} "
                f"Spearman={res.get('ic_spearman')} t={res.get('t_stat')} "
                f"verdict={res.get('verdict')} (folds={res.get('n_folds')})")

    n_pred = 0
    for m in ("lgb", "ridge"):
        try:
            pred = predict_latest(conn, model=m)
            n_pred += _upsert_tier1(conn, pred)
        except Exception as e:  # noqa: BLE001
            log(f"[Tier1] {m} 最新预测失败: {e}")
    log(f"[Tier1] 最新预测落表 {n_pred} 行")
    return {"results": results, "predictions": n_pred}


def _upsert_tier1(conn, pred: pd.DataFrame) -> int:
    cur = conn.cursor()
    for _, r in pred.iterrows():
        conf = min(100.0, abs(r["score"]) * 5000)
        cur.execute(
            """INSERT OR REPLACE INTO etf_predictions
               (date, code, model, forward_window, direction, score, probability,
                confidence, grade, features, created_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (r["date"], r["code"], r["model"], int(r["forward_window"]), int(r["direction"]),
             r["score"], r["score"], round(conf, 1),
             "A" if conf >= 70 else ("B" if conf >= 50 else ("C" if conf >= 30 else "D")),
             "lgbm_panel", dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        )
    conn.commit()
    return len(pred)


# ============ 风险预测（未来已实现波动率）============

def _r2(y_true, y_pred):
    from sklearn.metrics import r2_score
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)
    ok = np.isfinite(y_true) & np.isfinite(y_pred)
    if ok.sum() < 10:
        return np.nan
    return float(r2_score(y_true[ok], y_pred[ok]))


def risk_walkforward_evaluate(df: pd.DataFrame, window: int, model: str = "lgb",
                              n_splits: int = N_SPLITS,
                              embargo: int = EMBARGO_DAYS) -> dict:
    """预测未来 n 日已实现波动率 fwd_vol_n：回归 R²/IC + 高低波动分类 AUC。

    波动率有强自相关/聚类性，信噪比远高于价格方向，预期 R²/AUC 显著优于方向预测。
    """
    vol_col = f"fwd_vol_{window}"
    panel = df.dropna(subset=[vol_col]).copy()
    if panel.empty:
        return {"error": "no labeled rows"}
    dates = sorted(panel["date"].unique())
    pos = {d: i for i, d in enumerate(dates)}
    panel["_pos"] = panel["date"].map(pos)
    splits = walkforward_splits(len(dates), n_splits=n_splits, embargo=embargo)
    if not splits:
        return {"error": f"insufficient history ({len(dates)} days)"}

    X_all = panel[FEATURE_COLS].fillna(0.0)
    y_all = panel[vol_col].astype(float)
    ys, ps = [], []
    ys_cls, ps_cls = [], []
    for tr_end, ts_start, ts_end in splits:
        tr_mask = panel["_pos"] < tr_end
        te_mask = (panel["_pos"] >= ts_start) & (panel["_pos"] < ts_end)
        if tr_mask.sum() < 300 or te_mask.sum() < 30:
            continue
        X_tr, y_tr = X_all[tr_mask], y_all[tr_mask]
        X_te, y_te = X_all[te_mask], y_all[te_mask]
        if model == "lgb":
            mdl = _fit_lgb(X_tr, y_tr)
            p = mdl.predict(X_te)
        else:
            sc, mdl = _fit_ridge(X_tr, y_tr)
            p = _pred_ridge(sc, mdl, X_te)
        ys.extend(y_te.tolist())
        ps.extend(p.tolist())
        # 高低波动分类：以折内训练集波动率中位数为阈值
        thresh = float(np.median(y_tr))
        y_cls = (y_te.values > thresh).astype(int)
        if len(np.unique(y_cls)) == 2:
            ys_cls.extend(y_cls.tolist())
            ps_cls.extend(p.tolist())

    if not ys:
        return {"error": "no valid folds"}
    r2 = _r2(np.asarray(ys), np.asarray(ps))
    ic_p, _ = _ic(np.asarray(ys), np.asarray(ps))
    auc = None
    if len(ys_cls) >= 50 and len(np.unique(ys_cls)) == 2:
        from sklearn.metrics import roc_auc_score
        try:
            auc = float(roc_auc_score(ys_cls, ps_cls))
        except ValueError:
            auc = None
    return {
        "r2": round(r2, 4) if np.isfinite(r2) else None,
        "ic_pearson": round(ic_p, 4) if np.isfinite(ic_p) else None,
        "auc": round(auc, 4) if auc is not None else None,
        "n_test": len(ys),
    }


def run_risk_prediction(conn, log=print) -> dict:
    """风险预测主流程：walk-forward 评估波动率预测（lgb/ridge × 三窗口）。"""
    df = load_panel(conn)
    if df.empty:
        log("[Risk] 面板数据为空")
        return {"error": "empty panel"}
    log(f"[Risk] 面板 {len(df)} 行，波动率标签 fwd_vol_5/20/60")
    results = {}
    for w in WINDOWS:
        results[w] = {}
        for m in ("lgb", "ridge"):
            res = risk_walkforward_evaluate(df, w, model=m)
            res["window"] = w
            res["model"] = m
            results[w][m] = res
            log(f"[Risk] w={w} {m}: R²={res.get('r2')} IC={res.get('ic_pearson')} "
                f"AUC={res.get('auc')} (n={res.get('n_test')})")
    return {"results": results}


def _main():
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    from config.settings import DATABASE_PATH
    from src.utils.database import get_db_connection
    conn = get_db_connection(str(DATABASE_PATH))
    summ = run_tier1(conn, log=print)
    print("=== Tier1 汇总 ===")
    for w, by_m in summ.get("results", {}).items():
        for m, r in by_m.items():
            print(f"  w={w} {m}: IC={r.get('ic_pearson')} t={r.get('t_stat')} {r.get('verdict')}")
    print(f"  预测落表: {summ.get('predictions')}")


if __name__ == "__main__":
    _main()
