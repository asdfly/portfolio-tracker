"""风险模型上线门禁：必须在 OOS 上显著超越免费基线 vol_20d，否则 VETO。

背景
----
波动率有强自相关/聚类性，因此"预测未来 20 日已实现波动率"这件事本身极易做出好看的
指标——而最简单、零成本的做法是直接把昨日已实现波动率 vol_20d 外推。所以风险模型
的价值不取决于绝对 R²/IC，而取决于**是否显著超过了这条免费基线**。

判据：主判据为「日频截面 Spearman IC + HAC t」，R² 仅作参考输出
------------------------------------------------------------------
1. 模型自身有效：OOS 截面 IC(Spearman) ≥ 0.02 且 HAC(Newey-West) |t| ≥ 2.0。
2. 显著超越基线：日度截面 IC 差值序列 (IC_model − IC_vol20d) 的 HAC |t| ≥ 2.0
   且均值 > 0；这是"显著超越"的核心检验。
3. 截面 IC 不得低于基线（防止统计不显著但碰巧通过）。
4. 多窗口/多模型同时检验时，对第 2 步的 p 值做 BH-FDR 校正后再判显著。

HAC 滞后阶数取 max(预测窗口 h, NW 默认截断)：标签 fwd_vol_h 在相邻交易日重叠 h−1 个
观测，导致日度 IC 序列自相关可达 h−1 阶；滞后不足会低估标准误、放大 |t|。

R² 为什么不参与判定：walkforward_splits 按**行数**切折，删行会整体移动折边界，
R² 随之跳变（w=20 仅删 1.7% 的样本，R² 就移动 0.60），因此 R² 不可跨样本比较、
不适合做阈值判据。IC 是同一交易日截面内的配对比较，不受折边界影响。
r2 / base_r2 仍照常计算输出，仅供解读，永远不会触发 VETO。

任一判据不满足 → VETO，不准上线。统计工具复用 src/analysis/stats_utils.py。
"""
from typing import Dict, List, Optional, Sequence

import numpy as np
import pandas as pd

from src.analysis.stats_utils import _lag_truncation, benjamini_hochberg, newey_west_tstat
from src.analysis.predictor.models import (
    EMBARGO_DAYS, N_SPLITS, VETO_IC, VETO_T,
    FEATURE_COLS, _fit_lgb, _fit_ridge, _ic, _pred_ridge, _r2,
    walkforward_splits,
)

# 免费基线：历史 20 日已实现波动率直接外推（features.py: ret.rolling(20).std()，
# 与标签 fwd_vol_n「日收益标准差」同口径，无需任何训练）
BASELINE_FEATURE = "vol_20d"
_MIN_SECTION = 5          # 单日截面最少样本数，低于此不计算当日 IC
_MIN_DAILY_IC = 20        # 日度 IC 序列最短长度，低于此不计算 HAC t


def _daily_section_ic(frame: pd.DataFrame, pred_col: str) -> pd.Series:
    """按交易日截面算 Spearman IC，返回以日期为索引的日度 IC 序列。"""
    rec: Dict[pd.Timestamp, float] = {}
    for d, g in frame.groupby("date", sort=True):
        if len(g) < _MIN_SECTION:
            continue
        _, sp = _ic(g["_y"].to_numpy(dtype=float), g[pred_col].to_numpy(dtype=float))
        if sp is not None and np.isfinite(sp):
            rec[d] = float(sp)
    return pd.Series(rec, dtype=float).sort_index()


def _hac_lags(n: int, window: int) -> int:
    """HAC 滞后阶数：至少覆盖预测窗口 h。

    标签 fwd_vol_h 在相邻交易日重叠 h−1 个观测，日度 IC 序列自相关可达 h−1 阶；
    滞后阶数不足会低估标准误、系统性放大 |t|，故强制 lags ≥ h。
    """
    return max(int(window), _lag_truncation(n))


def _hac_t(ic_series: pd.Series, window: int) -> Optional[float]:
    if len(ic_series) < _MIN_DAILY_IC:
        return None
    return float(newey_west_tstat(ic_series.to_numpy(dtype=float),
                                  lags=_hac_lags(len(ic_series), window)))


def _p_from_t(t: Optional[float]) -> float:
    """双尾 p 值（正态近似，与 HAC 大样本 t 检验一致）。"""
    if t is None or not np.isfinite(t):
        return 1.0
    from scipy.stats import norm
    return float(2.0 * norm.sf(abs(t)))


def _auc(labels: Sequence[int], scores: Sequence[float]) -> Optional[float]:
    from sklearn.metrics import roc_auc_score
    if len(labels) < 50 or len(np.unique(labels)) != 2:
        return None
    try:
        return float(roc_auc_score(labels, scores))
    except ValueError:
        return None


def risk_walkforward_vs_baseline(df: pd.DataFrame, window: int, model: str = "lgb",
                                 baseline_feature: str = BASELINE_FEATURE,
                                 label_col: Optional[str] = None,
                                 n_splits: int = N_SPLITS,
                                 embargo: int = EMBARGO_DAYS,
                                 baseline_mode: str = "raw") -> dict:
    """walk-forward 评估风险模型，并在**同一 OOS 样本**上评估免费基线。

    与 models.risk_walkforward_evaluate 的折划分/embargo 完全一致，仅额外产出
    基线的 R²/IC/AUC 与日度 IC 序列，用于配对比较。

    baseline_mode:
        "raw"    —— vol_20d 直接外推（默认，零参数）。
        "linear" —— 折内用 y~a+b·vol_20d 做一元标定后再外推。仍是免费基线（1 个参数），
                    且是单调变换，故 IC/AUC 与 raw 完全相同，只改善 R²。用于排除
                    "基线只是没标定才输 R²"的质疑。
    """
    if label_col is None:
        label_col = f"fwd_vol_{window}"
    if baseline_feature not in df.columns:
        return {"error": f"缺少基线特征列 {baseline_feature}"}
    panel = df.dropna(subset=[label_col, baseline_feature]).copy()
    if panel.empty:
        return {"error": "no labeled rows"}

    dates = sorted(panel["date"].unique())
    pos = {d: i for i, d in enumerate(dates)}
    panel["_pos"] = panel["date"].map(pos)
    splits = walkforward_splits(len(dates), n_splits=n_splits, embargo=embargo)
    if not splits:
        return {"error": f"insufficient history ({len(dates)} days)"}

    X_all = panel[FEATURE_COLS].fillna(0.0)
    y_all = panel[label_col].astype(float)
    base_all = panel[baseline_feature].astype(float)  # 基线无需训练，直接外推

    rows, ys_cls, ps_cls, ps_base_cls = [], [], [], []
    for tr_end, ts_start, ts_end in splits:
        tr_mask = panel["_pos"] < tr_end
        te_mask = (panel["_pos"] >= ts_start) & (panel["_pos"] < ts_end)
        if tr_mask.sum() < 300 or te_mask.sum() < 30:
            continue
        X_tr, y_tr = X_all[tr_mask], y_all[tr_mask]
        X_te, y_te = X_all[te_mask], y_all[te_mask]
        if baseline_mode == "linear":
            b_tr = base_all[tr_mask].to_numpy(dtype=float)
            coef = np.polyfit(b_tr, y_tr.to_numpy(dtype=float), 1)
        else:
            coef = None
        if model == "lgb":
            p = _fit_lgb(X_tr, y_tr).predict(X_te)
        else:
            sc, mdl = _fit_ridge(X_tr, y_tr)
            p = _pred_ridge(sc, mdl, X_te)
        p = np.asarray(p, dtype=float)
        p_base = base_all[te_mask].to_numpy(dtype=float)
        if coef is not None:
            p_base = coef[0] * p_base + coef[1]
        sub = panel[te_mask][["date"]].copy()
        sub["_y"] = y_te.to_numpy(dtype=float)
        sub["_p"] = p
        sub["_p_base"] = p_base
        rows.append(sub)
        # 高低波动分类：折内训练集中位数为阈值，模型与基线共用同一套标签
        thresh = float(np.median(y_tr))
        y_cls = (y_te.to_numpy(dtype=float) > thresh).astype(int)
        if len(np.unique(y_cls)) == 2:
            ys_cls.extend(y_cls.tolist())
            ps_cls.extend(p.tolist())
            ps_base_cls.extend(p_base.tolist())

    if not rows:
        return {"error": "no valid folds"}
    oos = pd.concat(rows, ignore_index=True)

    r2 = _r2(oos["_y"].to_numpy(), oos["_p"].to_numpy())
    r2_base = _r2(oos["_y"].to_numpy(), oos["_p_base"].to_numpy())
    ic_p, _ = _ic(oos["_y"].to_numpy(), oos["_p"].to_numpy())
    ic_p_base, _ = _ic(oos["_y"].to_numpy(), oos["_p_base"].to_numpy())
    ic_series = _daily_section_ic(oos, "_p")
    ic_series_base = _daily_section_ic(oos, "_p_base")

    # 配对差值：同一交易日的两个 IC 相减，剔除日期不重合的部分
    joined = pd.concat([ic_series.rename("m"), ic_series_base.rename("b")],
                       axis=1, join="inner").dropna()
    diff = (joined["m"] - joined["b"]) if len(joined) else pd.Series(dtype=float)

    t_self = _hac_t(ic_series, window)
    t_base = _hac_t(ic_series_base, window)
    t_diff = _hac_t(diff, window)

    res = {
        "window": window,
        "model": model,
        "label_col": label_col,
        "baseline": baseline_feature,
        "n_test": int(len(oos)),
        "n_folds": len(rows),
        # 模型
        "r2": round(float(r2), 4) if np.isfinite(r2) else None,
        "ic_pearson": round(float(ic_p), 4) if np.isfinite(ic_p) else None,
        "ic_spearman": round(float(ic_series.mean()), 4) if len(ic_series) else None,
        "auc": round(_auc(ys_cls, ps_cls), 4) if _auc(ys_cls, ps_cls) is not None else None,
        "ic_t": round(t_self, 3) if t_self is not None else None,
        # 基线
        "base_r2": round(float(r2_base), 4) if np.isfinite(r2_base) else None,
        "base_ic_pearson": round(float(ic_p_base), 4) if np.isfinite(ic_p_base) else None,
        "base_ic_spearman": round(float(ic_series_base.mean()), 4) if len(ic_series_base) else None,
        "base_auc": round(_auc(ys_cls, ps_base_cls), 4) if _auc(ys_cls, ps_base_cls) is not None else None,
        "base_ic_t": round(t_base, 3) if t_base is not None else None,
        # 配对比较
        "ic_diff": round(float(diff.mean()), 4) if len(diff) else None,
        "ic_diff_t": round(t_diff, 3) if t_diff is not None else None,
        "ic_diff_p": round(_p_from_t(t_diff), 4),
        "n_ic_days": int(len(diff)),
        "hac_lags": _hac_lags(len(diff), window) if len(diff) else None,
        # r2 / base_r2 为参考输出，不参与门禁判定（见模块 docstring）
        "r2_note": "参考输出，不参与判定",
    }
    res["verdict"], res["reasons"] = _verdict_risk(res)
    return res


def _verdict_risk(res: dict) -> tuple:
    """单条评估的门禁判定。返回 (verdict, reasons)。

    verdict ∈ {"PASS", "VETO", "NA"}。只有同时满足「自身有效」+「显著超越基线」才 PASS。

    主判据全部基于日频截面 Spearman IC 及其 HAC t。**R² 不参与判定**：
    walkforward_splits 按行数切折，删行会整体移动折边界导致 R² 跳变，
    不可跨样本比较。r2 / base_r2 仅在结果里作参考输出。
    """
    ic = res.get("ic_spearman")
    t = res.get("ic_t")
    if ic is None:
        return "NA", ["无有效 OOS 截面 IC"]
    reasons: List[str] = []

    # 1. 模型自身有效（口径同方向预测线）
    if ic < VETO_IC:
        reasons.append(f"截面 IC {ic:.4f} < 否决线 {VETO_IC}")
    if t is None or abs(t) < VETO_T:
        reasons.append(f"自身 HAC |t| {t} < {VETO_T}")

    # 2. 显著超越免费基线（核心）
    d = res.get("ic_diff")
    t_diff = res.get("ic_diff_t")
    if d is None or t_diff is None:
        reasons.append("日度 IC 差值序列不足，无法检验是否超越基线")
    else:
        if d <= 0:
            reasons.append(f"截面 IC 未超过基线（ΔIC={d:+.4f} ≤ 0）")
        if abs(t_diff) < VETO_T:
            reasons.append(f"ΔIC 的 HAC |t| {t_diff} < {VETO_T}（未显著超越 {res.get('baseline')}）")

    # 3. 截面 IC 点估计不落后于基线（R² 不参与判定，仅作参考输出）
    b_ic = res.get("base_ic_spearman")
    if b_ic is not None and ic < b_ic:
        reasons.append(f"截面 IC {ic:.4f} 低于基线 {b_ic:.4f}")

    return ("VETO", reasons) if reasons else ("PASS", [])


def apply_fdr(results: Sequence[dict], alpha: float = 0.05) -> List[dict]:
    """对多窗口/多模型的「ΔIC 显著性」检验做 BH-FDR 校正，并回写 qval / verdict。

    校正后不显著的，即便单点 t 过关也降级为 VETO（控制多重检验伪显著）。
    """
    out = list(results)
    valid = [r for r in out if r.get("verdict") != "NA" and r.get("ic_diff_p") is not None]
    if not valid:
        return out
    adj = benjamini_hochberg([r["ic_diff_p"] for r in valid], alpha=alpha)
    for r, (_, _, q, rejected) in zip(valid, adj):
        r["ic_diff_qval"] = round(float(q), 4)
        r["ic_diff_significant_fdr"] = bool(rejected)
        if not rejected and r["verdict"] == "PASS":
            r["verdict"] = "VETO"
            r["reasons"] = list(r.get("reasons", [])) + [
                f"BH-FDR 校正后不显著（q={q:.4f} ≥ {alpha}）"]
    return out


def run_risk_gate(df: pd.DataFrame, windows=(5, 20, 60), models=("lgb",),
                  baseline_feature: str = BASELINE_FEATURE,
                  log=print) -> List[dict]:
    """对 (窗口 × 模型) 全组合跑门禁，返回带 verdict 的结果列表。"""
    results = []
    for w in windows:
        for m in models:
            res = risk_walkforward_vs_baseline(df, w, model=m,
                                               baseline_feature=baseline_feature)
            res["window"], res["model"] = w, m
            results.append(res)
            log(f"[RiskGate] w={w} {m}: R²={res.get('r2')} IC={res.get('ic_spearman')} "
                f"| 基线 {baseline_feature} R²={res.get('base_r2')} "
                f"IC={res.get('base_ic_spearman')} "
                f"| ΔIC={res.get('ic_diff')} t={res.get('ic_diff_t')} "
                f"→ {res.get('verdict')}")
    return apply_fdr(results)
