"""Tier0 规则集成基线：现有技术信号 × 历史命中率加权集成。

设计（对齐可行性报告「阶段1 Tier0 基线」）：
 - 零新依赖（pandas + sqlite）。
 - 输入：`signal_confidence_current`（当前信号方向 + 各窗口历史命中率，22 ETF × 7 indicator）
   + `signal_backtest_stats`（可选，更细的 regime×window 加权命中率）。
 - 输出：每只持仓 ETF × 窗口 {5,20,60} 的方向(direction)、集成得分(score)、
   期望命中率(exp_hit_rate)、置信度(confidence)、等级(grade) → 写 `etf_predictions` 表
   （model='tier0_signal_ensemble'）。
 - 红线：仅增强参考，不自动调仓、不做点位承诺。

注：`signal_confidence_current` 仅存当前快照（无历史多日期），故本基线不做严格信号
回放 IC；严格的样本外验证由 Tier1（walk-forward + embargo）承担。本模块提供
「期望命中率」作为基线质量的合理参考。
"""
import datetime as dt
from typing import Optional

import pandas as pd

MODEL_NAME = "tier0_signal_ensemble"
WINDOWS = (5, 20, 60)
# 窗口 → signal_confidence_current 的命中率列
_WINDOW_COL = {5: "hit_rate_5d", 20: "hit_rate_20d", 60: "hit_rate_60d"}


def load_current_signals(conn, as_of: Optional[str] = None) -> pd.DataFrame:
    """取 signal_confidence_current 最新日期（或指定日期）的全部信号行。"""
    if as_of is None:
        as_of = conn.execute("SELECT MAX(date) FROM signal_confidence_current").fetchone()[0]
    if not as_of:
        return pd.DataFrame()
    q = """
        SELECT date, code, indicator, signal_direction,
               hit_rate_5d, hit_rate_10d, hit_rate_20d, hit_rate_30d, hit_rate_60d,
               composite_confidence, direction_net_score, market_regime
        FROM signal_confidence_current
        WHERE date=?
    """
    return pd.read_sql_query(q, conn, params=(as_of,))


def _grade(conf: float) -> str:
    if conf >= 70:
        return "A"
    if conf >= 50:
        return "B"
    if conf >= 30:
        return "C"
    return "D"


def build_ensemble(sig: pd.DataFrame, as_of: str) -> pd.DataFrame:
    """对每 code × 窗口做信号集成打分。

    对窗口 w：score = Σ direction_i × (hit_rate_{w,i} - 0.5)（方向 × 命中优势），
    仅计入 hit_rate 非空的信号行；direction = sign(score)，0 为中性。
    期望命中率 = Σ|d_i|×hit_{w,i} / Σ|d_i|（方向性信号的加权命中率）。
    置信度 = |score| / (最大可能 |score|) 归一化到 0-100。
    """
    rows = []
    for code, g in sig.groupby("code"):
        for w in WINDOWS:
            col = _WINDOW_COL[w]
            sub = g.dropna(subset=[col])
            if sub.empty:
                rows.append((as_of, code, w, 0, 0.0, None, 0.0, "D"))
                continue
            score = float(((sub["signal_direction"].astype(float)) * (sub[col] - 0.5)).sum())
            abs_d = sub["signal_direction"].astype(float).abs()
            denom = abs_d.sum()
            exp_hit = float((abs_d * sub[col]).sum() / denom) if denom > 0 else None
            direction = 1 if score > 0.05 else (-1 if score < -0.05 else 0)
            # 最大可能 |score| = Σ|d_i|×0.5 → 用 Σ|d_i|×0.5 归一化
            max_score = 0.5 * abs_d.sum()
            conf = min(100.0, abs(score) / max_score * 100) if max_score > 0 else 0.0
            rows.append((as_of, code, w, direction, round(score, 4),
                         round(exp_hit, 4) if exp_hit is not None else None,
                         round(conf, 1), _grade(conf)))
    cols = ["date", "code", "forward_window", "direction", "score", "exp_hit_rate",
            "confidence", "grade"]
    return pd.DataFrame(rows, columns=cols)


def upsert_predictions(conn, df: pd.DataFrame) -> int:
    """写入 etf_predictions（PK: date, code, model, forward_window）。返回行数。"""
    cur = conn.cursor()
    for _, r in df.iterrows():
        cur.execute(
            """INSERT OR REPLACE INTO etf_predictions
               (date, code, model, forward_window, direction, score, probability,
                confidence, grade, features, created_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (r["date"], r["code"], MODEL_NAME, int(r["forward_window"]), int(r["direction"]),
             r["score"], r["exp_hit_rate"], r["confidence"], r["grade"], "signal_ensemble",
             dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        )
    conn.commit()
    return len(df)


def run_tier0(conn, as_of: Optional[str] = None, log=print) -> dict:
    """Tier0 完整流程：取信号 → 集成打分 → 写 etf_predictions。返回汇总。"""
    sig = load_current_signals(conn, as_of=as_of)
    if sig.empty:
        log("[Tier0] signal_confidence_current 无数据，跳过")
        return {"predictions": 0}
    as_of = sig["date"].iloc[0]
    ens = build_ensemble(sig, as_of)
    n = upsert_predictions(conn, ens)
    # 基线质量摘要：各窗口平均期望命中率（方向性信号）
    summ = {}
    for w in WINDOWS:
        sub = ens[(ens["forward_window"] == w) & (ens["exp_hit_rate"].notna())]
        summ[w] = round(float(sub["exp_hit_rate"].mean()), 4) if not sub.empty else None
    log(f"[Tier0] as_of={as_of} 预测 {n} 行；期望命中率均值 {summ}")
    return {"as_of": as_of, "predictions": n, "exp_hit_rate_by_window": summ}
