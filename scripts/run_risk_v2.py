"""用修正后的标签重跑风险模型训练 + OOS 门禁评估，结果写 model='risk_lgb_v2'。

数据安全：只新建表 etf_forward_returns_v2 存修正标签；不 UPDATE/DELETE 任何既有表，
不触碰 etf_predictions 里 model='risk_lgb' 的既有行。

口径说明（重要，影响与历史 0.74 R² 的可比性）
------------------------------------------
- 价格源 portfolio_snapshots.current_price，与原始 etf_forward_returns 完全一致。
- 特征源 etf_features 不变（37 维 FEATURE_COLS，fillna(0)）。
- 折划分 / embargo(60) / n_splits(5) 与 models.risk_walkforward_evaluate 完全一致，
  唯一差别是标签从"含未来函数"换成"干净的 [t+1..t+n]"。
- 基线 vol_20d 为历史 20 日简单收益标准差（features.py），与 fwd_vol 同量纲，直接外推，
  零训练成本，且在**同一批 OOS 行**上评估，保证配对比较公平。
"""
import datetime as dt
import sqlite3
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config.settings import DATABASE_PATH
from src.analysis.predictor.labels import build_labels, FORWARD_WINDOWS
from src.analysis.predictor.models import (FEATURE_COLS, WINDOWS, _fit_lgb, _r2, _ic)
from src.analysis.predictor.risk_gate import (
    BASELINE_FEATURE, risk_walkforward_vs_baseline, apply_fdr,
)

# 注意：本脚本读 etf_forward_returns_v2，该表已与生产表 etf_forward_returns 逐行等价
# （相关性 1.000000），保留仅供 2026-09-15 风险模型 VETO 结论复现留档。
# 新代码请一律使用生产表 etf_forward_returns。
NEW_LABEL_TABLE = "etf_forward_returns_v2"
NEW_MODEL_NAME = "risk_lgb_v2"

DDL = f"""
CREATE TABLE IF NOT EXISTS {NEW_LABEL_TABLE} (
    date TEXT NOT NULL,
    code TEXT NOT NULL,
    fwd_ret_5 REAL, fwd_ret_20 REAL, fwd_ret_60 REAL,
    is_up_5 INTEGER, is_up_20 INTEGER, is_up_60 INTEGER,
    fwd_vol_5 REAL, fwd_vol_20 REAL, fwd_vol_60 REAL,
    fwd_max_dd_5 REAL, fwd_max_dd_20 REAL, fwd_max_dd_60 REAL,
    PRIMARY KEY (date, code)
)
"""


def rebuild_labels(conn) -> int:
    codes = [r[0] for r in conn.execute("SELECT DISTINCT code FROM etf_features")]
    df = build_labels(conn, codes, FORWARD_WINDOWS)
    conn.execute(DDL)
    cur = conn.cursor()
    cols = list(df.columns)
    ph = ",".join("?" for _ in cols)
    upd = ",".join(f"{c}=excluded.{c}" for c in cols if c not in ("date", "code"))
    cur.executemany(
        f"INSERT INTO {NEW_LABEL_TABLE} ({','.join(cols)}) VALUES ({ph}) "
        f"ON CONFLICT(date, code) DO UPDATE SET {upd}",
        [[None if pd.isna(v) else v for v in row] for row in df.itertuples(index=False)],
    )
    conn.commit()
    return len(df)


def load_panel_v2(conn) -> pd.DataFrame:
    feat = pd.read_sql_query(
        f"SELECT date, code, {', '.join(FEATURE_COLS)} FROM etf_features", conn)
    lab = pd.read_sql_query(
        f"SELECT date, code, fwd_vol_5, fwd_vol_20, fwd_vol_60, "
        f"fwd_max_dd_5, fwd_max_dd_20, fwd_max_dd_60 FROM {NEW_LABEL_TABLE}", conn)
    df = feat.merge(lab, on=["date", "code"], how="inner")
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values(["date", "code"]).reset_index(drop=True)


def main():
    conn = sqlite3.connect(str(DATABASE_PATH))
    n = rebuild_labels(conn)
    print(f"[v2] 修正标签写入 {NEW_LABEL_TABLE}: {n} 行")
    print("[v2] is_up 缺失校验:", conn.execute(
        f"SELECT SUM(fwd_ret_5 IS NULL AND is_up_5 IS NOT NULL),"
        f" SUM(fwd_ret_20 IS NULL AND is_up_20 IS NOT NULL),"
        f" SUM(fwd_ret_60 IS NULL AND is_up_60 IS NOT NULL),"
        f" SUM(fwd_vol_20 IS NULL) FROM {NEW_LABEL_TABLE}").fetchone())

    df = load_panel_v2(conn)
    print(f"[v2] 面板 {len(df)} 行 / {df['code'].nunique()} 只 / {len(FEATURE_COLS)} 维特征")

    print("\n===== OOS 门禁（walk-forward, embargo=60, 5 折）=====")
    results = []
    for w in WINDOWS:
        for m in ("lgb", "ridge"):
            r = risk_walkforward_vs_baseline(df, w, model=m,
                                             baseline_feature=BASELINE_FEATURE)
            r["window"], r["model"] = w, m
            results.append(r)
            print(f"w={w:>2} {m:<5} | 模型 IC={r.get('ic_spearman')} "
                  f"HAC t={r.get('ic_t')} (滞后={r.get('hac_lags')}) "
                  f"| R²(参考)={r.get('r2')} AUC(参考)={r.get('auc')}")
            print(f"          | 基线 vol_20d IC={r.get('base_ic_spearman')} "
                  f"HAC t={r.get('base_ic_t')} | R²(参考)={r.get('base_r2')} "
                  f"AUC(参考)={r.get('base_auc')}")
            print(f"          | ΔIC={r.get('ic_diff')} HAC t={r.get('ic_diff_t')} "
                  f"(n_days={r.get('n_ic_days')}) → {r.get('verdict')}")
            for why in r.get("reasons", []):
                print(f"            - {why}")
    results = apply_fdr(results)

    # ---------- 用修正标签全量重训，落 model='risk_lgb_v2' ----------
    as_of = df["date"].max().strftime("%Y-%m-%d")
    latest = df[df["date"] == pd.to_datetime(as_of)]
    rows, now = [], dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for w in (20, 60):
        col = f"fwd_vol_{w}"
        panel = df.dropna(subset=[col])
        mdl = _fit_lgb(panel[FEATURE_COLS].fillna(0.0), panel[col].astype(float))
        pred = mdl.predict(latest[FEATURE_COLS].fillna(0.0))
        g = pd.DataFrame({"code": latest["code"].values, "pv": pred})
        med = g["pv"].median()
        for r in g.itertuples(index=False):
            rows.append((as_of, r.code, NEW_MODEL_NAME, w, 1 if r.pv >= med else -1,
                         float(r.pv), round(float(r.pv * 252 ** 0.5), 4),
                         round(float((g["pv"] < r.pv).mean() * 100), 1),
                         "R", "risk_panel_v2", now))
    cur = conn.cursor()
    cur.executemany(
        """INSERT OR REPLACE INTO etf_predictions
           (date, code, model, forward_window, direction, score, probability,
            confidence, grade, features, created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
        rows)
    conn.commit()
    print(f"\n[v2] as_of={as_of} 最新波动率预测落表 {len(rows)} 行 (model={NEW_MODEL_NAME})")

    pd.DataFrame([{k: v for k, v in r.items() if k != "reasons"} for r in results]).to_csv(
        ROOT / "data" / "risk_lgb_v2_oos.csv", index=False, encoding="utf-8-sig")
    print(f"[v2] 明细写出 data/risk_lgb_v2_oos.csv")
    conn.close()


if __name__ == "__main__":
    main()
