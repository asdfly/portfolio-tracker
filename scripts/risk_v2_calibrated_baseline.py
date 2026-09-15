"""补充检验：把免费基线换成"标定版 vol_20d"（y ~ a + b·vol_20d，折内拟合，仍免费）。

动机：主结果中模型的 OOS R² 高于基线，可能被质疑"只是 vol_20d 没标定"。
标定是单调变换 → Spearman IC / AUC 与 raw 版完全一致，只有 R² 会变。
若标定后基线的 R² 也超过模型，则模型在两个判据上**双双落后**，VETO 无争议。
"""
# 注意：本脚本读 etf_forward_returns_v2，该表已与生产表 etf_forward_returns 逐行等价
# （相关性 1.000000），保留仅供 2026-09-15 风险模型 VETO 结论复现留档。
# 新代码请一律使用生产表 etf_forward_returns。
import sqlite3
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config.settings import DATABASE_PATH
from src.analysis.predictor.models import FEATURE_COLS, WINDOWS
from src.analysis.predictor.risk_gate import (
    BASELINE_FEATURE, risk_walkforward_vs_baseline, apply_fdr,
)
from scripts.risk_v2_sensitivity import load_clean_panel


def load_panel(conn) -> pd.DataFrame:
    feat = pd.read_sql_query(
        f"SELECT date, code, {', '.join(FEATURE_COLS)} FROM etf_features", conn)
    # 表名说明见文件顶部注释（v2 表为留档，已与生产表等价）
    lab = pd.read_sql_query(
        "SELECT date, code, fwd_vol_5, fwd_vol_20, fwd_vol_60 FROM etf_forward_returns_v2",
        conn)
    df = feat.merge(lab, on=["date", "code"], how="inner")
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values(["date", "code"]).reset_index(drop=True)


def run(df, tag):
    print(f"\n--- {tag} ---")
    res = []
    for w in WINDOWS:
        r = risk_walkforward_vs_baseline(df, w, model="lgb",
                                         baseline_feature=BASELINE_FEATURE,
                                         baseline_mode="linear")
        r["window"], r["model"] = w, "lgb"
        res.append(r)
        print(f"w={w:>2} | 模型 R²={r.get('r2')} IC={r.get('ic_spearman')} AUC={r.get('auc')}")
        print(f"    | 标定基线 R²={r.get('base_r2')} IC={r.get('base_ic_spearman')} "
              f"AUC={r.get('base_auc')} | ΔIC={r.get('ic_diff')} t={r.get('ic_diff_t')} "
              f"→ {r.get('verdict')}")
    apply_fdr(res)


if __name__ == "__main__":
    conn = sqlite3.connect(str(DATABASE_PATH))
    run(load_panel(conn), "全样本（主口径）")
    run(load_clean_panel(conn), "剔除拆分/折算污染后（稳健性）")
    conn.close()
