"""对照实验：同一套代码/折划分，仅切换标签表，量化"未来函数"贡献了多少虚假 R²。

- 旧标签 etf_forward_returns（fwd_vol = log_ret.shift(-1).rolling(n).std()，含未来函数）
- 新标签 etf_forward_returns_v2（窗口 [t+1..t+n]，干净）

若旧标签下 R² 复现历史 ≈0.74、新标签下 ≈-0.24，即可确认 0.74 是标签泄漏的产物。
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
    BASELINE_FEATURE, risk_walkforward_vs_baseline,
)


def load_panel(conn, table: str) -> pd.DataFrame:
    feat = pd.read_sql_query(
        f"SELECT date, code, {', '.join(FEATURE_COLS)} FROM etf_features", conn)
    lab = pd.read_sql_query(
        f"SELECT date, code, fwd_vol_5, fwd_vol_20, fwd_vol_60 FROM {table}", conn)
    df = feat.merge(lab, on=["date", "code"], how="inner")
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values(["date", "code"]).reset_index(drop=True)


def main():
    conn = sqlite3.connect(str(DATABASE_PATH))
    print("=== 旧标签(含未来函数) vs 新标签(干净) —— w=20, LightGBM ===")
    # 表名说明见文件顶部注释（v2 表为留档，已与生产表等价）
    for table, tag in (("etf_forward_returns", "旧(有未来函数)"),
                       ("etf_forward_returns_v2", "新(已修正)")):
        df = load_panel(conn, table)
        r = risk_walkforward_vs_baseline(df, 20, model="lgb",
                                         baseline_feature=BASELINE_FEATURE)
        print(f"[{tag}] 模型 R²={r.get('r2'):>8} IC={r.get('ic_spearman'):>7} "
              f"AUC={r.get('auc')} | 基线 vol_20d R²={r.get('base_r2'):>8} "
              f"IC={r.get('base_ic_spearman')} | ΔIC={r.get('ic_diff')} "
              f"t={r.get('ic_diff_t')} → {r.get('verdict')}")

    # 两个标签表的相关性差异
    a = pd.read_sql_query("SELECT date,code,fwd_vol_20 o FROM etf_forward_returns", conn)
    # 表名说明见文件顶部注释（v2 表为留档，已与生产表等价）
    b = pd.read_sql_query("SELECT date,code,fwd_vol_20 n FROM etf_forward_returns_v2", conn)
    m = a.merge(b, on=["date", "code"]).dropna()
    print(f"\n新旧 fwd_vol_20 相关系数 = {m['o'].corr(m['n']):.4f} "
          f"（n={len(m)}）；旧/新 均值 = {m['o'].mean():.5f}/{m['n'].mean():.5f}")
    conn.close()


if __name__ == "__main__":
    main()
