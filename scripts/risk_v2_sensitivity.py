"""敏感性复核：剔除未复权的份额拆分/折算事件影响后重跑门禁。

背景（重要，避免误读主结果）
--------------------------
portfolio_snapshots.current_price 是**未复权**价格，历史上存在 10 次单日 |Δln P| > 0.35
的份额拆分/折算（如 512010 2021-06-28: 3.206→0.836；159300 2024-06-25: 0.973→3.462）。
这不是真实收益，却把未来窗口已实现波动率 fwd_vol 抬到 0.28~0.30（日波动 30%），
使 R² 的分母 SST 被少数几行主导 —— 模型与基线的 R² 同时被砸成负数。

主结果（scripts/run_risk_v2.py）按"不挑选样本"原则使用全样本，本脚本只作稳健性复核：
剔除"标签窗口 [t+1..t+n] 或特征回看窗口 [t-59..t] 内包含拆分日"的行后重跑。
若结论方向不变，说明 VETO 不是离群值驱动的。

注意：本口径**不是**为了让数字变好看而挑选样本 —— 它同时改善模型和基线的 R²，
且我们关心的是两者的**相对**高低与 ΔIC 显著性。
"""
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

SPLIT_THRESH = 0.35     # 单日 |Δln 收盘| 阈值：ETF 不可能真实发生
GUARD = 60              # 前后各屏蔽 60 个交易日


def split_mask(conn) -> pd.DataFrame:
    s = pd.read_sql_query(
        "SELECT date, code, current_price AS p FROM portfolio_snapshots ORDER BY code, date",
        conn)
    s["lr"] = np.log(s["p"]).groupby(s["code"]).diff()
    s["is_split"] = (s["lr"].abs() > SPLIT_THRESH).astype(int)
    return s[["date", "code", "is_split"]]


def load_clean_panel(conn, guard: int = GUARD) -> pd.DataFrame:
    sp = split_mask(conn)
    feat = pd.read_sql_query(
        f"SELECT date, code, {', '.join(FEATURE_COLS)} FROM etf_features", conn)
    lab = pd.read_sql_query(
        "SELECT date, code, fwd_vol_5, fwd_vol_20, fwd_vol_60 FROM etf_forward_returns_v2",
        conn)
    df = feat.merge(lab, on=["date", "code"], how="inner")
    contam = []
    for code, g in sp.groupby("code"):
        g = g.sort_values("date").reset_index(drop=True)
        # 以 [t-guard, t+guard] 内出现拆分日则标记该行为受污染
        flag = g["is_split"].rolling(2 * guard + 1, center=True, min_periods=1).max()
        g["bad"] = flag.astype(int)
        contam.append(g[["date", "code", "bad"]])
    df = df.merge(pd.concat(contam), on=["date", "code"], how="left")
    df["bad"] = df["bad"].fillna(0).astype(int)
    clean = df[df["bad"] == 0].drop(columns=["bad"]).copy()
    clean["date"] = pd.to_datetime(clean["date"])
    return clean.sort_values(["date", "code"]).reset_index(drop=True)


def main():
    conn = sqlite3.connect(str(DATABASE_PATH))
    n_split = int(split_mask(conn)["is_split"].sum())
    df = load_clean_panel(conn)
    print(f"[sens] 检出拆分/折算事件 {n_split} 次；清洗后面板 {len(df)} 行 "
          f"/ {df['code'].nunique()} 只（原始 34004 行）")
    print(f"[sens] fwd_vol_20 max = {df['fwd_vol_20'].max():.4f}（清洗前 0.3011）\n")

    results = []
    for w in WINDOWS:
        r = risk_walkforward_vs_baseline(df, w, model="lgb",
                                         baseline_feature=BASELINE_FEATURE)
        r["window"], r["model"] = w, "lgb"
        results.append(r)
        print(f"w={w:>2} lgb   | 模型 R²={r.get('r2')} IC={r.get('ic_spearman')} "
              f"AUC={r.get('auc')} t={r.get('ic_t')}")
        print(f"          | 基线 vol_20d R²={r.get('base_r2')} "
              f"IC={r.get('base_ic_spearman')} AUC={r.get('base_auc')}")
        print(f"          | ΔIC={r.get('ic_diff')} HAC t={r.get('ic_diff_t')} "
              f"q={r.get('ic_diff_p')} → {r.get('verdict')}")
        for why in r.get("reasons", []):
            print(f"            - {why}")
    apply_fdr(results)
    conn.close()


if __name__ == "__main__":
    main()
