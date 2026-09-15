"""只读复核：三表补数到最新后，门禁结论是否仍然成立。

背景：etf_features / etf_forward_returns / etf_price_history 三表已由 fix-pipeline-stale
补到最新（此前停在 2026-08-19），且 etf_forward_returns 已用修正后的 labels.py
（窗口 [t+1..t+n]）重建。本脚本评估口径相应变化：
  - 之前：etf_features(→08-19) ⋈ etf_forward_returns_v2(自建,→09-14)
  - 现在：etf_features(→最新) ⋈ etf_forward_returns(生产表, 已修正)

本脚本**全程只读**（以 mode=ro 打开），不写任何表，不会影响他人跑测。
另独立校验生产表的 fwd_vol 与自建 v2 表是否一致（不盲信他人结论）。
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
    BASELINE_FEATURE, apply_fdr, risk_walkforward_vs_baseline,
)


def connect_ro():
    p = Path(str(DATABASE_PATH)).resolve().as_posix()
    try:
        return sqlite3.connect(f"file:{p}?mode=ro", uri=True)
    except sqlite3.Error:
        return sqlite3.connect(str(DATABASE_PATH))


def main():
    conn = connect_ro()
    for t in ("etf_features", "etf_forward_returns", "etf_forward_returns_v2",
              "etf_price_history", "portfolio_snapshots"):
        try:
            print(f"  {t:26s} MAX(date)={conn.execute(f'SELECT MAX(date) FROM {t}').fetchone()[0]}"
                  f"  rows={conn.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0]}")
        except Exception as exc:
            print(f"  {t:26s} ERR {exc}")

    # 独立校验：生产表 etf_forward_returns 的 fwd_vol 是否与自建 v2 一致
    a = pd.read_sql_query(
        "SELECT date, code, fwd_vol_20 FROM etf_forward_returns", conn)
    b = pd.read_sql_query(
        "SELECT date, code, fwd_vol_20 FROM etf_forward_returns_v2", conn)
    m = a.merge(b, on=["date", "code"], suffixes=("_prod", "_v2")).dropna()
    same = float((m["fwd_vol_20_prod"] - m["fwd_vol_20_v2"]).abs().max())
    print(f"\n口径校验：生产表 vs v2 表 fwd_vol_20 最大绝对差 = {same:.10f}"
          f"（n={len(m)}，相关={m['fwd_vol_20_prod'].corr(m['fwd_vol_20_v2']):.6f}）")
    # is_up 缺失数（应为每标的窗口长度，无伪 0）
    print("is_up 缺失数（应 = 每标的窗口长度，且这些行 is_up 必须全为 NULL）：")
    for w in (5, 20, 60):
        bad = conn.execute(
            f"SELECT COUNT(*) FROM etf_forward_returns "
            f"WHERE fwd_ret_{w} IS NULL AND is_up_{w} IS NOT NULL").fetchone()[0]
        nulls = conn.execute(
            f"SELECT COUNT(*) FROM etf_forward_returns WHERE is_up_{w} IS NULL").fetchone()[0]
        print(f"  is_up_{w:>2}: NULL={nulls}, 伪0(应=0)={bad}")

    feat = pd.read_sql_query(
        f"SELECT date, code, {', '.join(FEATURE_COLS)} FROM etf_features", conn)
    lab = pd.read_sql_query(
        "SELECT date, code, fwd_vol_5, fwd_vol_20, fwd_vol_60 FROM etf_forward_returns", conn)
    df = feat.merge(lab, on=["date", "code"], how="inner")
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["date", "code"]).reset_index(drop=True)
    print(f"\n面板 {len(df)} 行 / {df['code'].nunique()} 只 / 日期 "
          f"{df['date'].min().date()} ~ {df['date'].max().date()}")

    print("\n===== 门禁复核（主判据：日频截面 Spearman IC + HAC t，R² 仅参考）=====")
    res = []
    for w in WINDOWS:
        for mdl in ("lgb", "ridge"):
            r = risk_walkforward_vs_baseline(df, w, model=mdl,
                                             baseline_feature=BASELINE_FEATURE)
            r["window"], r["model"] = w, mdl
            res.append(r)
            print(f"w={w:>2} {mdl:<5} | IC={r.get('ic_spearman')} 基线IC={r.get('base_ic_spearman')}"
                  f" | ΔIC={r.get('ic_diff')} HAC t={r.get('ic_diff_t')}(滞后={r.get('hac_lags')})"
                  f" | R²(参考)={r.get('r2')} 基线R²(参考)={r.get('base_r2')} → {r.get('verdict')}")
            for why in r.get("reasons", []):
                print(f"            - {why}")
    apply_fdr(res)
    n_veto = sum(1 for r in res if r.get("verdict") == "VETO")
    print(f"\n结论：{n_veto}/{len(res)} VETO"
          f"（FDR 后最小显著 q={min(r.get('ic_diff_qval', 1) for r in res)}）")
    conn.close()


if __name__ == "__main__":
    main()
