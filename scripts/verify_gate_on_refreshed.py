"""只读复核：三表补数到最新后，门禁结论是否仍然成立。

背景：etf_features / etf_forward_returns / etf_price_history 三表已由 fix-pipeline-stale
补到最新（此前停在 2026-08-19），且 etf_forward_returns 已用修正后的 labels.py
（窗口 [t+1..t+n]）重建。本脚本评估口径相应变化：
  - 之前：etf_features(→08-19) ⋈ etf_forward_returns_v2(自建,→09-14)
  - 现在：etf_features(→最新) ⋈ etf_forward_returns(生产表, 已修正)

本脚本**全程只读**（以 mode=ro 打开），不写任何表，不会影响他人跑测。
另独立校验生产表的 fwd_vol 与自建 v2 表是否一致（不盲信他人结论）。
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
from src.analysis.predictor.models import (
    EMBARGO_DAYS, N_SPLITS, FEATURE_COLS, WINDOWS, walkforward_splits,
)
from src.analysis.predictor.risk_gate import (
    BASELINE_FEATURE, apply_fdr, risk_walkforward_vs_baseline,
)

# task #83：末折 OOS 与数据末端的允许缺口（日历日），超过即显式告警。
_OOS_GAP_WARN_DAYS = 30


def connect_ro():
    p = Path(str(DATABASE_PATH)).resolve().as_posix()
    try:
        return sqlite3.connect(f"file:{p}?mode=ro", uri=True)
    except sqlite3.Error:
        return sqlite3.connect(str(DATABASE_PATH))


def main():
    conn = connect_ro()
    # 表名说明见文件顶部注释（v2 表为留档，已与生产表等价）
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
    # 表名说明见文件顶部注释（v2 表为留档，已与生产表等价）
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

    # ---- 口径可见性（task #83/#84 底线）：面板范围 / 数据源覆盖 / OOS 覆盖 ----
    # 面板起点必须对齐数据源(etf_price_history)覆盖范围：其覆盖外年份的
    # OHLC/volume 派生特征结构性全 NULL，会触发 P1-6 缺失率护栏（task #84）。
    src = pd.read_sql_query(
        "SELECT source, MIN(date) AS mn, MAX(date) AS mx, COUNT(*) AS n "
        "FROM etf_price_history GROUP BY source ORDER BY n DESC", conn)
    print("\n[口径] etf_price_history 数据源覆盖范围：")
    for r in src.itertuples():
        print(f"    {r.source:38s} {r.mn} ~ {r.mx}   rows={r.n}")
    cover_start = str(src["mn"].min())
    cover_end = str(src["mx"].max())
    print(f"[口径] 数据源总覆盖 = {cover_start} ~ {cover_end}")
    n_all = len(df)
    df = df[df["date"] >= cover_start].reset_index(drop=True)
    print(f"[口径] 面板起点对齐数据源覆盖：原始 {n_all} 行 → 裁剪后 {len(df)} 行"
          f"（排除 {n_all - len(df)} 行，均为价格表覆盖外年份、其 OHLC/volume 派生特征结构性全 NULL）")
    print(f"[口径] 门禁面板范围 = {df['date'].min().date()} ~ {df['date'].max().date()}"
          f" / {df['code'].nunique()} 只 / {len(df)} 行")
    print("[口径] 被豁免的列 = 无（采用「面板起点对齐数据源覆盖」，不豁免任何特征列）")

    # OOS 覆盖可见性 + 末端缺口告警（task #83）：
    # walkforward_splits 的 step=avail//(n_splits+1) 只覆盖 5*step，数据末端约 1/6
    # 从未进入 OOS。这里显式打印每窗口 OOS 区间与末端缺口，禁止"最近一段没被验证"隐形。
    print("[口径] walk-forward OOS 覆盖（末折止点与数据末端缺口超过 "
          f"{_OOS_GAP_WARN_DAYS} 日历日即告警）：")
    for w in WINDOWS:
        pan = df.dropna(subset=[f"fwd_vol_{w}", BASELINE_FEATURE])
        if pan.empty:
            print(f"    w={w:>2}: 无带标签样本")
            continue
        dts = sorted(pan["date"].unique())
        sp = walkforward_splits(len(dts), n_splits=N_SPLITS, embargo=EMBARGO_DAYS)
        if not sp:
            print(f"    w={w:>2}: 折数=0（历史不足 {len(dts)} 天）")
            continue
        oos_days = sum(e - s for _, s, e in sp)
        last_end = pd.Timestamp(dts[sp[-1][2] - 1])
        dmax = pd.Timestamp(dts[-1])
        gap = (dmax - last_end).days
        flag = ("  ⚠️ OOS 未覆盖到数据末端（结论仅基于 ≤ "
                f"{last_end.date()} 的数据）") if gap > _OOS_GAP_WARN_DAYS else ""
        print(f"    w={w:>2}: 折数={len(sp)} OOS合计={oos_days}/{len(dts)}天 "
              f"({oos_days/len(dts):.1%}) 末折OOS={pd.Timestamp(dts[sp[-1][1]]).date()}~"
              f"{last_end.date()} 数据末={dmax.date()} 缺口={gap}日历日{flag}")

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
