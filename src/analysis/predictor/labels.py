"""Phase 0 前瞻收益标签。

口径严格对齐 signal_backtest._compute_forward_returns：
    fwd_ret_n = close[t+n] / close[t] - 1
    is_up_n   = (fwd_ret_n > 0)
窗口 (5, 20, 60) 对应 1 周 / 1 月 / 1 季（交易日）。
标签只用未来收盘价，绝不参与特征构造（无未来函数）。
"""
from typing import Iterable, Optional

import numpy as np
import pandas as pd

FORWARD_WINDOWS = (5, 20, 60)  # 1周 / 1月 / 1季


def _sql_val(v):
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        return float(v)
    return v


def compute_forward_returns(close: pd.Series, windows=FORWARD_WINDOWS) -> pd.DataFrame:
    """计算前瞻收益与方向标签。close 为单标的收盘序列（索引为日期）。"""
    s = close.sort_index()
    out = pd.DataFrame(index=s.index)
    for n in windows:
        fwd = s.shift(-n) / s - 1.0
        out[f"fwd_ret_{n}"] = fwd
        out[f"is_up_{n}"] = (fwd > 0).astype("Int64")
    return out


def build_labels(conn, codes: Iterable[str], windows=FORWARD_WINDOWS) -> pd.DataFrame:
    """为给定 6 位代码集合构建前瞻收益标签表。"""
    codes = list(codes)
    placeholders = ",".join("?" for _ in codes)
    q = f"""
        SELECT date, code, current_price AS close
        FROM portfolio_snapshots
        WHERE code IN ({placeholders})
        ORDER BY code, date
    """
    snap = pd.read_sql_query(q, conn, params=codes)
    if snap.empty:
        return pd.DataFrame()
    snap["date"] = pd.to_datetime(snap["date"])
    frames = []
    for code, g in snap.groupby("code"):
        g = g.sort_values("date").set_index("date")
        lab = compute_forward_returns(g["close"], windows)
        lab["code"] = code
        frames.append(lab)
    res = pd.concat(frames).reset_index().rename(columns={"index": "date"})
    res["date"] = res["date"].dt.strftime("%Y-%m-%d")
    cols = ["date", "code"] + [f"fwd_ret_{n}" for n in windows] + [f"is_up_{n}" for n in windows]
    return res[cols]


def upsert_labels(conn, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    cols = list(df.columns)
    placeholders = ",".join("?" for _ in cols)
    col_sql = ",".join(cols)
    update_cols = ",".join(f"{c}=excluded.{c}" for c in cols if c not in ("date", "code"))
    sql = f"""
        INSERT INTO etf_forward_returns ({col_sql}) VALUES ({placeholders})
        ON CONFLICT(date, code) DO UPDATE SET {update_cols}
    """
    cur = conn.cursor()
    for _, row in df.iterrows():
        cur.execute(sql, [_sql_val(v) for v in row.tolist()])
    conn.commit()
    return len(df)
