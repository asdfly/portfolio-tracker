"""Phase 0 前瞻收益标签。

口径严格对齐 signal_backtest._compute_forward_returns：
    fwd_ret_n = close[t+n] / close[t] - 1
    is_up_n   = (fwd_ret_n > 0)
窗口 (5, 20, 60) 对应 1 周 / 1 月 / 1 季（交易日）。
标签只用未来收盘价，绝不参与特征构造；风险标签窗口严格为 [t+1..t+n]，无未来函数。
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
        # NaN > 0 得 False，直接 astype 会把它写成 0（伪"下跌"样本）。
        # 末段无未来数据的行必须保持缺失（pd.NA），不得参与训练/评估。
        is_up = (fwd > 0).astype("Int64")
        is_up[fwd.isna()] = pd.NA
        out[f"is_up_{n}"] = is_up
    return out


def compute_forward_volatility(close: pd.Series, windows=FORWARD_WINDOWS) -> pd.DataFrame:
    """计算前瞻风险标签：未来窗口已实现波动率 + 最大回撤。

    - fwd_vol_n[t]   = 未来 n 日日对数收益的标准差（已实现波动率，信噪比高于方向）。
    - fwd_max_dd_n[t] = 未来 n 日窗口内最大回撤（负值，越负越差）。
    标签窗口严格取 [t+1 .. t+n]，不含 t 及之前的任何已实现信息；标签只用于监督目标，
    绝不参与特征构造。
    """
    s = close.sort_index()
    out = pd.DataFrame(index=s.index)
    log_ret = np.log(s / s.shift(1))
    vals = s.values
    lr_vals = log_ret.to_numpy(dtype=float)
    N = len(vals)
    for n in windows:
        # 未来 n 日已实现波动率：std(log_ret[t+1..t+n])，窗口严格不含 t 及之前。
        # 原写法 log_ret.shift(-1).rolling(n).std() 的窗口是 log_ret[t-n+2..t+1]，
        # 即 n-1 个已实现值 + 1 个未来值 —— 标签可被 t 及之前的收益解释，属未来函数。
        # 这里显式取窗，避免 rolling 在位移域上出现口径歧义 / 静默丢行。
        fv = np.full(N, np.nan)
        for t in range(N):
            seg = lr_vals[t + 1:t + 1 + n]
            if len(seg) < n or not np.isfinite(seg).all():
                continue
            fv[t] = seg.std(ddof=1)
        out[f"fwd_vol_{n}"] = pd.Series(fv, index=s.index)
        # 未来 n 日窗口最大回撤
        dd = np.full(N, np.nan)
        for t in range(N):
            seg = vals[t + 1:t + 1 + n]
            if len(seg) < 2:
                break
            peak = np.maximum.accumulate(seg)
            dd[t] = (seg / peak - 1.0).min()
        out[f"fwd_max_dd_{n}"] = pd.Series(dd, index=s.index)
    return out


def build_labels(conn, codes: Iterable[str], windows=FORWARD_WINDOWS) -> pd.DataFrame:
    """为给定 6 位代码集合构建前瞻收益标签 + 风险标签表。"""
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
        ret = compute_forward_returns(g["close"], windows)
        vol = compute_forward_volatility(g["close"], windows)
        lab = pd.concat([ret, vol], axis=1)
        lab["code"] = code
        frames.append(lab)
    res = pd.concat(frames).reset_index().rename(columns={"index": "date"})
    res["date"] = res["date"].dt.strftime("%Y-%m-%d")
    cols = ["date", "code"]
    for n in windows:
        cols += [f"fwd_ret_{n}", f"is_up_{n}", f"fwd_vol_{n}", f"fwd_max_dd_{n}"]
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
