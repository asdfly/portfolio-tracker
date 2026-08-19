"""Phase 0 数值特征矩阵构建。

特征严格只用 t 及之前的信息（无未来函数）：
 - 技术特征：从 portfolio_snapshots.current_price（日收盘）计算
 - 资金流特征：从 fund_flows(category='etf') 聚合 5/20 日净流入
 - 市场因子：从 index_quotes(沪深300) 取 20 日收益/波动
 - 若 etf_price_history(OHLCV) 可用，补充 KDJ / ATR（需 high/low）

所有滚动窗口默认 min_periods < 窗口长度，且从不引用 t+1 及之后的数据。
"""
from typing import Iterable, Optional

import numpy as np
import pandas as pd

FEAT_VERSION = "v1"

TECH_COLS = [
    "ma5", "ma10", "ma20", "ma60", "macd", "macd_signal", "macd_hist", "rsi_14",
    "boll_mid", "boll_upper", "boll_lower", "boll_pctb", "kdj_k", "kdj_d", "kdj_j",
    "atr_14", "atr_pct", "ret_1d", "ret_5d", "ret_20d", "vol_20d", "mom_20d",
]
FLOW_COLS = ["ff_net_inflow_5d", "ff_net_inflow_20d", "ff_super_net_5d", "ff_large_net_5d"]
MARKET_COLS = ["hs300_ret_20d", "hs300_vol_20d"]
ALL_FEATURE_COLS = TECH_COLS + FLOW_COLS + MARKET_COLS


def _norm_code(code: str) -> str:
    """把 sh/sz/of 前缀的 6 位代码归一化为纯 6 位。"""
    s = str(code).lower()
    for p in ("sh", "sz", "of"):
        if s.startswith(p):
            s = s[len(p):]
    return s


def _sql_val(v):
    """把 pandas/numpy 标量转为 sqlite 安全值（NaN/NA -> None）。"""
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


def compute_technical_from_close(close: pd.Series, ohlc: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """给定单一标的的收盘序列（按日期排序、索引为日期），返回技术特征列。

    ohlc 可选，提供 open/high/low/close 时补充 KDJ / ATR；否则仅输出收盘派生指标。
    """
    s = close.sort_index()
    out = pd.DataFrame(index=s.index)
    out["ma5"] = s.rolling(5, min_periods=3).mean()
    out["ma10"] = s.rolling(10, min_periods=5).mean()
    out["ma20"] = s.rolling(20, min_periods=10).mean()
    out["ma60"] = s.rolling(60, min_periods=30).mean()

    ema12 = s.ewm(span=12, adjust=False).mean()
    ema26 = s.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    sig = macd.ewm(span=9, adjust=False).mean()
    out["macd"] = macd
    out["macd_signal"] = sig
    out["macd_hist"] = macd - sig

    delta = s.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    ag = gain.ewm(alpha=1 / 14, adjust=False).mean()
    al = loss.ewm(alpha=1 / 14, adjust=False).mean()
    rs = ag / al.replace(0.0, np.nan)
    out["rsi_14"] = 100.0 - 100.0 / (1.0 + rs)

    mid = s.rolling(20, min_periods=10).mean()
    sd = s.rolling(20, min_periods=10).std()
    upper = mid + 2.0 * sd
    lower = mid - 2.0 * sd
    out["boll_mid"] = mid
    out["boll_upper"] = upper
    out["boll_lower"] = lower
    out["boll_pctb"] = (s - lower) / (upper - lower)

    ret = s.pct_change()
    out["ret_1d"] = ret
    out["ret_5d"] = s.pct_change(5)
    out["ret_20d"] = s.pct_change(20)
    out["mom_20d"] = s / s.shift(20) - 1.0
    out["vol_20d"] = ret.rolling(20, min_periods=10).std()

    if ohlc is not None and not ohlc.empty and "high" in ohlc.columns and "low" in ohlc.columns:
        high = ohlc["high"]
        low = ohlc["low"]
        c = ohlc["close"] if "close" in ohlc.columns else s
        ln = low.rolling(9, min_periods=5).min()
        hn = high.rolling(9, min_periods=5).max()
        rsv = (c - ln) / (hn - ln) * 100.0
        k = rsv.ewm(alpha=1 / 3, adjust=False).mean()
        d = k.ewm(alpha=1 / 3, adjust=False).mean()
        out["kdj_k"] = k
        out["kdj_d"] = d
        out["kdj_j"] = 3.0 * k - 2.0 * d
        prev = c.shift(1)
        tr = pd.concat([(high - low), (high - prev).abs(), (low - prev).abs()], axis=1).max(axis=1)
        atr = tr.ewm(alpha=1 / 14, adjust=False).mean()
        out["atr_14"] = atr
        out["atr_pct"] = atr / c
    return out


def aggregate_fund_flows(conn, codes: Iterable[str], windows=(5, 20)) -> pd.DataFrame:
    """从 fund_flows(category='etf') 聚合每只 ETF 的 5/20 日净流入与主力净流入。"""
    codes = list(codes)
    placeholders = ",".join("?" for _ in codes)
    q = f"""
        SELECT date, code, net_inflow, super_large_inflow, large_inflow
        FROM fund_flows
        WHERE category='etf' AND code IN ({placeholders})
        ORDER BY code, date
    """
    df = pd.read_sql_query(q, conn, params=codes)
    if df.empty:
        return pd.DataFrame()
    df["date"] = pd.to_datetime(df["date"])
    frames = []
    for code, g in df.groupby("code"):
        g = g.sort_values("date").set_index("date")
        rec = pd.DataFrame(index=g.index)
        rec["ff_net_inflow_5d"] = g["net_inflow"].rolling(5, min_periods=1).sum()
        rec["ff_net_inflow_20d"] = g["net_inflow"].rolling(20, min_periods=1).sum()
        rec["ff_super_net_5d"] = g["super_large_inflow"].rolling(5, min_periods=1).sum()
        rec["ff_large_net_5d"] = g["large_inflow"].rolling(5, min_periods=1).sum()
        rec["code"] = code
        frames.append(rec)
    res = pd.concat(frames).reset_index().rename(columns={"index": "date"})
    res["date"] = res["date"].dt.strftime("%Y-%m-%d")
    return res


def market_factors(conn) -> pd.DataFrame:
    """沪深300 的 20 日收益与波动，作为跨标的共享市场因子。"""
    q = """
        SELECT date, close FROM index_quotes
        WHERE (code LIKE '%000300%' OR name LIKE '%沪深300%')
        ORDER BY date
    """
    df = pd.read_sql_query(q, conn)
    if df.empty:
        return pd.DataFrame(columns=["date"] + MARKET_COLS)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").set_index("date")
    close = df["close"]
    ret = close.pct_change()
    out = pd.DataFrame(index=df.index)
    out["hs300_ret_20d"] = close.pct_change(20)
    out["hs300_vol_20d"] = ret.rolling(20, min_periods=10).std()
    out = out.reset_index()
    out["date"] = out["date"].dt.strftime("%Y-%m-%d")
    return out


def load_ohlc(conn, codes: Iterable[str]) -> dict:
    """返回 {code: DataFrame(date,open,high,low,close)}，仅含 etf_price_history 中有的 code。"""
    codes = list(codes)
    try:
        placeholders = ",".join("?" for _ in codes)
        q = f"SELECT date, code, open, high, low, close FROM etf_price_history WHERE code IN ({placeholders})"
        df = pd.read_sql_query(q, conn, params=codes)
    except Exception:
        return {}
    if df.empty:
        return {}
    df["date"] = pd.to_datetime(df["date"])
    res = {}
    for code, g in df.groupby("code"):
        g = g.sort_values("date").set_index("date")
        res[code] = g[["open", "high", "low", "close"]]
    return res


def build_feature_matrix(conn, codes: Iterable[str], as_of: Optional[str] = None) -> pd.DataFrame:
    """组装 (date, code) 索引的数值特征矩阵。codes 为 6 位代码。"""
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
    ohlc_map = load_ohlc(conn, codes)
    for code, g in snap.groupby("code"):
        g = g.sort_values("date").set_index("date")
        close = g["close"]
        ohlc = ohlc_map.get(code)
        tech = compute_technical_from_close(close, ohlc)
        tech["code"] = code
        frames.append(tech)
    feat = pd.concat(frames)
    feat = feat.reset_index().rename(columns={"index": "date"})
    feat["date"] = feat["date"].dt.strftime("%Y-%m-%d")

    flow = aggregate_fund_flows(conn, codes)
    if not flow.empty:
        feat = feat.merge(flow, on=["date", "code"], how="left")

    mkt = market_factors(conn)
    if not mkt.empty:
        feat = feat.merge(mkt, on="date", how="left")

    feat["feat_version"] = FEAT_VERSION
    keep = ["date", "code", "feat_version"] + ALL_FEATURE_COLS
    feat = feat[[c for c in keep if c in feat.columns]].copy()
    if as_of:
        feat = feat[feat["date"] <= as_of]
    return feat


def upsert_features(conn, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    cols = list(df.columns)
    placeholders = ",".join("?" for _ in cols)
    col_sql = ",".join(cols)
    update_cols = ",".join(f"{c}=excluded.{c}" for c in cols if c not in ("date", "code"))
    sql = f"""
        INSERT INTO etf_features ({col_sql}) VALUES ({placeholders})
        ON CONFLICT(date, code) DO UPDATE SET {update_cols}
    """
    cur = conn.cursor()
    for _, row in df.iterrows():
        cur.execute(sql, [_sql_val(v) for v in row.tolist()])
    conn.commit()
    return len(df)
