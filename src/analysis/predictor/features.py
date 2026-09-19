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

# ⚠️ 量纲/语义红线（2026-09-15 事故后写死）：任何改变特征【量纲或语义】的改动
#   （如本次 绝对价→相对量）必须对 etf_features 做**全表重算**，**不能靠升 FEAT_VERSION
#   隔离** —— 因为 etf_features 的 PK=(date, code) 且 feat_version 不在键里
#   （src/utils/db_schema.py:501），同一 (date,code) 只能存在一行，升版本号只是给同一批
#   行换标签、无法新旧并存；增量重算会留下「同列两套尺度」的静默回归。FEAT_VERSION
#   仅作语义标记（读取方不过滤）：v2=绝对价尺度，v3=相对量尺度。
FEAT_VERSION = "v3"

# P1-6 特征整改（2026-09-15）：
#  - ret_20d 与 mom_20d 精确等价（corr 1.0000），已删 ret_20d 保留 mom_20d（消重，R4）。
#  - atr_14 绝对真幅来自 etf_price_history（复权基准异于 snapshot close），与 ma/macd
#    量纲不一致 -> 弃用，保留相对量 atr_pct（P1-6 R3）。
#  - ma*/macd* 由绝对价改为相对量（close/ma-1、macd/close），消除跨标的量纲差异（R3）。
TECH_COLS = [
    "ma5", "ma10", "ma20", "ma60", "macd", "macd_signal", "macd_hist", "rsi_14",
    "boll_mid", "boll_upper", "boll_lower", "boll_pctb", "kdj_k", "kdj_d", "kdj_j",
    "atr_pct", "ret_1d", "ret_5d", "vol_20d", "mom_20d",
    # v2 新增：波动率结构 + 多周期动量 + 量价
    "vol_5d", "vol_60d", "vol_ratio_5_20", "ret_60d", "mom_5d", "range_20d",
    "parkinson_vol_20d", "hl_range_20d", "volume_zscore_20d",
]
# P1-6 R1：4 个资金流特征在 etf_features 中 NULL 率约 83%（fund_flows 覆盖极稀），
# 置 0 后即成纯噪声维，已移出特征域（不再参与训练/推理）。
FLOW_COLS = []
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
    # P1-6 R3：绝对价 -> 相对量（close/ma - 1），消除跨标的量纲差异
    out["ma5"] = s / s.rolling(5, min_periods=3).mean() - 1.0
    out["ma10"] = s / s.rolling(10, min_periods=5).mean() - 1.0
    out["ma20"] = s / s.rolling(20, min_periods=10).mean() - 1.0
    out["ma60"] = s / s.rolling(60, min_periods=30).mean() - 1.0

    # P1-6 R3：MACD 由绝对差值改为相对量（macd/close），与 ma* 同一量纲
    ema12 = s.ewm(span=12, adjust=False).mean()
    ema26 = s.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    sig = macd.ewm(span=9, adjust=False).mean()
    out["macd"] = macd / s
    out["macd_signal"] = sig / s
    out["macd_hist"] = (macd - sig) / s

    delta = s.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    ag = gain.ewm(alpha=1 / 14, adjust=False).mean()
    al = loss.ewm(alpha=1 / 14, adjust=False).mean()
    rs = ag / al.replace(0.0, np.nan)
    out["rsi_14"] = 100.0 - 100.0 / (1.0 + rs)

    # P1-6 R3：BOLL 轨由绝对价改为相对量（各轨/close - 1），量纲统一
    mid = s.rolling(20, min_periods=10).mean()
    sd = s.rolling(20, min_periods=10).std()
    upper = mid + 2.0 * sd
    lower = mid - 2.0 * sd
    out["boll_mid"] = mid / s - 1.0
    out["boll_upper"] = upper / s - 1.0
    out["boll_lower"] = lower / s - 1.0
    out["boll_pctb"] = (s - lower) / (upper - lower)

    ret = s.pct_change()
    out["ret_1d"] = ret
    out["ret_5d"] = s.pct_change(5)
    out["mom_20d"] = s / s.shift(20) - 1.0
    out["vol_20d"] = ret.rolling(20, min_periods=10).std()
    # v2：波动率结构 + 多周期动量 + 振幅
    out["vol_5d"] = ret.rolling(5, min_periods=3).std()
    out["vol_60d"] = ret.rolling(60, min_periods=30).std()
    out["vol_ratio_5_20"] = out["vol_5d"] / out["vol_20d"]
    out["ret_60d"] = s.pct_change(60)
    out["mom_5d"] = s / s.shift(5) - 1.0
    hi20 = s.rolling(20, min_periods=10).max()
    lo20 = s.rolling(20, min_periods=10).min()
    out["range_20d"] = (hi20 - lo20) / s

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
        # P1-6 R3：atr_14 绝对真幅已弃用（不进特征域），仅保留相对量 atr_pct
        out["atr_pct"] = atr / c
        # v2：Parkinson 波动率 + 高低价差 + 成交量 zscore
        ln_hl = np.log(high / low)
        out["parkinson_vol_20d"] = np.sqrt((ln_hl ** 2).rolling(20, min_periods=10).mean() / (4.0 * np.log(2.0)))
        out["hl_range_20d"] = (high / low - 1.0).rolling(20, min_periods=10).mean()
        if "volume" in ohlc.columns:
            v = ohlc["volume"]
            vm = v.rolling(20, min_periods=10).mean()
            vs = v.rolling(20, min_periods=10).std()
            out["volume_zscore_20d"] = (v - vm) / vs
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
        q = f"SELECT date, code, open, high, low, close, volume FROM etf_price_history WHERE code IN ({placeholders})"
        df = pd.read_sql_query(q, conn, params=codes)
    except Exception:
        return {}
    if df.empty:
        return {}
    df["date"] = pd.to_datetime(df["date"])
    res = {}
    for code, g in df.groupby("code"):
        g = g.sort_values("date").set_index("date")
        res[code] = g[["open", "high", "low", "close", "volume"]]
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
        # P1-6 R3：close 全程统一取 portfolio_snapshots.current_price（单一源，全程覆盖
        #   2012-2026），ma/macd/boll 等由相对量公式计算 → 跨标的、跨期量纲统一。
        #   不可改用 etf_price_history.close：该表 2018 起，会丢失 2012-2017 全部行，
        #   且与 snapshot 造成同列两套尺度（PK=(date,code) 且 feat_version 不在键里）。
        #   OHLC 派生特征（KDJ/ATR/高低幅/Parkinson）均为同源比值/对数比，尺度不变，
        #   不会与 snapshot close 产生跨源量纲混用。
        close = g["close"]
        ohlc = ohlc_map.get(code)
        tech = compute_technical_from_close(close, ohlc)
        # 问题十二（份额折算）：拆分日伪收益修复。
        # etf_price_history.close 是前复权连续序列（拆分日无跳变），而快照
        # current_price 在拆分日合法跳变（每股真减半）。若直接对 current_price 取
        # pct_change，会把拆分当成 ±200% 的伪收益喂给 predictor。
        # 故 return/momentum/volatility 类特征改从 qfq 价算；无 qfq 覆盖的
        # (code,date)（如 510500 2015 拆分期，行情表 2018 才起）保留 NaN
        # -> upsert 落 NULL，绝不伪造。ma/macd/boll 等位置类特征仍用原始价
        # （见 docs/handover/07_known_data_issues.md 问题十二）。
        if ohlc is not None and "close" in ohlc.columns:
            qc = ohlc["close"].reindex(g.index)
            if qc.notna().any():
                qret = qc.pct_change()
                tech["ret_1d"] = qret
                tech["ret_5d"] = qc.pct_change(5)
                tech["ret_60d"] = qc.pct_change(60)
                tech["mom_5d"] = qc / qc.shift(5) - 1.0
                tech["mom_20d"] = qc / qc.shift(20) - 1.0
                tech["vol_5d"] = qret.rolling(5, min_periods=3).std()
                tech["vol_20d"] = qret.rolling(20, min_periods=10).std()
                tech["vol_60d"] = qret.rolling(60, min_periods=30).std()
                tech["vol_ratio_5_20"] = tech["vol_5d"] / tech["vol_20d"]
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
