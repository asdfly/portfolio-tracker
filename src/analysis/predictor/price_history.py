"""Phase 0: 用 akshare 补采 ETF 日频 OHLCV 入 etf_price_history。

说明：
 - OHLCV 是"增强项"而非核心依赖：核心技术特征来自 portfolio_snapshots.current_price（收盘），
   KDJ/ATR 在 OHLCV 可用时自动补充。
 - 网络/接口不可用时本模块会逐代码优雅跳过，绝不阻塞主流程 build_feature_matrix / build_labels。
"""
import datetime as dt
import logging
import math
from typing import Iterable, Optional

import pandas as pd

logger = logging.getLogger(__name__)
SOURCE = "akshare_fund_etf_hist_em"
SOURCE_SINA = "akshare_fund_etf_hist_sina"


def fetch_etf_ohlcv_akshare(code6: str, start: str = "20180101", end: Optional[str] = None) -> pd.DataFrame:
    """用 akshare fund_etf_hist_em 拉取单只 ETF 的日频 OHLCV（前复权）。

    code6 为 6 位代码，如 '512010'。返回列：date,open,high,low,close,volume,amount,adj_close,code,source。
    """
    import akshare as ak

    end = end or dt.date.today().strftime("%Y%m%d")
    df = ak.fund_etf_hist_em(symbol=code6, period="daily", start_date=start, end_date=end, adjust="qfq")
    if df is None or df.empty:
        return pd.DataFrame()
    out = pd.DataFrame()
    out["date"] = pd.to_datetime(df["日期"]).dt.strftime("%Y-%m-%d")
    out["open"] = df["开盘"].astype(float)
    out["high"] = df["最高"].astype(float)
    out["low"] = df["最低"].astype(float)
    out["close"] = df["收盘"].astype(float)
    out["volume"] = df["成交量"].astype(float)
    out["amount"] = df["成交额"].astype(float)
    out["adj_close"] = out["close"]
    out["code"] = code6
    out["source"] = SOURCE
    return out


def _code6_to_symbol(code6: str) -> str:
    """6 位代码 -> akshare 新浪 symbol（5 开头=沪市 sh，其余=深市 sz）。"""
    return ("sh" if code6.startswith("5") else "sz") + code6


def fetch_etf_ohlcv_sina(code6: str, start: str = "20180101", end: Optional[str] = None) -> pd.DataFrame:
    """备用源：用 akshare fund_etf_hist_sina 拉取单只 ETF 日频 OHLCV。

    与 EM 主源返回同构（date,open,high,low,close,volume,amount,adj_close,code,source），
    但 source 标记为 SOURCE_SINA。新浪接口不支持 start/end 参数，全量拉取后本地过滤。
    """
    import akshare as ak

    df = ak.fund_etf_hist_sina(symbol=_code6_to_symbol(code6))
    if df is None or df.empty:
        return pd.DataFrame()
    out = pd.DataFrame()
    out["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    out["open"] = df["open"].astype(float)
    out["high"] = df["high"].astype(float)
    out["low"] = df["low"].astype(float)
    out["close"] = df["close"].astype(float)
    out["volume"] = df["volume"].astype(float)
    out["amount"] = df["amount"].astype(float)
    out["adj_close"] = out["close"]
    out["code"] = code6
    out["source"] = SOURCE_SINA
    if start:
        s = pd.to_datetime(start).strftime("%Y-%m-%d")
        out = out[out["date"] >= s]
    if end:
        e = pd.to_datetime(end).strftime("%Y-%m-%d")
        out = out[out["date"] <= e]
    return out.reset_index(drop=True)


def _f(v) -> Optional[float]:
    """数值安全转换：NaN/Inf/None -> None（sqlite 存 NULL），其余 -> float。"""
    try:
        fv = float(v)
        return fv if math.isfinite(fv) else None
    except (TypeError, ValueError):
        return None


FETCHERS = {
    "em": fetch_etf_ohlcv_akshare,
    "sina": fetch_etf_ohlcv_sina,
}


def _last_date_for_code(conn, code: str) -> Optional[str]:
    """该标的在 etf_price_history 中已有的最新日期（YYYY-MM-DD），无为 None。"""
    cur = conn.cursor()
    cur.execute("SELECT MAX(date) FROM etf_price_history WHERE code=?", (code,))
    row = cur.fetchone()
    return row[0] if row and row[0] else None


def backfill_etf_price_history(conn, codes: Iterable[str], start: str = "20180101",
                                end: Optional[str] = None, force: bool = False,
                                sources: Iterable[str] = ("em", "sina"), log=print) -> int:
    """把 etf_price_history 增量追到 end（或今日）。返回写入行数。

    增量口径（force=False，默认值）：
      - 标的在库中已有数据 → 从 MAX(date) 当天开始拉取（含当天，以覆盖未收盘的半日行），
        只补缺口部分；
      - 已覆盖到 end 的标的直接跳过，零网络请求；
      - 库中无数据的标的 → 从 start 起全量拉取。
    force=True 时忽略库中进度，按 start 全量重拉并覆盖（用于标签/口径变更后重刷）。

    写入为 INSERT OR REPLACE，重复运行幂等。sources 依次尝试，第一个成功的源作为
    该次补数的数据源（默认 EM 优先、新浪兜底）——注：同一标的历史行可能来自不同源，
    source 列逐行记录实际来源。
    """
    total = 0
    for code in codes:
        try:
            if force:
                start_i = start
            else:
                last = _last_date_for_code(conn, code)
                if last:
                    end_norm = (pd.to_datetime(end).strftime("%Y-%m-%d")
                                if end else dt.date.today().strftime("%Y-%m-%d"))
                    if last >= end_norm:
                        log(f"[OHLCV] {code} 已覆盖至 {last}，跳过")
                        continue
                    # 含末日重取：抹掉最后一次跑批可能落库的未收盘半日行
                    start_i = last.replace("-", "")
                else:
                    start_i = start
            df = None
            used = None
            for src in sources:
                try:
                    cand = FETCHERS[src](code, start=start_i, end=end)
                    if cand is not None and not cand.empty:
                        df, used = cand, src
                        break
                    log(f"[OHLCV] {code} {src} 返回空")
                except Exception as e:  # 该源网络/接口异常：尝试下一个源
                    log(f"[OHLCV] {code} {src} 失败: {type(e).__name__}")
            if df is None or df.empty:
                log(f"[OHLCV] {code} 所有数据源均失败，跳过")
                continue
            cur = conn.cursor()
            for _, r in df.iterrows():
                cur.execute(
                    """INSERT OR REPLACE INTO etf_price_history
                       (date, code, open, high, low, close, volume, amount, adj_close, source)
                       VALUES (?,?,?,?,?,?,?,?,?,?)""",
                    (r["date"], r["code"], _f(r["open"]), _f(r["high"]), _f(r["low"]),
                     _f(r["close"]), _f(r["volume"]), _f(r["amount"]),
                     _f(r["adj_close"]), r["source"]),
                )
            conn.commit()
            total += len(df)
            log(f"[OHLCV] {code} 补采 {len(df)} 行 (source={used})")
        except Exception as e:  # 兜底：单代码异常不影响其余
            log(f"[OHLCV] {code} 补采失败（已跳过）: {type(e).__name__}: {e}")
            continue
    return total
