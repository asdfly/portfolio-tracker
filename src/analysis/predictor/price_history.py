"""Phase 0: 补采 ETF 日频 OHLCV 入 etf_price_history。

说明：
 - OHLCV 是"增强项"而非核心依赖：核心技术特征来自 portfolio_snapshots.current_price（收盘），
   KDJ/ATR 在 OHLCV 可用时自动补充。
 - 网络/接口不可用时本模块会逐代码优雅跳过，绝不阻塞主流程 build_feature_matrix / build_labels。
 - **复权口径铁律**：etf_price_history 只允许写入前复权(qfq)价。默认回退链为 em(qfq) -> tx(qfq)，
   sina 源无 adjust 参数（返回未复权价），已从默认链路移除，仅保留以备显式调用。
"""
import datetime as dt
import logging
import math
import time
from typing import Iterable, Optional

import pandas as pd

logger = logging.getLogger(__name__)
SOURCE = "akshare_fund_etf_hist_em"
SOURCE_SINA = "akshare_fund_etf_hist_sina"
# 腾讯行情前复权日线（web.ifzq.gtimg.cn fqkline，param 尾部 qfq）
SOURCE_TX = "akshare_stock_zh_a_hist_tx_qfq"
_TX_URL = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
_TX_MAX_RETRIES = 6
_TX_BACKOFF_BASE = 0.75
_TX_TIMEOUT = 20
_TX_COUNT = 320   # 请求行数上限（服务端另封顶约 640）
_TX_HEADERS = {"User-Agent": "Mozilla/5.0", "Referer": "https://gu.qq.com/"}
# 与 EM 主源 fetch_etf_ohlcv_akshare 输出完全一致的口径与列序
_COLUMNS = ["date", "open", "high", "low", "close", "volume", "amount", "adj_close", "code", "source"]
# ⚠️ 量纲/口径约定（跨写入方，勿凭直觉改）：
#   - 本端点每行仅 6 字段 [date,open,close,high,low,volume]，**无成交额**，故 amount 恒 NULL、不做推算。
#     （成交额确实存在于同厂另一端点 newfqkline 的 index 7，单位万元；如需 amount 应换端点而非推算。）
#   - 本函数 volume 单位 = **手**，与 EM 主源「成交量」逐日精确相等（实测 320 重叠日比值恒 1）。
#   - 但 akshare 包装器 stock_zh_a_hist_tx 走的是 newfqkline，且把 volume ×100 成**股**；
#     source 同为 SOURCE_TX 的历史行曾由该包装器写入，故两批行 volume 一度相差 100 倍。
#   - 【已归一】该历史 7,018 行已于 2026-09-16 按 ÷100 归一为**手**（与 EM 段一致），
#     全表 volume 现统一为手。后续任何写入方（含本函数）都不得再产出「股」，
#     否则会在拼接处重新引入 100× 断层，直接污染 volume_zscore_20d 等滚动特征。


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
    """6 位代码 -> 行情 symbol（5 开头=沪市 sh，其余=深市 sz）。sina / 腾讯 通用。"""
    return ("sh" if code6.startswith("5") else "sz") + code6


def fetch_etf_ohlcv_sina(code6: str, start: str = "20180101", end: Optional[str] = None) -> pd.DataFrame:
    """备用源：用 akshare fund_etf_hist_sina 拉取单只 ETF 日频 OHLCV。

    与 EM 主源返回同构（date,open,high,low,close,volume,amount,adj_close,code,source），
    但 source 标记为 SOURCE_SINA。新浪接口不支持 start/end 参数，全量拉取后本地过滤。

    ⚠️ 禁止用于填充行情表：`fund_etf_hist_sina` 接口**没有 adjust 参数**，返回的是**未复权价**。
    未复权序列在基金份额折算日（如 159220 于 2025-11-10 的 1:2 拆分）会表现为单日约 -50% 的
    假收益，污染标签与特征。该函数仅为兼容/人工排查保留，**不得**放回
    backfill_etf_price_history 的默认 sources（默认已改为 em -> tx）。
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


def fetch_etf_ohlcv_tx(code6: str, start: str = "20180101", end: Optional[str] = None) -> pd.DataFrame:
    """腾讯行情前复权日线（fqkline，param 尾部 qfq）——EM 主源不可用时的默认回退源。

    与 EM 主源返回**完全同构**：date,open,high,low,close,volume,amount,adj_close,code,source，
    其中 adj_close = close（腾讯 qfq 已是前复权价）。

    实现要点（均为实测结论，勿凭直觉改）：
      - 每行字段顺序为 [date, open, close, high, low, volume] —— **close 在 index 2，排在 high/low 之前**，
        不能按 OHLC 顺序解析。
      - 接口不返回成交额，amount 一律置 None（入库即 NULL）。**禁止**用 volume×price 等口径伪造。
      - volume 单位 = **手**，与 EM 主源一致；**禁止**在此乘/除 100 —— 全表已在 2026-09-16
        统一为手（历史 7018 行 TX 行已 ÷100 归一），任何换算都会重新引入 100× 断层。
      - 响应中 qfqday 缺失时返回空 DataFrame，**绝不**回退取未复权的 `day` 键：宁可该标的今天不更新，
        也不能把未复权价写进 etf_price_history。
      - 该接口有间歇性 RemoteDisconnected，故带 6 次指数退避重试（base 0.75s）。
      - 服务端对返回行数有硬上限（count 参数被封顶，实测 count=2000 也只回 640 行≈2.5 年），
        因此**只适合增量补数**，不能用于全量历史重取（全量请走 EM 或专门的 qfq 重取工具）。
    """
    import requests

    end = end or dt.date.today().strftime("%Y%m%d")
    s = pd.to_datetime(start).strftime("%Y-%m-%d")
    e = pd.to_datetime(end).strftime("%Y-%m-%d")
    symbol = _code6_to_symbol(code6)
    param = f"{symbol},day,{s},{e},{_TX_COUNT},qfq"

    last_err: Optional[Exception] = None
    resp = None
    # trust_env=True：出口依赖系统代理，关掉会直接连不上（注意它是 Session 属性而非请求参数）
    sess = requests.Session()
    sess.trust_env = True
    for attempt in range(_TX_MAX_RETRIES):
        try:
            r = sess.get(_TX_URL, params={"param": param},
                         timeout=_TX_TIMEOUT, headers=_TX_HEADERS)
            r.raise_for_status()
            resp = r.json()
            break
        except Exception as ex:  # 网络抖动 / 非 JSON 响应：退避后重试
            last_err = ex
            if attempt < _TX_MAX_RETRIES - 1:
                time.sleep(_TX_BACKOFF_BASE * (2 ** attempt))
    if resp is None:
        raise last_err if last_err else RuntimeError("tx kline 请求失败")

    node = (resp.get("data") or {}).get(symbol) or {}
    rows = node.get("qfqday")
    if not rows:
        # 缺 qfqday 一律视为无数据，不回退 day（未复权）
        logger.warning("[OHLCV] %s 腾讯接口无 qfqday，返回空（不回退未复权 day）", code6)
        return pd.DataFrame()

    recs = []
    for row in rows:
        if not row or len(row) < 6:
            continue
        recs.append({
            "date": str(row[0]),
            "open": float(row[1]),
            "close": float(row[2]),   # index 2 = close（在 high/low 之前）
            "high": float(row[3]),
            "low": float(row[4]),
            "volume": float(row[5]),
        })
    if not recs:
        return pd.DataFrame()

    out = pd.DataFrame(recs)
    out["amount"] = None          # 腾讯接口不提供成交额 -> NULL，不做任何推算
    out["adj_close"] = out["close"]
    out["code"] = code6
    out["source"] = SOURCE_TX
    # 显式对齐 EM fetcher 的列顺序（腾讯原始行是 close 在前，不重排会与 EM 列序不同）
    out = out[_COLUMNS]
    out = out[(out["date"] >= s) & (out["date"] <= e)]
    if len(rows) >= _TX_COUNT and not out.empty and out["date"].iloc[0] > s:
        # 命中请求行数上限 => 服务端截断：明确告警，避免"静默短历史"
        # （注意：仅"首个交易日 > 起始日"不算截断——起始日可能落在周末/节假日）
        logger.warning("[OHLCV] %s 腾讯接口返回被截断：请求 %s 起，实际最早 %s（命中 %d 行上限）",
                       code6, s, out["date"].iloc[0], _TX_COUNT)
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
    "tx": fetch_etf_ohlcv_tx,
    "sina": fetch_etf_ohlcv_sina,   # 未复权源，禁用：见 fetch_etf_ohlcv_sina 文档串
}


def _last_date_for_code(conn, code: str) -> Optional[str]:
    """该标的在 etf_price_history 中已有的最新日期（YYYY-MM-DD），无为 None。"""
    cur = conn.cursor()
    cur.execute("SELECT MAX(date) FROM etf_price_history WHERE code=?", (code,))
    row = cur.fetchone()
    return row[0] if row and row[0] else None


def backfill_etf_price_history(conn, codes: Iterable[str], start: str = "20180101",
                                end: Optional[str] = None, force: bool = False,
                                sources: Iterable[str] = ("em", "tx"), log=print) -> int:
    """把 etf_price_history 增量追到 end（或今日）。返回写入行数。

    增量口径（force=False，默认值）：
      - 标的在库中已有数据 → 从 MAX(date) 当天开始拉取（含当天，以覆盖未收盘的半日行），
        只补缺口部分；
      - 已覆盖到 end 的标的直接跳过，零网络请求；
      - 库中无数据的标的 → 从 start 起全量拉取。
    force=True 时忽略库中进度，按 start 全量重拉并覆盖（用于标签/口径变更后重刷）。

    写入为 INSERT OR REPLACE，重复运行幂等。sources 依次尝试，第一个成功的源作为
    该次补数的数据源——注：同一标的历史行可能来自不同源，source 列逐行记录实际来源。

    ⚠️ 默认 sources=("em","tx") **均为前复权(qfq)源**。**不得**把 "sina" 放回默认链路：
    `fund_etf_hist_sina` 无 adjust 参数、返回未复权价，写入后份额折算日会出现约 -50% 的假收益，
    污染标签与特征（历史事故见 159220 2025-11-10 拆分）。sina fetcher 仅为人工排查保留。
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
