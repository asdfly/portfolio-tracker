# -*- coding: utf-8 -*-
"""ETF 价格高低位量化评估引擎 (Positioning Engine) — 参考实现 / 算法原型

核心定位: 本模块回答的是「当前价格在历史中处于高/低位」(状态定位, state location),
而非「未来会涨/跌」(方向预测, direction forecasting)。
项目已用 walk-forward 证明 ETF 短期方向不可测 (Tier1 VETO, IC<0.02);
但「当前处在什么位置」是描述性、可回溯的问题, 数据充分时置信度可以很高。

输出: 对单一 ETF 给出
  - P  (Position Score ∈ [-100, +100]):  -100=极低/便宜, 0=中位/合理, +100=极高/昂贵
  - C  (Confidence ∈ [0, 1]):            综合「数据充分度 × 因子一致性」
  - 分级标签 + 因子拆解 (可解释, 用户看到 WHY)

三因子:
  F1 价格分布 (price)      —— etf_price_history.adj_close, 8.6yr  -> 现在即可用, 高置信
  F3 资金流 (flow)         —— fund_flows.net_inflow, 4yr/10mo    -> 现在可用, 中置信(反向)
  F2 估值 (valuation)      —— index_pe_history (需 5yr 积累)      -> 数据不足时自动禁用, 成熟后自动启用

设计原则:
  - 多周期百分位(非参数, 稳健) + 稳健 z 分数(median/MAD, 抗离群) + 52周高低距, 三者互证
  - 估值因子带「数据就绪闸门」: 历史 < 250 交易日(≈1年)直接不可用; >=1250(≈5年)才给满置信
  - 绝不引入未来函数; 所有窗口只用过去数据
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# ETF -> 追踪指数 (复用 etf_fundamental.ETF_TO_INDEX; 此处内联以保证原型自洽)
ETF_TO_INDEX = {
    "510300": "000300", "159300": "000300", "510500": "000905", "512100": "000852",
    "159949": "399673", "588000": "000688", "512010": "399989", "159992": "931152",
    "515120": "931152", "159732": "930006", "159770": "930006", "159819": "930713",
    "159267": "399959", "512810": "399959", "159796": "931157", "516160": "399808",
    "561910": "931157", "515010": "399975", "159220": "h11118", "563020": "H30269",
    # 债券 ETF (511520 政金债 / 159650 国开债 / 511380 可转债) 无权益 PE, 走独立利率定位(本原型不覆盖)
}
BOND_ETFS = {"511520", "159650", "511380"}

# 宽基 ETF -> index_quotes 代码; 该表含 2002 年起的长历史, 远长于 ETF 自身成立日,
# 用作价格定位基准可大幅提升百分位置信度 (需注意: index_quotes.close 为价格指数, 非全收益)
ETF_TO_INDEX_QUOTES = {
    "510300": "sh000300", "159300": "sh000300", "510500": "sh000905",
    "512100": "sh000852", "588000": "sh000688", "159949": "sz399673",
    "512010": "sz399989",
}


# --------------------------------------------------------------------------- #
# 数据加载
# --------------------------------------------------------------------------- #
def _conn(db_path: str) -> sqlite3.Connection:
    return sqlite3.connect(db_path)


def load_price_series(conn, code: str) -> pd.Series:
    """返回按日期升序的 adj_close 序列 (含分红复权, 价格定位的正确基准)。"""
    df = pd.read_sql_query(
        "SELECT date, adj_close FROM etf_price_history WHERE code=? ORDER BY date",
        conn, params=[code])
    if df.empty:
        return pd.Series(dtype=float)
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date")["adj_close"].astype(float)
    return df


def load_flow_series(conn, code: str) -> pd.Series:
    """返回按日期升序的 net_inflow 序列。"""
    df = pd.read_sql_query(
        "SELECT date, net_inflow FROM fund_flows WHERE code=? AND net_inflow IS NOT NULL ORDER BY date",
        conn, params=[code])
    if df.empty:
        return pd.Series(dtype=float)
    df["date"] = pd.to_datetime(df["date"])
    return df.set_index("date")["net_inflow"].astype(float)


def load_pe_history(conn, index_code: str) -> List[float]:
    rows = conn.execute(
        "SELECT pe FROM index_pe_history WHERE index_code=? AND pe>0 ORDER BY date",
        (index_code,)).fetchall()
    return [r[0] for r in rows]


def load_index_price_series(conn, idx_code: str) -> pd.Series:
    """从 index_quotes 读取宽基指数的长历史收盘价 (用于更稳健的价格定位基准)。"""
    df = pd.read_sql_query(
        "SELECT date, close FROM index_quotes WHERE code=? ORDER BY date",
        conn, params=[idx_code])
    if df.empty:
        return pd.Series(dtype=float)
    df["date"] = pd.to_datetime(df["date"])
    return df.set_index("date")["close"].astype(float)


# --------------------------------------------------------------------------- #
# 统计基元
# --------------------------------------------------------------------------- #
def _pct_rank(arr: np.ndarray, value: float) -> Optional[float]:
    """value 在 arr 中的分位 (小于 value 的占比), 返回 [0,1]。"""
    arr = arr[~np.isnan(arr)]
    if len(arr) < 10:
        return None
    return float((arr < value).mean())


def _robust_z(arr: np.ndarray, value: float) -> Optional[float]:
    """中位数/MAD 稳健 z 分数 (抗离群, 比 mean/std 更适合肥尾金融序列)。"""
    arr = arr[~np.isnan(arr)]
    if len(arr) < 20:
        return None
    med = np.median(arr)
    mad = np.median(np.abs(arr - med))
    if mad == 0:
        return None
    return float((value - med) / (1.4826 * mad))


def _clip(x: float, lo: float = -100.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, x))


# --------------------------------------------------------------------------- #
# 因子 F1: 价格分布定位
# --------------------------------------------------------------------------- #
def price_position(series: pd.Series) -> Tuple[float, float, Dict]:
    """返回 (P_price, C_price, detail)。"""
    if len(series) < 60:
        return 0.0, 0.0, {"available": False, "reason": "价格数据不足"}
    latest = float(series.iloc[-1])
    arr_full = series.values.astype(float)

    # --- 多周期百分位 (非参数, 最稳健), 权重偏向中长期 ---
    pct_scores, pct_confs = [], []
    for W, w in [(252, 0.20), (756, 0.30), (1260, 0.30), (None, 0.20)]:
        sub = arr_full if W is None else arr_full[-W:]
        pr = _pct_rank(sub, latest)
        if pr is not None:
            pct_scores.append((pr - 0.5) * 200.0)  # -> [-100,100]
            need = 60 if W is None else W
            pct_confs.append(min(1.0, len(sub) / need))
    P_pct = float(np.average(pct_scores, weights=pct_confs)) if pct_scores else 0.0
    C_pct = float(np.mean(pct_confs)) if pct_confs else 0.0

    # --- 稳健 z 分数 (60/120/250 日) ---
    z_scores, z_confs = [], []
    for W in [60, 120, 250]:
        sub = arr_full[-W:]
        z = _robust_z(sub, latest)
        if z is not None:
            z_scores.append(_clip(z * 25.0))  # z=4 -> +100
            z_confs.append(min(1.0, len(sub) / W))
    P_z = float(np.average(z_scores, weights=z_confs)) if z_scores else 0.0
    C_z = float(np.mean(z_confs)) if z_confs else 0.0

    # --- 52 周高低距 ---
    sub_252 = arr_full[-252:]
    lo, hi = float(np.min(sub_252)), float(np.max(sub_252))
    P_hl = _clip(2.0 * (latest - lo) / (hi - lo) - 1.0, -1.0, 1.0) * 100.0 if hi > lo else 0.0

    # 组合: 百分位为主(0.5), z(0.3), 高低距(0.2)
    P_price = 0.5 * P_pct + 0.3 * P_z + 0.2 * P_hl
    # 因子内一致性: 三子指标离散度越小越可信
    subs = [s for s in [P_pct, P_z, P_hl] if abs(s) > 1e-6]
    disp = (max(subs) - min(subs)) / 2.0 if len(subs) > 1 else 0.0
    C_agree_inner = _clip(1.0 - disp / 100.0, 0.3, 1.0)
    C_price = _clip(C_pct * 0.5 + C_z * 0.3 + 0.85 * 0.2, 0.0, 1.0) * C_agree_inner

    return float(P_price), float(C_price), {
        "available": True, "latest": round(latest, 4), "basis": "etf_adj_close",
        "pct_score": round(P_pct, 1), "z_score": round(P_z, 1),
        "hl_score": round(P_hl, 1), "pct_conf": round(C_pct, 2),
        "n_points": int(len(series)),
    }


# --------------------------------------------------------------------------- #
# 因子 F3: 资金流定位 (反向/逆向指标)
# --------------------------------------------------------------------------- #
def flow_position(series: pd.Series) -> Tuple[Optional[float], float, Dict]:
    """净流入越大=市场越狂热=越靠近高位(+); 极端净流出=恐慌=低位(-)。反向解读。"""
    if len(series) < 40:
        return None, 0.0, {"available": False, "reason": "资金流数据不足"}
    # 日度净流入噪声大 -> 先聚合成 20 日滚动和, 再算 z
    roll = series.rolling(20, min_periods=10).sum().dropna()
    if len(roll) < 60:
        return None, 0.0, {"available": False, "reason": "聚合后样本不足"}
    latest = float(roll.iloc[-1])
    arr = roll.values.astype(float)
    z = _robust_z(arr[-120:], latest)  # 相对近 120 个滚动点(≈半年)的分布
    if z is None:
        return None, 0.0, {"available": False, "reason": "z 计算失败"}
    P_flow = _clip(z * 20.0)  # 净流入 z 正 -> 高位
    # 资金流噪声大, 置信上限封顶 0.65
    C_flow = _clip(min(1.0, len(roll) / 120.0) * 0.65, 0.0, 0.65)
    return float(P_flow), float(C_flow), {
        "available": True, "z_inflow": round(z, 2),
        "n_points": int(len(roll)),
    }


# --------------------------------------------------------------------------- #
# 因子 F2: 估值定位 (带数据就绪闸门)
# --------------------------------------------------------------------------- #
VAL_MIN_DAYS = 250      # ≈1 年, 低于此直接不可用
VAL_FULL_DAYS = 1250    # ≈5 年, 达到此给满置信


def valuation_position(pe_hist: List[float], current_pe: Optional[float] = None
                       ) -> Tuple[Optional[float], float, Dict]:
    if len(pe_hist) < VAL_MIN_DAYS:
        return None, 0.0, {
            "available": False,
            "reason": f"PE 历史仅 {len(pe_hist)} 日(<{VAL_MIN_DAYS} 闸门), 估值分位不可信",
            "n_pe": len(pe_hist),
        }
    hist = np.array([x for x in pe_hist if x > 0], dtype=float)
    cur = float(hist[-1]) if current_pe is None else float(current_pe)
    pr = _pct_rank(hist, cur)
    if pr is None:
        return None, 0.0, {"available": False, "reason": "分位计算失败"}
    P_val = (pr - 0.5) * 200.0
    # 置信随历史长度爬升: 250->0.6, 1250->1.0
    C_val = _clip(0.6 + 0.4 * (len(hist) - VAL_MIN_DAYS) / (VAL_FULL_DAYS - VAL_MIN_DAYS), 0.6, 1.0)
    return float(P_val), float(C_val), {
        "available": True, "pe_percentile": round(pr * 100, 1),
        "current_pe": round(cur, 2), "n_pe": int(len(hist)),
    }


# --------------------------------------------------------------------------- #
# 集成
# --------------------------------------------------------------------------- #
def _label(p: float) -> str:
    if p <= -60: return "极低(黄金区)"
    if p <= -25: return "偏低"
    if p < 25:   return "中性"
    if p < 60:   return "偏高"
    return "极高(警惕区)"


def evaluate(code: str, db_path: str) -> Dict:
    """对单一 ETF 做完整高低位评估。"""
    conn = _conn(db_path)
    try:
        p_price, c_price, d_price = price_position(load_price_series(conn, code))
        # 宽基 ETF: 若 index_quotes 有 >=1260 交易日长历史, 用它作价格定位基准(置信更高)
        idxq = ETF_TO_INDEX_QUOTES.get(code)
        if idxq:
            idx_series = load_index_price_series(conn, idxq)
            if len(idx_series) >= 1260:
                p_price, c_price, d_price = price_position(idx_series)
                d_price["basis"] = f"index_close({idxq})"

        # 债券 ETF: 估值/价格定位逻辑不同, 仅给价格定位 + 标注
        if code in BOND_ETFS:
            return {
                "code": code, "type": "bond",
                "P": round(p_price, 1), "C": round(c_price, 3),
                "label": _label(p_price),
                "factors": {"price": d_price},
                "note": "债券ETF: 价格高低位由利率/久期驱动, 本原型仅作价格分布定位, 不建议用权益估值口径",
            }

        factors: Dict[str, Tuple[float, float]] = {"price": (p_price, c_price)}
        detail: Dict[str, Dict] = {"price": d_price}

        p_flow, c_flow, d_flow = flow_position(load_flow_series(conn, code))
        if p_flow is not None:
            factors["flow"] = (p_flow, c_flow)
            detail["flow"] = d_flow

        idx = ETF_TO_INDEX.get(code)
        if idx:
            p_val, c_val, d_val = valuation_position(load_pe_history(conn, idx))
            if p_val is not None:
                factors["valuation"] = (p_val, c_val)
            detail["valuation"] = d_val

        # 权重: 估值成熟后权重最高(0.55), 价格 0.45, 资金流 0.25; 缺失因子自动剔除并归一
        base_w = {"price": 0.45, "flow": 0.25, "valuation": 0.55}
        w = {k: base_w[k] for k in factors}
        wsum = sum(w.values())
        P = sum(w[k] * factors[k][0] for k in factors) / wsum
        C_data = sum(w[k] * factors[k][1] for k in factors) / wsum

        # 因子一致性: 离散度越小越可信
        ps = [factors[k][0] for k in factors]
        disp = (max(ps) - min(ps)) / 2.0 if len(ps) > 1 else 0.0
        C_agree = _clip(1.0 - disp / 100.0, 0.3, 1.0)
        C = _clip(C_data * C_agree, 0.0, 1.0)

        return {
            "code": code, "type": "equity",
            "P": round(P, 1), "C": round(C, 3),
            "label": _label(P),
            "C_data": round(C_data, 3), "C_agree": round(C_agree, 3),
            "factors": detail,
            "n_factors": len(factors),
        }
    finally:
        conn.close()


def evaluate_all(db_path: str, codes: Optional[List[str]] = None) -> List[Dict]:
    if codes is None:
        conn = _conn(db_path)
        try:
            codes = [r[0] for r in conn.execute(
                "SELECT DISTINCT code FROM etf_price_history ORDER BY code")]
        finally:
            conn.close()
    return [evaluate(c, db_path) for c in codes]


if __name__ == "__main__":
    import os
    DB = os.path.join(os.path.dirname(__file__), "..", "..", "data", "database", "portfolio.db")
    DB = os.path.abspath(DB)
    results = evaluate_all(DB)
    print(f"{'code':<8}{'P':>8}{'C':>7}  {'label':<12}{'nF':>3}  factors")
    print("-" * 78)
    for r in results:
        fac = ",".join(
            f"{k}:{d.get('pct_score' if k=='price' else ('z_inflow' if k=='flow' else 'pe_percentile'), d.get('reason','-'))}"
            for k, d in r["factors"].items())
        print(f"{r['code']:<8}{r['P']:>8}{r['C']:>7}  {r['label']:<12}{r.get('n_factors','-'):>3}  {fac}")
