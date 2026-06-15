# -*- coding: utf-8 -*-
"""
同类 ETF 穿透对比模块 — P2 同类穿透

基于 etf_top_holdings + etf_fundamental 数据，提供：
1. 重仓股重叠度计算（Jaccard 相似度）
2. 费率/规模/流动性排名
3. 差异化分析（独有重仓股识别）
"""

import pandas as pd
import numpy as np
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple


# ======== 重仓股重叠度 ========

@dataclass
class OverlapResult:
    """两只 ETF 的重仓股重叠结果"""
    code_a: str
    code_b: str
    jaccard_index: float        # Jaccard 相似度 0-1
    common_stocks: list          # 共同重仓股 [{"stock_code", "stock_name", "weight_a", "weight_b"}, ...]
    only_in_a: list              # 仅 A 持有的重仓股
    only_in_b: list              # 仅 B 持有的重仓股
    overlap_detail: str          # 文字描述


def jaccard_index(set_a, set_b):
    """计算两个集合的 Jaccard 相似度。"""
    if not set_a and not set_b:
        return 0.0  # 空集视为完全相同
    if not set_a or not set_b:
        return 0.0
    intersection = len(set_a & set_b)
    union = len(set_a | set_b)
    return round(intersection / union, 4)


def compute_overlap(
    code_a: str,
    code_b: str,
    holdings_df: pd.DataFrame,
    min_weight: float = 0.01,
) -> OverlapResult:
    """计算两只 ETF 的重仓股重叠度。

    Parameters
    ----------
    code_a, code_b : str — ETF 代码
    holdings_df : pd.DataFrame — etf_top_holdings 数据
    min_weight : float — 最低纳入权重阈值（默认 1%）

    Returns OverlapResult
    """
    h_a = holdings_df[(holdings_df["code"] == code_a) & (holdings_df["weight_pct"] >= min_weight)]
    h_b = holdings_df[(holdings_df["code"] == code_b) & (holdings_df["weight_pct"] >= min_weight)]

    stocks_a = set(h_a["stock_code"].tolist())
    stocks_b = set(h_b["stock_code"].tolist())
    common = stocks_a & stocks_b

    ji = jaccard_index(stocks_a, stocks_b)

    # 共同重仓股详情
    common_stocks = []
    for sc in common:
        row_a = h_a[h_a["stock_code"] == sc].iloc[0]
        row_b = h_b[h_b["stock_code"] == sc].iloc[0]
        common_stocks.append({
            "stock_code": sc,
            "stock_name": row_a.get("stock_name", sc),
            "weight_a": row_a["weight_pct"],
            "weight_b": row_b["weight_pct"],
        })
    common_stocks.sort(key=lambda x: (x["weight_a"] + x["weight_b"]) / 2, reverse=True)

    # 独有重仓股
    only_in_a = [{"stock_code": sc, "stock_name": h_a[h_a["stock_code"] == sc].iloc[0].get("stock_name", sc),
                  "weight": h_a[h_a["stock_code"] == sc].iloc[0]["weight_pct"]}
                 for sc in (stocks_a - stocks_b)]
    only_in_a.sort(key=lambda x: x["weight"], reverse=True)

    only_in_b = [{"stock_code": sc, "stock_name": h_b[h_b["stock_code"] == sc].iloc[0].get("stock_name", sc),
                  "weight": h_b[h_b["stock_code"] == sc].iloc[0]["weight_pct"]}
                 for sc in (stocks_b - stocks_a)]
    only_in_b.sort(key=lambda x: x["weight"], reverse=True)

    # 描述文字
    if ji >= 0.8:
        detail = "高度重叠，持仓策略极为相似"
    elif ji >= 0.6:
        detail = "较高重叠，核心持仓基本一致"
    elif ji >= 0.4:
        detail = "中度重叠，存在一定差异化"
    elif ji >= 0.2:
        detail = "低度重叠，持仓风格差异明显"
    else:
        detail = "几乎无重叠，属于不同策略"

    return OverlapResult(
        code_a=code_a, code_b=code_b,
        jaccard_index=ji,
        common_stocks=common_stocks,
        only_in_a=only_in_a,
        only_in_b=only_in_b,
        overlap_detail=detail,
    )


# ======== 批量对比 ========

def compute_peer_overlap_matrix(code: str, peer_codes: List[str], holdings_df: pd.DataFrame) -> List[OverlapResult]:
    """计算目标 ETF 与所有同类 ETF 的重叠度。

    Parameters
    ----------
    code : str — 目标 ETF 代码
    peer_codes : list[str] — 同类 ETF 代码列表
    holdings_df : pd.DataFrame — etf_top_holdings 数据

    Returns list[OverlapResult]
    """
    results = []
    for pc in peer_codes:
        if pc == code:
            continue
        try:
            result = compute_overlap(code, pc, holdings_df)
            results.append(result)
        except (KeyError, IndexError, ValueError):
            continue
    return sorted(results, key=lambda x: x.jaccard_index, reverse=True)


# ======== 费率/规模/流动性排名 ========

@dataclass
class PeerRanking:
    """同类 ETF 排名结果"""
    code: str
    name: str
    rank_size: int              # 规模排名
    rank_discount: int           # 折价率排名（最有利优先）
    rank_turnover: int          # 换手率排名（流动性）
    rank_inflow: int            # 资金净流入排名
    rank_volume_ratio: int      # 量比排名
    total_mv: float             # 规模（亿）
    discount_rate: float        # 折价率%
    turnover_rate: float        # 换手率%
    main_net_inflow: float      # 资金净流入（万）
    volume_ratio: float         # 量比
    composite_rank: float      # 综合排名分（越低越好）


def compute_peer_ranking(code: str, peer_df: pd.DataFrame) -> List[PeerRanking]:
    """计算同类 ETF 的多维排名。

    Parameters
    ----------
    code : str — 目标 ETF 代码
    peer_df : pd.DataFrame — 同类 ETF 的 etf_fundamental 数据

    Returns list[PeerRanking] — 按综合排名排序
    """
    if peer_df.empty:
        return []

    df = peer_df.copy()
    df["total_mv_yi"] = df["total_mv"].fillna(0) / 1e8
    df["main_net_inflow_wan"] = df["main_net_inflow"].fillna(0) / 1e4

    # 各维度排名（升序，1=最优）
    df["rank_size"] = df["total_mv_yi"].rank(ascending=False, method="min").astype(int)
    df["rank_discount"] = df["discount_rate"].rank(ascending=False, method="min").astype(int)  # 折价率高=溢价低=好
    df["rank_turnover"] = df["turnover_rate"].rank(ascending=False, method="min").astype(int)
    df["rank_inflow"] = df["main_net_inflow_wan"].rank(ascending=False, method="min").astype(int)
    df["rank_volume_ratio"] = df["volume_ratio"].rank(ascending=False, method="min").astype(int)

    # 综合排名分（等权平均）
    rank_cols = ["rank_size", "rank_discount", "rank_turnover", "rank_inflow", "rank_volume_ratio"]
    df["composite_rank"] = df[rank_cols].mean(axis=1).round(2)

    results = []
    for _, row in df.iterrows():
        results.append(PeerRanking(
            code=str(row["code"]),
            name=row.get("name", ""),
            rank_size=int(row["rank_size"]),
            rank_discount=int(row["rank_discount"]),
            rank_turnover=int(row["rank_turnover"]),
            rank_inflow=int(row["rank_inflow"]),
            rank_volume_ratio=int(row["rank_volume_ratio"]),
            total_mv=round(row["total_mv_yi"], 2),
            discount_rate=round(row.get("discount_rate", 0), 3),
            turnover_rate=round(row.get("turnover_rate", 0), 3),
            main_net_inflow=round(row["main_net_inflow_wan"], 2),
            volume_ratio=round(row.get("volume_ratio", 0), 2),
            composite_rank=round(row["composite_rank"], 2),
        ))

    return sorted(results, key=lambda x: x.composite_rank)


# ======== 综合穿透对比 ========

@dataclass
class PeerPenetration:
    """同类 ETF 穿透对比综合结果"""
    target_code: str
    target_name: str
    overlap_results: list       # list[OverlapResult]
    ranking_results: list       # list[PeerRanking]
    target_rank: Optional[PeerRanking]

def compute_peer_penetration(
    code: str,
    name: str,
    peer_df: pd.DataFrame,
    holdings_df: pd.DataFrame,
) -> PeerPenetration:
    """计算单只 ETF 的同类穿透对比。

    Parameters
    ----------
    code, name : str — 目标 ETF
    peer_df : pd.DataFrame — 同类 ETF 行情数据
    holdings_df : pd.DataFrame — 重仓股数据

    Returns PeerPenetration
    """
    peer_codes = peer_df["code"].astype(str).tolist()

    # 重叠度
    overlap_results = compute_peer_overlap_matrix(code, peer_codes, holdings_df)

    # 排名
    ranking_results = compute_peer_ranking(code, peer_df)
    target_rank = next((r for r in ranking_results if r.code == code), None)

    return PeerPenetration(
        target_code=code,
        target_name=name,
        overlap_results=overlap_results,
        ranking_results=ranking_results,
        target_rank=target_rank,
    )
