# -*- coding: utf-8 -*-
"""股债性价比模型 (ERP) — P3 进阶能力

基于指数PE分位数 + 无风险利率(10Y国债)计算股权风险溢价，
判断当前股/债相对吸引力。

核心公式: ERP = 1/PE_指数 - 无风险利率
  - 1/PE 代表股票盈利收益率
  - 无风险利率取10年期国债收益率
  - ERP > 历史均值: 股票便宜，偏多
  - ERP < 历史均值: 股票贵，偏空
"""
import logging

import numpy as np
import pandas as pd
from dataclasses import dataclass
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

from src.utils.database import get_db_connection

@dataclass
class ERPResult:
    """股债性价比分析结果"""
    index_code: str
    index_name: str
    current_pe: float
    earnings_yield: float       # 1/PE, 盈利收益率%
    risk_free_rate: float        # 10Y国债收益率%
    erp: float                   # ERP = earnings_yield - risk_free_rate
    erp_percentile: float         # ERP历史分位数 0-100
    signal: str                  # 偏多/中性/偏空
    detail: str


# 指数代码到名称的映射
INDEX_NAMES = {
    "sh000300": "沪深300", "sh000905": "中证500", "sh000852": "中证1000",
    "sh000015": "上证50", "sh000688": "科创50", "sz399006": "创业板指",
    "sh000001": "上证指数", "sz399001": "深证成指",
    "sz399673": "创业板50", "sz399987": "中证2000",
}

# 指数到中证PE API代码的映射 (用于akshare)
INDEX_PE_API = {
    "sh000300": "000300", "sh000905": "000905", "sh000852": "000852",
    "sh000015": "000016", "sh000688": "000688", "sz399006": "399006",
}


def compute_erp(current_pe, risk_free_rate_pct):
    """计算单个时点的ERP。

    Parameters
    ----------
    current_pe : float — 当前指数PE
    risk_free_rate_pct : float — 10Y国债收益率(%)

    Returns float — ERP(%)
    """
    if current_pe <= 0 or risk_free_rate_pct < 0:
        return 0.0
    earnings_yield = 1.0 / current_pe * 100
    return round(earnings_yield - risk_free_rate_pct, 4)


def classify_erp_signal(erp_value, erp_history):
    """基于ERP历史分位数判断信号。

    Parameters
    ----------
    erp_value : float — 当前ERP
    erp_history : list[float] — 历史ERP序列

    Returns (signal, detail) : tuple
    """
    if not erp_history or len(erp_history) < 20:
        return ("数据不足", "历史ERP数据不足20个点")

    hist = pd.Series(erp_history)
    median = hist.median()
    percentile = (hist < erp_value).sum() / len(hist) * 100

    if percentile >= 70:
        signal = "偏多"
        detail = f"ERP={erp_value:.2f}% 处于历史{percentile:.0f}%分位(>中位{median:.2f}%)，股权吸引力较强"
    elif percentile >= 50:
        signal = "中性略偏多"
        detail = f"ERP={erp_value:.2f}% 处于历史{percentile:.0f}%分位(>中位{median:.2f}%)，股权吸引力适中"
    elif percentile >= 30:
        signal = "中性略偏空"
        detail = f"ERP={erp_value:.2f}% 处于历史{percentile:.0f}%分位(<中位{median:.2f}%)，股权吸引力偏弱"
    else:
        signal = "偏空"
        detail = f"ERP={erp_value:.2f}% 处于历史{percentile:.0f}%分位(远低于中位{median:.2f}%)，股权吸引力弱"

    return (signal, detail)

def load_index_pe_from_db(index_code, days=365*3):
    """从index_pe_history表加载指数PE历史。

    Returns pd.DataFrame: date, pe
    """
    from src.utils.database import get_db_connection
    conn = get_db_connection()
    try:
        df = pd.read_sql_query(
            "SELECT date, pe FROM index_pe_history WHERE index_code=? AND date >= date('now', ?) ORDER BY date",
            conn, params=[index_code, f"-{days} days"]
        )
    except Exception:
        df = pd.DataFrame()
    finally:
        conn.close()
    return df


def load_risk_free_rate(days=30):
    """获取近期10Y国债收益率（从macro_daily或bond_yield表）。

    Returns float or None — 最新10Y国债收益率%
    """
    from src.utils.database import get_db_connection
    conn = get_db_connection()
    try:
        # Try bond_yield table first
        df = pd.read_sql_query(
            "SELECT value FROM macro_daily WHERE indicator_code='10Y_BOND' ORDER BY date DESC LIMIT 1",
            conn
        )
        if df.empty:
            # Try alternative indicators
            for code in ['BOND_10Y', 'CHINA_10Y_BOND', 'GCN10']:
                df = pd.read_sql_query(
                    "SELECT value FROM macro_daily WHERE indicator_code=? ORDER BY date DESC LIMIT 1",
                    conn, params=[code]
                )
                if not df.empty:
                    break
    except Exception:
        return None
    except (sqlite3.OperationalError, pd.errors.DatabaseError, KeyError) as e:
        logger.warning(f"DB error loading risk-free rate: {e}")
        return None
    finally:
        conn.close()

    if not df.empty and pd.notna(df.iloc[0]["value"]):
        return float(df.iloc[0]["value"])
    return None


def compute_erp_for_index(index_code, risk_free_rate=None):
    """计算单个指数的完整ERP分析。

    Parameters
    ----------
    index_code : str — 指数代码
    risk_free_rate : float, optional — 覆盖无风险利率

    Returns ERPResult or None
    """
    pe_df = load_index_pe_from_db(index_code)
    if pe_df.empty:
        return None

    current_pe = float(pe_df.iloc[-1]["pe"])
    if current_pe <= 0:
        return None

    if risk_free_rate is None:
        risk_free_rate = load_risk_free_rate()
    if risk_free_rate is None:
        risk_free_rate = 2.5  # 默认值

    # 计算历史ERP序列
    pe_series = pe_df["pe"].values
    earnings_yields = [1.0 / p * 100 if p > 0 else 0 for p in pe_series]
    erp_history = [round(ey - risk_free_rate, 4) for ey in earnings_yields]

    current_ey = earnings_yields[-1]
    erp = compute_erp(current_pe, risk_free_rate)

    # 分位数
    signal, detail = classify_erp_signal(erp, erp_history)
    percentile = 0.0
    if erp_history:
        percentile = round(sum(1 for e in erp_history if e < erp) / len(erp_history) * 100, 1)

    return ERPResult(
        index_code=index_code,
        index_name=INDEX_NAMES.get(index_code, index_code),
        current_pe=round(current_pe, 2),
        earnings_yield=round(current_ey, 2),
        risk_free_rate=round(risk_free_rate, 2),
        erp=erp,
        erp_percentile=percentile,
        signal=signal,
        detail=detail,
    )


def compute_erp_multi(indices=None):
    """批量计算多个指数的ERP。

    Parameters
    ----------
    indices : list[str], optional — 指数代码列表，默认主流宽基

    Returns list[ERPResult]
    """
    if indices is None:
        indices = ["sh000300", "sh000905", "sh000015", "sh000688", "sz399006", "sh000852"]
    results = []
    for code in indices:
        r = compute_erp_for_index(code)
        if r is not None:
            results.append(r)
    return results