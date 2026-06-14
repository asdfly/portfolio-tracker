"""估值历史分位数模块 — 采集并存储追踪指数PE历史数据，计算当前PE分位数。

功能:
  1. fetch_index_pe_history: 从中证指数公司获取历史PE数据
  2. save_pe_history: 存入 index_pe_history 表
  3. load_pe_percentile: 查询当前PE在历史中的分位数
  4. compute_pe_percentile: 计算单个指数PE的历史分位数
"""
import logging
from typing import Dict, List, Optional
from datetime import datetime

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


def fetch_index_pe_history(index_code: str) -> pd.DataFrame:
    """获取追踪指数历史PE数据(中证指数公司)。

    Args:
        index_code: 中证指数代码, e.g. "000300"

    Returns:
        DataFrame [date, pe] 或空DataFrame
    """
    import akshare as ak
    try:
        df = ak.stock_zh_index_value_csindex(symbol=index_code)
    except Exception as e:
        logger.warning(f"指数{index_code}历史估值获取失败: {e}")
        return pd.DataFrame()

    if df is None or df.empty:
        return pd.DataFrame()

    # 标准化列名
    col_map = {
        "日期": "date", "市盈率1": "pe1", "市盈率2": "pe2",
        "股息率1": "div_yield1", "股息率2": "div_yield2",
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

    # 确保date是字符串
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

    # 过滤有效PE
    if "pe1" in df.columns:
        df["pe"] = pd.to_numeric(df["pe1"], errors="coerce")
        df = df.dropna(subset=["pe"])
        df = df[df["pe"] > 0]
    else:
        return pd.DataFrame()

    return df[["date", "pe"]].reset_index(drop=True)


def save_pe_history(conn, index_code: str, df: pd.DataFrame) -> int:
    """将PE历史数据存入 index_pe_history 表。

    Args:
        conn: 数据库连接
        index_code: 指数代码
        df: DataFrame [date, pe]

    Returns:
        写入行数
    """
    if df is None or df.empty:
        return 0

    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS index_pe_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            index_code TEXT NOT NULL,
            date TEXT NOT NULL,
            pe REAL,
            UNIQUE(index_code, date)
        )
    """)
    conn.commit()

    count = 0
    for _, row in df.iterrows():
        try:
            cursor.execute(
                "INSERT OR REPLACE INTO index_pe_history (index_code, date, pe) VALUES (?, ?, ?)",
                (index_code, str(row["date"]), float(row["pe"])))
            count += 1
        except Exception as e:
            logger.debug(f"写入PE历史失败: {index_code}/{row['date']}: {e}")
    conn.commit()
    return count


def compute_pe_percentile(current_pe: float, history_pe: List[float],
                          window_years: int = 5) -> Dict[str, float]:
    """计算当前PE在历史中的分位数。

    Args:
        current_pe: 当前PE值
        history_pe: 历史PE列表
        window_years: 统计窗口(年), None表示全历史

    Returns:
        {percentile_3y, percentile_5y, percentile_all, pe_min, pe_max, pe_median}
    """
    if not history_pe or current_pe <= 0:
        return {
            "percentile_3y": 50.0, "percentile_5y": 50.0, "percentile_all": 50.0,
            "pe_min": None, "pe_max": None, "pe_median": None, "history_count": 0,
        }

    pe_arr = np.array([float(x) for x in history_pe if x > 0])
    count = len(pe_arr)

    def _pct(arr):
        if len(arr) == 0:
            return 50.0
        below = np.sum(arr < current_pe)
        return round(float(below / len(arr) * 100), 1)

    return {
        "percentile_3y": _pct(pe_arr[-750:]) if count > 250 else _pct(pe_arr),
        "percentile_5y": _pct(pe_arr[-1250:]) if count > 500 else _pct(pe_arr),
        "percentile_all": _pct(pe_arr),
        "pe_min": float(np.min(pe_arr)),
        "pe_max": float(np.max(pe_arr)),
        "pe_median": float(np.median(pe_arr)),
        "history_count": count,
    }


def load_pe_percentile(conn, index_code: str, current_pe: float = None) -> Dict[str, float]:
    """从数据库加载历史PE并计算分位数。

    Args:
        conn: 数据库连接
        index_code: 指数代码
        current_pe: 当前PE, None则取最新

    Returns:
        分位数结果 dict
    """
    rows = conn.execute(
        "SELECT pe FROM index_pe_history WHERE index_code=? ORDER BY date",
        (index_code,)).fetchall()

    history = [r[0] for r in rows if r[0] is not None and r[0] > 0]

    if current_pe is None and history:
        current_pe = history[-1]

    return compute_pe_percentile(current_pe or 50.0, history)
