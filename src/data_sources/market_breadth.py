"""市场广度/涨停梯队采集模块 — 涨停池 + 跌停池（东方财富 EM，可靠源）。

支撑两个融入点：
  - P1-5 极端情绪量化：涨停/跌停家数、连板高度、涨停股平均换手率 + 已有融资数据交叉验证
  - P2-7 涨停梯队/主线线索：最高连板数（梯队高度）+ 涨停最集中行业（主线线索）

数据源选择说明：
  - 乐咕乐股 `stock_market_activity_legu`（涨跌家数/活跃度）经实测**间歇性 SSL 故障**（legulegu.com
    时通时断），不可作为自动化采集基础，故弃用。
  - 改用东方财富 EM 的 `stock_zt_pool_em`（涨停池）/ `stock_zt_pool_dtgc_em`（跌停池），
    与项目现有 akshare 栈一致、经实测稳定返回。

表 `market_breadth` 由本模块自建（CREATE TABLE IF NOT EXISTS），未注册 db_schema.TABLE_DEFS
（与 index_pe_history 同策略，避免影响 test_db_schema 的表数断言）。
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Dict, Optional

import pandas as pd

logger = logging.getLogger(__name__)

TABLE = "market_breadth"


def _ensure_table(conn) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS market_breadth (
            date TEXT PRIMARY KEY,
            zt_count INTEGER,
            dt_count INTEGER,
            max_lianban INTEGER,
            zt_avg_turnover REAL,
            top_industry TEXT,
            top_industry_count INTEGER,
            updated_at TEXT
        )
    """)
    conn.commit()


def fetch_zt_pool(date_str: str) -> Optional[pd.DataFrame]:
    """涨停池（含连板数/所属行业/换手率）。"""
    import akshare as ak
    try:
        return ak.stock_zt_pool_em(date=date_str)
    except Exception as e:
        logger.warning("涨停池采集失败 %s: %s", date_str, e)
        return None


def fetch_dtgc_pool(date_str: str) -> Optional[pd.DataFrame]:
    """跌停池。"""
    import akshare as ak
    try:
        return ak.stock_zt_pool_dtgc_em(date=date_str)
    except Exception as e:
        logger.warning("跌停池采集失败 %s: %s", date_str, e)
        return None


def _num(s) -> float:
    try:
        return float(s)
    except (TypeError, ValueError):
        return 0.0


def collect_market_breadth(conn, date_str: str) -> Dict:
    """采集某日涨停/跌停家数 + 连板高度 + 主线行业，幂等落库。"""
    zt = fetch_zt_pool(date_str)
    dt = fetch_dtgc_pool(date_str)

    zt_count = len(zt) if zt is not None and len(zt) else 0
    dt_count = len(dt) if dt is not None and len(dt) else 0
    max_lb = 0
    avg_to: Optional[float] = None
    top_ind: Optional[str] = None
    top_ind_n = 0

    if zt is not None and len(zt):
        if "连板数" in zt.columns:
            max_lb = int(pd.to_numeric(zt["连板数"], errors="coerce").fillna(0).max())
        if "换手率" in zt.columns:
            avg_to = float(pd.to_numeric(zt["换手率"], errors="coerce").mean())
        if "所属行业" in zt.columns:
            vc = zt["所属行业"].value_counts()
            if len(vc):
                top_ind = str(vc.index[0])
                top_ind_n = int(vc.iloc[0])

    _ensure_table(conn)
    conn.execute(
        "INSERT OR REPLACE INTO market_breadth "
        "(date, zt_count, dt_count, max_lianban, zt_avg_turnover, "
        " top_industry, top_industry_count, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (date_str, zt_count, dt_count, max_lb, avg_to,
         top_ind, top_ind_n, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
    )
    conn.commit()
    return {
        "date": date_str, "zt_count": zt_count, "dt_count": dt_count,
        "max_lianban": max_lb, "zt_avg_turnover": avg_to,
        "top_industry": top_ind, "top_industry_count": top_ind_n,
    }


def load_latest_breadth(conn) -> Dict:
    """读取最新一行市场广度。"""
    row = conn.execute(
        "SELECT date, zt_count, dt_count, max_lianban, zt_avg_turnover, "
        "top_industry, top_industry_count FROM market_breadth ORDER BY date DESC LIMIT 1"
    ).fetchone()
    if not row:
        return {}
    return {
        "date": row[0], "zt_count": row[1], "dt_count": row[2],
        "max_lianban": row[3], "zt_avg_turnover": row[4],
        "top_industry": row[5], "top_industry_count": row[6],
    }
