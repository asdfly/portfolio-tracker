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

from src.utils.holiday_write_guard import check_write_date

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
    """采集某日涨停/跌停家数 + 连板高度 + 主线行业，幂等落库。

    P1#11 采集侧（2026-10-27）：`date_str` 非交易日 ⇒ **不采集、不落库**，返回
    `skipped=True`。实测污染：本表在 2026-10-01~10-07 国庆 6 个非交易日均有行，
    与 `fund_flows` / `sector_daily_change` 同型 —— 休市日拿到的涨停池是前值残留。

    为什么在这里早退而不是只在写库前拦：涨停池/跌停池是两次外部 HTTP 请求，
    休市日跑出来的结果注定要丢弃，先取再丢纯属浪费配额并放大被限频的风险。
    调用方（`run_analysis`）据 `skipped` 走 `stage=skipped` 而非 `ok`。
    """
    # 建表提前到守卫之前：本表 DDL 由本模块自建（未注册 db_schema.TABLE_DEFS）。
    # 若把 CREATE TABLE 留在守卫之后，空库首次运行恰好落在休市日时**表就不会被创建**，
    # 下游 `fetch_market_data.local_breadth_local()` 的裸 `SELECT MAX(date)` 会直接抛
    # OperationalError（该函数没有 try 包）。建表是幂等无害 DDL，与"是否写数据"无关，
    # 故先建表、再判能不能写。
    _ensure_table(conn)

    _allow, _st, _rs = check_write_date(date_str, table=TABLE)
    if not _allow:
        logger.warning("%s ⇒ 跳过市场广度采集与落库", _rs)
        return {
            "date": date_str, "zt_count": None, "dt_count": None,
            "max_lianban": None, "zt_avg_turnover": None,
            "top_industry": None, "top_industry_count": None,
            "skipped": True, "skip_reason": _rs,
        }

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

    _ensure_table(conn)   # 幂等；函数入口已建过一次，此处保留以防未来调整调用顺序
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
        "skipped": False,
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
