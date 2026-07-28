"""ETF基本面数据采集模块 — 聚焦持仓ETF的F10数据。

采集维度:
  1. fund_etf_spot_em: 实时行情+份额/市值/折价率/资金流
  2. fund_portfolio_industry_allocation_em: 行业配置
  3. stock_zh_index_value_csindex: 追踪指数估值(PE/股息率)
  4. fund_portfolio_hold_em: 前N大重仓股
"""
import logging
import time
from typing import Dict, List, Optional, Any
from datetime import datetime

import pandas as pd
import sqlite3

logger = logging.getLogger(__name__)


# ETF代码 -> 追踪指数代码映射 (csindex 格式)
ETF_TO_INDEX = {
    # 宽基
    "510300": "000300",  # 沪深300
    "159300": "000300",  # 沪深300
    "510500": "000905",  # 中证500
    "512100": "000852",  # 中证1000
    "159949": "399673",  # 创业板50
    "588000": "000688",  # 科创50
    # 医药
    "512010": "399989",  # 中证医药
    "159992": "931152",  # 中证创新药
    "515120": "931152",  # 中证创新药
    # 科技
    "159732": "931743",  # 中证消费电子
    "159770": "930006",  # 中证机器人
    "159819": "930713",  # 中证人工智能
    # 军工
    "159267": "399959",  # 中证航天军工
    "512810": "399959",  # 中证军工
    # 新能源
    "159796": "931157",  # 中证电池
    "516160": "399808",  # 中证新能源
    "561910": "931157",  # 中证电池
    # 金融
    "515010": "399975",  # 中证证券
    # 红利
    "159220": "h11118",  # 恒生港股通高股息低波动
    "563020": "H30269",  # 中证红利低波动
    # 债券 (无追踪指数PE, 不加映射)
}


def fetch_etf_spot_batch(codes: List[str]) -> pd.DataFrame:
    """批量获取ETF实时行情(含份额/折价率/资金流)。

    一次调接口获取全量ETF列表, 再过滤目标代码, 避免逐个请求。

    Args:
        codes: ETF代码列表 (6位数字)

    Returns:
        DataFrame, 标准化列名
    """
    import akshare as ak
    try:
        df = ak.fund_etf_spot_em()
    except Exception as e:
        logger.warning(f"ETF实时行情获取失败: {e}")
        return pd.DataFrame()

    if df is None or df.empty:
        return pd.DataFrame()

    col_map = {
        "代码": "code", "名称": "name",
        "最新价": "price", "IOPV实时估值": "iopv",
        "基金折价率": "discount_rate",
        "涨跌额": "change", "涨跌幅": "change_pct",
        "成交量": "volume", "成交额": "amount",
        "开盘价": "open", "最高价": "high",
        "最低价": "low", "昨收": "pre_close",
        "振幅": "amplitude", "换手率": "turnover_rate",
        "量比": "volume_ratio",
        "主力净流入-净额": "main_net_inflow",
        "主力净流入-净占比": "main_net_inflow_pct",
        "超大单净流入-净额": "super_large_net_inflow",
        "超大单净流入-净占比": "super_large_net_pct",
        "大单净流入-净额": "large_net_inflow",
        "大单净流入-净占比": "large_net_pct",
        "最新份额": "shares",
        "流通市值": "float_mv", "总市值": "total_mv",
        "数据日期": "data_date",
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
    df = df[df["code"].isin(codes)]
    df = df.reset_index(drop=True)
    return df


def fetch_industry_allocation(code: str) -> pd.DataFrame:
    """获取ETF行业配置。

    Args:
        code: ETF代码
    Returns:
        DataFrame with columns: [行业, 占净值比例, 市值]
    """
    import akshare as ak
    year = str(datetime.now().year)
    try:
        df = ak.fund_portfolio_industry_allocation_em(symbol=code, date=year)
    except Exception as e:
        logger.debug(f"{code} 行业配置获取失败: {e}")
        return pd.DataFrame()

    if df is None or df.empty:
        return pd.DataFrame()

    col_map = {
        "序号": "_seq",
        "行业类别": "industry",
        "占净值比例": "weight_pct",
        "市值": "market_value",
        "截止时间": "report_date",
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
    if "_seq" in df.columns:
        df = df.drop(columns=["_seq"])
    df["code"] = code
    return df


def fetch_top_holdings(code: str, top_n: int = 10) -> pd.DataFrame:
    """获取ETF前N大重仓股。

    Args:
        code: ETF代码
        top_n: 返回前N大持仓
    Returns:
        DataFrame with columns: [stock_code, stock_name, weight_pct, market_value]
    """
    import akshare as ak
    year = str(datetime.now().year)
    try:
        df = ak.fund_portfolio_hold_em(symbol=code, date=year)
    except Exception as e:
        logger.debug(f"{code} 重仓股获取失败: {e}")
        return pd.DataFrame()

    if df is None or df.empty:
        return pd.DataFrame()

    col_map = {
        "序号": "_seq",
        "股票代码": "stock_code",
        "股票名称": "stock_name",
        "占净值比例": "weight_pct",
        "持股数": "holding_qty",
        "持仓市值": "market_value",
        "季度": "quarter",
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
    if "_seq" in df.columns:
        df = df.drop(columns=["_seq"])
    df["code"] = code
    return df.head(top_n)


def fetch_index_valuation(index_code: str) -> Dict[str, float]:
    """获取追踪指数估值(PE/股息率)。

    Args:
        index_code: 中证指数代码(6位数字), e.g. "000300"
    Returns:
        Dict with keys: pe1, pe2, div_yield1, div_yield2, date
    """
    import akshare as ak
    try:
        df = ak.stock_zh_index_value_csindex(symbol=index_code)
    except Exception as e:
        logger.debug(f"指数{index_code}估值获取失败: {e}")
        return {}

    if df is None or df.empty:
        return {}

    row = df.iloc[0]
    return {
        "pe1": float(row.get("市盈率1", 0) or 0),
        "pe2": float(row.get("市盈率2", 0) or 0),
        "div_yield1": float(row.get("股息率1", 0) or 0),
        "div_yield2": float(row.get("股息率2", 0) or 0),
        "date": str(row.get("日期", "")),
    }


def save_to_db(conn: sqlite3.Connection, table: str,
               df: pd.DataFrame, unique_cols: List[str]) -> int:
    """INSERT OR REPLACE 通用写入函数."""
    if df is None or df.empty:
        return 0
    df = df.copy()
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].apply(
                lambda x: None if pd.isna(x) or str(x).strip() == "" else str(x))
        else:
            df[col] = df[col].apply(lambda x: None if pd.isna(x) else x)

    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table})")
    table_cols = {row[1] for row in cursor.fetchall()}
    cols = [c for c in df.columns if c in table_cols]
    if not cols:
        return 0
    df = df[cols]

    count = 0
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for _, row in df.iterrows():
        values = {col: row[col] for col in cols}
        if "created_at" in cols and not values.get("created_at"):
            values["created_at"] = now_str
        if "updated_at" in cols and not values.get("updated_at"):
            values["updated_at"] = now_str
        ph = ", ".join(["?"] * len(cols))
        cn = ", ".join(cols)
        try:
            cursor.execute(
                f"INSERT OR REPLACE INTO {table} ({cn}) VALUES ({ph})",
                list(values.values()))
            count += 1
        except sqlite3.OperationalError as e:
            logger.debug(f"写入{table}失败: {e}")
    conn.commit()
    return count


def _ensure_tables(conn: sqlite3.Connection):
    """创建基本面数据表(如不存在)。"""
    cursor = conn.cursor()

    cursor.executescript("""
    CREATE TABLE IF NOT EXISTS etf_fundamental (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT NOT NULL,
        code TEXT NOT NULL,
        name TEXT,
        sector TEXT,
        price REAL, iopv REAL, discount_rate REAL,
        change_pct REAL, volume REAL, amount REAL,
        turnover_rate REAL, volume_ratio REAL,
        main_net_inflow REAL, main_net_inflow_pct REAL,
        super_large_net_inflow REAL, super_large_net_pct REAL,
        large_net_inflow REAL, large_net_pct REAL,
        shares REAL, float_mv REAL, total_mv REAL,
        -- 追踪指数估值
        index_code TEXT, index_name TEXT,
        pe1 REAL, pe2 REAL,
        div_yield1 REAL, div_yield2 REAL,
        created_at TEXT,
        UNIQUE(date, code)
    );

    CREATE TABLE IF NOT EXISTS etf_industry_alloc (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        code TEXT NOT NULL,
        industry TEXT NOT NULL,
        weight_pct REAL,
        market_value REAL,
        report_date TEXT,
        updated_at TEXT,
        UNIQUE(code, industry)
    );

    CREATE TABLE IF NOT EXISTS etf_top_holdings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        code TEXT NOT NULL,
        stock_code TEXT NOT NULL,
        stock_name TEXT,
        weight_pct REAL,
        holding_qty REAL,
        market_value REAL,
        quarter TEXT,
        updated_at TEXT,
        UNIQUE(code, stock_code, quarter)
    );
    """)
    conn.commit()


def run_etf_fundamental_collection(
    codes: List[str],
    etf_categories: Dict[str, dict],
    target_date: Optional[str] = None,
) -> Dict[str, Any]:
    """采集ETF基本面数据(聚焦持仓)。

    Args:
        codes: 持仓ETF代码列表
        etf_categories: ETF分类配置{code: {name, sector, color}}
        target_date: 目标日期 YYYY-MM-DD

    Returns:
        采集统计
    """
    from src.utils.database import get_db_connection

    if target_date is None:
        target_date = datetime.now().strftime("%Y-%m-%d")

    logger.info(f"[ETF基本面] 开始采集, {len(codes)}只ETF, 日期: {target_date}")
    stats = {"spot": 0, "industry": 0, "holdings": 0, "valuation": 0, "errors": []}

    conn = get_db_connection()
    try:
        _ensure_tables(conn)

        # 1. ETF实时行情(含份额/折价/资金流)
        try:
            df_spot = fetch_etf_spot_batch(codes)
            if not df_spot.empty:
                # 添加sector信息
                df_spot["sector"] = df_spot["code"].map(
                    lambda c: etf_categories.get(c, {}).get("sector", ""))
                # 添加跟踪指数估值
                index_valuations = {}
                for code in codes:
                    idx = ETF_TO_INDEX.get(code)
                    if idx:
                        val = fetch_index_valuation(idx)
                        if val:
                            index_valuations[code] = val
                            time.sleep(0.5)

                df_spot["index_code"] = df_spot["code"].map(
                    lambda c: ETF_TO_INDEX.get(c, ""))
                df_spot["index_name"] = df_spot["index_code"].map(
                    lambda c: ETF_TO_INDEX.get(c, "") if c else "")
                # 将index_code重映射为ETF代码
                for col in ["pe1","pe2","div_yield1","div_yield2","date"]:
                    df_spot[col] = df_spot["code"].map(
                        lambda c: index_valuations.get(c, {}).get(col, None))
                stats["valuation"] = len(index_valuations)

                df_spot["date"] = target_date
                stats["spot"] = save_to_db(conn, "etf_fundamental", df_spot, ["date","code"])
                logger.info(f'  ETF行情: {stats["spot"]}条, 估值: {stats["valuation"]}条')
        except (KeyError, ValueError, TypeError) as e:
            stats["errors"].append(f"ETF行情: {e}")
            logger.warning(f"  ETF行情失败: {e}")

        # 2. 行业配置(逐个请求,控制频率)
        industry_all = []
        for code in codes:
            try:
                df_ind = fetch_industry_allocation(code)
                if not df_ind.empty:
                    industry_all.append(df_ind)
                    time.sleep(0.5)
            except Exception as e:
                stats["errors"].append(f"行业{code}: {e}")
        if industry_all:
            df_ind_all = pd.concat(industry_all, ignore_index=True)
            stats["industry"] = save_to_db(
                conn, "etf_industry_alloc", df_ind_all, ["code","industry"])
            logger.info(f'  行业配置: {stats["industry"]}条')

        # 3. 前N大重仓股
        holdings_all = []
        for code in codes:
            try:
                df_hold = fetch_top_holdings(code, top_n=10)
                if not df_hold.empty:
                    holdings_all.append(df_hold)
                    time.sleep(0.5)
            except Exception as e:
                stats["errors"].append(f"重仓{code}: {e}")
        if holdings_all:
            df_hold_all = pd.concat(holdings_all, ignore_index=True)
            stats["holdings"] = save_to_db(
                conn, "etf_top_holdings", df_hold_all, ["code","stock_code","quarter"])
            logger.info(f'  重仓股: {stats["holdings"]}条')

    finally:
        conn.close()

    total = sum(v for k, v in stats.items() if k != "errors")
    logger.info(f"[ETF基本面] 采集完成, 合计{total}条")
    return stats