"""交易记录导入工具

支持从 CSV/Excel 文件导入交易记录到 trades 表。
适配招商证券等常见券商导出格式。
"""

import sqlite3
import pandas as pd
from typing import Optional, Dict, List
from datetime import datetime

from src.utils.database import get_db_connection


# 招商证券导出格式列映射（中文表头）
CMS_COLUMNS = {
    "发生日期": "date",
    "证券代码": "code",
    "证券名称": "name",
    "操作": "direction",
    "成交均价": "price",
    "成交数量": "quantity",
    "手续费": "fee",
    "备注": "note",
}

# 通华/同花顺导出格式
THS_COLUMNS = {
    "成交日期": "date",
    "证券代码": "code",
    "证券名称": "name",
    "买卖方向": "direction",
    "成交价格": "price",
    "成交数量": "quantity",
    "手续费": "fee",
    "摘要": "note",
}


def normalize_direction(value: str) -> Optional[str]:
    """标准化买卖方向。"""
    if pd.isna(value):
        return None
    v = str(value).strip()
    if v in ("买入", "BUY", "买", "证券买入"):
        return "BUY"
    elif v in ("卖出", "SELL", "卖", "证券卖出"):
        return "SELL"
    return None


def normalize_code(value: str) -> str:
    """标准化证券代码（去除后缀，补齐6位）。"""
    if pd.isna(value):
        return ""
    v = str(value).strip()
    # Remove exchange suffix
    for suffix in (".SH", ".SZ", ".sh", ".sz", "上海", "深圳"):
        v = v.replace(suffix, "")
    return v.zfill(6)


def normalize_date(value) -> str:
    """标准化日期为 YYYY-MM-DD 格式。"""
    if pd.isna(value):
        return ""
    if isinstance(value, str):
        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y%m%d", "%Y年%m月%d日"):
            try:
                return datetime.strptime(value.strip(), fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
    elif isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    return str(value)


def import_from_dataframe(df: pd.DataFrame, column_map: Optional[Dict] = None,
                          dry_run: bool = False) -> Dict:
    """从 DataFrame 导入交易记录。

    Parameters
    ----------
    df : pd.DataFrame
        原始交易数据
    column_map : dict, optional
        列名映射 {原始列名: 标准列名}，默认自动检测
    dry_run : bool
        如果为 True，只验证不写入

    Returns
    -------
    dict : {success: int, failed: int, errors: list, preview: DataFrame}
    """
    if df.empty:
        return {"success": 0, "failed": 0, "errors": ["空数据"], "preview": pd.DataFrame()}

    # Auto-detect column mapping
    if column_map is None:
        columns = list(df.columns)
        if any(c in columns for c in CMS_COLUMNS):
            column_map = CMS_COLUMNS
        elif any(c in columns for c in THS_COLUMNS):
            column_map = THS_COLUMNS
        else:
            # Try case-insensitive match
            lower_cols = {c.lower(): c for c in columns}
            column_map = {}
            for orig, std in {**CMS_COLUMNS, **THS_COLUMNS}.items():
                if orig.lower() in lower_cols:
                    column_map[lower_cols[orig.lower()]] = std

    if not column_map:
        return {"success": 0, "failed": 0, "errors": ["无法识别列名格式"], "preview": df.head()}

    # Rename columns
    renamed = df.rename(columns=column_map)

    # Filter only mapped columns
    std_cols = ["date", "code", "name", "direction", "price", "quantity", "fee", "note"]
    available = [c for c in std_cols if c in renamed.columns]
    renamed = renamed[available].copy()

    # Normalize values
    if "direction" in renamed.columns:
        renamed["direction"] = renamed["direction"].apply(normalize_direction)
    if "code" in renamed.columns:
        renamed["code"] = renamed["code"].apply(normalize_code)
    if "date" in renamed.columns:
        renamed["date"] = renamed["date"].apply(normalize_date)
    if "price" in renamed.columns:
        renamed["price"] = pd.to_numeric(renamed["price"], errors="coerce")
    if "quantity" in renamed.columns:
        renamed["quantity"] = pd.to_numeric(renamed["quantity"], errors="coerce").astype("Int64")
    if "fee" in renamed.columns:
        renamed["fee"] = pd.to_numeric(renamed["fee"], errors="coerce").fillna(0)

    # Remove invalid rows
    valid = renamed.dropna(subset=["date", "code", "direction", "price", "quantity"])
    invalid_count = len(renamed) - len(valid)

    if valid.empty:
        return {"success": 0, "failed": len(df), "errors": ["无有效行（缺少必填字段）"],
                "preview": df.head()}

    if dry_run:
        return {
            "success": len(valid),
            "failed": invalid_count,
            "errors": [],
            "preview": valid,
        }

    # Write to database
    conn = get_db_connection()
    try:
        count = 0
        for _, row in valid.iterrows():
            conn.execute(
                "INSERT INTO trades (date, code, name, direction, price, quantity, fee, note) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    row.get("date", ""),
                    row.get("code", ""),
                    row.get("name", ""),
                    row.get("direction", ""),
                    float(row.get("price", 0)),
                    int(row.get("quantity", 0)),
                    float(row.get("fee", 0)),
                    str(row.get("note", "")),
                )
            )
            count += 1
        conn.commit()
        return {
            "success": count,
            "failed": invalid_count,
            "errors": [],
            "preview": valid.head(10),
        }
    except Exception as e:
        conn.rollback()
        return {"success": 0, "failed": len(valid), "errors": [str(e)], "preview": valid.head()}
    finally:
        conn.close()


def import_from_csv(file_path: str, column_map: Optional[Dict] = None,
                    encoding: str = "utf-8", dry_run: bool = False) -> Dict:
    """从 CSV 文件导入交易记录。

    Parameters
    ----------
    file_path : str
        CSV 文件路径
    column_map : dict, optional
        列名映射
    encoding : str
        文件编码
    dry_run : bool
        仅验证不写入

    Returns
    -------
    dict : 导入结果
    """
    try:
        df = pd.read_csv(file_path, encoding=encoding)
    except UnicodeDecodeError:
        df = pd.read_csv(file_path, encoding="gbk")
    return import_from_dataframe(df, column_map, dry_run)


def import_from_excel(file_path: str, column_map: Optional[Dict] = None,
                      dry_run: bool = False) -> Dict:
    """从 Excel 文件导入交易记录。

    Parameters
    ----------
    file_path : str
        Excel 文件路径
    column_map : dict, optional
        列名映射
    dry_run : bool
        仅验证不写入

    Returns
    -------
    dict : 导入结果
    """
    df = pd.read_excel(file_path)
    return import_from_dataframe(df, column_map, dry_run)


def load_trades(code: Optional[str] = None, start_date: Optional[str] = None,
                end_date: Optional[str] = None) -> pd.DataFrame:
    """加载交易记录。

    Parameters
    ----------
    code : str, optional
        ETF 代码筛选
    start_date : str, optional
        起始日期
    end_date : str, optional
        结束日期

    Returns
    -------
    pd.DataFrame
    """
    conn = get_db_connection()
    try:
        query = "SELECT * FROM trades WHERE 1=1"
        params = []
        if code:
            query += " AND code = ?"
            params.append(code)
        if start_date:
            query += " AND date >= ?"
            params.append(start_date)
        if end_date:
            query += " AND date <= ?"
            params.append(end_date)
        query += " ORDER BY date"
        df = pd.read_sql_query(query, conn, params=params)
        return df if not df.empty else pd.DataFrame()
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()


def compute_trade_analysis(code: str) -> Dict:
    """计算单只 ETF 的交易复盘分析。

    Parameters
    ----------
    code : str
        ETF 代码

    Returns
    -------
    dict : {
        total_trades, buy_count, sell_count,
        total_buy_amount, total_sell_amount,
        avg_buy_price, avg_sell_price,
        realized_pnl, win_rate,
        trades: DataFrame
    }
    """
    trades = load_trades(code)
    if trades.empty:
        return {}

    buys = trades[trades["direction"] == "BUY"]
    sells = trades[trades["direction"] == "SELL"]

    total_buy_amount = (buys["price"] * buys["quantity"]).sum()
    total_sell_amount = (sells["price"] * sells["quantity"]).sum()
    total_fee = trades["fee"].sum()

    avg_buy = buys["price"].mean() if not buys.empty else 0
    avg_sell = sells["price"].mean() if not sells.empty else 0

    # 简单实现：按买入/卖出配对计算盈亏
    # FIFO matching
    buy_queue = []
    realized_pnl = 0
    winning_trades = 0
    losing_trades = 0

    for _, trade in trades.sort_values("date").iterrows():
        if trade["direction"] == "BUY":
            buy_queue.append({"price": trade["price"], "qty": trade["quantity"]})
        elif trade["direction"] == "SELL" and buy_queue:
            remaining = trade["quantity"]
            while remaining > 0 and buy_queue:
                oldest = buy_queue[0]
                match_qty = min(remaining, oldest["qty"])
                pnl = (trade["price"] - oldest["price"]) * match_qty
                realized_pnl += pnl
                if pnl > 0:
                    winning_trades += 1
                else:
                    losing_trades += 1
                remaining -= match_qty
                oldest["qty"] -= match_qty
                if oldest["qty"] <= 0:
                    buy_queue.pop(0)

    total_matched = winning_trades + losing_trades
    win_rate = (winning_trades / total_matched * 100) if total_matched > 0 else 0

    return {
        "total_trades": len(trades),
        "buy_count": len(buys),
        "sell_count": len(sells),
        "total_buy_amount": round(total_buy_amount, 2),
        "total_sell_amount": round(total_sell_amount, 2),
        "total_fee": round(total_fee, 2),
        "avg_buy_price": round(avg_buy, 4),
        "avg_sell_price": round(avg_sell, 4),
        "realized_pnl": round(realized_pnl, 2),
        "win_rate": round(win_rate, 1),
        "trades": trades,
    }
