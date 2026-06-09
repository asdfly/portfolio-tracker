#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""数据加载与计算引擎模块 — 从 dashboard.py 拆分

包含所有数据库查询（load_*）、计算引擎（compute_*）、
数据清洗（_cleanse_*）和数据库初始化函数。
"""

import sqlite3
from datetime import datetime

import numpy as np
import pandas as pd
import streamlit as st

from config.settings import CHART_DAYS, DATABASE_PATH, ETF_CATEGORIES, INDEX_CODES, SECTOR_COLORS

# ==================== 数据库初始化 ====================

def _ensure_indexes():
    """确保数据库索引存在（只执行一次）"""
    conn = get_db_connection()
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_snap_date ON portfolio_snapshots(date)",
        "CREATE INDEX IF NOT EXISTS idx_snap_code_date ON portfolio_snapshots(code, date)",
        "CREATE INDEX IF NOT EXISTS idx_summary_date ON portfolio_summary(date)",
        "CREATE INDEX IF NOT EXISTS idx_idx_quote_code_date ON index_quotes(code, date)",
        "CREATE INDEX IF NOT EXISTS idx_tech_date ON etf_technical(date)",
        "CREATE INDEX IF NOT EXISTS idx_tech_code_date ON etf_technical(code, date)",
    ]
    for sql in indexes:
        try:
            conn.execute(sql)
        except sqlite3.OperationalError:  # 索引创建失败可忽略
            pass
    conn.commit()
    conn.close()

def get_db_connection(db_path=None):
    """获取数据库连接

    Args:
        db_path: 数据库文件路径，默认为 DATABASE_PATH。
            类方法中传入 self.db_path 即可复用同一接口。
    """
    path = str(db_path) if db_path else str(DATABASE_PATH)
    return sqlite3.connect(path, check_same_thread=False)

def load_positions(date_str=None):
    """加载持仓数据"""
    conn = get_db_connection()
    if date_str:
        query = "SELECT * FROM portfolio_snapshots WHERE date = ? ORDER BY market_value DESC"
        df = pd.read_sql_query(query, conn, params=(date_str,))
    else:
        query = """
            SELECT * FROM portfolio_snapshots 
            WHERE date = (SELECT MAX(date) FROM portfolio_snapshots)
            ORDER BY market_value DESC
        """
        df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def load_summary(days=60, end_date=None):
    """加载组合汇总历史"""
    conn = get_db_connection()
    if end_date:
        query = "SELECT * FROM portfolio_summary WHERE date <= ? ORDER BY date DESC LIMIT ?"
        df = pd.read_sql_query(query, conn, params=(end_date, days))
    else:
        query = "SELECT * FROM portfolio_summary ORDER BY date DESC LIMIT ?"
        df = pd.read_sql_query(query, conn, params=(days,))
    df = df.sort_values("date").reset_index(drop=True)
    conn.close()
    return df

def load_index_quotes(code="sh000300", days=60, end_date=None):
    """加载指数行情"""
    conn = get_db_connection()
    if end_date:
        query = """
            SELECT date, close, volume 
            FROM index_quotes 
            WHERE code = ? AND date <= ? 
            ORDER BY date DESC LIMIT ?
        """
        df = pd.read_sql_query(query, conn, params=(code, end_date, days))
    else:
        query = """
            SELECT date, close, volume 
            FROM index_quotes 
            WHERE code = ? 
            ORDER BY date DESC LIMIT ?
        """
        df = pd.read_sql_query(query, conn, params=(code, days))
    df = df.sort_values("date").reset_index(drop=True)
    conn.close()
    return df

def load_technical():
    """加载技术指标，关联ETF名称"""
    conn = get_db_connection()
    query = """
        SELECT t.*, p.name 
        FROM etf_technical t 
        LEFT JOIN portfolio_snapshots p ON t.code = p.code AND t.date = p.date
        WHERE t.date = (SELECT MAX(date) FROM etf_technical)
    """
    df = pd.read_sql_query(query, conn)
    if not df.empty:
        df["name"] = df["name"].fillna(df["code"])
    conn.close()
    return df

def load_alerts(limit=10):
    """加载告警"""
    conn = get_db_connection()
    query = "SELECT * FROM alerts ORDER BY created_at DESC LIMIT ?"
    df = pd.read_sql_query(query, conn, params=(limit,))
    conn.close()
    return df

def load_execution_logs(limit=10):
    """加载执行日志"""
    conn = get_db_connection()
    query = "SELECT * FROM execution_logs ORDER BY created_at DESC LIMIT ?"
    df = pd.read_sql_query(query, conn, params=(limit,))
    conn.close()
    return df

def get_available_dates():
    """获取所有交易日日期"""
    conn = get_db_connection()
    query = "SELECT DISTINCT date FROM portfolio_snapshots ORDER BY date DESC"
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df["date"].tolist()

def load_calendar_data():
    """加载全部日历收益数据（年/月/日汇总）"""
    conn = get_db_connection()
    query = "SELECT date, daily_pnl, daily_return, total_value FROM portfolio_summary ORDER BY date"
    df = pd.read_sql_query(query, conn)
    conn.close()
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    # daily_return 在数据库中以百分比形式存储，改用 total_value.pct_change()
    df["daily_return"] = df["total_value"].pct_change()
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    df["day"] = df["date"].dt.day
    return df

def _cleanse_daily_returns(df, return_col="daily_return", threshold=5.0, max_tail=500):
    """清洗日收益率数据：过滤异常值 + 截断早期高波动区间

    Args:
        df: 包含 daily_return 列的 DataFrame
        return_col: 收益率列名
        threshold: 异常值阈值（%），默认5%（ETF单日正常波动上限）
        max_tail: 最大采样条数，默认500（约2个交易年），避免早期高波动区间污染

    Returns:
        (cleaned_df, stats) 元组
        stats = {'original': n, 'after_filter': n, 'after_tail': n, 'filtered': n, 'tailed': n}
    """
    original_count = len(df)

    # 步骤1: 过滤 |return| > threshold 的异常值
    mask = df[return_col].abs() <= threshold
    filtered_df = df[mask].copy()
    filtered_count = original_count - len(filtered_df)

    # 步骤2: 截断到最近 max_tail 条，排除早期高波动区间
    if len(filtered_df) > max_tail:
        tailed_df = filtered_df.tail(max_tail).copy()
        tailed_count = len(filtered_df) - len(tailed_df)
    else:
        tailed_df = filtered_df
        tailed_count = 0

    stats = {
        "original": original_count,
        "after_filter": len(filtered_df),
        "after_tail": len(tailed_df),
        "filtered": filtered_count,
        "tailed": tailed_count,
    }

    if filtered_count > 0 or tailed_count > 0:
        import logging

        logger = logging.getLogger(__name__)
        logger.info(
            f"日收益率清洗: {original_count}条 -> 过滤|ret|>{threshold}%: {filtered_count}条, "
            f"截断早期: {tailed_count}条, 剩余{len(tailed_df)}条"
        )

    return tailed_df, stats

def compute_extended_risk_metrics(end_date=None, min_date="2025-08-01"):
    """计算扩展风险指标（基于持仓稳定后的日收益率）
    
    Args:
        end_date: 截止日期，None表示最新
        min_date: 起始日期，默认2025-08-01（全部ETF覆盖日），
                  因为回填脚本用当前quantity×历史price，早期持仓少时
                  total_value极低导致风险指标严重失真
    """
    conn = get_db_connection()
    query = "SELECT date, daily_return, daily_pnl, total_value FROM portfolio_summary ORDER BY date"
    df = pd.read_sql_query(query, conn)
    conn.close()
    if df.empty or len(df) < 10:
        return {}
    df["date"] = pd.to_datetime(df["date"])
    if min_date:
        df = df[df["date"] >= pd.Timestamp(min_date)]
    if end_date:
        df = df[df["date"] <= pd.Timestamp(end_date)]
    if len(df) < 10:
        return {}

    # daily_return 在数据库中以百分比形式存储，改用 total_value.pct_change() 获取正确的小数日收益率
    returns = df["total_value"].pct_change().dropna()
    pnls = df["daily_pnl"]

    # Sortino Ratio (downside deviation)
    neg_returns = returns[returns < 0]
    downside_std = neg_returns.std() * np.sqrt(252) if len(neg_returns) > 1 else np.nan
    annual_return = returns.mean() * 252
    annual_std = returns.std() * np.sqrt(252)
    sortino = annual_return / downside_std if downside_std and downside_std > 0 else np.nan

    # Max Drawdown Duration (最大回撤持续时间)
    max_dd_duration = 0
    current_dd_duration = 0
    if "total_value" in df.columns:
        cummax = df["total_value"].cummax()
        in_drawdown = df["total_value"] < cummax
        for is_dd in in_drawdown:
            if is_dd:
                current_dd_duration += 1
                max_dd_duration = max(max_dd_duration, current_dd_duration)
            else:
                _current_dd_duration = 0

    # Calmar Ratio (annual return / max drawdown)
    cummax = df["total_value"].cummax() if "total_value" in df.columns else None
    if cummax is not None:
        dd = (df["total_value"] - cummax) / cummax * 100
        max_dd_abs = abs(dd.min())
        calmar = annual_return / max_dd_abs if max_dd_abs > 0 else np.nan
    else:
        calmar = np.nan

    # Win rate
    win_days = len(pnls[pnls > 0])
    total_days = len(pnls[pnls != 0])
    win_rate = win_days / total_days * 100 if total_days > 0 else np.nan

    # Profit/Loss ratio
    avg_win = pnls[pnls > 0].mean() if win_days > 0 else 0
    avg_loss = abs(pnls[pnls < 0].mean()) if len(pnls[pnls < 0]) > 0 else 1
    pl_ratio = avg_win / avg_loss if avg_loss > 0 else np.nan

    # Max consecutive win/loss days
    max_consec_win, max_consec_loss = 0, 0
    consec_win, consec_loss = 0, 0
    for p in pnls:
        if p > 0:
            consec_win += 1
            consec_loss = 0
            max_consec_win = max(max_consec_win, consec_win)
        elif p < 0:
            consec_loss += 1
            consec_win = 0
            max_consec_loss = max(max_consec_loss, consec_loss)
        else:
            consec_win, consec_loss = 0, 0

    # Skewness & Kurtosis
    skewness = returns.skew()
    kurtosis = returns.kurtosis()

    return {
        "sortino": sortino,
        "calmar": calmar,
        "win_rate": win_rate,
        "pl_ratio": pl_ratio,
        "max_consec_win": max_consec_win,
        "max_consec_loss": max_consec_loss,
        "max_dd_duration": max_dd_duration,
        "skewness": skewness,
        "kurtosis": kurtosis,
        "annual_return": annual_return,
        "annual_std": annual_std,
    }

def compute_monthly_returns():
    """计算月度收益率矩阵（年份 x 月份，含年度合计列和汇总行）"""
    conn = get_db_connection()
    query = "SELECT date, daily_return, total_value FROM portfolio_summary ORDER BY date"
    df = pd.read_sql_query(query, conn)
    conn.close()
    if df.empty:
        return pd.DataFrame()
    df["date"] = pd.to_datetime(df["date"])
    # daily_return 在数据库中以百分比形式存储，改用 total_value.pct_change()
    df["daily_return"] = df["total_value"].pct_change()
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    monthly = df.groupby(["year", "month"])["daily_return"].sum().reset_index()
    pivot = monthly.pivot(index="year", columns="month", values="daily_return")
    pivot.columns = [f"{m}月" for m in pivot.columns]
    # 年度合计列（各月收益率简单求和作为年度累计收益率）
    pivot["年累计"] = pivot.sum(axis=1)
    # 汇总行（各年份同月收益率均值，作为月均收益率参考）
    summary_row = pivot.mean(axis=0)
    summary_row.name = "月均"
    pivot = pd.concat([pivot, summary_row.to_frame().T])
    return pivot

def compute_rolling_metrics(window=60, end_date=None):
    """计算滚动夏普比率和滚动波动率（支持end_date过滤）"""
    conn = get_db_connection()
    query = "SELECT date, daily_return, total_value FROM portfolio_summary ORDER BY date"
    df = pd.read_sql_query(query, conn)
    conn.close()
    if df.empty or len(df) < window:
        return pd.DataFrame()
    df["date"] = pd.to_datetime(df["date"])
    if end_date:
        df = df[df["date"] <= pd.Timestamp(end_date)]
    if len(df) < window:
        return pd.DataFrame()
    # daily_return 在数据库中以百分比形式存储，改用 total_value.pct_change()
    ret = df["total_value"].pct_change()
    rolling_sharpe = ret.rolling(window).mean() / ret.rolling(window).std() * np.sqrt(252)
    rolling_vol = ret.rolling(window).std() * np.sqrt(252)
    result = pd.DataFrame({"date": df["date"], "rolling_sharpe": rolling_sharpe, "rolling_vol": rolling_vol}).dropna()
    return result

def load_correlation_matrix(days=CHART_DAYS["default"], end_date=None):
    """计算持仓ETF之间的皮尔逊相关系数矩阵（基于各ETF市值变动）"""
    conn = get_db_connection()
    if end_date:
        query = """
            SELECT date, code, market_value 
            FROM portfolio_snapshots 
            WHERE date <= ? 
            ORDER BY date DESC
        """
        df = pd.read_sql_query(query, conn, params=(end_date,))
    else:
        query = """
            SELECT date, code, market_value 
            FROM portfolio_snapshots 
            ORDER BY date DESC
        """
        df = pd.read_sql_query(query, conn)
    conn.close()
    if df.empty:
        return pd.DataFrame(), []

    # 取最近N个交易日
    dates = df["date"].unique()[:days]
    df = df[df["date"].isin(dates)]

    # 构建透视表：行=日期, 列=code, 值=market_value
    pivot = df.pivot_table(index="date", columns="code", values="market_value", aggfunc="first")

    # 只保留有足够数据的ETF（至少80%的交易日有数据）
    min_count = int(len(pivot) * 0.8)
    valid_cols = pivot.columns[pivot.notna().sum() >= min_count]
    pivot = pivot[valid_cols]

    if pivot.shape[1] < 2:
        return pd.DataFrame(), []

    # 计算日收益率
    returns = pivot.pct_change().dropna()

    # 计算相关系数矩阵
    corr = returns.corr()

    # 获取ETF名称
    conn = get_db_connection()
    names = {}
    for code in corr.columns:
        row = conn.execute(
            "SELECT name FROM portfolio_snapshots WHERE code = ? ORDER BY date DESC LIMIT 1", (code,)
        ).fetchone()
        names[code] = row[0] if row else code
    conn.close()

    # 简化名称（取前4个字 + "..."）
    short_names = {}
    for code, name in names.items():
        if len(name) > 6:
            short_names[code] = name[:6] + ".."
        else:
            short_names[code] = name

    return corr, short_names

def load_etf_detail(code, days=CHART_DAYS["short"], end_date=None):
    """加载单只ETF的快照数据和技术指标（含成本价、持仓量）"""
    conn = get_db_connection()

    # 从snapshots获取市值变化
    if end_date:
        query_snap = """
            SELECT date, current_price, market_value, quantity, cost_price,
                   pnl, pnl_rate, ytd_return, beta
            FROM portfolio_snapshots
            WHERE code = ? AND date <= ?
            ORDER BY date DESC LIMIT ?
        """
        df_snap = pd.read_sql_query(query_snap, conn, params=(code, end_date, days))
    else:
        query_snap = """
            SELECT date, current_price, market_value, quantity, cost_price,
                   pnl, pnl_rate, ytd_return, beta
            FROM portfolio_snapshots
            WHERE code = ?
            ORDER BY date DESC LIMIT ?
        """
        df_snap = pd.read_sql_query(query_snap, conn, params=(code, days))

    df_snap = df_snap.sort_values("date").reset_index(drop=True)

    # 从etf_technical获取技术指标
    if end_date:
        query_tech = """
            SELECT date, rsi_value, rsi_status, ma_signal, macd_signal, trend,
                   kdj_signal, bollinger_position, atr_pct
            FROM etf_technical
            WHERE code = ? AND date <= ?
            ORDER BY date DESC LIMIT ?
        """
        df_tech = pd.read_sql_query(query_tech, conn, params=(code, end_date, days))
    else:
        query_tech = """
            SELECT date, rsi_value, rsi_status, ma_signal, macd_signal, trend,
                   kdj_signal, bollinger_position, atr_pct
            FROM etf_technical
            WHERE code = ?
            ORDER BY date DESC LIMIT ?
        """
        df_tech = pd.read_sql_query(query_tech, conn, params=(code, days))

    df_tech = df_tech.sort_values("date").reset_index(drop=True)

    # 获取ETF名称
    name_row = conn.execute(
        "SELECT name FROM portfolio_snapshots WHERE code = ? ORDER BY date DESC LIMIT 1", (code,)
    ).fetchone()
    etf_name = name_row[0] if name_row else code

    conn.close()

    # 合并数据
    if not df_snap.empty and not df_tech.empty:
        df = pd.merge(df_snap, df_tech, on="date", how="outer")
        df = df.sort_values("date").reset_index(drop=True)
    elif not df_snap.empty:
        df = df_snap
    else:
        df = pd.DataFrame()

    return df, etf_name

def load_etf_price_history(code, days=CHART_DAYS["default"], end_date=None):
    """加载单只ETF的价格历史，用于绘制K线/走势图"""
    conn = get_db_connection()
    if end_date:
        query = """
            SELECT date, current_price as close, market_value, quantity
            FROM portfolio_snapshots
            WHERE code = ? AND date <= ?
            ORDER BY date DESC LIMIT ?
        """
        df = pd.read_sql_query(query, conn, params=(code, end_date, days))
    else:
        query = """
            SELECT date, current_price as close, market_value, quantity
            FROM portfolio_snapshots
            WHERE code = ?
            ORDER BY date DESC LIMIT ?
        """
        df = pd.read_sql_query(query, conn, params=(code, days))
    df = df.sort_values("date").reset_index(drop=True)
    conn.close()

    # 计算简单统计
    if not df.empty:
        df["returns"] = df["close"].pct_change()
        df["ma5"] = df["close"].rolling(5).mean()
        df["ma20"] = df["close"].rolling(20).mean()
        df["ma60"] = df["close"].rolling(60).mean()

    return df

def load_benchmark_comparison(code, days=CHART_DAYS["default"], end_date=None):
    """加载指定基准指数行情，用于净值曲线对比"""
    conn = get_db_connection()
    if end_date:
        query = """
            SELECT date, close 
            FROM index_quotes 
            WHERE code = ? AND date <= ? 
            ORDER BY date DESC LIMIT ?
        """
        df = pd.read_sql_query(query, conn, params=(code, end_date, days))
    else:
        query = """
            SELECT date, close 
            FROM index_quotes 
            WHERE code = ? 
            ORDER BY date DESC LIMIT ?
        """
        df = pd.read_sql_query(query, conn, params=(code, days))
    df = df.sort_values("date").reset_index(drop=True)
    conn.close()
    return df

def load_sector_weights(days=CHART_DAYS["default"], end_date=None):
    """加载按行业聚合的持仓权重历史（堆叠面积图数据源）"""
    query = """
        SELECT ps.date, ps.code, ps.market_value, ps.quantity, ps.current_price
        FROM portfolio_snapshots ps
        WHERE ps.date >= (
            SELECT DISTINCT date FROM portfolio_snapshots
            ORDER BY date DESC
            LIMIT 1 OFFSET ?
        )
    """
    if end_date:
        query += " AND ps.date <= ?"
    query += " ORDER BY ps.date, ps.code"

    try:
        conn = get_db_connection()
        params = [days]
        if end_date:
            params.append(end_date)
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
    except sqlite3.OperationalError as e:
        import logging

        logging.getLogger(__name__).warning(f"load_sector_weights 查询失败: {e}")
        return pd.DataFrame(), {}

    if df.empty:
        return pd.DataFrame(), {}

    # 按行业分类
    df["sector"] = df["code"].map(lambda c: ETF_CATEGORIES.get(c, {}).get("sector", "其他"))

    # 每日各行业总市值
    pivot = df.pivot_table(index="date", columns="sector", values="market_value", aggfunc="sum", fill_value=0)
    # 计算每日权重百分比
    daily_total = pivot.sum(axis=1)
    weight_df = pivot.div(daily_total, axis=0) * 100

    # 确定显示顺序（按最新日期的权重降序）
    if not weight_df.empty:
        latest = weight_df.iloc[-1].sort_values(ascending=False)
        weight_df = weight_df[latest.index]

    # 扇区颜色映射
    sector_color_map = {}
    for sector in weight_df.columns:
        sector_color_map[sector] = SECTOR_COLORS.get(sector, "#6b7280")

    return weight_df, sector_color_map

def run_monte_carlo(days=252, n_simulations=500, end_date=None):
    """蒙特卡洛模拟：基于历史日收益率分布，生成未来N日组合净值路径

    数据清洗：
    1. 移除 |daily_return| > 15% 的异常值（历史脏数据/数据迁移错误）
    2. 默认仅使用近2年数据采样，避免早期高波动数据污染预测
    3. 近期数据指数加权，更贴近当前市场状态

    Args:
        days: 模拟未来交易日天数
        n_simulations: 模拟路径数量
        end_date: 截止日期

    Returns:
        dict: {
            'paths': np.ndarray (n_simulations, days+1),
            'percentiles': DataFrame (date, p5, p25, p50, p75, p95),
            'last_value': float,
            'mean_return': float,
            'daily_std': float,
            'sample_count': int,      # 采样池大小
            'filtered_count': int,    # 过滤掉的异常值数
            'sample_start': str,      # 采样起始日期
        }
    """
    conn = get_db_connection()
    query = "SELECT date, daily_return, total_value FROM portfolio_summary ORDER BY date"
    df = pd.read_sql(query, conn)
    conn.close()

    if df.empty or len(df) < 30:
        return None

    if end_date:
        df = df[df["date"] <= end_date]

    # 获取最新市值
    conn2 = get_db_connection()
    query2 = "SELECT total_value FROM portfolio_summary WHERE date <= ? ORDER BY date DESC LIMIT 1"
    last_row = pd.read_sql(query2, conn2, params=(str(df["date"].max()),))
    conn2.close()

    if last_row.empty:
        return None

    last_value = float(last_row["total_value"].iloc[0])
    # daily_return 在数据库中以百分比形式存储，改用 total_value.pct_change()
    df["daily_return"] = df["total_value"].pct_change()
    returns = df["daily_return"].dropna()

    # ===== 数据清洗（统一使用 _cleanse_daily_returns）=====
    df_clean, clean_stats = _cleanse_daily_returns(
        df[["date", "daily_return"]], return_col="daily_return", threshold=5.0, max_tail=500
    )
    returns = df_clean["daily_return"]
    filtered_count = clean_stats["filtered"]

    sample_start = str(df_clean["date"].iloc[0])

    mean_ret = float(returns.mean())
    std_ret = float(returns.std())

    if std_ret <= 0:
        std_ret = 1e-8

    # ===== Bootstrap 采样（指数加权，近期数据权重更高） =====
    np.random.seed(42)
    hist_returns = returns.values

    # 指数加权：最近的数据权重最大，半年前的权重约为0.5
    n_hist = len(hist_returns)
    half_life = 126  # 半衰期约6个月(126个交易日)
    weights = np.array([2 ** (-i / half_life) for i in range(n_hist)])
    weights = weights[::-1]  # 最近的在末尾，权重最大
    weights /= weights.sum()  # 归一化

    paths = np.zeros((n_simulations, days + 1))
    paths[:, 0] = last_value

    for t in range(1, days + 1):
        # 加权 Bootstrap 采样
        indices = np.random.choice(n_hist, size=n_simulations, replace=True, p=weights)
        samples = hist_returns[indices]
        paths[:, t] = paths[:, t - 1] * (1 + samples / 100)

    # 计算百分位
    percentiles_data = {"day": list(range(days + 1))}
    for p in [5, 25, 50, 75, 95]:
        percentiles_data[f"p{p}"] = np.percentile(paths, p, axis=0)
    percentiles_df = pd.DataFrame(percentiles_data)

    return {
        "paths": paths,
        "percentiles": percentiles_df,
        "last_value": last_value,
        "mean_return": mean_ret,
        "daily_std": std_ret,
        "sample_count": len(returns),
        "filtered_count": filtered_count,
        "sample_start": sample_start,
    }

def compute_return_attribution(days=CHART_DAYS["default"], end_date=None):
    """Brinson 收益归因：将组合收益分解为行业配置效应和选股效应

    使用基准指数（沪深300）的行业权重作为参考基准。

    Returns:
        dict: {
            'total_return': float,       # 组合总收益率
            'benchmark_return': float,   # 基准总收益率
            'allocation_effect': dict,   # 行业配置效应 {sector: value}
            'selection_effect': dict,    # 选股效应 {sector: value}
            'sector_returns': dict,      # 各行业实际收益率
            'sector_weights': dict,      # 组合各行业权重
            'bench_weights': dict,       # 基准各行业权重（近似）
        }
    """
    conn = get_db_connection()

    # 获取组合持仓快照
    query_snap = """
        SELECT ps.date, ps.code, ps.market_value, ps.pnl_rate
        FROM portfolio_snapshots ps
        WHERE ps.date = (SELECT MAX(date) FROM portfolio_snapshots WHERE date <= :end)
        AND ps.market_value > 0
    """
    if end_date:
        df_snap = pd.read_sql(query_snap, conn, params={"end": end_date})
    else:
        df_snap = pd.read_sql(query_snap, conn, params={"end": "9999-12-31"})

    if df_snap.empty:
        conn.close()
        return None

    # 获取N天前快照
    query_prev = """
        SELECT ps.code, ps.market_value as prev_mv
        FROM portfolio_snapshots ps
        WHERE ps.date = (
            SELECT DISTINCT date FROM portfolio_snapshots 
            WHERE date <= :end 
            ORDER BY date DESC 
            LIMIT 1 OFFSET :skip
        )
        AND ps.market_value > 0
    """
    skip = days
    if end_date:
        df_prev = pd.read_sql(query_prev, conn, params={"end": end_date, "skip": skip})
    else:
        df_prev = pd.read_sql(query_prev, conn, params={"end": "9999-12-31", "skip": skip})

    conn.close()

    if df_prev.empty:
        return None

    # 行业分类
    def get_sector(code):
        clean = code.replace("sh", "").replace("sz", "")
        cat = ETF_CATEGORIES.get(clean, {})
        return cat.get("sector", "其他")

    # 当前快照按行业聚合
    df_snap["sector"] = df_snap["code"].apply(get_sector)
    total_mv = df_snap["market_value"].sum()
    sector_weights = {}
    for sector, grp in df_snap.groupby("sector"):
        sector_weights[sector] = float(grp["market_value"].sum() / total_mv)

    # 计算各行业收益率
    df_prev["sector"] = df_prev["code"].apply(get_sector)

    # 计算每只ETF的N日收益率
    current_mv = df_snap.set_index("code")["market_value"]
    prev_mv = df_prev.set_index("code")["prev_mv"]

    # 匹配代码
    common_codes = current_mv.index.intersection(prev_mv.index)
    if len(common_codes) == 0:
        return None

    etf_returns = current_mv[common_codes] / prev_mv[common_codes] - 1
    etf_returns_df = etf_returns.reset_index()
    etf_returns_df.columns = ["code", "return"]
    etf_returns_df["sector"] = etf_returns_df["code"].apply(get_sector)

    # 各行业加权收益率
    sector_returns = {}
    for sector, grp in etf_returns_df.groupby("sector"):
        sector_returns[sector] = float(grp["return"].mean())

    # 基准行业权重（近似：均匀分布，实际应用中应从指数成分获取）
    n_sectors = len(sector_weights)
    bench_weights = {s: 1.0 / max(n_sectors, 1) for s in sector_weights}

    # 组合总收益率
    total_return = float(df_snap["market_value"].sum() / df_prev["prev_mv"].sum() - 1)

    # 基准收益率
    conn3 = get_db_connection()
    query_bench = "SELECT close FROM index_quotes WHERE code='sh000300' ORDER BY date DESC LIMIT 1"
    query_bench_prev = "SELECT close FROM index_quotes WHERE code='sh000300' ORDER BY date DESC LIMIT 1 OFFSET ?"
    bench_now = pd.read_sql(query_bench, conn3)
    bench_prev = pd.read_sql(query_bench_prev, conn3, params=(days,))
    conn3.close()

    benchmark_return = 0.0
    if not bench_now.empty and not bench_prev.empty:
        benchmark_return = float(bench_now["close"].iloc[0] / bench_prev["close"].iloc[0] - 1)

    # Brinson 分解
    all_sectors = set(list(sector_weights.keys()) + list(bench_weights.keys()))
    allocation_effect = {}
    selection_effect = {}

    for s in all_sectors:
        w_p = sector_weights.get(s, 0)  # 组合权重
        w_b = bench_weights.get(s, 0)  # 基准权重
        r_p = sector_returns.get(s, 0)  # 行业组合收益
        r_b = sector_returns.get(s, 0)  # 行业基准收益（简化：使用同值）

        allocation_effect[s] = (w_p - w_b) * r_b
        selection_effect[s] = w_p * (r_p - r_b)

    return {
        "total_return": total_return,
        "benchmark_return": benchmark_return,
        "allocation_effect": allocation_effect,
        "selection_effect": selection_effect,
        "sector_returns": sector_returns,
        "sector_weights": sector_weights,
        "bench_weights": bench_weights,
    }

def compute_rebalance_suggestion(target_weights=None, threshold=0.05):
    """计算再平衡建议：基于目标权重与实际权重的偏离，生成调仓方案

    Args:
        target_weights: dict {sector: target_pct}，None则使用等权重
        threshold: 最小偏离阈值（百分比），低于此值不触发调仓

    Returns:
        dict or None
    """
    if target_weights is None:
        target_weights = {
            "医药": 0.15,
            "金融": 0.10,
            "军工": 0.10,
            "新能源": 0.15,
            "科技": 0.15,
            "宽基": 0.20,
            "红利": 0.10,
            "债券": 0.05,
        }

    conn = get_db_connection()
    query = """
        SELECT code, name, market_value, current_price, quantity, cost_price
        FROM portfolio_snapshots 
        WHERE date = (SELECT MAX(date) FROM portfolio_snapshots)
        AND market_value > 0
    """
    df = pd.read_sql(query, conn)
    conn.close()

    if df.empty:
        return None

    total_mv = df["market_value"].sum()

    def get_sector(code):
        clean = code.replace("sh", "").replace("sz", "")
        cat = ETF_CATEGORIES.get(clean, {})
        return cat.get("sector", "其他")

    df["sector"] = df["code"].apply(get_sector)

    # 当前行业权重
    current_weights = {}
    sector_etfs = {}
    for sector, grp in df.groupby("sector"):
        current_weights[sector] = float(grp["market_value"].sum() / total_mv)
        sector_etfs[sector] = grp

    # 计算偏离
    suggestions = []
    all_sectors = set(list(target_weights.keys()) + list(current_weights.keys()))

    for sector in all_sectors:
        target = target_weights.get(sector, 0)
        current = current_weights.get(sector, 0)
        diff = current - target  # 正值=超配，负值=低配

        if abs(diff) < threshold:
            continue

        # 调仓金额
        trade_value = -diff * total_mv  # 负diff(低配) => 正trade(买入)

        etfs = sector_etfs.get(sector)
        if etfs is None or etfs.empty:
            continue

        # 等权分配到该行业的各ETF
        n_etfs = len(etfs)
        per_etf_value = trade_value / n_etfs

        for _, etf in etfs.iterrows():
            if abs(per_etf_value) < 100:  # 忽略小额
                continue
            shares = int(per_etf_value / etf["current_price"]) if etf["current_price"] > 0 else 0
            if shares == 0:
                continue
            suggestions.append(
                {
                    "sector": sector,
                    "code": etf["code"],
                    "name": etf["name"],
                    "current_weight": current,
                    "target_weight": target,
                    "diff": diff,
                    "trade_value": per_etf_value,
                    "shares": shares,
                    "direction": "买入" if per_etf_value > 0 else "卖出",
                    "price": etf["current_price"],
                }
            )

    return {
        "current_weights": current_weights,
        "target_weights": target_weights,
        "suggestions": suggestions,
        "total_value": total_mv,
        "threshold": threshold,
    }

def _load_latest_news(_categories):
    """加载最新新闻（带缓存）"""
    conn = get_db_connection()
    try:
        placeholders = ",".join(["?" for _ in _categories])
        return pd.read_sql_query(
            f"SELECT date, category, title, source, url, summary, publish_time "
            f"FROM daily_news WHERE category IN ({placeholders}) "
            f"ORDER BY date DESC, publish_time DESC LIMIT 30",
            conn,
            params=list(_categories),
        )
    finally:
        conn.close()

def _load_tech_signals(_codes, _full=False):
    """加载技术指标信号（带缓存）"""
    if not _codes:
        return pd.DataFrame()
    conn = get_db_connection()
    try:
        ph = ",".join(["?" for _ in _codes])
        if _full:
            cols = "*"
        else:
            cols = "code, ma_signal, macd_signal, rsi_status, kdj_signal, bollinger_position, trend"
        return pd.read_sql_query(
            f"SELECT {cols} FROM etf_technical WHERE code IN ({ph}) ORDER BY date DESC", conn, params=list(_codes)
        )
    finally:
        conn.close()
