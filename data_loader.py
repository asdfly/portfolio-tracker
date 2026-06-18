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

from src.models import (
    MonteCarloResult,
    RebalanceSuggestion,
    RebalanceTrade,
    ReturnAttribution,
    RiskMetrics,
)

# ==================== 数据库初始化 ====================

from src.analysis.signal_score import compute_signal_score, compute_signal_scores
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

def load_technical(end_date=None):
    """加载技术指标，关联ETF名称"""
    conn = get_db_connection()
    date_filter = f"WHERE t.date = '{end_date}'" if end_date else "WHERE t.date = (SELECT MAX(date) FROM etf_technical)"
    query = f"""
        SELECT t.*, p.name
        FROM etf_technical t
        LEFT JOIN portfolio_snapshots p ON t.code = p.code AND t.date = p.date
        {date_filter}
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
    # daily_return 在数据库中以百分比形式存储，转为小数（已由 rebuild 校正持仓变化）
    df["daily_return"] = df["daily_return"] / 100
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
        return RiskMetrics.empty()
    df["date"] = pd.to_datetime(df["date"])
    if min_date:
        df = df[df["date"] >= pd.Timestamp(min_date)]
    if end_date:
        df = df[df["date"] <= pd.Timestamp(end_date)]
    if len(df) < 10:
        return RiskMetrics.empty()

    # daily_return 在数据库中以百分比形式存储，转为小数（已由 rebuild 校正持仓变化）
    returns = (df["daily_return"] / 100).dropna()
    pnls = df["daily_pnl"]

    # Sortino Ratio (downside deviation)
    neg_returns = returns[returns < 0]
    downside_std = neg_returns.std() * np.sqrt(252) if len(neg_returns) > 1 else np.nan
    annual_return = returns.mean() * 252
    annual_std = returns.std() * np.sqrt(252)
    sortino = (annual_return - 0.025) / downside_std if downside_std and downside_std > 0 else np.nan

    # Max Drawdown Duration（使用 corrected daily_return 累积净值，避免 total_value 跳变）
    max_dd_duration = 0
    current_dd_duration = 0
    cumret = (1 + returns).cumprod()
    peak = cumret.cummax()
    in_drawdown = cumret < peak
    for is_dd in in_drawdown:
        if is_dd:
            current_dd_duration += 1
            max_dd_duration = max(max_dd_duration, current_dd_duration)
        else:
            current_dd_duration = 0

    # Calmar Ratio（使用 corrected daily_return 累积净值计算回撤）
    dd_series = (cumret / peak - 1)
    max_dd_abs = abs(dd_series.min()) if len(dd_series) > 0 else np.nan
    calmar = annual_return / max_dd_abs if max_dd_abs and max_dd_abs > 0 else np.nan

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

    return RiskMetrics(
        sortino=sortino,
        calmar=calmar,
        win_rate=win_rate,
        pl_ratio=pl_ratio,
        max_consec_win=max_consec_win,
        max_consec_loss=max_consec_loss,
        max_dd_duration=max_dd_duration,
        skewness=skewness,
        kurtosis=kurtosis,
        annual_return=annual_return,
        annual_std=annual_std,
    )

def compute_monthly_returns():
    """计算月度收益率矩阵（年份 x 月份，含年度合计列和汇总行）"""
    conn = get_db_connection()
    query = "SELECT date, daily_return, total_value FROM portfolio_summary ORDER BY date"
    df = pd.read_sql_query(query, conn)
    conn.close()
    if df.empty:
        return pd.DataFrame()
    df["date"] = pd.to_datetime(df["date"])
    # daily_return 在数据库中以百分比形式存储，转为小数（已由 rebuild 校正持仓变化）
    df["daily_return"] = df["daily_return"] / 100
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    # 使用月首末日 total_value 计算正确的月度收益率
    monthly = df.groupby(["year", "month"]).agg(
        first_value=("total_value", "first"),
        last_value=("total_value", "last"),
    ).reset_index()
    monthly["monthly_return"] = monthly["last_value"] / monthly["first_value"] - 1
    pivot = monthly.pivot(index="year", columns="month", values="monthly_return")
    pivot.columns = [f"{m}月" for m in pivot.columns]
    # 年度合计列
    yearly = df.groupby("year").agg(
        first_value=("total_value", "first"), last_value=("total_value", "last")
    ).reset_index()
    yearly["yearly_return"] = yearly["last_value"] / yearly["first_value"] - 1
    pivot = pivot.merge(
        yearly[["year", "yearly_return"]].rename(columns={"yearly_return": "年累计"}),
        left_index=True, right_on="year", how="left",
    ).set_index("year")
    # 汇总行（各年份同月收益率均值，年累计为年均复合收益率）
    summary_row = pivot.drop(columns=["年累计"]).mean(axis=0)
    summary_row["年累计"] = (1 + pivot["年累计"]).prod() ** (1 / len(pivot)) - 1
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
    # daily_return 在数据库中以百分比形式存储，转为小数（已由 rebuild 校正持仓变化）
    ret = df["daily_return"] / 100
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
    # daily_return 在数据库中以百分比形式存储，转为小数（已由 rebuild 校正持仓变化）
    df["daily_return"] = df["daily_return"] / 100
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
        paths[:, t] = paths[:, t - 1] * (1 + samples)

    # 计算百分位
    percentiles_data = {"day": list(range(days + 1))}
    for p in [5, 25, 50, 75, 95]:
        percentiles_data[f"p{p}"] = np.percentile(paths, p, axis=0)
    percentiles_df = pd.DataFrame(percentiles_data)

    return MonteCarloResult(
        paths=paths,
        percentiles=percentiles_df,
        last_value=last_value,
        mean_return=mean_ret,
        daily_std=std_ret,
        sample_count=len(returns),
        filtered_count=filtered_count,
        sample_start=sample_start,
    )

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

    return ReturnAttribution(
        total_return=total_return,
        benchmark_return=benchmark_return,
        allocation_effect=allocation_effect,
        selection_effect=selection_effect,
        sector_returns=sector_returns,
        sector_weights=sector_weights,
        bench_weights=bench_weights,
    )

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
                RebalanceTrade(
                    sector=sector,
                    code=etf["code"],
                    name=etf["name"],
                    current_weight=current,
                    target_weight=target,
                    diff=diff,
                    trade_value=per_etf_value,
                    shares=shares,
                    direction="买入" if per_etf_value > 0 else "卖出",
                    price=etf["current_price"],
                )
            )

    return RebalanceSuggestion(
        current_weights=current_weights,
        target_weights=target_weights,
        suggestions=suggestions,
        total_value=total_mv,
        threshold=threshold,
    )

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


# ==================== ETF基本面数据加载 ====================

def load_etf_fundamental(date_str: str = None) -> pd.DataFrame:
    """加载指定日期的ETF基本面快照(行情+估值)。
    
    Args:
        date_str: 日期 YYYY-MM-DD, 默认最新
    Returns:
        DataFrame with code as index
    """
    conn = get_db_connection()
    try:
        if date_str is None:
            latest = pd.read_sql_query(
                "SELECT MAX(date) as d FROM etf_fundamental", conn)
            date_str = latest["d"].iloc[0] if not latest.empty and pd.notna(latest["d"].iloc[0]) else None
            if not date_str:
                return pd.DataFrame()
        return pd.read_sql_query(
            "SELECT * FROM etf_fundamental WHERE date=? ORDER BY code", 
            conn, params=[date_str])
    finally:
        conn.close()


def load_etf_industry_alloc(code: str) -> pd.DataFrame:
    """加载指定ETF的行业配置。
    
    Args:
        code: ETF代码
    Returns:
        DataFrame [industry, weight_pct, market_value]
    """
    conn = get_db_connection()
    try:
        return pd.read_sql_query(
            "SELECT industry, weight_pct, market_value FROM etf_industry_alloc "
            "WHERE code=? ORDER BY weight_pct DESC", conn, params=[code])
    finally:
        conn.close()


def load_etf_top_holdings(code: str, top_n: int = 10) -> pd.DataFrame:
    """加载指定ETF的前N大重仓股。
    
    Args:
        code: ETF代码
        top_n: 前N大持仓
    Returns:
        DataFrame [stock_code, stock_name, weight_pct, market_value]
    """
    conn = get_db_connection()
    try:
        return pd.read_sql_query(
            "SELECT stock_code, stock_name, weight_pct, market_value "
            "FROM etf_top_holdings WHERE code=? ORDER BY weight_pct DESC LIMIT ?",
            conn, params=[code, top_n])
    finally:
        conn.close()
    """加载所有 ETF 的最新技术信号评分。

    Returns
    -------
    pd.DataFrame : columns [code, total_score, grade, trend_score, momentum_score, ...]
    """
    conn = get_db_connection()
    try:
        df = pd.read_sql_query(
            "SELECT * FROM etf_technical WHERE date = (SELECT MAX(date) FROM etf_technical)",
            conn
        )
        if df.empty:
            return pd.DataFrame()
        return compute_signal_scores(df)
    except (sqlite3.OperationalError, pd.errors.DatabaseError) as e:
        logger.warning(f"DB error loading top holdings: {e}")
        return pd.DataFrame()
    except (KeyError, IndexError, ImportError) as e:
        logger.warning(f"Data format error in top holdings: {e}")
        return pd.DataFrame()
    finally:
        conn.close()


def load_peer_etfs(code):
    """加载与给定 ETF 同类（同指数或同行业）的 ETF 列表。

    Parameters
    ----------
    code : str
        ETF 代码

    Returns
    -------
    pd.DataFrame : 同类 ETF 的基本面数据（从 etf_fundamental 表）
    """
    conn = get_db_connection()
    try:
        # 1. 获取目标 ETF 的 index_code 或 sector
        target = pd.read_sql_query(
            "SELECT index_code, sector FROM etf_fundamental WHERE code = ? LIMIT 1",
            conn, params=(code,)
        )
        if target.empty:
            return pd.DataFrame()

        idx_code = target.iloc[0].get("index_code")
        sector = target.iloc[0].get("sector")

        # 2. 查找同类：相同 index_code（宽基/红利）或相同 sector（行业/主题）
        peers = pd.read_sql_query(
            "SELECT * FROM etf_fundamental WHERE "
            "(index_code = ? AND index_code IS NOT NULL) OR sector = ?",
            conn, params=(idx_code, sector)
        )
        return peers
    except (sqlite3.OperationalError, pd.errors.DatabaseError) as e:
        logger.warning(f"DB error loading peer ETFs: {e}")
        return pd.DataFrame()
    except (KeyError, IndexError) as e:
        logger.warning(f"Data format error in peer ETFs: {e}")
        return pd.DataFrame()
    finally:
        conn.close()
    """加载所有 ETF 最新技术信号评分。

    Returns
    -------
    pd.DataFrame : code, date, total_score, grade 及各维度评分
    """
    conn = get_db_connection()
    try:
        if date:
            df = pd.read_sql_query(
                "SELECT * FROM etf_technical WHERE date = ?", conn, params=(date,)
            )
        else:
            df = pd.read_sql_query(
                "SELECT * FROM etf_technical WHERE date = "
                "(SELECT MAX(date) FROM etf_technical)",
                conn
            )
        if df.empty:
            return pd.DataFrame()
        from src.analysis.signal_score import compute_signal_scores
        return compute_signal_scores(df)
    except (sqlite3.OperationalError, pd.errors.DatabaseError) as e:
        logger.warning(f"DB error computing signal scores: {e}")
        return pd.DataFrame()
    except (ImportError, KeyError, IndexError) as e:
        logger.warning(f"Compute error for signal scores: {e}")
        return pd.DataFrame()
    finally:
        conn.close()


# === P0-A: 同类 ETF 横向对比 ===
# 按跟踪指数或行业分类，将同一类别的 ETF 聚合对比
PEER_GROUPS = {
    "沪深300": ["510300", "159300"],
    "中证500": ["510500"],
    "中证1000": ["512100"],
    "创业板50": ["159949"],
    "科创50": ["588000"],
    "医药": ["512010", "159992", "515120"],
    "金融": ["515010"],
    "军工": ["512810", "159267"],
    "新能源": ["516160", "561910", "159796"],
    "科技/AI": ["159819", "159770", "159732"],
    "红利": ["159220", "563020"],
    "债券": ["511520", "159650", "511380"],
}


def load_peer_etfs(code):
    """加载与指定 ETF 同类（同指数/同行业）的其他 ETF 基本面数据。

    Parameters
    ----------
    code : str
        ETF 代码

    Returns
    -------
    tuple : (group_name, pd.DataFrame)
    """
    for group_name, codes in PEER_GROUPS.items():
        if code in codes:
            break
    else:
        return None, pd.DataFrame()

    conn = get_db_connection()
    try:
        placeholders = ",".join(["?"] * len(codes))
        df = pd.read_sql_query(
            "SELECT code, name, sector, price, iopv, discount_rate, change_pct, "
            "volume, amount, turnover_rate, volume_ratio, main_net_inflow, main_net_inflow_pct, "
            "super_large_net_inflow, shares, float_mv, total_mv "
            "FROM etf_fundamental WHERE code IN (%s)" % placeholders,
            conn, params=codes
        )
        return group_name, df
    except (sqlite3.OperationalError, pd.errors.DatabaseError) as e:
        logger.warning(f"DB error loading peer ETF group: {e}")
        return group_name, pd.DataFrame()
    except (KeyError, IndexError) as e:
        logger.warning(f"Data format error in peer ETF group: {e}")
        return group_name, pd.DataFrame()


# === P0-B: 技术信号综合评分 ===
def load_signal_score(code, end_date=None):
    """加载单只 ETF 最新技术信号评分。

    Parameters
    ----------
    code : str
        ETF 代码
    end_date : str, optional
        截止日期

    Returns
    -------
    dict or None : {total_score, grade, signals}
    """
    from src.analysis.signal_score import compute_signal_score
    conn = get_db_connection()
    try:
        if end_date:
            df = pd.read_sql_query(
                "SELECT * FROM etf_technical WHERE code = ? AND date <= ? ORDER BY date DESC LIMIT 1",
                conn, params=(code, str(end_date))
            )
        else:
            df = pd.read_sql_query(
                "SELECT * FROM etf_technical WHERE code = ? ORDER BY date DESC LIMIT 1",
                conn, params=(code,)
            )
        if df.empty:
            return None
        return compute_signal_score(df.iloc[0])
    except (sqlite3.OperationalError, pd.errors.DatabaseError) as e:
        logger.warning(f"DB error loading signal score for {{code}}: {e}")
        return None
    except (KeyError, IndexError, ImportError) as e:
        logger.warning(f"Compute error for signal score {{code}}: {e}")
        return None


def load_all_signal_scores(end_date=None):
    """加载所有 ETF 最新技术信号评分。

    Parameters
    ----------
    end_date : str, optional
        截止日期

    Returns
    -------
    pd.DataFrame
    """
    from src.analysis.signal_score import compute_signal_scores
    conn = get_db_connection()
    try:
        if end_date:
            df = pd.read_sql_query(
                "SELECT e.* FROM etf_technical e INNER JOIN "
                "(SELECT code, MAX(date) as dt FROM etf_technical WHERE date <= ? GROUP BY code) l "
                "ON e.code = l.code AND e.date = l.dt",
                conn, params=(str(end_date),)
            )
        else:
            df = pd.read_sql_query(
                "SELECT e.* FROM etf_technical e INNER JOIN "
                "(SELECT code, MAX(date) as dt FROM etf_technical GROUP BY code) l "
                "ON e.code = l.code AND e.date = l.dt",
                conn
            )
        return compute_signal_scores(df)
    except (sqlite3.OperationalError, pd.errors.DatabaseError) as e:
        logger.warning(f"DB error loading all signal scores: {e}")
        return pd.DataFrame()
    except (ImportError, KeyError) as e:
        logger.warning(f"Compute error for all signal scores: {e}")
        return pd.DataFrame()


# === P1-H: 单品风险全景扫描 ===
def load_etf_risk_scan(code):
    """加载单只 ETF 的风险全景评分。

    Parameters
    ----------
    code : str
        ETF 代码

    Returns
    -------
    dict or None : compute_etf_risk_scan 返回的评分字典
    """
    from src.analysis.etf_risk_scan import compute_etf_risk_scan
    conn = get_db_connection()
    try:
        tech_df = pd.read_sql_query(
            "SELECT * FROM etf_technical WHERE code = ? ORDER BY date DESC LIMIT 1",
            conn, params=(code,)
        )
        fund_df = pd.read_sql_query(
            "SELECT * FROM etf_fundamental WHERE code = ?", conn, params=(code,)
        )
        snap_df = pd.read_sql_query(
            "SELECT * FROM portfolio_snapshots WHERE code = ? ORDER BY date DESC",
            conn, params=(code,)
        )
        tech_row = tech_df.iloc[-1] if not tech_df.empty else None
        fund_row = fund_df.iloc[0] if not fund_df.empty else None
        hist_prices = snap_df["current_price"] if not snap_df.empty else None
        hist_snapshot = snap_df if not snap_df.empty else None
        return compute_etf_risk_scan(code, tech_row, fund_row, hist_prices, hist_snapshot)
    except (sqlite3.OperationalError, pd.errors.DatabaseError) as e:
        logger.warning(f"DB error loading risk scan for {code}: {e}")
        return None
    except (KeyError, IndexError, ImportError) as e:
        logger.warning(f"Compute error for risk scan {code}: {e}")
        return None
    finally:
        conn.close()


def load_all_etf_risk_scans():
    """加载所有 ETF 的风险全景评分。

    Returns
    -------
    pd.DataFrame : code, total_score, risk_level, grade, 各维度分数
    """
    from src.analysis.etf_risk_scan import compute_all_etf_risk_scans
    conn = get_db_connection()
    try:
        tech_df = pd.read_sql_query(
            "SELECT * FROM etf_technical WHERE date = (SELECT MAX(date) FROM etf_technical)",
            conn
        )
        fund_df = pd.read_sql_query("SELECT * FROM etf_fundamental", conn)
        snap_df = pd.read_sql_query("SELECT * FROM portfolio_snapshots", conn)
        return compute_all_etf_risk_scans(tech_df, fund_df, snap_df)
    except (sqlite3.OperationalError, pd.errors.DatabaseError) as e:
        logger.warning(f"DB error loading all risk scans: {e}")
        return pd.DataFrame()
    except (ImportError, KeyError) as e:
        logger.warning(f"Compute error for all risk scans: {e}")
        return pd.DataFrame()
    finally:
        conn.close()


# === P1-C: ETF 资金流向与异动监控 ===
def load_etf_fund_flow(code, days=60):
    """加载单只 ETF 的资金流向数据。

    Parameters
    ----------
    code : str
        ETF 代码
    days : int
        回溯天数，默认 60

    Returns
    -------
    pd.DataFrame : date, net_inflow, super/large/medium/small_inflow, net_inflow_pct
    """
    conn = get_db_connection()
    try:
        df = pd.read_sql_query(
            "SELECT date, code, name, net_inflow, net_inflow_pct, "
            "super_large_inflow, super_large_pct, large_inflow, large_pct, "
            "medium_inflow, medium_pct, small_inflow, small_pct "
            "FROM fund_flows WHERE code = ? AND category = 'etf' "
            "ORDER BY date DESC LIMIT ?",
            conn, params=(code, days)
        )
        return df.sort_values("date") if not df.empty else pd.DataFrame()
    except (sqlite3.OperationalError, pd.errors.DatabaseError) as e:
        logger.warning(f"DB error loading fund flow for {code}: {e}")
        return pd.DataFrame()
    except (KeyError, IndexError) as e:
        logger.warning(f"Data format error in fund flow {code}: {e}")
        return pd.DataFrame()
    finally:
        conn.close()


def load_etf_fund_flow_alerts(threshold_pct=200):
    """检测 ETF 资金异动（净流入环比变动超阈值）。

    Parameters
    ----------
    threshold_pct : float
        异动阈值（百分比），默认 200%

    Returns
    -------
    pd.DataFrame : code, name, date, net_inflow, prev_inflow, change_pct
    """
    conn = get_db_connection()
    try:
        # 获取最近两天的 ETF 资金流
        df = pd.read_sql_query(
            "SELECT f.* FROM fund_flows f "
            "INNER JOIN (SELECT code, MAX(date) as latest FROM fund_flows "
            "WHERE category='etf' GROUP BY code) l "
            "ON f.code = l.code AND f.date = l.latest "
            "WHERE f.category = 'etf'",
            conn
        )
        if df.empty:
            return pd.DataFrame()

        # 获取前一天数据
        alerts = []
        for _, row in df.iterrows():
            prev = pd.read_sql_query(
                "SELECT net_inflow FROM fund_flows WHERE code = ? AND category = 'etf' "
                "AND date < ? ORDER BY date DESC LIMIT 1",
                conn, params=(row["code"], row["date"])
            )
            if not prev.empty and prev.iloc[0]["net_inflow"] != 0:
                change_pct = (row["net_inflow"] - prev.iloc[0]["net_inflow"]) / abs(prev.iloc[0]["net_inflow"]) * 100
                if abs(change_pct) >= threshold_pct:
                    alerts.append({
                        "code": row["code"],
                        "name": row["name"],
                        "date": row["date"],
                        "net_inflow": row["net_inflow"],
                        "prev_inflow": prev.iloc[0]["net_inflow"],
                        "change_pct": round(change_pct, 1),
                    })
        return pd.DataFrame(alerts) if alerts else pd.DataFrame()
    except (sqlite3.OperationalError, pd.errors.DatabaseError) as e:
        logger.warning(f"DB error loading fund flow alerts: {e}")
        return pd.DataFrame()
    except (KeyError, IndexError, ZeroDivisionError) as e:
        logger.warning(f"Data format error in fund flow alerts: {e}")
        return pd.DataFrame()
    finally:
        conn.close()


# === P1-D: 交易历史复盘 ===
def load_trade_history(code=None, start_date=None, end_date=None):
    """加载交易记录。

    Parameters
    ----------
    code : str, optional
        ETF 代码
    start_date, end_date : str, optional
        日期范围

    Returns
    -------
    pd.DataFrame
    """
    from src.utils.trade_importer import load_trades
    return load_trades(code, start_date, end_date)


def load_trade_analysis(code):
    """加载单只 ETF 的交易复盘分析。

    Parameters
    ----------
    code : str
        ETF 代码

    Returns
    -------
    dict : 交易分析结果
    """
    from src.utils.trade_importer import compute_trade_analysis
    return compute_trade_analysis(code)


# === P1-E: 券商研报集成与行业观点聚合 ===
def load_etf_industry_news(code, days=30):
    """加载与 ETF 相关的行业新闻/观点。

    Parameters
    ----------
    code : str
        ETF 代码
    days : int
        回溯天数

    Returns
    -------
    pd.DataFrame : 匹配的新闻列表
    """
    from src.data_sources.research import load_etf_industry_news
    return load_etf_industry_news(code, days)


def load_sector_sentiment(code, days=30):
    """计算 ETF 对应行业的新闻情绪。

    Parameters
    ----------
    code : str
        ETF 代码
    days : int
        回溯天数

    Returns
    -------
    dict : {avg_sentiment, positive/negative/neutral_count, news_count, top_headlines}
    """
    from src.data_sources.research import load_sector_sentiment
    return load_sector_sentiment(code, days)


def load_pre_market_report():
    """生成盘前研判报告。

    Returns
    -------
    PreMarketReport
    """
    from src.analysis.pre_post_market import generate_pre_market_report
    conn = get_db_connection()
    try:
        return generate_pre_market_report(conn)
    finally:
        conn.close()


def load_post_market_report():
    """生成盘后复盘报告。

    Returns
    -------
    PostMarketReport
    """
    from src.analysis.pre_post_market import generate_post_market_report
    conn = get_db_connection()
    try:
        return generate_post_market_report(conn)
    finally:
        conn.close()


def load_multi_factor_scores(positions):
    """加载所有持仓ETF的多因子综合评分。

    Parameters
    ----------
    positions : pd.DataFrame - 持仓数据，需含code, name列

    Returns
    -------
    List[MultiFactorScore] - 按综合评分降序排列
    """
    from src.analysis.multi_factor_score import compute_multi_factor_scores
    return compute_multi_factor_scores(positions)


def load_position_advices(positions):
    """加载所有持仓ETF的仓位管理建议。

    Parameters
    ----------
    positions : pd.DataFrame - 持仓数据

    Returns
    -------
    List[PositionAdvice]
    """
    from src.analysis.position_advisor import compute_all_position_advice
    from src.analysis.multi_factor_score import compute_multi_factor_scores
    mf_scores = compute_multi_factor_scores(positions)
    return compute_all_position_advice(positions, mf_scores)


def load_sector_exposures(positions):
    """加载行业暴露度汇总。

    Returns
    -------
    List[SectorExposure]
    """
    from src.analysis.position_advisor import compute_sector_exposures
    return compute_sector_exposures(positions)


def load_pe_percentile(index_code, current_pe=None):
    """加载指数PE历史分位数。

    Returns
    -------
    dict : {percentile_3y, percentile_5y, percentile_all, pe_min, pe_max, pe_median}
    """
    from src.data_sources.valuation_percentile import load_pe_percentile as _lpp
    from src.utils.database import get_db_connection
    conn = get_db_connection()
    try:
        return _lpp(conn, index_code, current_pe)
    finally:
        conn.close()


def load_news_sentiment_for_positions(held_sectors, days=30):
    """加载持仓板块的新闻情绪分析。

    Parameters
    ----------
    held_sectors : list[str] — 持仓板块列表
    days : int — 回溯天数

    Returns
    -------
    PortfolioSentimentSummary
    """
    from src.analysis.news_sentiment import compute_portfolio_sentiment
    from src.utils.database import get_db_connection
    conn = get_db_connection()
    try:
        news_df = pd.read_sql_query(
            "SELECT category, title, sentiment_score, date FROM daily_news "
            "WHERE sentiment_score IS NOT NULL AND date >= date('now', ?) "
            "ORDER BY date DESC",
            conn, params=[f"-{days} days"]
        )
    except (sqlite3.OperationalError, pd.errors.DatabaseError) as e:
        logger.warning(f"DB error loading news sentiment: {e}")
        news_df = pd.DataFrame()
    finally:
        conn.close()
    return compute_portfolio_sentiment(news_df, held_sectors, trend_days=days)


def load_peer_penetration(code, name, sector):
    """加载同类ETF穿透对比数据。

    Parameters
    ----------
    code, name, sector : str

    Returns
    -------
    PeerPenetration
    """
    from src.analysis.peer_comparison import compute_peer_penetration
    from config.settings import ETF_CATEGORIES
    from src.utils.database import get_db_connection
    conn = get_db_connection()
    try:
        peer_codes = [c for c, info in ETF_CATEGORIES.items()
                      if info.get("sector") == sector]
        if not peer_codes:
            return None
        ph = ",".join(["?"] * len(peer_codes))
        fund_df = pd.read_sql_query(
            f"SELECT code, name, total_mv, discount_rate, turnover_rate, "
            f"volume_ratio, main_net_inflow, main_net_inflow_pct, price, iopv "
            f"FROM etf_fundamental WHERE code IN ({ph}) "
            f"ORDER BY date DESC",
            conn, params=peer_codes
        )
        fund_df = fund_df.drop_duplicates("code", keep="first")
        holdings_df = pd.read_sql_query(
            f"SELECT code, stock_code, stock_name, weight_pct FROM etf_top_holdings "
            f"WHERE code IN ({ph})",
            conn, params=peer_codes
        )
    except (sqlite3.OperationalError, pd.errors.DatabaseError) as e:
        logger.warning(f"DB error loading peer penetration: {e}")
        return None
    except (KeyError, IndexError) as e:
        logger.warning(f"Data format error in peer penetration: {e}")
        return None
    finally:
        conn.close()
    return compute_peer_penetration(code, name, fund_df, holdings_df)

def load_news_sentiment_for_positions(held_sectors, days=30):
    """Load news sentiment for portfolio sectors."""
    from src.analysis.news_sentiment import compute_portfolio_sentiment
    from src.utils.database import get_db_connection
    conn = get_db_connection()
    try:
        news_df = pd.read_sql_query(
            "SELECT category, title, sentiment_score, date FROM daily_news "
            "WHERE sentiment_score IS NOT NULL AND date >= date('now', ?) "
            "ORDER BY date DESC",
            conn, params=[f"-{days} days"]
        )
    except (sqlite3.OperationalError, pd.errors.DatabaseError) as e:
        logger.warning(f"DB error loading news sentiment: {e}")
        news_df = pd.DataFrame()
    finally:
        conn.close()
    return compute_portfolio_sentiment(news_df, held_sectors, trend_days=days)


def load_peer_penetration(code, name, sector):
    """Load peer ETF penetration comparison data."""
    from src.analysis.peer_comparison import compute_peer_penetration
    from config.settings import ETF_CATEGORIES
    from src.utils.database import get_db_connection
    conn = get_db_connection()
    try:
        peer_codes = [c for c, info in ETF_CATEGORIES.items() if info.get("sector") == sector]
        if not peer_codes:
            return None
        ph = ",".join(["?"] * len(peer_codes))
        fund_df = pd.read_sql_query(
            f"SELECT code, name, total_mv, discount_rate, turnover_rate, "
            f"volume_ratio, main_net_inflow, main_net_inflow_pct "
            f"FROM etf_fundamental WHERE code IN ({ph}) ORDER BY date DESC",
            conn, params=peer_codes
        )
        fund_df = fund_df.drop_duplicates("code", keep="first")
        holdings_df = pd.read_sql_query(
            f"SELECT code, stock_code, stock_name, weight_pct FROM etf_top_holdings "
            f"WHERE code IN ({ph})", conn, params=peer_codes
        )
    except (sqlite3.OperationalError, pd.errors.DatabaseError) as e:
        logger.warning(f"DB error loading peer penetration: {e}")
        return None
    except (KeyError, IndexError) as e:
        logger.warning(f"Data format error in peer penetration: {e}")
        return None
    finally:
        conn.close()
    return compute_peer_penetration(code, name, fund_df, holdings_df)

def load_peer_penetration(code, name, sector):
    """Load peer ETF penetration comparison data."""
    from src.analysis.peer_comparison import compute_peer_penetration
    from config.settings import ETF_CATEGORIES
    from src.utils.database import get_db_connection
    conn = get_db_connection()
    try:
        peer_codes = [c for c, info in ETF_CATEGORIES.items() if info.get('sector') == sector]
        if not peer_codes:
            return None
        ph = ",".join(["?"] * len(peer_codes))
        fund_df = pd.read_sql_query(
            f"SELECT code, name, total_mv, discount_rate, turnover_rate, "
            f"volume_ratio, main_net_inflow, main_net_inflow_pct "
            f"FROM etf_fundamental WHERE code IN ({ph}) ORDER BY date DESC",
            conn, params=peer_codes
        )
        fund_df = fund_df.drop_duplicates("code", keep="first")
        holdings_df = pd.read_sql_query(
            f"SELECT code, stock_code, stock_name, weight_pct FROM etf_top_holdings "
            f"WHERE code IN ({ph})", conn, params=peer_codes
        )
    except (sqlite3.OperationalError, pd.errors.DatabaseError) as e:
        logger.warning(f"DB error loading peer penetration: {e}")
        return None
    except (KeyError, IndexError) as e:
        logger.warning(f"Data format error in peer penetration: {e}")
        return None
    finally:
        conn.close()
    return compute_peer_penetration(code, name, fund_df, holdings_df)

# ============================================================
# P3: ERP股债性价比加载
# ============================================================

def load_erp_analysis(indices=None):
    """加载股债性价比(ERP)分析结果。

    Parameters
    ----------
    indices : list[str], optional - 指数代码列表

    Returns list[ERPResult]
    """
    try:
        from src.analysis.equity_risk_premium import compute_erp_multi
        return compute_erp_multi(indices)
    except ImportError as e:
        logger.warning(f"Import error in load_erp_analysis: {e}")
        return []
    except (ValueError, TypeError, KeyError) as e:
        logger.warning(f"Compute error in load_erp_analysis: {e}")
        return []


def load_erp_for_etf(etf_code):
    """根据ETF代码获取对应指数的ERP。

    Returns ERPResult or None
    """
    try:
        from src.analysis.equity_risk_premium import compute_erp_for_index
        from src.utils.database import get_db_connection
        # ETF to index mapping
        etf_to_index = {
            "510300": "sh000300", "159919": "sh000300",
            "510500": "sh000905", "510500": "sh000905",
            "159901": "sh000905", "510050": "sh000015",
            "588000": "sh000688", "159915": "sz399006",
            "510880": "sh000001",
        }
        index_code = etf_to_index.get(etf_code)
        if index_code is None:
            return None
        return compute_erp_for_index(index_code)
    except ImportError as e:
        logger.warning(f"Import error in load_erp_for_etf: {e}")
        return None
    except (ValueError, TypeError, KeyError) as e:
        logger.warning(f"Compute error in load_erp_for_etf: {e}")
        return None


# ============================================================
# P3: 定投回测加载
# ============================================================

def load_dca_backtest(etf_code, period_amount=1000, freq="W"):
    """加载ETF定投回测结果。

    Parameters
    ----------
    etf_code : str
    period_amount : float - 每期投入
    freq : str - "W"每周 "2W"每两周 "ME"每月

    Returns DCAResult or None
    """
    try:
        from src.analysis.dca_backtest import backtest_dca_uniform
        prices = load_etf_price_history(etf_code)
        if prices is None or prices.empty:
            return None
        return backtest_dca_uniform(prices, period_amount, freq)
    except ImportError as e:
        logger.warning(f"Import error in load_dca_backtest: {e}")
        return None
    except (ValueError, TypeError) as e:
        logger.warning(f"Compute error in load_dca_backtest: {e}")
        return None


# ============================================================
# P3: 行业景气度加载
# ============================================================

def load_industry_boom(etf_code):
    """加载持仓ETF的行业景气度分析。

    Returns IndustryBoomResult or None
    """
    try:
        from src.analysis.industry_boom import compute_boom_for_position
        return compute_boom_for_position(etf_code)
    except ImportError as e:
        logger.warning(f"Import error in load_industry_boom: {e}")
        return None
    except (ValueError, TypeError, KeyError) as e:
        logger.warning(f"Compute error in load_industry_boom: {e}")
        return None


def load_all_industry_booms(etf_codes=None):
    """批量加载行业景气度。

    Returns list[IndustryBoomResult]
    """
    try:
        from src.analysis.industry_boom import compute_boom_multi
        return compute_boom_multi(etf_codes)
    except ImportError as e:
        logger.warning(f"Import error in load_all_industry_booms: {e}")
        return []
    except (ValueError, TypeError) as e:
        logger.warning(f"Compute error in load_all_industry_booms: {e}")
        return []


# ============================================================
# P3: 智能预警加载
# ============================================================

def load_smart_alerts(etf_code, etf_name=""):
    """对单只ETF执行全维度预警扫描。

    Returns list[AlertEvent]
    """
    try:
        from src.analysis.smart_alert import scan_all_alerts
        from src.utils.database import get_db_connection
        import pandas as pd

        # 获取最新价格和行情数据
        conn = get_db_connection()
        try:
            price_df = pd.read_sql_query(
                "SELECT close, date FROM etf_daily WHERE code=? ORDER BY date DESC LIMIT 60",
                conn, params=[etf_code]
            )
        except (sqlite3.OperationalError, pd.errors.DatabaseError) as e:
            logger.warning(f"DB error loading price for smart alerts: {e}")
            price_df = pd.DataFrame()
        finally:
            conn.close()

        if price_df.empty or len(price_df) < 20:
            return []

        current_price = float(price_df.iloc[0]["close"])
        ma20 = float(price_df.tail(20)["close"].mean())
        ma60 = float(price_df.tail(60)["close"].mean()) if len(price_df) >= 60 else ma20
        prev_close = float(price_df.iloc[1]["close"]) if len(price_df) > 1 else current_price
        drop_pct = round((current_price - prev_close) / prev_close * 100, 2)

        # 资金流向
        fund_data = load_etf_fund_flow(etf_code)
        net_today = fund_data.get("net_today", 0) if isinstance(fund_data, dict) else 0
        net_5d = fund_data.get("net_5d", 0) if isinstance(fund_data, dict) else 0

        # 估值
        pe_data = load_pe_percentile(etf_code)
        pe_pct = pe_data.get("pe_percentile", 50) if isinstance(pe_data, dict) else 50

        # 波动率
        returns = price_df["close"].pct_change().dropna()
        current_vol = float(returns.tail(20).std() * 100 * np.sqrt(252)) if len(returns) >= 20 else 0
        avg_vol = float(returns.std() * 100 * np.sqrt(252)) if len(returns) > 0 else 0
        vol_std = float(returns.rolling(20).std().std() * 100 * np.sqrt(252)) if len(returns) > 20 else 0

        return scan_all_alerts(
            etf_code=etf_code, etf_name=etf_name,
            current_price=current_price, ma20=ma20, ma60=ma60,
            drop_pct=drop_pct,
            net_inflow_today=net_today, net_inflow_5d=net_5d,
            current_vol=current_vol, avg_vol=avg_vol, vol_std=vol_std,
            pe_percentile=pe_pct,
        )
    except (KeyError, IndexError, TypeError, ValueError, ZeroDivisionError) as e:
        logger.warning(f"Data error in load_smart_alerts: {e}")
        return []


def load_all_smart_alerts():
    """对所有持仓ETF执行预警扫描。

    Returns AlertSummary
    """
    try:
        from src.analysis.smart_alert import summarize_alerts
        positions = load_positions()
        all_events = []
        for _, row in positions.iterrows():
            code = str(row.get("code", ""))
            name = str(row.get("name", ""))
            events = load_smart_alerts(code, name)
            all_events.extend(events)
        return summarize_alerts(all_events)
    except ImportError as e:
        logger.warning(f"Import error in load_all_smart_alerts: {e}")
        from src.analysis.smart_alert import AlertSummary
        return AlertSummary()
    except (KeyError, IndexError, TypeError, ValueError) as e:
        logger.warning(f"Compute error in load_all_smart_alerts: {e}")
        from src.analysis.smart_alert import AlertSummary
        return AlertSummary()
