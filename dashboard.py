#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
投资组合跟踪分析系统 - Streamlit 可视化 Dashboard
启动方式: streamlit run dashboard.py

性能优化:
  - @st.cache_data 缓存所有数据库查询，相同参数命中缓存零延迟
  - 图表数据自动降采样，4000天数据压缩到<=500个点
  - SQLite 索引加速查询
"""

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

import base64
import calendar
import sqlite3
from datetime import datetime

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from config.settings import DATABASE_PATH, ETF_CATEGORIES, INDEX_CODES, SECTOR_COLORS


# ==================== 数据库索引 ====================
def _ensure_indexes():
    """确保数据库索引存在（只执行一次）"""
    conn = sqlite3.connect(str(DATABASE_PATH))
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
        except Exception:
            pass
    conn.commit()
    conn.close()


_ensure_indexes()

# ==================== 页面配置 ====================
st.set_page_config(page_title="投资组合跟踪分析", page_icon="📊", layout="wide", initial_sidebar_state="expanded")

# ==================== 降采样工具 ====================


# ==================== 图表辅助函数 ====================
def _add_min_max_annotations(fig, x_data, y_data, row=None, col=None, y_label=None, date_format="%m-%d"):
    """在时间轴图表中标记最大值和最小值的位置及数值。

    Args:
        fig: plotly 图表对象 (go.Figure 或 make_subplots 子图)
        x_data: x 轴数据序列 (日期)
        y_data: y 轴数据序列 (数值)
        row, col: 子图位置 (make_subplots 时使用)，默认 None 表示单图
        y_label: y 轴标签，用于标注文字前缀
        date_format: 日期格式化字符串
    """
    import numpy as np

    x_arr = np.array(x_data)
    y_arr = np.array(y_data, dtype=float)

    # 过滤 NaN
    valid = ~np.isnan(y_arr)
    x_arr, y_arr = x_arr[valid], y_arr[valid]

    if len(x_arr) < 2:
        return

    max_idx = np.argmax(y_arr)
    min_idx = np.argmin(y_arr)

    max_x, max_y = x_arr[max_idx], y_arr[max_idx]
    min_x, min_y = x_arr[min_idx], y_arr[min_idx]

    # 格式化日期
    if hasattr(max_x, "strftime"):
        max_date_str = max_x.strftime(date_format)
        min_date_str = min_x.strftime(date_format)
    else:
        _max_date_str = str(max_x)
        _min_date_str = str(min_x)

    # 格式化数值
    def fmt_val(v):
        if abs(v) >= 1000:
            return f"{v:,.0f}"
        elif abs(v) >= 1:
            return f"{v:.2f}"
        else:
            return f"{v:.4f}"

    max_text = f"Max {fmt_val(max_y)}"
    min_text = f"Min {fmt_val(min_y)}"

    # 添加散点标记
    scatter_kwargs = dict(
        mode="markers+text",
        hoverinfo="skip",
        showlegend=False,
    )

    if row is not None and col is not None:
        # make_subplots 子图
        fig.add_trace(
            go.Scatter(
                x=[max_x],
                y=[max_y],
                marker=dict(color="#22c55e", size=8, symbol="triangle-down"),
                text=[max_text],
                textposition="top center",
                textfont=dict(size=9, color="#22c55e"),
                **scatter_kwargs,
            ),
            row=row,
            col=col,
        )
        fig.add_trace(
            go.Scatter(
                x=[min_x],
                y=[min_y],
                marker=dict(color="#ef4444", size=8, symbol="triangle-up"),
                text=[min_text],
                textposition="bottom center",
                textfont=dict(size=9, color="#ef4444"),
                **scatter_kwargs,
            ),
            row=row,
            col=col,
        )
    else:
        # 单图
        fig.add_trace(
            go.Scatter(
                x=[max_x],
                y=[max_y],
                marker=dict(color="#22c55e", size=8, symbol="triangle-down"),
                text=[max_text],
                textposition="top center",
                textfont=dict(size=9, color="#22c55e"),
                **scatter_kwargs,
            )
        )
        fig.add_trace(
            go.Scatter(
                x=[min_x],
                y=[min_y],
                marker=dict(color="#ef4444", size=8, symbol="triangle-up"),
                text=[min_text],
                textposition="bottom center",
                textfont=dict(size=9, color="#ef4444"),
                **scatter_kwargs,
            )
        )


def downsample(df, date_col="date", max_points=500):
    """将时间序列降采样到max_points个点，保留边界值"""
    n = len(df)
    if n <= max_points:
        return df

    # 确保首尾在结果中
    step = max(1, (n - 2) // (max_points - 2))
    indices = list(range(0, n, step))
    if indices[-1] != n - 1:
        indices.append(n - 1)
    if indices[0] != 0:
        indices.insert(0, 0)

    # 去重排序
    indices = sorted(set(indices))
    return df.iloc[indices].reset_index(drop=True)


# ==================== 数据读取工具（带缓存） ====================
def get_db_connection():
    """获取数据库连接"""
    return sqlite3.connect(str(DATABASE_PATH), check_same_thread=False)


@st.cache_data(ttl=300, show_spinner=False)
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


@st.cache_data(ttl=300, show_spinner=False)
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


@st.cache_data(ttl=300, show_spinner=False)
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


@st.cache_data(ttl=300, show_spinner=False)
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


@st.cache_data(ttl=300, show_spinner=False)
def load_alerts(limit=10):
    """加载告警"""
    conn = get_db_connection()
    query = "SELECT * FROM alerts ORDER BY created_at DESC LIMIT ?"
    df = pd.read_sql_query(query, conn, params=(limit,))
    conn.close()
    return df


@st.cache_data(ttl=300, show_spinner=False)
def load_execution_logs(limit=10):
    """加载执行日志"""
    conn = get_db_connection()
    query = "SELECT * FROM execution_logs ORDER BY created_at DESC LIMIT ?"
    df = pd.read_sql_query(query, conn, params=(limit,))
    conn.close()
    return df


@st.cache_data(ttl=600, show_spinner=False)
def get_available_dates():
    """获取所有交易日日期"""
    conn = get_db_connection()
    query = "SELECT DISTINCT date FROM portfolio_snapshots ORDER BY date DESC"
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df["date"].tolist()


@st.cache_data(ttl=600, show_spinner=False)
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


@st.cache_data(ttl=600, show_spinner=False)
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


@st.cache_data(ttl=600, show_spinner=False)
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


@st.cache_data(ttl=600, show_spinner=False)
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


# ==================== P1: 持仓相关性矩阵 ====================
@st.cache_data(ttl=600, show_spinner=False)
def load_correlation_matrix(days=250, end_date=None):
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


# ==================== P1: 单只ETF详情数据 ====================
@st.cache_data(ttl=300, show_spinner=False)
def load_etf_detail(code, days=120, end_date=None):
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


def _render_etf_detail_panel(row, selected_date, total_value=0):
    """渲染ETF增强版详情面板：核心指标 + 价格走势 + 技术分析"""
    code = row["code"]
    name = row["name"]

    # 加载详细数据（命中缓存时零延迟）
    detail_df, etf_name = load_etf_detail(code, days=120, end_date=selected_date)
    price_df = load_etf_price_history(code, days=250, end_date=selected_date)

    # ===== 第一行：核心指标卡片（6列） =====
    mv = row.get("market_value", 0)
    pnl = row.get("pnl", 0)
    pnl_rate = row.get("pnl_rate", 0)
    cost = row.get("cost_price", 0)
    current = row.get("current_price", 0)
    _qty = row.get("quantity", 0)

    c1, c2, c3, c4, c5, c6 = st.columns(6)

    with c1:
        st.metric("市值", f"¥{mv:,.0f}")
    with c2:
        st.metric("累计盈亏", f"¥{pnl:,.0f}", delta=f"{pnl_rate:+.2f}%")
    with c3:
        if pd.notna(row.get("ytd_return")):
            yt = row["ytd_return"]
            st.metric("年内收益", f"{yt:+.2f}%")
        else:
            st.metric("年内收益", "--")
    with c4:
        if pd.notna(row.get("beta")):
            st.metric("Beta", f"{row['beta']:.2f}")
        else:
            st.metric("Beta", "--")
    with c5:
        cost_val = f"{cost:.3f}" if pd.notna(cost) else "--"
        st.metric("成本价", cost_val)
    with c6:
        price_diff = current - cost if pd.notna(cost) and pd.notna(current) else None
        delta_str = f"{price_diff:+.3f}" if price_diff is not None else None
        st.metric("现价", f"{current:.3f}" if pd.notna(current) else "--", delta=delta_str)

    # ===== 第二行：价格走势图 + 技术指标详情 =====
    if not price_df.empty:
        col_chart, col_tech = st.columns([3, 1])

        with col_chart:
            st.markdown(
                '<div class="tip-title" style="font-size:14px;border-bottom:none;padding:5px 0;">价格走势（近250日）<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">展示该ETF近250个交易日的收盘价走势，叠加MA5/MA10/MA20均线，并标注买入成本参考线。</span></div>',
                unsafe_allow_html=True,
            )
            df = price_df.sort_values("date").copy()

            # 降采样
            if len(df) > 500:
                step = max(1, len(df) // 500)
                df_plot = df.iloc[::step].copy()
            else:
                df_plot = df.copy()

            fig = go.Figure()
            fig.add_trace(
                go.Scatter(
                    x=df_plot["date"],
                    y=df_plot["close"],
                    mode="lines",
                    name="收盘价",
                    line=dict(color="#58a6ff", width=1.5),
                    fill="tozeroy",
                    fillcolor="rgba(88,166,255,0.05)",
                    hovertemplate="%{x|%m-%d}<br>价格: %{y:.3f}<extra></extra>",
                )
            )

            # 添加成本线
            if pd.notna(cost) and cost > 0:
                fig.add_hline(
                    y=cost,
                    line_dash="dash",
                    line_color="#f59e0b",
                    annotation_text=f"成本 {cost:.3f}",
                    annotation_position="top left",
                    annotation_font=dict(size=10, color="#f59e0b"),
                )

            # 标记最高价和最低价
            _add_min_max_annotations(fig, df_plot["date"], df_plot["close"], y_label="价格")

            fig.update_layout(
                height=220,
                plot_bgcolor="#0d1117",
                paper_bgcolor="#0d1117",
                font=dict(color="#c9d1d9", size=11),
                margin=dict(l=40, r=15, t=10, b=30),
                xaxis=dict(showgrid=False, tickformat="%m-%d", dtick="M1"),
                yaxis=dict(showgrid=True, gridcolor="#21262d", tickformat=".3f"),
                hovermode="x unified",
            )
            st.plotly_chart(fig, width="stretch")

        with col_tech:
            st.markdown(
                '<div class="tip-title" style="font-size:14px;border-bottom:none;padding:5px 0;">技术指标<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">展示该ETF的RSI、MACD、KDJ、布林带等常用技术指标信号。</span></div>',
                unsafe_allow_html=True,
            )
            if not detail_df.empty:
                latest = detail_df.iloc[-1]

                trend_map = {
                    "bullish": ("看多", "#22c55e"),
                    "bearish": ("看空", "#ef4444"),
                    "neutral": ("中性", "#f59e0b"),
                    None: ("--", "#888"),
                }
                trend_label, trend_color = trend_map.get(latest.get("trend"), ("--", "#888"))

                # 技术指标卡片
                indicators = [
                    ("趋势", trend_label, trend_color),
                    (
                        "RSI",
                        f"{latest.get('rsi_value', '--'):.1f}" if pd.notna(latest.get("rsi_value")) else "--",
                        (
                            "#22c55e"
                            if latest.get("rsi_status") in ("oversold",)
                            else "#ef4444" if latest.get("rsi_status") in ("overbought",) else "#c9d1d9"
                        ),
                    ),
                    ("MA信号", str(latest.get("ma_signal", "--")), "#c9d1d9"),
                    ("MACD", str(latest.get("macd_signal", "--")), "#c9d1d9"),
                    ("KDJ", str(latest.get("kdj_signal", "--")), "#c9d1d9"),
                    ("布林位置", str(latest.get("bollinger_position", "--")), "#c9d1d9"),
                    (
                        "ATR%",
                        f"{latest.get('atr_pct', '--'):.1f}%" if pd.notna(latest.get("atr_pct")) else "--",
                        "#c9d1d9",
                    ),
                ]

                for label, value, color in indicators:
                    st.markdown(
                        f'<div style="display:flex;justify-content:space-between;padding:4px 8px;'
                        f'border-bottom:1px solid #21262d;font-size:12px;">'
                        f'<span style="color:#8b949e;">{label}</span>'
                        f'<span style="color:{color};font-weight:bold;">{value}</span>'
                        f"</div>",
                        unsafe_allow_html=True,
                    )

                # RSI 仪表条
                rsi_val = latest.get("rsi_value", None)
                if pd.notna(rsi_val):
                    rsi_clamped = max(0, min(100, float(rsi_val)))
                    bar_color = "#ef4444" if rsi_clamped > 70 else "#22c55e" if rsi_clamped < 30 else "#f59e0b"
                    st.markdown(
                        f'<div style="margin-top:8px;font-size:11px;color:#8b949e;">RSI 位置</div>'
                        f'<div style="background:#21262d;border-radius:4px;height:8px;position:relative;">'
                        f'<div style="background:{bar_color};border-radius:4px;height:8px;width:{rsi_clamped}%;"></div>'
                        f'<div style="position:absolute;top:-2px;left:70%;width:1px;height:12px;background:#ef4444;opacity:0.5;"></div>'
                        f'<div style="position:absolute;top:-2px;left:30%;width:1px;height:12px;background:#22c55e;opacity:0.5;"></div>'
                        f"</div>"
                        f'<div style="display:flex;justify-content:space-between;font-size:9px;color:#484f58;">'
                        f"<span>超卖 30</span><span>中性</span><span>超买 70</span></div>",
                        unsafe_allow_html=True,
                    )
            else:
                st.info("暂无技术指标数据")

    # ===== 第三行：收益率分布 + 关键统计 =====
    if not detail_df.empty:
        col_stats, col_dist = st.columns([1, 2])

        with col_stats:
            st.markdown(
                '<div class="tip-title" style="font-size:14px;border-bottom:none;padding:5px 0;">关键统计<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">展示该ETF的日均收益、标准差、最大涨跌幅等关键统计指标。</span></div>',
                unsafe_allow_html=True,
            )
            df_detail = detail_df.sort_values("date")
            daily_returns = df_detail["current_price"].pct_change().dropna() if len(df_detail) > 1 else pd.Series()

            stats_items = []
            if len(daily_returns) > 0:
                stats_items.append(("日均收益", f"{daily_returns.mean()*100:+.3f}%"))
                stats_items.append(("日收益标准差", f"{daily_returns.std()*100:.3f}%"))
                stats_items.append(("最大单日涨幅", f"{daily_returns.max()*100:+.2f}%"))
                stats_items.append(("最大单日跌幅", f"{daily_returns.min()*100:+.2f}%"))
            stats_items.append(("数据天数", f"{len(df_detail)} 天"))
            stats_items.append(("持仓市值占比", f"{mv/total_value*100:.1f}%" if total_value > 0 else "--"))

            for label, value in stats_items:
                st.markdown(
                    f'<div style="display:flex;justify-content:space-between;padding:4px 8px;'
                    f'border-bottom:1px solid #21262d;font-size:12px;">'
                    f'<span style="color:#8b949e;">{label}</span>'
                    f'<span style="color:#c9d1d9;font-weight:bold;">{value}</span>'
                    f"</div>",
                    unsafe_allow_html=True,
                )

        with col_dist:
            if len(daily_returns) > 5:
                st.markdown(
                    '<div class="tip-title" style="font-size:14px;border-bottom:none;padding:5px 0;">日收益率分布<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">统计该ETF日收益率的频率分布，可判断收益的正态性和肥尾特征。</span></div>',
                    unsafe_allow_html=True,
                )
                fig_hist = go.Figure()
                colors = ["#22c55e" if v >= 0 else "#ef4444" for v in daily_returns]
                fig_hist.add_trace(
                    go.Histogram(
                        x=daily_returns * 100,
                        marker_color="#58a6ff",
                        nbinsx=30,
                        opacity=0.7,
                        hovertemplate="区间: %{x:.2f}%<br>次数: %{y}<extra></extra>",
                    )
                )
                # 标记零线
                fig_hist.add_vline(x=0, line_dash="dash", line_color="#f59e0b", line_width=1)
                fig_hist.update_layout(
                    height=180,
                    plot_bgcolor="#0d1117",
                    paper_bgcolor="#0d1117",
                    font=dict(color="#c9d1d9", size=11),
                    margin=dict(l=40, r=15, t=10, b=30),
                    xaxis=dict(title="日收益率 %", showgrid=False),
                    yaxis=dict(title="频次", showgrid=True, gridcolor="#21262d"),
                    bargap=0.05,
                )
                st.plotly_chart(fig_hist, width="stretch")


@st.cache_data(ttl=300, show_spinner=False)
def load_etf_price_history(code, days=250, end_date=None):
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


# ==================== P1: 多基准指数对比数据 ====================
@st.cache_data(ttl=600, show_spinner=False)
def load_benchmark_comparison(code, days=250, end_date=None):
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


# ==================== 样式工具 ====================


@st.cache_data(ttl=300, show_spinner=False)
def load_sector_weights(days=250, end_date=None):
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
        conn = sqlite3.connect(str(DATABASE_PATH))
        params = [days]
        if end_date:
            params.append(end_date)
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
    except Exception as e:
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


@st.cache_data(ttl=600, show_spinner=False)
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


@st.cache_data(ttl=600, show_spinner=False)
def compute_return_attribution(days=250, end_date=None):
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


@st.cache_data(ttl=600, show_spinner=False)
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


def export_positions_csv(positions_df, filename="持仓数据"):
    """导出持仓数据为CSV"""

    csv = positions_df.to_csv(index=False, encoding="utf-8-sig")
    b64 = base64.b64encode(csv.encode("utf-8-sig")).decode()
    href = f"data:text/csv;charset=utf-8-sig;base64,{b64}"
    return href, f"{filename}.csv"


def export_summary_csv(summary_df, filename="收益数据"):
    """导出收益数据为CSV"""
    csv = summary_df.to_csv(index=False, encoding="utf-8-sig")
    b64 = base64.b64encode(csv.encode("utf-8-sig")).decode()
    href = f"data:text/csv;charset=utf-8-sig;base64,{b64}"
    return href, f"{filename}.csv"


@st.cache_data(ttl=0, show_spinner=False)
def capture_dashboard_screenshot(port=8501):
    """截取 Dashboard 页面截图（PNG）

    通过 Selenium headless Chrome + webdriver_manager 自动管理 ChromeDriver。
    智能等待 Plotly 图表渲染完成后全页截图。

    Args:
        port: Streamlit 端口号

    Returns:
        str: PNG 文件路径，失败返回 None
    """
    try:
        import time

        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        from selenium.webdriver.common.by import By
        from webdriver_manager.chrome import ChromeDriverManager
    except ImportError:
        print("截图失败: 缺少 selenium 或 webdriver-manager，请执行 pip install selenium webdriver-manager")
        return None

    output_dir = PROJECT_ROOT / "output"
    output_dir.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    png_path = str(output_dir / f"dashboard_{timestamp}.png")

    try:
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--window-size=1920,3000")

        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.get(f"http://localhost:{port}")

        # Step 1: 等待 Streamlit App 容器就绪
        for i in range(30):
            try:
                el = driver.find_element(By.CSS_SELECTOR, "[data-testid='stApp']")
                if el.is_displayed():
                    break
            except Exception:
                pass
            time.sleep(1)

        # Step 2: 等待 Plotly 图表渲染（至少2个SVG出现）
        for i in range(45):
            try:
                charts = driver.find_elements(By.CSS_SELECTOR, ".js-plotly-plot .main-svg")
                if len(charts) >= 2:
                    time.sleep(2)  # 等待剩余图表
                    break
            except Exception:
                pass
            time.sleep(1)

        # Step 3: 滚动到底部触发懒加载，再滚回顶部
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(1)

        # Step 4: 截取完整页面
        driver.save_screenshot(png_path)
        driver.quit()
        return png_path
    except Exception as e:
        print(f"截图失败: {e}")
        return None


def export_dashboard_pdf(port=8501):
    """导出 Dashboard 为 PDF

    通过 Selenium headless Chrome + CDP Page.printToPDF 实现，A3 宽幅输出。
    智能等待 Plotly 图表渲染完成后导出。

    Args:
        port: Streamlit 端口号

    Returns:
        str: PDF 文件路径，失败返回 None
    """
    try:
        import base64
        import time

        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        from selenium.webdriver.common.by import By
        from webdriver_manager.chrome import ChromeDriverManager
    except ImportError:
        print("PDF导出失败: 缺少 selenium 或 webdriver-manager，请执行 pip install selenium webdriver-manager")
        return None

    output_dir = PROJECT_ROOT / "output"
    output_dir.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    pdf_path = str(output_dir / f"dashboard_{timestamp}.pdf")

    try:
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--window-size=1920,3000")

        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.get(f"http://localhost:{port}")

        # Step 1: 等待 Streamlit App 容器就绪
        for i in range(30):
            try:
                el = driver.find_element(By.CSS_SELECTOR, "[data-testid='stApp']")
                if el.is_displayed():
                    break
            except Exception:
                pass
            time.sleep(1)

        # Step 2: 等待 Plotly 图表渲染
        for i in range(45):
            try:
                charts = driver.find_elements(By.CSS_SELECTOR, ".js-plotly-plot .main-svg")
                if len(charts) >= 2:
                    time.sleep(2)
                    break
            except Exception:
                pass
            time.sleep(1)

        # Step 3: 滚动触发懒加载
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(1)

        # Step 4: CDP printToPDF (A3 宽幅)
        pdf_result = driver.execute_cdp_cmd(
            "Page.printToPDF",
            {
                "landscape": False,
                "displayHeaderFooter": False,
                "printBackground": True,
                "paperWidth": 13.0,
                "paperHeight": 19.0,
                "marginTop": 0.4,
                "marginBottom": 0.4,
                "marginLeft": 0.4,
                "marginRight": 0.4,
            },
        )

        pdf_bytes = base64.b64decode(pdf_result["data"])
        with open(pdf_path, "wb") as f:
            f.write(pdf_bytes)
        driver.quit()
        return pdf_path
    except Exception as e:
        print(f"PDF导出失败: {e}")
        return None


def format_value(val, prefix="", suffix="", decimals=2):
    """格式化数值"""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return "N/A"
    if isinstance(val, (int, float)):
        return f"{prefix}{val:,.{decimals}f}{suffix}"
    return str(val)


# ==================== 主页面 ====================



def get_indicator_color(value, thresholds, default="#888"):
    """通用阈值→颜色映射。

    Args:
        value: 数值（None/NaN 返回 default）
        thresholds: list of (upper_bound, color)，按优先级从高到低
        default: value 为 None 时的返回值

    Example:
        get_indicator_color(-12.5, [(10, "red"), (5, "yellow"), (0, "green")]) -> "red"
        get_indicator_color(None, [(10, "red"), (0, "green")]) -> "#888"
    """
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return default
    for upper, color in thresholds:
        if abs(value) > upper:
            return color
    return thresholds[-1][1]


def get_risk_color(risk_score):
    """风险评分→颜色。"""
    return "#22c55e" if risk_score >= 70 else "#f59e0b" if risk_score >= 40 else "#ef4444"


def get_risk_label(risk_score):
    """风险评分→中文标签。"""
    return "低风险" if risk_score >= 70 else "中等风险" if risk_score >= 40 else "高风险"

def compute_risk_score(volatility, max_dd, sharpe):
    """计算风险评分（0-100分）。满分=低风险。"""
    score = 100
    if volatility is not None and not np.isnan(volatility):
        if volatility > 30: score -= 30
        elif volatility > 20: score -= 15
        elif volatility > 15: score -= 5
    if max_dd is not None and not np.isnan(max_dd):
        if abs(max_dd) > 15: score -= 30
        elif abs(max_dd) > 10: score -= 20
        elif abs(max_dd) > 5: score -= 10
    if sharpe is not None and not np.isnan(sharpe):
        if sharpe < 0: score -= 20
        elif sharpe < 0.5: score -= 10
    return max(0, min(100, score))






def get_warnings(positions, max_dd, volatility, sharpe, profit_count, loss_count):
    """Generate risk warning list. Returns list of (icon, title, desc)."""
    warnings = []
    if not positions.empty:
        total_mv = positions["market_value"].sum()
        if total_mv > 0:
            max_pos = positions.loc[positions["market_value"].idxmax()]
            max_weight = max_pos["market_value"] / total_mv * 100
            if max_weight > 30:
                warnings.append(("🔴", "集中度风险", f'「{max_pos["name"]}」占比 {max_weight:.1f}%，超过30%阈值'))
            elif max_weight > 20:
                warnings.append(("🟡", "集中度风险", f'「{max_pos["name"]}」占比 {max_weight:.1f}%，接近30%警戒线'))
            top3_w = positions.nlargest(3, "market_value")["market_value"].sum() / total_mv
            if top3_w > 60:
                warnings.append(("🟡", "集中度风险", f"前3大持仓合计占比 {top3_w:.1f}%"))
        beta_avail = positions[positions["beta"].notna() & (positions["beta"] > 0)]
        if not beta_avail.empty:
            port_beta = (beta_avail["beta"] * beta_avail["market_value"]).sum() / beta_avail["market_value"].sum()
            if port_beta > 1.2:
                warnings.append(("🟡", "Beta风险", f"组合加权Beta为 {port_beta:.2f}，系统性风险偏高"))
            elif port_beta < 0.8:
                warnings.append(("🔵", "Beta风险", f"组合加权Beta为 {port_beta:.2f}，防御性较强"))
    if max_dd is not None and not np.isnan(max_dd):
        dd_pct = abs(max_dd)
        if dd_pct > 15:
            warnings.append(("🔴", "回撤风险", f"历史最大回撤 {dd_pct:.2f}%，超过15%警戒线"))
        elif dd_pct > 10:
            warnings.append(("🟡", "回撤风险", f"历史最大回撤 {dd_pct:.2f}%，较高水平"))
        elif dd_pct > 5:
            warnings.append(("🔵", "回撤风险", f"历史最大回撤 {dd_pct:.2f}%，正常波动"))
    if volatility is not None and not np.isnan(volatility):
        if volatility > 25:
            warnings.append(("🟡", "波动率风险", f"年化波动率 {volatility:.2f}%，波动较大"))
        elif volatility < 8:
            warnings.append(("🔵", "波动率风险", f"年化波动率 {volatility:.2f}%，波动较低"))
    if profit_count is not None and loss_count is not None and (profit_count + loss_count) > 0:
        wr = profit_count / (profit_count + loss_count) * 100
        if wr < 40:
            warnings.append(("🟡", "胜率偏低", f"当前胜率 {wr:.1f}%"))
        elif wr > 70:
            warnings.append(("🟢", "胜率优异", f"当前胜率 {wr:.1f}%"))
    if not positions.empty:
        loss_pos = positions[positions["pnl"] < 0]
        if not loss_pos.empty:
            max_loss = loss_pos.loc[loss_pos["pnl_rate"].idxmin()]
            if max_loss["pnl_rate"] < -15:
                warnings.append(("🔴", "个股预警", f'「{max_loss["name"]}」亏损 {max_loss["pnl_rate"]:.2f}%'))
            elif len(loss_pos) > len(positions) * 0.5:
                warnings.append(("🟡", "持仓预警", f"亏损标的 {len(loss_pos)} 只，占比 {len(loss_pos)/len(positions)*100:.0f}%"))
        total_pnl = positions["pnl"].sum()
        if total_pnl < 0:
            warnings.append(("🟡", "组合亏损", f"当前总盈亏 ¥{total_pnl:,.0f}"))
    return warnings


def compute_comprehensive_score(positions, summary, volatility, effective_max_dd, tech_df):
    """Compute comprehensive portfolio score (0-100) across 4 dimensions.
    
    Returns dict with keys:
        score_return, score_risk, tech_score, score_health,
        total_score, score_color, score_label, tech_signals
    """
    # 收益评分 (30分)
    port_daily = summary["total_value"].pct_change().dropna()
    total_ret = (
        (summary["total_value"].iloc[-1] / summary["total_value"].iloc[0] - 1)
        if summary["total_value"].iloc[0] > 0
        else 0
    )
    ann_ret = port_daily.mean() * 252 if len(port_daily) > 0 else 0
    if total_ret > 0.1:
        score_return = 30
    elif total_ret > 0.05:
        score_return = 24
    elif total_ret > 0:
        score_return = 18
    elif total_ret > -0.05:
        score_return = 10
    else:
        score_return = 5

    # 风险评分 (30分)
    score_risk = 15
    if volatility and not np.isnan(volatility):
        if volatility < 10:
            score_risk = 28
        elif volatility < 15:
            score_risk = 24
        elif volatility < 20:
            score_risk = 18
        elif volatility < 25:
            score_risk = 12
        else:
            score_risk = 6
    else:
        score_risk = 15

    if effective_max_dd and not np.isnan(effective_max_dd):
        dd = abs(effective_max_dd)
        if dd < 5:
            score_risk = min(score_risk + 2, 30)
        elif dd > 15:
            score_risk = max(score_risk - 5, 0)

    # 技术面评分 (25分)
    tech_score = 0
    tech_signals = []
    if not tech_df.empty:
        latest_tech = tech_df.drop_duplicates("code", keep="first")
        for _, tr in latest_tech.iterrows():
            etf_name = ETF_CATEGORIES.get(str(tr["code"]), {}).get("name", tr["code"])
            etf_score = 0
            if tr.get("ma_signal") == "多头排列":
                etf_score += 3
                tech_signals.append(f"{etf_name}: 均线多头排列")
            elif tr.get("ma_signal") == "空头排列":
                etf_score -= 1
            if tr.get("macd_signal") == "金叉":
                etf_score += 2
                tech_signals.append(f"{etf_name}: MACD金叉")
            elif tr.get("macd_signal") == "死叉":
                etf_score -= 1
            if tr.get("rsi_status") in ("超卖", "偏低"):
                etf_score += 1
            elif tr.get("rsi_status") in ("超买", "偏高"):
                etf_score -= 1
            if tr.get("trend") == "上涨":
                etf_score += 2
            elif tr.get("trend") == "下跌":
                etf_score -= 1
            tech_score += etf_score
        tech_score = max(0, min(25, 10 + tech_score))

    # 持仓健康度评分 (15分)
    score_health = 15
    total_mv = positions["market_value"].sum()
    max_weight = positions["market_value"].max() / total_mv if total_mv > 0 else 0
    if max_weight > 30:
        score_health -= 5
    elif max_weight > 20:
        score_health -= 2
    loss_ratio = len(positions[positions["pnl"] < 0]) / len(positions) if len(positions) > 0 else 0
    if loss_ratio > 0.6:
        score_health -= 5
    elif loss_ratio > 0.4:
        score_health -= 2
    score_health = max(0, score_health)

    total_score = score_return + score_risk + tech_score + score_health
    score_color = "#22c55e" if total_score >= 70 else "#f59e0b" if total_score >= 45 else "#ef4444"
    score_label = (
        "优秀"
        if total_score >= 70
        else "良好" if total_score >= 55 else "一般" if total_score >= 40 else "较差"
    )

    return {
        "score_return": score_return,
        "score_risk": score_risk,
        "tech_score": tech_score,
        "score_health": score_health,
        "total_score": total_score,
        "score_color": score_color,
        "score_label": score_label,
        "tech_signals": tech_signals,
    }

def _generate_oneclick_report(positions, summary, technical, selected_date, selected_benchmark):
    """生成综合分析报告 HTML"""
    import math

    if positions.empty or summary.empty:
        return None

    total_value = positions["market_value"].sum()
    total_cost = summary.iloc[-1].get("total_cost", 0)
    total_pnl = positions["pnl"].sum()
    total_return = (total_pnl / total_cost * 100) if total_cost > 0 else 0

    port_daily = summary["total_value"].pct_change().dropna()
    ann_ret = port_daily.mean() * 252 * 100 if len(port_daily) > 0 else 0
    ann_vol = port_daily.std() * math.sqrt(252) * 100 if len(port_daily) > 1 else 0
    sharpe = (port_daily.mean() / port_daily.std() * math.sqrt(252)) if port_daily.std() > 0 else 0
    cummax = summary["total_value"].cummax()
    max_dd = ((summary["total_value"] - cummax) / cummax * 100).min()

    pc = len(positions[positions["pnl"] > 0])
    lc = len(positions[positions["pnl"] < 0])
    wr = (pc / (pc + lc) * 100) if (pc + lc) > 0 else 0

    pnl_color = "#22c55e" if total_pnl >= 0 else "#ef4444"
    ret_color = "#22c55e" if total_return >= 0 else "#ef4444"

    # 持仓明细表
    pos_rows = ""
    for _, pos in positions.iterrows():
        p_color = "#22c55e" if pos["pnl"] >= 0 else "#ef4444"
        pos_rows += (
            f'<tr style="border-bottom:1px solid #eee;">'
            f'<td style="padding:6px 8px;">{pos["name"]}</td>'
            f'<td style="padding:6px 8px;">{pos["code"]}</td>'
            f'<td style="padding:6px 8px;text-align:right;">{pos["quantity"]:,.0f}</td>'
            f'<td style="padding:6px 8px;text-align:right;">{pos["cost_price"]:.3f}</td>'
            f'<td style="padding:6px 8px;text-align:right;">{pos["current_price"]:.3f}</td>'
            f'<td style="padding:6px 8px;text-align:right;">¥{pos["market_value"]:,.0f}</td>'
            f'<td style="padding:6px 8px;text-align:right;color:{p_color};">¥{pos["pnl"]:,.0f}</td>'
            f'<td style="padding:6px 8px;text-align:right;color:{p_color};">{pos["pnl_rate"]:+.2f}%</td>'
            f"</tr>"
        )

    # 技术信号摘要
    tech_rows = ""
    if technical is not None and not technical.empty:
        tech_latest = technical.drop_duplicates("code", keep="first")
        for _, tr in tech_latest.iterrows():
            name = tr.get("name", tr["code"])
            trend = tr.get("trend", "--")
            ma = tr.get("ma_signal", "--")
            macd = tr.get("macd_signal", "--")
            rsi_st = tr.get("rsi_status", "--")
            tech_rows += (
                f'<tr style="border-bottom:1px solid #eee;">'
                f'<td style="padding:5px 8px;">{name}</td>'
                f'<td style="padding:5px 8px;">{trend}</td>'
                f'<td style="padding:5px 8px;">{ma}</td>'
                f'<td style="padding:5px 8px;">{macd}</td>'
                f'<td style="padding:5px 8px;">{rsi_st}</td>'
                f'<td style="padding:5px 8px;">{tr.get("rsi_value", "--"):.1f}</td>'
                f"</tr>"
            )

    bench_name = INDEX_CODES.get(selected_benchmark, selected_benchmark)

    html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>投资组合分析报告 {selected_date}</title>
<style>
body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; max-width: 960px; margin: 0 auto; padding: 20px; color: #333; }}
h1 {{ font-size: 22px; border-bottom: 2px solid #4a90d9; padding-bottom: 8px; }}
h2 {{ font-size: 16px; color: #4a90d9; margin-top: 24px; }}
.meta {{ font-size: 12px; color: #888; margin-bottom: 20px; }}
.metrics {{ display: flex; gap: 16px; flex-wrap: wrap; margin: 12px 0; }}
.metric-card {{ background: #f8f9fa; border-radius: 8px; padding: 12px 16px; min-width: 140px; }}
.metric-label {{ font-size: 11px; color: #888; }}
.metric-value {{ font-size: 20px; font-weight: bold; }}
table {{ width: 100%; border-collapse: collapse; font-size: 12px; margin: 8px 0; }}
th {{ background: #f0f2f5; padding: 6px 8px; text-align: left; font-size: 11px; color: #666; }}
td {{ padding: 5px 8px; }}
.section {{ margin: 16px 0; padding: 12px; background: #fafbfc; border-radius: 6px; border-left: 3px solid #4a90d9; }}
.footer {{ font-size: 11px; color: #aaa; text-align: center; margin-top: 30px; border-top: 1px solid #eee; padding-top: 12px; }}
</style></head><body>
<h1>📊 投资组合分析报告</h1>
<div class="meta">生成时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} | 数据截至: {selected_date} | 基准: {bench_name}</div>

<h2>一、组合概览</h2>
<div class="metrics">
  <div class="metric-card"><div class="metric-label">总市值</div><div class="metric-value">¥{total_value:,.0f}</div></div>
  <div class="metric-card"><div class="metric-label">总盈亏</div><div class="metric-value" style="color:{pnl_color};">¥{total_pnl:,.0f}</div></div>
  <div class="metric-card"><div class="metric-label">总收益率</div><div class="metric-value" style="color:{ret_color};">{total_return:+.2f}%</div></div>
  <div class="metric-card"><div class="metric-label">年化收益率</div><div class="metric-value">{ann_ret:+.2f}%</div></div>
  <div class="metric-card"><div class="metric-label">夏普比率</div><div class="metric-value">{sharpe:.3f}</div></div>
  <div class="metric-card"><div class="metric-label">最大回撤</div><div class="metric-value" style="color:#ef4444;">{max_dd:.2f}%</div></div>
  <div class="metric-card"><div class="metric-label">年化波动率</div><div class="metric-value">{ann_vol:.2f}%</div></div>
  <div class="metric-card"><div class="metric-label">胜率</div><div class="metric-value">{wr:.1f}% ({pc}盈/{lc}亏)</div></div>
</div>

<h2>二、持仓明细</h2>
<table><thead><tr>
<th>名称</th><th>代码</th><th style="text-align:right;">持仓量</th>
<th style="text-align:right;">成本价</th><th style="text-align:right;">现价</th>
<th style="text-align:right;">市值</th><th style="text-align:right;">盈亏</th>
<th style="text-align:right;">收益率</th>
</tr></thead><tbody>{pos_rows}</tbody></table>

<h2>三、技术信号</h2>
{"<table><thead><tr><th>ETF</th><th>趋势</th><th>均线</th><th>MACD</th><th>RSI状态</th><th>RSI值</th></tr></thead><tbody>" + tech_rows + "</tbody></table>" if tech_rows else "<p style='color:#888;'>暂无技术信号数据</p>"}

<h2>四、风险提示</h2>
<div class="section">
<ul style="font-size:13px;line-height:1.8;">
<li>最大回撤 <b>{max_dd:.2f}%</b>，{'超过15%警戒线，需注意控制下行风险' if abs(max_dd) > 15 else '处于正常波动范围'}</li>
<li>年化波动率 <b>{ann_vol:.2f}%</b>，{'波动较大，注意风险管理' if ann_vol > 25 else '处于合理水平'}</li>
<li>胜率 <b>{wr:.1f}%</b>，{'持仓中大部分标的处于盈利状态' if wr > 60 else '盈利标的占比较低，需关注'}</li>
</ul></div>

<div class="footer">投资组合跟踪分析系统 v2.0 | 本报告仅供参考，不构成投资建议</div>
</body></html>"""
    return html


@st.cache_data(ttl=600, show_spinner=False)
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


@st.cache_data(ttl=300, show_spinner=False)
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


def _render_overview(positions, summary, technical, effective_max_dd):
    """概览指标区：卡片行 + 快速指标条"""
    _render_overview(positions, summary, technical, effective_max_dd)
    latest_summary = summary.iloc[-1] if not summary.empty else {}
    total_value = latest_summary.get("total_value", 0)
    total_cost = latest_summary.get("total_cost", 0)
    total_pnl = latest_summary.get("total_pnl", 0)
    total_return = (total_pnl / total_cost * 100) if total_cost > 0 else 0
    daily_return = latest_summary.get("daily_return", 0)
    daily_pnl = latest_summary.get("daily_pnl", 0)
    sharpe = latest_summary.get("sharpe_ratio")
    max_dd = latest_summary.get("max_drawdown")
    # early computation of effective_max_dd for use in overview cards (before tab3)
    _early_ext = compute_extended_risk_metrics(end_date=selected_date)
    effective_max_dd = _early_ext.get("max_drawdown", max_dd)
    volatility = latest_summary.get("volatility")
    profit_count = latest_summary.get("profit_count", 0)
    loss_count = latest_summary.get("loss_count", 0)

    # 概览卡片行
    cols = st.columns(6)
    with cols[0]:
        st.markdown(
            f'<div style="padding:10px;border-radius:8px;background:#161b22;border-left:3px solid #58a6ff;">'
            f'<div style="font-size:11px;color:#8b949e;cursor:help;border-bottom:1px dotted #8b949e;display:inline;" title="当前所有持仓证券的市值总和">总市值 ℹ</div>'
            f'<div style="font-size:20px;font-weight:bold;color:#58a6ff;">¥{format_value(total_value)}</div>'
            f"</div>",
            unsafe_allow_html=True,
        )
    with cols[1]:
        pnl_color = "#22c55e" if total_pnl >= 0 else "#ef4444"
        st.markdown(
            f'<div style="padding:10px;border-radius:8px;background:#161b22;border-left:3px solid {pnl_color};">'
            f'<div style="font-size:11px;color:#8b949e;cursor:help;border-bottom:1px dotted #8b949e;display:inline;" title="所有持仓的累计盈亏金额和收益率，基于买入成本计算">总盈亏 ℹ</div>'
            f'<div style="font-size:20px;font-weight:bold;color:{pnl_color};">{format_value(total_pnl, prefix="¥")}</div>'
            f'<div style="font-size:11px;color:#8b949e;">{format_value(total_return, suffix="%")}</div>'
            f"</div>",
            unsafe_allow_html=True,
        )
    with cols[2]:
        dr_color = get_indicator_color(daily_return, [(0, "#ef4444"), (-1e-9, "#22c55e")], default="#888")
        st.markdown(
            f'<div style="padding:10px;border-radius:8px;background:#161b22;border-left:3px solid {dr_color};">'
            f'<div style="font-size:11px;color:#8b949e;cursor:help;border-bottom:1px dotted #8b949e;display:inline;" title="选定日期相对于前一交易日的收益率(%)和盈亏金额(元)">日收益 ℹ</div>'
            f'<div style="font-size:20px;font-weight:bold;color:{dr_color};">{format_value(daily_return, suffix="%")}</div>'
            f'<div style="font-size:11px;color:#8b949e;">{format_value(daily_pnl, prefix="¥")}</div>'
            f"</div>",
            unsafe_allow_html=True,
        )
    with cols[3]:
        sharpe_color = "#22c55e" if (sharpe and sharpe > 0.5) else "#f59e0b" if sharpe else "#888"  # get_indicator_color不适合此三元逻辑，保留
        st.markdown(
            f'<div style="padding:10px;border-radius:8px;background:#161b22;border-left:3px solid {sharpe_color};">'
            f'<div style="font-size:11px;color:#8b949e;cursor:help;border-bottom:1px dotted #8b949e;display:inline;" title="风险调整后收益指标 = (年化收益率 - 无风险利率) / 年化波动率。>1为优秀，>0.5为良好">夏普比率 ℹ</div>'
            f'<div style="font-size:20px;font-weight:bold;color:{sharpe_color};">{format_value(sharpe, decimals=3)}</div>'
            f"</div>",
            unsafe_allow_html=True,
        )
    with cols[4]:
        dd_color = get_indicator_color(effective_max_dd, [(10, "#ef4444"), (5, "#f59e0b"), (0, "#22c55e")])
        st.markdown(
            f'<div style="padding:10px;border-radius:8px;background:#161b22;border-left:3px solid {dd_color};">'
            f'<div style="font-size:11px;color:#8b949e;cursor:help;border-bottom:1px dotted #8b949e;display:inline;" title="选定时间段内，组合从历史最高点到最低点的最大跌幅(%)">最大回撤 ℹ</div>'
            f'<div style="font-size:20px;font-weight:bold;color:{dd_color};">{format_value(effective_max_dd, suffix="%")}</div>'
            f"</div>",
            unsafe_allow_html=True,
        )
    with cols[5]:
        vol_color = get_indicator_color(volatility, [(25, "#ef4444"), (15, "#f59e0b"), (0, "#22c55e")])
        st.markdown(
            f'<div style="padding:10px;border-radius:8px;background:#161b22;border-left:3px solid {vol_color};">'
            f'<div style="font-size:11px;color:#8b949e;cursor:help;border-bottom:1px dotted #8b949e;display:inline;" title="日收益率标准差的年化值，反映组合收益的波动幅度。值越高表示风险越大">年化波动率 ℹ</div>'
            f'<div style="font-size:20px;font-weight:bold;color:{vol_color};">{format_value(volatility, suffix="%")}</div>'
            f"</div>",
            unsafe_allow_html=True,
        )


def _render_tab1_body(tab1, positions, summary, selected_date, show_days, selected_benchmark, rolling_data, effective_max_dd, technical=None, volatility=None, max_dd=None, sharpe=None):
    """Tab1: 净值走势（委托 tabs/tab1_net_value）"""
    from tabs.tab1_net_value import render_tab1
    with tab1:
        render_tab1(
            positions, summary, None,
            selected_date=selected_date,
            selected_benchmark=selected_benchmark,
            show_days=show_days,
            technical=technical,
            volatility=volatility,
            max_dd=max_dd,
            sharpe=sharpe,
        )


def _render_tab2_position(tab2, positions, summary, selected_date, selected_benchmark=None, technical=None, volatility=None, max_dd=None, sharpe=None):
    """Tab2: 持仓分布（委托 tabs/tab2_position）"""
    from tabs.tab2_position import render_tab2
    with tab2:
        render_tab2(
            positions, summary, None,
            selected_date=selected_date,
            selected_benchmark=selected_benchmark,
            technical=technical,
            volatility=volatility,
            max_dd=max_dd,
            sharpe=sharpe,
        )


def _render_tab3_risk(tab3, positions, summary, technical, selected_date, selected_benchmark=None, ext_risk=None, volatility=None, max_dd=None, sharpe=None):
    """Tab3: 风险分析（委托 tabs/tab3_risk）"""
    from tabs.tab3_risk import render_tab3
    with tab3:
        render_tab3(
            positions, summary, None,
            selected_date=selected_date,
            selected_benchmark=selected_benchmark,
            technical=technical,
            volatility=volatility,
            max_dd=max_dd,
            sharpe=sharpe,
        )


def _render_tab4_calendar(tab4, positions, summary, selected_date=None, selected_benchmark=None):
    """Tab4: 收益日历"""
    from tabs.tab4_calendar import render_tab4
    with tab4:
        render_tab4(positions, summary, index_quotes=None, selected_date=selected_date, selected_benchmark=selected_benchmark)
        cal_data = load_calendar_data()
def _render_tab6_technical(tab6, technical, selected_date=None, selected_benchmark=None, positions=None, summary=None):
    """Tab6: 技术信号"""
    from tabs.tab6_technical import render_tab6
    with tab6:
        render_tab6(positions, summary, index_quotes=None, selected_date=selected_date, selected_benchmark=selected_benchmark, technical=technical)


def _render_tab7_news(tab7, positions, summary, technical, selected_date=None, selected_benchmark=None):
    """Tab7: 资讯与评估"""
    from tabs.tab7_news import render_tab7
    with tab7:
        render_tab7(positions, summary, index_quotes=None, selected_date=selected_date, selected_benchmark=selected_benchmark)


def _render_tab8_advice(tab8, positions, summary, technical, selected_date=None, selected_benchmark=None):
    """Tab8: 操作建议"""
    from tabs.tab8_advice import render_tab8
    with tab8:
        render_tab8(positions, summary, index_quotes=None, selected_date=selected_date, selected_benchmark=selected_benchmark)

def _render_tab5_advanced(tab5, positions, summary, technical, selected_date=None, selected_benchmark=None):
    """Tab5: 高级分析"""
    from tabs.tab5_advanced import render_tab5
    with tab5:
        render_tab5(positions, summary, index_quotes=None, selected_date=selected_date, selected_benchmark=selected_benchmark)

def _render_tab9_custom(tab9, positions, summary=None, selected_date=None, selected_benchmark=None):
    """Tab9: 自定义指标"""
    from tabs.tab9_custom import render_tab9
    with tab9:
        render_tab9(positions, summary, index_quotes=None, selected_date=selected_date, selected_benchmark=selected_benchmark)

def _render_tab10_fund_flow(tab10, positions, summary, selected_date=None, selected_benchmark=None):
    """Tab10: 资金动向"""
    from tabs.tab10_fund_flow import render_tab10
    with tab10:
        render_tab10(positions, summary, index_quotes=None, selected_date=selected_date, selected_benchmark=selected_benchmark)

def _render_tab11_gold(tab11, positions, summary, selected_date, selected_benchmark):
    """Tab11: 黄金市场分析"""
    from tabs.tab11_gold import render_tab11
    with tab11:
        render_tab11(positions, summary, selected_date=selected_date, selected_benchmark=selected_benchmark)


def _render_tab12_macro(tab12):
    """Tab12: 宏观市场数据面板"""
    from tabs.tab12_macro import render_tab12
    with tab12:
        render_tab12()


def _render_tab13_data_quality(tab13, positions=None, summary=None, selected_date=None, selected_benchmark=None):
    """Tab13: 数据质量监控面板"""
    from tabs.tab13_data_quality import render_tab13
    with tab13:
        render_tab13()


def _inject_custom_css():
    """Inject custom dark-theme CSS styles."""
    # 自定义CSS
    st.markdown(
        """
        <style>
        .stApp { background-color: #0d1117; }
        .main-header {
            font-size: 28px; font-weight: bold; color: #58a6ff;
            text-align: center; padding: 20px 0 10px 0;
        }
        .sub-header {
            font-size: 14px; color: #8b949e; text-align: center; padding-bottom: 15px;
        }
        .section-title {
            font-size: 18px; font-weight: bold; color: #c9d1d9;
            padding: 10px 0 5px 0; border-bottom: 1px solid #30363d;
        }
        .tip-title {
            font-size: 18px; font-weight: bold; color: #c9d1d9;
            padding: 10px 0 5px 0; border-bottom: 1px solid #30363d;
            display: inline-block; cursor: help;
        }
        .tip-title::after {
            content: ' ℹ';
            font-size: 11px; color: #58a6ff; font-weight: normal;
        }
        .tip-title .tip-text {
            visibility: hidden; opacity: 0;
            position: absolute; z-index: 999;
            background: #1c2333; color: #c9d1d9;
            border: 1px solid #30363d; border-radius: 6px;
            padding: 8px 12px; font-size: 12px; font-weight: normal;
            line-height: 1.5; width: max-content; max-width: 360px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.4);
            transition: opacity 0.2s, visibility 0.2s;
            margin-top: 6px; margin-left: 0;
        }
        .tip-title:hover .tip-text {
            visibility: visible; opacity: 1;
        }
        .tip-title .tip-arrow {
            visibility: hidden; opacity: 0;
            position: absolute; z-index: 999;
            border-left: 6px solid transparent;
            border-right: 6px solid transparent;
            border-bottom: 6px solid #30363d;
            transition: opacity 0.2s, visibility 0.2s;
        }
        .tip-title:hover .tip-arrow {
            visibility: visible; opacity: 1;
        }


        .cal-table { border-collapse: collapse; margin: 0 auto; }
        .cal-table th { padding: 6px 8px; font-size: 12px; color: #8b949e; font-weight: normal; }
        .cal-table td {
            width: 50px; height: 44px; text-align: center; vertical-align: middle;
            border: 1px solid #21262d; border-radius: 4px; cursor: default;
            position: relative; padding: 2px;
        }
        .cal-table td.cal-today { border: 2px solid #58a6ff; }
        .cal-table td.cal-non-trading {
            background: #0d1117; color: #30363d;
        }
        .cal-table td.cal-trading {
            background: #161b22;
        }
        .cal-table td.cal-profit { background: #0d2818; }
        .cal-table td.cal-loss { background: #2d1215; }
        .cal-day { font-size: 12px; color: #c9d1d9; }
        .cal-pnl { font-size: 10px; display: block; line-height: 1.2; }
        .cal-pnl-profit { color: #22c55e; }
        .cal-pnl-loss { color: #ef4444; }
        .cal-pnl-zero { color: #484f58; }
        .yr-pill {
            display: inline-block; padding: 4px 14px; margin: 2px;
            border-radius: 14px; font-size: 13px; cursor: pointer;
            background: #21262d; color: #c9d1d9; border: 1px solid #30363d;
        }
        .yr-pill.active { background: #1f6feb; color: #ffffff; border-color: #1f6feb; }
        .mo-pill {
            display: inline-block; padding: 3px 12px; margin: 2px;
            border-radius: 12px; font-size: 12px; cursor: pointer;
            background: #161b22; color: #8b949e; border: 1px solid #21262d;
        }
        .mo-pill.active { background: #238636; color: #ffffff; border-color: #238636; }
        .cal-summary {
            display: inline-block; padding: 4px 12px; margin: 2px 6px;
            border-radius: 6px; font-size: 12px; background: #161b22;
        }
        .cal-summary-profit { color: #22c55e; }
        .cal-summary-loss { color: #ef4444; }

        /* 主标签栏换行 */
        .stTabs [data-baseweb="tab-list"] {
            display: flex; flex-wrap: wrap; gap: 2px 4px;
            max-width: 100%; overflow: visible;
        }
        .stTabs [data-baseweb="tab"] {
            flex: 0 0 auto !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

def _render_sidebar(available_dates):

    # ===== 数据新鲜度提示 =====
    latest_avail = available_dates[0] if available_dates else None
    if latest_avail:
        from datetime import datetime
        try:
            last_dt = datetime.strptime(str(latest_avail), "%Y-%m-%d")
            gap = (datetime.now() - last_dt).days
            if gap <= 1:
                st.sidebar.success(f"数据已更新至 {latest_avail}")
            elif gap <= 3:
                st.sidebar.warning(f"数据截止 {latest_avail}（{gap}天前）")
            else:
                st.sidebar.error(f"数据截止 {latest_avail}（{gap}天前），请检查采集服务")
        except Exception:
            st.sidebar.caption(f"数据截止: {latest_avail}")

    # Render sidebar controls. Returns (selected_date, show_days, selected_benchmark).
    with st.sidebar:
        st.markdown("### 🔧 控制面板")

        selected_date = st.selectbox(
            "选择日期",
            available_dates,
            index=0,
            format_func=lambda x: f"{x} {'(最新)' if x == available_dates[0] else ''}",
            key="select_date",
        )

        # 快捷预设
        preset = st.radio(
            "时间范围", ["3个月", "6个月", "1年", "2年", "5年", "全部", "自定义"], horizontal=True, index=2,
            key="radio_preset",
        )
        preset_days = {"3个月": 60, "6个月": 120, "1年": 250, "2年": 500, "5年": 1250, "全部": 4000}
        if preset == "自定义":
            show_days = st.slider("自定义天数", min_value=10, max_value=4000, value=250, step=10, key="slider_custom_days")
        else:
            show_days = preset_days[preset]

        st.markdown("---")
        st.markdown("### 📋 系统信息")

        logs = load_execution_logs(5)
        if not logs.empty:
            for _, log in logs.iterrows():
                status_icon = "✅" if log["status"] == "success" else "❌" if log["status"] == "failed" else "⏳"
                st.markdown(f"{status_icon} `{log['task_name']}` - {log['status']}")
                if pd.notna(log.get("duration_seconds")):
                    st.caption(f"  耗时: {log['duration_seconds']:.1f}s")

        # 基准指数选择（P1改进）
        st.markdown("### 📌 基准指数")
        benchmark_options = {k: v for k, v in INDEX_CODES.items()}
        # 默认选中沪深300
        default_bench = "sh000300"
        benchmark_keys = list(benchmark_options.keys())
        default_idx = benchmark_keys.index(default_bench) if default_bench in benchmark_keys else 0
        selected_benchmark = st.selectbox(
            "对比基准",
            options=benchmark_keys,
            index=default_idx,
            format_func=lambda x: benchmark_options[x],
            key="benchmark_select",
        )

        st.markdown("---")
        st.markdown(f"*数据更新: {available_dates[0]}*")

        st.markdown("---")
        st.markdown("### 📊 快速指标")
        st.markdown('<span style="font-size:11px;color:#484f58;">↓ 详见下方概览指标条</span>', unsafe_allow_html=True)

    return selected_date, show_days, selected_benchmark

def _render_overview_cards(total_value, total_pnl, total_return, daily_return, daily_pnl,
                              sharpe, effective_max_dd, volatility):
    """Render the 6-column overview metric cards row."""
    # 概览卡片行
    cols = st.columns(6)
    with cols[0]:
        st.markdown(
            f'<div style="padding:10px;border-radius:8px;background:#161b22;border-left:3px solid #58a6ff;">'
            f'<div style="font-size:11px;color:#8b949e;cursor:help;border-bottom:1px dotted #8b949e;display:inline;" title="当前所有持仓证券的市值总和">总市值 ℹ</div>'
            f'<div style="font-size:20px;font-weight:bold;color:#58a6ff;">¥{format_value(total_value)}</div>'
            f"</div>",
            unsafe_allow_html=True,
        )
    with cols[1]:
        pnl_color = "#22c55e" if total_pnl >= 0 else "#ef4444"
        st.markdown(
            f'<div style="padding:10px;border-radius:8px;background:#161b22;border-left:3px solid {pnl_color};">'
            f'<div style="font-size:11px;color:#8b949e;cursor:help;border-bottom:1px dotted #8b949e;display:inline;" title="所有持仓的累计盈亏金额和收益率，基于买入成本计算">总盈亏 ℹ</div>'
            f'<div style="font-size:20px;font-weight:bold;color:{pnl_color};">{format_value(total_pnl, prefix="¥")}</div>'
            f'<div style="font-size:11px;color:#8b949e;">{format_value(total_return, suffix="%")}</div>'
            f"</div>",
            unsafe_allow_html=True,
        )
    with cols[2]:
        dr_color = get_indicator_color(daily_return, [(0, "#ef4444"), (-1e-9, "#22c55e")], default="#888")
        st.markdown(
            f'<div style="padding:10px;border-radius:8px;background:#161b22;border-left:3px solid {dr_color};">'
            f'<div style="font-size:11px;color:#8b949e;cursor:help;border-bottom:1px dotted #8b949e;display:inline;" title="选定日期相对于前一交易日的收益率(%)和盈亏金额(元)">日收益 ℹ</div>'
            f'<div style="font-size:20px;font-weight:bold;color:{dr_color};">{format_value(daily_return, suffix="%")}</div>'
            f'<div style="font-size:11px;color:#8b949e;">{format_value(daily_pnl, prefix="¥")}</div>'
            f"</div>",
            unsafe_allow_html=True,
        )
    with cols[3]:
        sharpe_color = "#22c55e" if (sharpe and sharpe > 0.5) else "#f59e0b" if sharpe else "#888"  # get_indicator_color不适合此三元逻辑，保留
        st.markdown(
            f'<div style="padding:10px;border-radius:8px;background:#161b22;border-left:3px solid {sharpe_color};">'
            f'<div style="font-size:11px;color:#8b949e;cursor:help;border-bottom:1px dotted #8b949e;display:inline;" title="风险调整后收益指标 = (年化收益率 - 无风险利率) / 年化波动率。>1为优秀，>0.5为良好">夏普比率 ℹ</div>'
            f'<div style="font-size:20px;font-weight:bold;color:{sharpe_color};">{format_value(sharpe, decimals=3)}</div>'
            f"</div>",
            unsafe_allow_html=True,
        )
    with cols[4]:
        dd_color = get_indicator_color(effective_max_dd, [(10, "#ef4444"), (5, "#f59e0b"), (0, "#22c55e")])
        st.markdown(
            f'<div style="padding:10px;border-radius:8px;background:#161b22;border-left:3px solid {dd_color};">'
            f'<div style="font-size:11px;color:#8b949e;cursor:help;border-bottom:1px dotted #8b949e;display:inline;" title="选定时间段内，组合从历史最高点到最低点的最大跌幅(%)">最大回撤 ℹ</div>'
            f'<div style="font-size:20px;font-weight:bold;color:{dd_color};">{format_value(effective_max_dd, suffix="%")}</div>'
            f"</div>",
            unsafe_allow_html=True,
        )
    with cols[5]:
        vol_color = get_indicator_color(volatility, [(25, "#ef4444"), (15, "#f59e0b"), (0, "#22c55e")])
        st.markdown(
            f'<div style="padding:10px;border-radius:8px;background:#161b22;border-left:3px solid {vol_color};">'
            f'<div style="font-size:11px;color:#8b949e;cursor:help;border-bottom:1px dotted #8b949e;display:inline;" title="日收益率标准差的年化值，反映组合收益的波动幅度。值越高表示风险越大">年化波动率 ℹ</div>'
            f'<div style="font-size:20px;font-weight:bold;color:{vol_color};">{format_value(volatility, suffix="%")}</div>'
            f"</div>",
            unsafe_allow_html=True,
        )


def _render_quick_stats(positions, profit_count, loss_count, technical):
    """Render the quick stats summary bar below overview cards."""
    # ========== 快速指标条 ==========
    if not positions.empty:
        total_mv = positions["market_value"].sum()
        pc = profit_count if profit_count else 0
        lc = loss_count if loss_count else 0
        total_held = pc + lc
        wr = (pc / total_held * 100) if total_held > 0 else 0
        wr_color = "#22c55e" if wr >= 60 else "#f59e0b" if wr >= 40 else "#ef4444"

        # 最大持仓
        max_pos = positions.loc[positions["market_value"].idxmax()]
        max_wt = (max_pos["market_value"] / total_mv * 100) if total_mv > 0 else 0
        wt_color = "#ef4444" if max_wt > 30 else "#f59e0b" if max_wt > 20 else "#22c55e"

        # 技术信号统计
        buy_sig = sell_sig = 0
        if technical is not None and not technical.empty:
            for _, tr in technical.iterrows():
                if tr.get("ma_signal") in ("多头排列", "金叉") or tr.get("macd_signal") == "金叉":
                    buy_sig += 1
                if tr.get("ma_signal") in ("空头排列", "死叉") or tr.get("macd_signal") == "死叉":
                    sell_sig += 1
        sig_color = "#22c55e" if buy_sig > sell_sig else "#ef4444" if sell_sig > buy_sig else "#f59e0b"

        # 行业分布
        sector_dist = {}
        for _, pos in positions.iterrows():
            code = str(pos["code"])
            cat_info = ETF_CATEGORIES.get(code)
            if cat_info:
                sec = cat_info["sector"]
                sector_dist[sec] = sector_dist.get(sec, 0) + pos["market_value"]
        sector_tags = ""
        if sector_dist and total_mv > 0:
            top_sec = sorted(sector_dist.items(), key=lambda x: x[1], reverse=True)[:4]
            sector_tags = " ".join(
                f'<span style="font-size:11px;color:{SECTOR_COLORS.get(s, "#8b949e")};background:{SECTOR_COLORS.get(s, "#8b949e")}15;padding:2px 6px;border-radius:3px;">{s} {(v/total_mv*100):.0f}%</span>'
                for s, v in top_sec
            )

        st.markdown(
            f'<div style="display:flex;gap:20px;flex-wrap:wrap;padding:8px 4px;margin-bottom:4px;font-size:13px;">'
            f'<span style="color:#8b949e;">胜率: <b style="color:{wr_color};">{wr:.1f}%</b> <span style="color:#484f58;font-size:11px;">({pc}盈/{lc}亏)</span></span>'
            f'<span style="color:#8b949e;">最大持仓: <b style="color:{wt_color};">{max_pos["name"]}</b> <span style="color:#484f58;font-size:11px;">{max_wt:.1f}%</span></span>'
            f'<span style="color:#8b949e;">技术信号: <b style="color:{sig_color};">{buy_sig}多 / {sell_sig}空</b></span>'
            f"</div>"
            f'<div style="padding:2px 4px 8px;">{sector_tags}</div>',
            unsafe_allow_html=True,
        )


def _render_tab14_market_events(tab14):
    """Tab14: 市场事件面板"""
    from tabs.tab14_market_events import render_tab14
    with tab14:
        render_tab14()


def main():
    global sharpe, volatility, max_dd, effective_max_dd, total_return, total_value, total_pnl, daily_return, daily_pnl, profit_count, loss_count, total_mv, show_days, technical
    import pandas as pd
    rolling_data = pd.DataFrame()
    ext_risk = {}
    _inject_custom_css()

    # 标题
    st.markdown('<div class="main-header">📊 投资组合跟踪分析系统</div>', unsafe_allow_html=True)

    # 获取数据
    available_dates = get_available_dates()
    if not available_dates:
        st.warning("暂无数据，请先运行 run_analysis.py")
        return

    selected_date, show_days, selected_benchmark = _render_sidebar(available_dates)
    # 加载数据（带缓存，相同参数不重复查询）
    positions = load_positions(selected_date)
    summary = load_summary(show_days, selected_date)
    technical = load_technical()

    # 预生成缓存：最近10个交易日 x 各时间预设，后台静默触发一次
    _preset_days_list = [60, 120, 250, 500, 1250, 4000]
    _recent = available_dates[:10]  # 最近10个交易日
    with st.spinner(""):
        for _d in _recent:
            load_positions(_d)
            load_summary(show_days, _d)
            load_benchmark_comparison(selected_benchmark, show_days, _d)
        for _days in _preset_days_list:
            load_summary(_days, available_dates[0])
            load_benchmark_comparison(selected_benchmark, _days, available_dates[0])

    if positions.empty:
        st.warning(f"{selected_date} 无持仓数据")
        return

    # ========== 概览指标 ==========
    latest_summary = summary.iloc[-1] if not summary.empty else {}
    total_value = latest_summary.get("total_value", 0)
    total_cost = latest_summary.get("total_cost", 0)
    total_pnl = latest_summary.get("total_pnl", 0)
    total_return = (total_pnl / total_cost * 100) if total_cost > 0 else 0
    daily_return = latest_summary.get("daily_return", 0)
    daily_pnl = latest_summary.get("daily_pnl", 0)
    sharpe = latest_summary.get("sharpe_ratio")
    max_dd = latest_summary.get("max_drawdown")
    # early computation of effective_max_dd for use in overview cards (before tab3)
    _early_ext = compute_extended_risk_metrics(end_date=selected_date)
    effective_max_dd = _early_ext.get("max_drawdown", max_dd)
    volatility = latest_summary.get("volatility")
    profit_count = latest_summary.get("profit_count", 0)
    loss_count = latest_summary.get("loss_count", 0)

    _render_overview_cards(total_value, total_pnl, total_return, daily_return, daily_pnl,
                            sharpe, effective_max_dd, volatility)
    # ========== 图表行1: 净值曲线 + 收益分布 ==========
    tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9, tab10, tab11, tab12, tab13, tab14 = st.tabs(
        [
            "📈 净值走势",
            "📊 持仓分布",
            "⚠️ 风险分析",
            "📅 收益日历",
            "💠 高级分析",
            "📡 技术信号",
            "📰 资讯与评估",
            "💡 操作建议",
            "🔬 自定义指标",
            "💰 资金动向",
            "🥇 黄金市场",
            "🌐 宏观市场",
            "📊 数据质量",
            "📋 市场事件",
        ]
    )

    _render_quick_stats(positions, profit_count, loss_count, technical)
    _render_tab1_body(tab1, positions, summary, selected_date, show_days, selected_benchmark, rolling_data, effective_max_dd, technical, volatility, max_dd, sharpe)

    _render_tab2_position(tab2, positions, summary, selected_date, selected_benchmark, technical, volatility, max_dd, sharpe)

    _render_tab3_risk(tab3, positions, summary, technical, selected_date, selected_benchmark, ext_risk, volatility, max_dd, sharpe)

    _render_tab4_calendar(tab4, positions, summary, selected_date, selected_benchmark)

    _render_tab6_technical(tab6, technical, selected_date, selected_benchmark, positions, summary)

    _render_tab7_news(tab7, positions, summary, technical, selected_date, selected_benchmark)

    _render_tab8_advice(tab8, positions, summary, technical, selected_date, selected_benchmark)

    _render_tab5_advanced(tab5, positions, summary, technical, selected_date, selected_benchmark)

    _render_tab9_custom(tab9, positions, summary, selected_date, selected_benchmark)

    _render_tab10_fund_flow(tab10, positions, summary, selected_date, selected_benchmark)
    _render_tab11_gold(tab11, positions, summary, selected_date, selected_benchmark)
    _render_tab12_macro(tab12)
    _render_tab13_data_quality(tab13, positions, summary, selected_date, selected_benchmark)
    _render_tab14_market_events(tab14)

if __name__ == "__main__":
    main()
