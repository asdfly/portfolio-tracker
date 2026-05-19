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


def _render_tab1_body(tab1, positions, summary, selected_date, show_days, selected_benchmark, rolling_data, effective_max_dd):
    """Extracted from main() - tab1 renderer"""
    with tab1:
        st.caption("📈 展示组合净值走势与基准对比、日收益率分布、每日盈亏及滚动风险指标")
        col_left, col_right = st.columns([2, 1])

        with col_left:
            st.markdown(
                '<div class="tip-title" style="">组合净值走势<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">以组合总市值为基准归一化到100，展示组合净值随时间的变化趋势，同时叠加基准指数走势进行对比。</span></div>',
                unsafe_allow_html=True,
            )
            if not summary.empty and len(summary) > 1:
                # 计算累计净值（基准100）
                base_value = summary.iloc[0]["total_value"]
                summary_plot = summary.copy()
                summary_plot["nav"] = summary_plot["total_value"] / base_value * 100

                # 降采样用于图表渲染
                chart_data = downsample(summary_plot, max_points=500)

                # 基准指数对比（使用侧边栏选择的基准）
                bench_name = INDEX_CODES.get(selected_benchmark, selected_benchmark)
                bench_df = load_benchmark_comparison(selected_benchmark, show_days + 10, selected_date)
                if not bench_df.empty:
                    bench_base = bench_df.iloc[0]["close"]
                    bench_plot = bench_df.copy()
                    bench_plot["nav"] = bench_plot["close"] / bench_base * 100
                    bench_chart = downsample(bench_plot, max_points=500)

                    fig = go.Figure()
                    fig.add_trace(
                        go.Scatter(
                            x=bench_chart["date"],
                            y=bench_chart["nav"],
                            mode="lines",
                            name=bench_name,
                            line=dict(color="#8b949e", width=1.5, dash="dash"),
                        )
                    )
                    fig.add_trace(
                        go.Scatter(
                            x=chart_data["date"],
                            y=chart_data["nav"],
                            mode="lines",
                            name="投资组合",
                            line=dict(color="#58a6ff", width=2),
                        )
                    )

                    # 标记净值最高和最低
                    _add_min_max_annotations(fig, chart_data["date"], chart_data["nav"], y_label="净值")

                else:
                    fig = go.Figure()
                    fig.add_trace(
                        go.Scatter(
                            x=chart_data["date"],
                            y=chart_data["nav"],
                            mode="lines",
                            name="投资组合",
                            line=dict(color="#58a6ff", width=2),
                        )
                    )

                    # 标记净值最高和最低
                    _add_min_max_annotations(fig, chart_data["date"], chart_data["nav"], y_label="净值")

                fig.update_layout(
                    height=350,
                    plot_bgcolor="#0d1117",
                    paper_bgcolor="#0d1117",
                    font=dict(color="#c9d1d9", size=11),
                    margin=dict(l=50, r=20, t=10, b=40),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(size=11)),
                    xaxis=dict(showgrid=False),
                    yaxis=dict(title="净值 (基准100)", showgrid=True, gridcolor="#21262d"),
                )
                st.plotly_chart(fig, width="stretch")

        with col_right:
            st.markdown(
                '<div class="tip-title" style="">日收益率分布<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">统计选定时间范围内每日收益率(%)的频率分布。橙色虚线为均值，黄色区间为±1个标准差范围，绿色虚线为±2个标准差。</span></div>',
                unsafe_allow_html=True,
            )
            if not summary.empty and "total_value" in summary.columns and len(summary) > 5:
                # daily_return 在数据库中以百分比形式存储，改用 total_value.pct_change()
                daily_rets = (summary["total_value"].pct_change().dropna() * 100).values
                if len(daily_rets) > 0:
                    fig_hist = go.Figure()
                    fig_hist.add_trace(
                        go.Histogram(
                            x=daily_rets,
                            nbinsx=40,
                            marker_color="#58a6ff",
                            marker_line_color="#0d1117",
                            marker_line_width=0.5,
                            opacity=0.85,
                        )
                    )
                    mean_ret = np.mean(daily_rets)
                    fig_hist.add_vline(
                        x=mean_ret, line_dash="dash", line_color="#f59e0b", annotation_text=f"均值 {mean_ret:.3f}%"
                    )
                    fig_hist.update_layout(
                        height=200,
                        plot_bgcolor="#0d1117",
                        paper_bgcolor="#0d1117",
                        font=dict(color="#c9d1d9", size=11),
                        margin=dict(l=50, r=20, t=10, b=40),
                        xaxis=dict(title="日收益率 (%)", showgrid=True, gridcolor="#21262d"),
                        yaxis=dict(title="天数", showgrid=True, gridcolor="#21262d"),
                    )
                    st.plotly_chart(fig_hist, width="stretch")

        # 日收益柱状图（降采样）
        st.markdown(
            '<div class="tip-title" style="">每日盈亏<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">展示每个交易日的盈亏金额(元)。绿色柱体表示盈利日，红色柱体表示亏损日，可直观观察收益的连续性和波动幅度。</span></div>',
            unsafe_allow_html=True,
        )
        if not summary.empty and "daily_pnl" in summary.columns and len(summary) > 1:
            bar_data = downsample(summary[["date", "daily_pnl"]].copy(), max_points=500)
            colors = ["#22c55e" if dp >= 0 else "#ef4444" for dp in bar_data["daily_pnl"]]
            fig_bar = go.Figure()
            fig_bar.add_trace(go.Bar(x=bar_data["date"], y=bar_data["daily_pnl"], marker_color=colors, name="日盈亏"))
            # 标记最大盈亏
            _add_min_max_annotations(fig_bar, bar_data["date"], bar_data["daily_pnl"], y_label="盈亏")

            fig_bar.update_layout(
                height=200,
                plot_bgcolor="#0d1117",
                paper_bgcolor="#0d1117",
                font=dict(color="#c9d1d9", size=11),
                margin=dict(l=50, r=20, t=10, b=40),
                xaxis=dict(showgrid=False, tickfont=dict(size=9)),
                yaxis=dict(title="盈亏 (¥)", showgrid=True, gridcolor="#21262d"),
            )
            st.plotly_chart(fig_bar, width="stretch")

        # ---------- 滚动指标图表 ----------
        r1, r2 = st.columns([1, 3])
        with r1:
            rolling_window = st.selectbox(
                "滚动窗口", options=[60, 120, 250], format_func=lambda x: f"{x}日", index=0, key="rolling_window"
            )
        rolling_data = compute_rolling_metrics(window=rolling_window, end_date=selected_date)
        if not rolling_data.empty and len(rolling_data) > 5:
            st.markdown(
                f'<div class="tip-title">'
                f"滚动风险指标（{rolling_window}日窗口）"
                f'<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span>'
                f'<span class="tip-text" style="left: 4px; top: calc(100% + 10px);">'
                f"使用{rolling_window}日滚动窗口计算的夏普比率和年化波动率。滚动夏普比率反映近期风险调整收益的稳定程度；滚动波动率反映近期市场波动水平的演变。</span>"
                f"</div>",
                unsafe_allow_html=True,
            )
            rolling_chart = downsample(rolling_data, max_points=500)

            fig_roll = make_subplots(
                rows=2,
                cols=1,
                shared_xaxes=True,
                vertical_spacing=0.08,
                subplot_titles=("滚动夏普比率", "滚动年化波动率"),
            )
            fig_roll.add_trace(
                go.Scatter(
                    x=rolling_chart["date"],
                    y=rolling_chart["rolling_sharpe"],
                    mode="lines",
                    name="滚动夏普",
                    line=dict(color="#58a6ff", width=1.5),
                    fill="tozeroy",
                    fillcolor="rgba(88,166,255,0.08)",
                ),
                row=1,
                col=1,
            )
            fig_roll.add_hline(y=0, line_dash="dash", line_color="#484f58", row=1, col=1)
            fig_roll.add_hline(y=1, line_dash="dot", line_color="#22c55e", annotation_text="优秀线(1.0)", row=1, col=1)

            # 标记滚动夏普最高最低
            _add_min_max_annotations(fig_roll, rolling_chart["date"], rolling_chart["rolling_sharpe"], row=1, col=1)

            fig_roll.add_trace(
                go.Scatter(
                    x=rolling_chart["date"],
                    y=rolling_chart["rolling_vol"],
                    mode="lines",
                    name="滚动波动率",
                    line=dict(color="#f59e0b", width=1.5),
                    fill="tozeroy",
                    fillcolor="rgba(245,158,11,0.08)",
                ),
                row=2,
                col=1,
            )

            # 标记滚动波动率最高最低
            _add_min_max_annotations(fig_roll, rolling_chart["date"], rolling_chart["rolling_vol"], row=2, col=1)

            fig_roll.update_layout(
                height=350,
                plot_bgcolor="#0d1117",
                paper_bgcolor="#0d1117",
                font=dict(color="#c9d1d9", size=11),
                margin=dict(l=50, r=20, t=35, b=40),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(size=10)),
                showlegend=False,
            )
            fig_roll.update_xaxes(showgrid=False, row=1, col=1)
            fig_roll.update_xaxes(showgrid=False, row=2, col=1)
            fig_roll.update_yaxes(title_text="夏普比率", showgrid=True, gridcolor="#21262d", row=1, col=1)
            fig_roll.update_yaxes(title_text="波动率 (%)", showgrid=True, gridcolor="#21262d", row=2, col=1)
            st.plotly_chart(fig_roll, width="stretch")

        # ---------- 基准对比表 ----------
        if not summary.empty and len(summary) > 1:
            bench_df_raw = load_benchmark_comparison(selected_benchmark, show_days, selected_date)
            if not bench_df_raw.empty:
                st.markdown(
                    '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">基准对比详情<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">数值化展示组合与基准指数的收益对比，包括累计收益率、年化收益率、夏普比率、最大回撤、年化波动率和超额收益(Alpha)。</span></div>',
                    unsafe_allow_html=True,
                )
                import math

                # 组合指标
                port_start_val = summary.iloc[0]["total_value"]
                port_end_val = summary.iloc[-1]["total_value"]
                port_total_ret = (port_end_val / port_start_val - 1) * 100 if port_start_val > 0 else 0
                # daily_return 在数据库中以百分比形式存储（如0.51表示51%），需除以100转为小数
                port_daily = summary["total_value"].pct_change().dropna()
                port_ann_ret = port_daily.mean() * 252 * 100 if len(port_daily) > 0 else 0
                port_vol = port_daily.std() * math.sqrt(252) * 100 if len(port_daily) > 1 else 0
                port_sharpe = (
                    (port_daily.mean() / port_daily.std() * math.sqrt(252))
                    if port_daily.std() > 0 and port_daily.mean() > 0
                    else 0
                )
                port_cummax = summary["total_value"].cummax()
                port_drawdown = ((summary["total_value"] - port_cummax) / port_cummax * 100).min()
                # 基准指标
                bench_start = bench_df_raw.iloc[0]["close"]
                bench_end = bench_df_raw.iloc[-1]["close"]
                bench_total_ret = (bench_end / bench_start - 1) * 100 if bench_start > 0 else 0
                bench_daily_ret = bench_df_raw["close"].pct_change().dropna()
                bench_ann_ret = bench_daily_ret.mean() * 252 * 100 if len(bench_daily_ret) > 0 else 0
                bench_vol = bench_daily_ret.std() * math.sqrt(252) * 100 if len(bench_daily_ret) > 1 else 0
                bench_sharpe = (
                    (bench_daily_ret.mean() / bench_daily_ret.std() * math.sqrt(252))
                    if bench_daily_ret.std() > 0 and bench_daily_ret.mean() > 0
                    else 0
                )
                bench_cummax = bench_df_raw["close"].cummax()
                bench_drawdown = ((bench_df_raw["close"] - bench_cummax) / bench_cummax * 100).min()
                # 对齐日期计算超额收益
                merged = summary[["date", "total_value"]].merge(bench_df_raw[["date", "close"]], on="date", how="inner")
                if not merged.empty:
                    excess_ret = (
                        (merged["total_value"].iloc[-1] / merged["total_value"].iloc[0] - 1)
                        - (merged["close"].iloc[-1] / merged["close"].iloc[0] - 1)
                    ) * 100
                    # 日超额收益率序列
                    port_daily_aligned = merged["total_value"].pct_change().dropna()
                    bench_daily_aligned = merged["close"].pct_change().dropna()
                    excess_daily = port_daily_aligned - bench_daily_aligned
                    tracking_error = excess_daily.std() * math.sqrt(252) * 100 if len(excess_daily) > 1 else 0
                    info_ratio = (
                        (excess_daily.mean() * math.sqrt(252) / excess_daily.std()) if excess_daily.std() > 0 else 0
                    )
                else:
                    excess_ret = port_total_ret - bench_total_ret
                    tracking_error = 0
                    info_ratio = 0

                # 渲染HTML表格
                def _fmt_cell(val, suffix="", decimals=2, invert_color=False):
                    """格式化数值并着色"""
                    try:
                        v = float(val)
                    except (TypeError, ValueError):
                        return f'<span style="color:#8b949e;">--</span>'
                    color = "#22c55e" if (v >= 0 and not invert_color) or (v < 0 and invert_color) else "#ef4444"
                    if abs(v) < 0.01:
                        color = "#c9d1d9"
                    return (
                        f'<span style="color:{color};font-weight:bold;">{v:+.{decimals}f}{suffix}</span>'
                        if v != 0
                        else f'<span style="color:#c9d1d9;">{v:.{decimals}f}{suffix}</span>'
                    )

                alpha_color = "#22c55e" if excess_ret > 0 else "#ef4444"
                alpha_sign = "+" if excess_ret > 0 else ""
                bench_name = INDEX_CODES.get(selected_benchmark, selected_benchmark)
                bench_total_str = f'<span style="color:{"#22c55e" if bench_total_ret >= 0 else "#ef4444"};">{bench_total_ret:+.2f}%</span>'
                bench_ann_str = f'<span style="color:{"#22c55e" if bench_ann_ret >= 0 else "#ef4444"};">{bench_ann_ret:+.2f}%</span>'
                bench_dd_str = f'<span style="color:#ef4444;">{bench_drawdown:.2f}%</span>'
                bench_sharpe_str = f'<span style="color:#8b949e;">{bench_sharpe:.3f}</span>'
                bench_vol_str = f'<span style="color:#8b949e;">{bench_vol:.2f}%</span>'
                port_total_str = f'<span style="color:{"#22c55e" if port_total_ret >= 0 else "#ef4444"};">{port_total_ret:+.2f}%</span>'
                port_ann_str = (
                    f'<span style="color:{"#22c55e" if port_ann_ret >= 0 else "#ef4444"};">{port_ann_ret:+.2f}%</span>'
                )
                port_dd_str = f'<span style="color:#ef4444;">{port_drawdown:.2f}%</span>'
                port_sharpe_str = f'<span style="color:#8b949e;">{port_sharpe:.3f}</span>'
                port_vol_str = f'<span style="color:#8b949e;">{port_vol:.2f}%</span>'

                html_table = f"""
                <div style="overflow-x:auto;">
                <table style="width:100%;border-collapse:collapse;font-size:13px;">
                <thead><tr style="background:#161b22;">
                <th style="padding:8px 12px;color:#8b949e;text-align:left;font-size:12px;">指标</th>
                <th style="padding:8px 12px;color:#58a6ff;text-align:center;font-size:12px;">投资组合</th>
                <th style="padding:8px 12px;color:#f59e0b;text-align:center;font-size:12px;">{bench_name}</th>
                <th style="padding:8px 12px;color:#c9d1d9;text-align:center;font-size:12px;">差异</th>
                </tr></thead><tbody>
                <tr style="border-bottom:1px solid #21262d;">
                <td style="padding:7px 12px;color:#c9d1d9;">累计收益率</td>
                <td style="padding:7px 12px;text-align:center;">{port_total_str}</td>
                <td style="padding:7px 12px;text-align:center;">{bench_total_str}</td>
                <td style="padding:7px 12px;text-align:center;font-weight:bold;color:{"#22c55e" if excess_ret >= 0 else "#ef4444"};">{alpha_sign}{excess_ret:.2f}%</td>
                </tr>
                <tr style="background:#161b22;border-bottom:1px solid #21262d;">
                <td style="padding:7px 12px;color:#c9d1d9;">年化收益率</td>
                <td style="padding:7px 12px;text-align:center;">{port_ann_str}</td>
                <td style="padding:7px 12px;text-align:center;">{bench_ann_str}</td>
                <td style="padding:7px 12px;text-align:center;">{_fmt_cell(port_ann_ret - bench_ann_ret, suffix="%")}</td>
                </tr>
                <tr style="border-bottom:1px solid #21262d;">
                <td style="padding:7px 12px;color:#c9d1d9;">夏普比率</td>
                <td style="padding:7px 12px;text-align:center;">{port_sharpe_str}</td>
                <td style="padding:7px 12px;text-align:center;">{bench_sharpe_str}</td>
                <td style="padding:7px 12px;text-align:center;">{_fmt_cell(port_sharpe - bench_sharpe, decimals=3)}</td>
                </tr>
                <tr style="background:#161b22;border-bottom:1px solid #21262d;">
                <td style="padding:7px 12px;color:#c9d1d9;">最大回撤</td>
                <td style="padding:7px 12px;text-align:center;">{port_dd_str}</td>
                <td style="padding:7px 12px;text-align:center;">{bench_dd_str}</td>
                <td style="padding:7px 12px;text-align:center;">{_fmt_cell(port_drawdown - bench_drawdown, suffix="%", invert_color=True)}</td>
                </tr>
                <tr style="border-bottom:1px solid #21262d;">
                <td style="padding:7px 12px;color:#c9d1d9;">年化波动率</td>
                <td style="padding:7px 12px;text-align:center;">{port_vol_str}</td>
                <td style="padding:7px 12px;text-align:center;">{bench_vol_str}</td>
                <td style="padding:7px 12px;text-align:center;">{_fmt_cell(port_vol - bench_vol, suffix="%", invert_color=True)}</td>
                </tr>
                <tr style="background:#161b22;">
                <td style="padding:7px 12px;color:#c9d1d9;">信息比率</td>
                <td style="padding:7px 12px;text-align:center;" colspan="2"></td>
                <td style="padding:7px 12px;text-align:center;">{_fmt_cell(info_ratio, decimals=3)}</td>
                </tr>
                </tbody></table></div>"""
                st.markdown(html_table, unsafe_allow_html=True)

                # ========== 多基准对比 & 区间分析 ==========
                st.markdown("---")
                compare_tab1, compare_tab2 = st.tabs(["📊 多基准对比", "📅 区间收益分析"])

                with compare_tab1:
                    st.markdown(
                        '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">'
                        "多基准叠加对比"
                        '<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span>'
                        '<span class="tip-text" style="left: 4px; top: calc(100% + 10px);">'
                        "将投资组合与多个基准指数归一化到同一起点，直观比较不同时间段的相对走势强弱。"
                        "</span></div>",
                        unsafe_allow_html=True,
                    )

                    # 多基准选择器
                    bench_options = {k: v for k, v in INDEX_CODES.items()}
                    default_benches = ["sh000300", "sz399006", "sh000852"]
                    selected_benches = st.multiselect(
                        "选择对比基准（最多5个）",
                        options=list(bench_options.keys()),
                        default=default_benches,
                        format_func=lambda x: bench_options[x],
                        max_selections=5,
                        key="multi_bench_select",
                    )

                    if not summary.empty and len(summary) > 1 and selected_benches:
                        base_value = summary.iloc[0]["total_value"]
                        summary_plot = summary.copy()
                        summary_plot["nav"] = summary_plot["total_value"] / base_value * 100
                        chart_data = downsample(summary_plot, max_points=500)

                        fig_multi = go.Figure()
                        # 组合线（粗线）
                        fig_multi.add_trace(
                            go.Scatter(
                                x=chart_data["date"],
                                y=chart_data["nav"],
                                mode="lines",
                                name="投资组合",
                                line=dict(color="#58a6ff", width=2.5),
                            )
                        )
                        _add_min_max_annotations(fig_multi, chart_data["date"], chart_data["nav"], y_label="净值")

                        # 基准线（虚线，不同颜色）
                        bench_colors = ["#f59e0b", "#22c55e", "#ef4444", "#a855f7", "#06b6d4"]
                        bench_stats = []

                        for idx, bcode in enumerate(selected_benches):
                            bname = bench_options.get(bcode, bcode)
                            bdf = load_benchmark_comparison(bcode, show_days + 10, selected_date)
                            if not bdf.empty:
                                b_base = bdf.iloc[0]["close"]
                                b_plot = bdf.copy()
                                b_plot["nav"] = b_plot["close"] / b_base * 100
                                b_chart = downsample(b_plot, max_points=500)

                                fig_multi.add_trace(
                                    go.Scatter(
                                        x=b_chart["date"],
                                        y=b_chart["nav"],
                                        mode="lines",
                                        name=bname,
                                        line=dict(color=bench_colors[idx % len(bench_colors)], width=1.2, dash="dash"),
                                    )
                                )

                                # 计算基准统计
                                b_start = bdf.iloc[0]["close"]
                                b_end = bdf.iloc[-1]["close"]
                                b_ret = (b_end / b_start - 1) * 100 if b_start > 0 else 0
                                bench_stats.append({"基准": bname, "累计收益": f"{b_ret:+.2f}%"})

                        fig_multi.update_layout(
                            height=350,
                            plot_bgcolor="#0d1117",
                            paper_bgcolor="#0d1117",
                            font=dict(color="#c9d1d9", size=11),
                            margin=dict(l=50, r=20, t=10, b=40),
                            legend=dict(
                                orientation="h",
                                yanchor="bottom",
                                y=1.02,
                                xanchor="right",
                                x=1,
                                font=dict(size=10, color="#8b949e"),
                            ),
                            xaxis=dict(showgrid=False),
                            yaxis=dict(title="净值 (基准100)", showgrid=True, gridcolor="#21262d"),
                            hovermode="x unified",
                        )
                        st.plotly_chart(fig_multi, width="stretch")

                        # 多基准收益排行卡片
                        if bench_stats:
                            port_end = summary.iloc[-1]["total_value"]
                            port_ret = (port_end / base_value - 1) * 100 if base_value > 0 else 0
                            all_items = [{"基准": "投资组合", "累计收益": f"{port_ret:+.2f}%"}] + bench_stats
                            all_items.sort(
                                key=lambda x: float(x["累计收益"].replace("%", "").replace("+", "")), reverse=True
                            )
                            n_cards = len(all_items)
                            card_cols = st.columns(min(n_cards, 6))
                            for i, item in enumerate(all_items):
                                val = float(item["累计收益"].replace("%", "").replace("+", ""))
                                c = "#22c55e" if val >= 0 else "#ef4444"
                                rank_icon = "🥇" if i == 0 else "🥈" if i == 1 else "🥉" if i == 2 else f"{i+1}."
                                with card_cols[i % len(card_cols)]:
                                    st.markdown(
                                        f'<div style="padding:6px 10px;border-radius:6px;background:#161b22;'
                                        f'border-left:3px solid {c};text-align:center;">'
                                        f'<div style="font-size:10px;color:#8b949e;">{rank_icon} {item["基准"]}</div>'
                                        f'<div style="font-size:14px;font-weight:bold;color:{c};">{item["累计收益"]}</div>'
                                        f"</div>",
                                        unsafe_allow_html=True,
                                    )

                with compare_tab2:
                    st.markdown(
                        '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">'
                        "区间收益分析"
                        '<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span>'
                        '<span class="tip-text" style="left: 4px; top: calc(100% + 10px);">'
                        "选择起止日期，查看该时间段内组合与基准的累计收益、年化收益、最大回撤、波动率等核心指标对比。"
                        "</span></div>",
                        unsafe_allow_html=True,
                    )

                    if not summary.empty and len(summary) > 1:
                        all_dates_list = summary["date"].tolist()
                        date_range = st.columns(2)
                        with date_range[0]:
                            start_dt = st.selectbox(
                                "起始日期", all_dates_list, index=min(len(all_dates_list) - 1, 60), key="range_start"
                            )
                        with date_range[1]:
                            end_dt = st.selectbox("结束日期", all_dates_list, index=0, key="range_end")

                        if start_dt < end_dt:
                            mask = (summary["date"] >= start_dt) & (summary["date"] <= end_dt)
                            range_data = summary[mask].copy()
                            if len(range_data) > 1:
                                import math as _math

                                # 组合区间指标
                                r_start_val = range_data.iloc[0]["total_value"]
                                r_end_val = range_data.iloc[-1]["total_value"]
                                r_cum_ret = (r_end_val / r_start_val - 1) * 100 if r_start_val > 0 else 0
                                # daily_return 在数据库中以百分比形式存储，改用 total_value.pct_change()
                                r_daily = range_data["total_value"].pct_change().dropna()
                                n_days = len(r_daily)
                                r_ann_ret = (r_daily.mean() * 252 * 100) if n_days > 0 else 0
                                r_vol = (r_daily.std() * _math.sqrt(252) * 100) if n_days > 1 else 0
                                r_sharpe = (
                                    (r_daily.mean() / r_daily.std() * _math.sqrt(252)) if r_daily.std() > 0 else 0
                                )
                                r_cummax = range_data["total_value"].cummax()
                                r_dd = ((range_data["total_value"] - r_cummax) / r_cummax * 100).min()
                                # 最大单日涨跌
                                r_best_day = r_daily.max() if len(r_daily) > 0 else 0
                                r_worst_day = r_daily.min() if len(r_daily) > 0 else 0
                                # 正负天数
                                r_up_days = (r_daily > 0).sum()
                                r_dn_days = (r_daily < 0).sum()
                                r_wr = (r_up_days / n_days * 100) if n_days > 0 else 0
                                # 盈亏比
                                avg_win = r_daily[r_daily > 0].mean() if r_up_days > 0 else 0
                                avg_loss = abs(r_daily[r_daily < 0].mean()) if r_dn_days > 0 else 0.0001
                                r_pnl_ratio = avg_win / avg_loss if avg_loss > 0 else 0

                                # 基准区间指标
                                bench_code = selected_benchmark
                                bname = INDEX_CODES.get(bench_code, bench_code)
                                bdf = load_benchmark_comparison(bench_code, show_days, selected_date)
                                b_stats = {}
                                if not bdf.empty:
                                    b_mask = (bdf["date"] >= start_dt) & (bdf["date"] <= end_dt)
                                    b_range = bdf[b_mask].copy()
                                    if len(b_range) > 1:
                                        b_start_c = b_range.iloc[0]["close"]
                                        b_end_c = b_range.iloc[-1]["close"]
                                        b_daily = b_range["close"].pct_change().dropna()
                                        b_stats = {
                                            "cum_ret": (b_end_c / b_start_c - 1) * 100 if b_start_c > 0 else 0,
                                            "ann_ret": b_daily.mean() * 252 * 100 if len(b_daily) > 0 else 0,
                                            "vol": b_daily.std() * _math.sqrt(252) * 100 if len(b_daily) > 1 else 0,
                                            "sharpe": (
                                                (b_daily.mean() / b_daily.std() * _math.sqrt(252))
                                                if b_daily.std() > 0
                                                else 0
                                            ),
                                            "dd": (
                                                (b_range["close"] - b_range["close"].cummax())
                                                / b_range["close"].cummax()
                                                * 100
                                            ).min(),
                                        }

                                # 渲染区间分析卡片
                                ic1, ic2, ic3, ic4 = st.columns(4)
                                with ic1:
                                    c = "#22c55e" if r_cum_ret >= 0 else "#ef4444"
                                    st.markdown(
                                        f'<div style="padding:8px;border-radius:6px;background:#161b22;border-left:3px solid {c};">'
                                        f'<div style="font-size:10px;color:#8b949e;">累计收益</div>'
                                        f'<div style="font-size:16px;font-weight:bold;color:{c};">{r_cum_ret:+.2f}%</div>'
                                        f"</div>",
                                        unsafe_allow_html=True,
                                    )
                                with ic2:
                                    c2 = "#22c55e" if r_sharpe >= 0.5 else "#f59e0b" if r_sharpe >= 0 else "#ef4444"
                                    st.markdown(
                                        f'<div style="padding:8px;border-radius:6px;background:#161b22;border-left:3px solid {c2};">'
                                        f'<div style="font-size:10px;color:#8b949e;">区间夏普</div>'
                                        f'<div style="font-size:16px;font-weight:bold;color:{c2};">{r_sharpe:.3f}</div>'
                                        f"</div>",
                                        unsafe_allow_html=True,
                                    )
                                with ic3:
                                    c3 = "#ef4444" if abs(r_dd) > 15 else "#f59e0b" if abs(r_dd) > 8 else "#22c55e"
                                    st.markdown(
                                        f'<div style="padding:8px;border-radius:6px;background:#161b22;border-left:3px solid {c3};">'
                                        f'<div style="font-size:10px;color:#8b949e;">最大回撤</div>'
                                        f'<div style="font-size:16px;font-weight:bold;color:{c3};">{r_dd:.2f}%</div>'
                                        f"</div>",
                                        unsafe_allow_html=True,
                                    )
                                with ic4:
                                    c4 = "#22c55e" if r_wr >= 60 else "#f59e0b" if r_wr >= 45 else "#ef4444"
                                    st.markdown(
                                        f'<div style="padding:8px;border-radius:6px;background:#161b22;border-left:3px solid {c4};">'
                                        f'<div style="font-size:10px;color:#8b949e;">胜率 / 盈亏比</div>'
                                        f'<div style="font-size:14px;font-weight:bold;color:{c4};">{r_wr:.0f}% / {r_pnl_ratio:.2f}</div>'
                                        f"</div>",
                                        unsafe_allow_html=True,
                                    )

                                # 详细对比表
                                def _fmt(v, suffix="", dec=2, inv=False):
                                    try:
                                        fv = float(v)
                                    except:
                                        return '<span style="color:#8b949e;">--</span>'
                                    c = "#22c55e" if (fv >= 0 and not inv) or (fv < 0 and inv) else "#ef4444"
                                    if abs(fv) < 0.005:
                                        c = "#c9d1d9"
                                    return f'<span style="color:{c};font-weight:bold;">{fv:+.{dec}f}{suffix}</span>'

                                b_cum_s = _fmt(b_stats.get("cum_ret", 0), "%")
                                b_ann_s = _fmt(b_stats.get("ann_ret", 0), "%")
                                b_sh_s = f'<span style="color:#8b949e;">{b_stats.get("sharpe", 0):.3f}</span>'
                                b_dd_s = f'<span style="color:#ef4444;">{b_stats.get("dd", 0):.2f}%</span>'
                                b_vol_s = f'<span style="color:#8b949e;">{b_stats.get("vol", 0):.2f}%</span>'

                                alpha = r_cum_ret - b_stats.get("cum_ret", 0)
                                alpha_c = "#22c55e" if alpha >= 0 else "#ef4444"

                                html_range = f"""
                                <div style="overflow-x:auto;">
                                <table style="width:100%;border-collapse:collapse;font-size:13px;">
                                <thead><tr style="background:#161b22;">
                                <th style="padding:7px 10px;color:#8b949e;text-align:left;font-size:12px;">指标</th>
                                <th style="padding:7px 10px;color:#58a6ff;text-align:center;font-size:12px;">投资组合</th>
                                <th style="padding:7px 10px;color:#f59e0b;text-align:center;font-size:12px;">{bname}</th>
                                <th style="padding:7px 10px;color:#c9d1d9;text-align:center;font-size:12px;">差异</th>
                                </tr></thead><tbody>
                                <tr style="border-bottom:1px solid #21262d;">
                                <td style="padding:6px 10px;color:#c9d1d9;">累计收益率</td>
                                <td style="padding:6px 10px;text-align:center;">{_fmt(r_cum_ret, "%")}</td>
                                <td style="padding:6px 10px;text-align:center;">{b_cum_s}</td>
                                <td style="padding:6px 10px;text-align:center;font-weight:bold;color:{alpha_c};">{alpha:+.2f}%</td>
                                </tr>
                                <tr style="background:#161b22;border-bottom:1px solid #21262d;">
                                <td style="padding:6px 10px;color:#c9d1d9;">年化收益率</td>
                                <td style="padding:6px 10px;text-align:center;">{_fmt(r_ann_ret, "%")}</td>
                                <td style="padding:6px 10px;text-align:center;">{b_ann_s}</td>
                                <td style="padding:6px 10px;text-align:center;">{_fmt(r_ann_ret - b_stats.get("ann_ret", 0), "%")}</td>
                                </tr>
                                <tr style="border-bottom:1px solid #21262d;">
                                <td style="padding:6px 10px;color:#c9d1d9;">夏普比率</td>
                                <td style="padding:6px 10px;text-align:center;"><span style="color:#8b949e;">{r_sharpe:.3f}</span></td>
                                <td style="padding:6px 10px;text-align:center;">{b_sh_s}</td>
                                <td style="padding:6px 10px;text-align:center;">{_fmt(r_sharpe - b_stats.get("sharpe", 0), dec=3)}</td>
                                </tr>
                                <tr style="background:#161b22;border-bottom:1px solid #21262d;">
                                <td style="padding:6px 10px;color:#c9d1d9;">最大回撤</td>
                                <td style="padding:6px 10px;text-align:center;"><span style="color:#ef4444;">{r_dd:.2f}%</span></td>
                                <td style="padding:6px 10px;text-align:center;">{b_dd_s}</td>
                                <td style="padding:6px 10px;text-align:center;">{_fmt(r_dd - b_stats.get("dd", 0), "%", inv=True)}</td>
                                </tr>
                                <tr style="border-bottom:1px solid #21262d;">
                                <td style="padding:6px 10px;color:#c9d1d9;">年化波动率</td>
                                <td style="padding:6px 10px;text-align:center;"><span style="color:#8b949e;">{r_vol:.2f}%</span></td>
                                <td style="padding:6px 10px;text-align:center;">{b_vol_s}</td>
                                <td style="padding:6px 10px;text-align:center;">{_fmt(r_vol - b_stats.get("vol", 0), "%", inv=True)}</td>
                                </tr>
                                <tr style="background:#161b22;border-bottom:1px solid #21262d;">
                                <td style="padding:6px 10px;color:#c9d1d9;">胜率</td>
                                <td style="padding:6px 10px;text-align:center;"><span style="color:#8b949e;">{r_wr:.1f}%</span></td>
                                <td style="padding:6px 10px;text-align:center;" colspan="2"><span style="color:#484f58;">--</span></td>
                                </tr>
                                <tr style="border-bottom:1px solid #21262d;">
                                <td style="padding:6px 10px;color:#c9d1d9;">盈亏比</td>
                                <td style="padding:6px 10px;text-align:center;"><span style="color:#8b949e;">{r_pnl_ratio:.2f}</span></td>
                                <td style="padding:6px 10px;text-align:center;" colspan="2"><span style="color:#484f58;">--</span></td>
                                </tr>
                                <tr style="background:#161b22;">
                                <td style="padding:6px 10px;color:#c9d1d9;">最佳/最差单日</td>
                                <td style="padding:6px 10px;text-align:center;">{_fmt(r_best_day, "%")} / {_fmt(r_worst_day, "%")}</td>
                                <td style="padding:6px 10px;text-align:center;" colspan="2"><span style="color:#484f58;">--</span></td>
                                </tr>
                                </tbody></table></div>"""
                                st.markdown(html_range, unsafe_allow_html=True)
                                st.caption(f"*区间: {start_dt} ~ {end_dt}，共 {n_days} 个交易日*")
                            else:
                                st.info("所选区间内交易日不足，请调整日期范围")
                        else:
                            st.warning("起始日期须早于结束日期")




def _render_tab2_position(tab2, positions, summary, selected_date):
    """Extracted from main() - tab2 renderer"""
    with tab2:
        st.caption("📊 展示持仓分布饼图、持仓明细表格、行业权重变化趋势及持仓相关性矩阵")

        # ===== ETF 多维筛选器 =====
        st.markdown(
            '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">'
            "ETF 智能筛选"
            '<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span>'
            '<span class="tip-text" style="left: 4px; top: calc(100% + 10px);">'
            "按行业、收益表现、持仓规模等维度筛选和排序持仓ETF，快速定位关注品种。"
            "</span></div>",
            unsafe_allow_html=True,
        )

        if not positions.empty:
            filter_col1, filter_col2, filter_col3 = st.columns(3)
            with filter_col1:
                held_sectors = set()
                for _, pos in positions.iterrows():
                    code = str(pos["code"])
                    cat_info = ETF_CATEGORIES.get(code)
                    if cat_info:
                        held_sectors.add(cat_info["sector"])
                filter_sector = st.selectbox(
                    "行业筛选",
                    ["全部"] + sorted(held_sectors),
                    key="etf_filter_sector",
                    label_visibility="collapsed",
                    format_func=lambda x: f"\U0001f4cb 行业: {x}" if x != "全部" else "\U0001f4cb 全部行业",
                )
            with filter_col2:
                filter_pnl = st.selectbox(
                    "收益状态",
                    ["全部", "盈利", "亏损", "高收益(>10%)", "深度亏损(<-10%)"],
                    key="etf_filter_pnl",
                    label_visibility="collapsed",
                    format_func=lambda x: f"\U0001f4b0 {x}",
                )
            with filter_col3:
                filter_sort = st.selectbox(
                    "排序方式",
                    [
                        "市值\u2193",
                        "市值\u2191",
                        "收益率\u2193",
                        "收益率\u2191",
                        "盈亏\u2193",
                        "盈亏\u2191",
                        "持仓量\u2193",
                        "持仓量\u2191",
                    ],
                    key="etf_filter_sort",
                    label_visibility="collapsed",
                    format_func=lambda x: f"\U0001f522 {x}",
                )

            filtered = positions.copy()
            if filter_sector != "全部":
                filtered = filtered[
                    filtered.apply(
                        lambda r: ETF_CATEGORIES.get(str(r["code"]), {}).get("sector") == filter_sector, axis=1
                    )
                ]
            if filter_pnl == "盈利":
                filtered = filtered[filtered["pnl"] > 0]
            elif filter_pnl == "亏损":
                filtered = filtered[filtered["pnl"] < 0]
            elif filter_pnl == "高收益(>10%)":
                filtered = filtered[filtered["pnl_rate"] > 10]
            elif filter_pnl == "深度亏损(<-10%)":
                filtered = filtered[filtered["pnl_rate"] < -10]

            sort_map = {
                "市值\u2193": ("market_value", False),
                "市值\u2191": ("market_value", True),
                "收益率\u2193": ("pnl_rate", False),
                "收益率\u2191": ("pnl_rate", True),
                "盈亏\u2193": ("pnl", False),
                "盈亏\u2191": ("pnl", True),
                "持仓量\u2193": ("quantity", False),
                "持仓量\u2191": ("quantity", True),
            }
            if filter_sort in sort_map:
                sort_col, ascending = sort_map[filter_sort]
                filtered = filtered.sort_values(sort_col, ascending=ascending)

            total_mv = positions["market_value"].sum()
            filtered_mv = filtered["market_value"].sum() if not filtered.empty else 0
            filter_ratio = filtered_mv / total_mv * 100 if total_mv > 0 else 0

            st.markdown(
                f'<div style="display:flex;gap:16px;padding:6px 0;font-size:12px;color:#8b949e;">'
                f'<span>筛选结果: <b style="color:#c9d1d9;">{len(filtered)}只</b> / {len(positions)}只</span>'
                f'<span>筛选市值: <b style="color:#c9d1d9;">\u00a5{filtered_mv:,.0f}</b> '
                f'(占比 <b style="color:#58a6ff;">{filter_ratio:.1f}%</b>)</span>'
                f"</div>",
                unsafe_allow_html=True,
            )

            if not filtered.empty:
                n_show = min(len(filtered), 8)
                card_cols = st.columns(min(n_show, 4))
                for idx, (_, frow) in enumerate(filtered.head(8).iterrows()):
                    code = str(frow["code"])
                    pnl_r = frow.get("pnl_rate", 0)
                    pnl_c = "#22c55e" if pnl_r >= 0 else "#ef4444"
                    sector = ETF_CATEGORIES.get(code, {}).get("sector", "未知")
                    s_color = SECTOR_COLORS.get(sector, "#8b949e")
                    with card_cols[idx % len(card_cols)]:
                        st.markdown(
                            f'<div style="padding:6px 8px;border-radius:6px;background:#161b22;'
                            f'border-left:3px solid {s_color};cursor:pointer;">'
                            f'<div style="font-size:11px;color:#c9d1d9;font-weight:bold;'
                            f'white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">{frow["name"]}</div>'
                            f'<div style="font-size:10px;color:#484f58;margin:2px 0;">{sector} | \u00a5{frow["market_value"]:,.0f}</div>'
                            f'<div style="font-size:12px;font-weight:bold;color:{pnl_c};">{pnl_r:+.2f}%</div>'
                            f"</div>",
                            unsafe_allow_html=True,
                        )

        st.markdown("---")

        col_dist, col_table = st.columns([1, 1])

        with col_dist:
            st.markdown(
                '<div class="tip-title" style="">持仓分布<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">以环形饼图展示各只ETF的市值占比，中心空白区域显示总持仓数量。鼠标悬停可查看具体金额和百分比。</span></div>',
                unsafe_allow_html=True,
            )
            if not positions.empty:
                fig_pie = go.Figure(
                    go.Pie(
                        labels=positions["name"],
                        values=positions["market_value"],
                        hole=0.45,
                        textinfo="label+percent",
                        textfont=dict(size=10),
                        marker=dict(
                            colors=[
                                "#58a6ff",
                                "#22c55e",
                                "#f59e0b",
                                "#ef4444",
                                "#a855f7",
                                "#06b6d4",
                                "#f97316",
                                "#ec4899",
                                "#84cc16",
                                "#6366f1",
                                "#14b8a6",
                                "#e11d48",
                                "#8b5cf6",
                                "#0ea5e9",
                                "#d946ef",
                                "#10b981",
                                "#f43f5e",
                                "#6d28d9",
                                "#0891b2",
                                "#c026d3",
                                "#65a30d",
                                "#be123c",
                                "#7c3aed",
                            ]
                        ),
                    )
                )
                fig_pie.update_layout(
                    height=400,
                    plot_bgcolor="#0d1117",
                    paper_bgcolor="#0d1117",
                    font=dict(color="#c9d1d9"),
                    margin=dict(l=10, r=10, t=10, b=10),
                    showlegend=False,
                )
                st.plotly_chart(fig_pie, width="stretch")

        with col_table:
            st.markdown(
                '<div class="tip-title" style="">持仓明细<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">展示所有持仓ETF的详细信息，包括名称、代码、持仓量、成本价、现价、市值、盈亏和收益率。点击下拉框可查看单只ETF的技术分析详情。</span></div>',
                unsafe_allow_html=True,
            )
            if not positions.empty:
                # 格式化显示列
                display_df = positions[
                    ["name", "code", "quantity", "cost_price", "current_price", "market_value", "pnl", "pnl_rate"]
                ].copy()
                display_df.columns = ["名称", "代码", "持仓量", "成本价", "现价", "市值", "盈亏", "收益率%"]
                display_df["持仓量"] = display_df["持仓量"].apply(lambda x: f"{x:,.0f}")
                display_df["成本价"] = display_df["成本价"].apply(lambda x: f"{x:.3f}")
                display_df["现价"] = display_df["现价"].apply(lambda x: f"{x:.3f}")
                display_df["市值"] = display_df["市值"].apply(lambda x: f"¥{x:,.0f}")
                display_df["盈亏"] = display_df["盈亏"].apply(lambda x: f"¥{x:,.0f}")
                display_df["收益率%"] = display_df["收益率%"].apply(lambda x: f"{x:+.2f}%")
                # 技术信号列
                signal_list = []
                if technical is not None and not technical.empty:
                    tech_by_code = technical.drop_duplicates("code", keep="first").set_index("code")
                    for _, pos_row in positions.iterrows():
                        code = str(pos_row["code"])
                        if code in tech_by_code.index:
                            tr = tech_by_code.loc[code]
                            parts = []
                            trend = tr.get("trend", "")
                            if trend == "上涨":
                                parts.append('<span style="color:#22c55e;">↑</span>')
                            elif trend == "下跌":
                                parts.append('<span style="color:#ef4444;">↓</span>')
                            else:
                                parts.append('<span style="color:#f59e0b;">→</span>')
                            ma = tr.get("ma_signal", "")
                            if ma == "多头排列":
                                parts.append('<span style="color:#22c55e;">多</span>')
                            elif ma == "空头排列":
                                parts.append('<span style="color:#ef4444;">空</span>')
                            macd = tr.get("macd_signal", "")
                            if macd == "金叉":
                                parts.append('<span style="color:#22c55e;">金</span>')
                            elif macd == "死叉":
                                parts.append('<span style="color:#ef4444;">死</span>')
                            rsi_st = tr.get("rsi_status", "")
                            if rsi_st in ("超买", "偏高"):
                                parts.append('<span style="color:#ef4444;">R高</span>')
                            elif rsi_st in ("超卖", "偏低"):
                                parts.append('<span style="color:#22c55e;">R低</span>')
                            signal_list.append(" ".join(parts))
                        else:
                            signal_list.append('<span style="color:#484f58;">--</span>')
                else:
                    signal_list = ['<span style="color:#484f58;">--</span>'] * len(positions)

                display_df["技术信号"] = signal_list

                # HTML表格渲染（st.dataframe不支持HTML标签）
                html_rows = []
                for idx, row_data in display_df.iterrows():
                    zebra = "background:#161b22;" if idx % 2 == 0 else ""
                    html_rows.append(
                        f'<tr style="{zebra}">'
                        f'<td style="padding:5px 8px;color:#c9d1d9;border-bottom:1px solid #21262d;white-space:nowrap;">{row_data["名称"]}</td>'
                        f'<td style="padding:5px 8px;color:#8b949e;border-bottom:1px solid #21262d;">{row_data["代码"]}</td>'
                        f'<td style="padding:5px 8px;text-align:right;color:#c9d1d9;border-bottom:1px solid #21262d;">{row_data["持仓量"]}</td>'
                        f'<td style="padding:5px 8px;text-align:right;color:#c9d1d9;border-bottom:1px solid #21262d;">{row_data["成本价"]}</td>'
                        f'<td style="padding:5px 8px;text-align:right;color:#c9d1d9;border-bottom:1px solid #21262d;">{row_data["现价"]}</td>'
                        f'<td style="padding:5px 8px;text-align:right;color:#c9d1d9;border-bottom:1px solid #21262d;">{row_data["市值"]}</td>'
                        f'<td style="padding:5px 8px;text-align:right;border-bottom:1px solid #21262d;">{row_data["盈亏"]}</td>'
                        f'<td style="padding:5px 8px;text-align:right;border-bottom:1px solid #21262d;">{row_data["收益率%"]}</td>'
                        f'<td style="padding:5px 8px;text-align:center;border-bottom:1px solid #21262d;white-space:nowrap;">{row_data["技术信号"]}</td>'
                        f"</tr>"
                    )

                st.markdown(
                    f'<div style="overflow-x:auto;"><table style="width:100%;border-collapse:collapse;font-size:12px;">'
                    f'<thead><tr style="background:#0d1117;">'
                    f'<th style="padding:6px 8px;color:#8b949e;text-align:left;font-size:11px;">名称</th>'
                    f'<th style="padding:6px 8px;color:#8b949e;text-align:left;font-size:11px;">代码</th>'
                    f'<th style="padding:6px 8px;color:#8b949e;text-align:right;font-size:11px;">持仓量</th>'
                    f'<th style="padding:6px 8px;color:#8b949e;text-align:right;font-size:11px;">成本价</th>'
                    f'<th style="padding:6px 8px;color:#8b949e;text-align:right;font-size:11px;">现价</th>'
                    f'<th style="padding:6px 8px;color:#8b949e;text-align:right;font-size:11px;">市值</th>'
                    f'<th style="padding:6px 8px;color:#8b949e;text-align:right;font-size:11px;">盈亏</th>'
                    f'<th style="padding:6px 8px;color:#8b949e;text-align:right;font-size:11px;">收益率%</th>'
                    f'<th style="padding:6px 8px;color:#8b949e;text-align:center;font-size:11px;">技术信号</th>'
                    f'</tr></thead><tbody>{"".join(html_rows)}</tbody></table></div>',
                    unsafe_allow_html=True,
                )

        # ETF 详情选择器（点击持仓表格行或下拉框选择）
        if not positions.empty:
            selected_etf = st.selectbox(
                "查看 ETF 详细分析",
                options=["-- 请选择 --"] + [f"{r['name']}（{r['code']}）" for _, r in positions.iterrows()],
                key="etf_detail_selector",
                label_visibility="collapsed",
            )
            if selected_etf and selected_etf != "-- 请选择 --":
                match = positions[positions.apply(lambda r: f"{r['name']}（{r['code']}）" == selected_etf, axis=1)]
                if not match.empty:
                    row = match.iloc[0]
                    st.markdown(
                        f'<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">{row["name"]}（{row["code"]}）详细分析<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span></div>',
                        unsafe_allow_html=True,
                    )
                    _render_etf_detail_panel(row, selected_date, total_value)

        # ===== 行业权重堆叠面积图 =====
        st.markdown(
            '<div class="tip-title" style="">行业权重变化趋势<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">以堆叠面积图展示各行业ETF在组合中的权重占比随时间的变化，可观察仓位配置的调整趋势。</span></div>',
            unsafe_allow_html=True,
        )
        sector_weight_df, sector_colors = load_sector_weights(days=show_days, end_date=selected_date)
        if not sector_weight_df.empty:
            fig_sector = go.Figure()
            for col in sector_weight_df.columns:
                fig_sector.add_trace(
                    go.Scatter(
                        x=sector_weight_df.index,
                        y=sector_weight_df[col],
                        name=col,
                        mode="lines",
                        stackgroup="one",
                        line=dict(width=0.5),
                        fillcolor=sector_colors.get(col, "#6b7280"),
                        hovertemplate=f"<b>{col}</b><br>权重: %{{y:.1f}}%<extra></extra>",
                    )
                )
            fig_sector.update_layout(
                height=280,
                plot_bgcolor="#0d1117",
                paper_bgcolor="#0d1117",
                font=dict(color="#c9d1d9", size=11),
                margin=dict(l=50, r=20, t=10, b=40),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(size=10)),
                xaxis=dict(showgrid=False, tickformat="%m-%d"),
                yaxis=dict(title="权重 %", showgrid=True, gridcolor="#21262d"),
                hovermode="x unified",
            )
            st.plotly_chart(fig_sector, width="stretch")

            # 行业权重摘要卡片
            latest_weights = sector_weight_df.iloc[-1]
            n_sectors = len(latest_weights[latest_weights > 1])
            max_sector = latest_weights.idxmax()
            min_sector = latest_weights[latest_weights > 0].idxmin()
            st.caption(
                f"覆盖 {n_sectors} 个行业 | 最大: **{max_sector}** {latest_weights[max_sector]:.1f}% | "
                f"最小: **{min_sector}** {latest_weights[min_sector]:.1f}% | 数据截至 {selected_date}"
            )
        else:
            st.info("持仓历史数据不足，暂无法展示行业权重变化")

        st.markdown("---")
        # ===== 相关性矩阵热力图 =====
        st.markdown("---")
        st.markdown(
            '<div class="tip-title" style="">持仓相关性矩阵（日收益率 Pearson）<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">基于最近250个交易日的日收益率计算各ETF之间的Pearson相关系数。数值接近1表示同向变动，接近-1表示反向变动，接近0表示无相关性。</span></div>',
            unsafe_allow_html=True,
        )
        corr_df, short_names = load_correlation_matrix(days=250, end_date=selected_date)
        if not corr_df.empty and len(short_names) >= 2:
            fig_corr = go.Figure(
                go.Heatmap(
                    z=corr_df.values,
                    x=[short_names.get(c, c) for c in corr_df.columns],
                    y=[short_names.get(c, c) for c in corr_df.index],
                    colorscale=[[0, "#0d419d"], [0.25, "#1a6bb5"], [0.5, "#21262d"], [0.75, "#b5411a"], [1, "#9d0d0d"]],
                    zmin=-1,
                    zmax=1,
                    text=corr_df.values.round(2),
                    texttemplate="%{text}",
                    textfont=dict(size=9),
                    hovertemplate="<b>%{x} vs %{y}</b><br>相关系数: %{z:.3f}<extra></extra>",
                    colorbar=dict(thickness=15, len=0.9, outlinewidth=0, tickfont=dict(size=10, color="#8b949e")),
                )
            )
            fig_corr.update_layout(
                height=max(500, len(corr_df) * 28),
                plot_bgcolor="#0d1117",
                paper_bgcolor="#0d1117",
                font=dict(color="#c9d1d9", size=11),
                margin=dict(l=5, r=40, t=10, b=5),
                xaxis=dict(tickangle=45, side="bottom", tickfont=dict(size=9)),
                yaxis=dict(tickfont=dict(size=9), autorange="reversed"),
            )
            fig_corr.update_xaxes(showgrid=False)
            fig_corr.update_yaxes(showgrid=False)
            st.plotly_chart(fig_corr, width="stretch")
            st.caption(f"基于最近250个交易日的市值日收益率计算 | 数据截至 {selected_date}")
        else:
            st.info("持仓数据不足，暂无法计算相关性矩阵")

        # ---------- 累计盈亏柱状图 ----------
        if not positions.empty:
            st.markdown(
                '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">各ETF累计盈亏<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">以柱状图展示每只ETF的累计盈亏金额，绿色为盈利、红色为亏损，一目了然地识别组合中的盈利与亏损来源。</span></div>',
                unsafe_allow_html=True,
            )
            pnl_sorted = positions.sort_values("pnl", ascending=True)
            colors = ["#ef4444" if v < 0 else "#22c55e" for v in pnl_sorted["pnl"]]
            fig_pnl = go.Figure(
                go.Bar(
                    y=pnl_sorted["name"],
                    x=pnl_sorted["pnl"],
                    orientation="h",
                    marker_color=colors,
                    text=[f"¥{v:,.0f}" for v in pnl_sorted["pnl"]],
                    textposition="outside",
                    textfont=dict(size=10, color="#c9d1d9"),
                    hovertemplate="<b>%{y}</b><br>累计盈亏: ¥%{x:,.0f}<extra></extra>",
                )
            )
            fig_pnl.update_layout(
                height=max(300, len(pnl_sorted) * 32),
                plot_bgcolor="#0d1117",
                paper_bgcolor="#0d1117",
                font=dict(color="#c9d1d9", size=11),
                margin=dict(l=120, r=60, t=15, b=30),
                xaxis=dict(
                    title="盈亏金额 (¥)",
                    showgrid=True,
                    gridcolor="#21262d",
                    tickformat=",.0f",
                    zeroline=True,
                    zerolinecolor="#30363d",
                    zerolinewidth=1,
                ),
                yaxis=dict(
                    showgrid=False,
                    tickfont=dict(size=10),
                ),
                bargap=0.35,
            )
            st.plotly_chart(fig_pnl, width="stretch")
            # 汇总统计
            total_pnl = positions["pnl"].sum()
            profit_positions = positions[positions["pnl"] > 0]
            loss_positions = positions[positions["pnl"] < 0]
            st.markdown(
                f'<div style="display:flex;gap:20px;font-size:13px;padding:8px 0;">'
                f'<span style="color:#8b949e;">总盈亏: <b style="color:{"#22c55e" if total_pnl >= 0 else "#ef4444"};">¥{total_pnl:,.0f}</b></span>'
                f'<span style="color:#8b949e;">盈利: <b style="color:#22c55e;">{len(profit_positions)}只 / ¥{profit_positions["pnl"].sum():,.0f}</b></span>'
                f'<span style="color:#8b949e;">亏损: <b style="color:#ef4444;">{len(loss_positions)}只 / ¥{loss_positions["pnl"].sum():,.0f}</b></span>'
                f"</div>",
                unsafe_allow_html=True,
            )




def _render_tab3_risk(tab3, positions, summary, technical, selected_date, ext_risk):
    """Extracted from main() - tab3 renderer"""
    with tab3:
        st.caption("⚠️ 展示风险评分仪表盘、风险指标详情、回撤曲线及Brinson收益归因分析")
        col_risk_gauge, col_risk_detail = st.columns([1, 1])

        with col_risk_gauge:
            st.markdown(
                '<div class="tip-title" style="">风险指标仪表盘<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">综合年化波动率和最大回撤计算风险评分（0-100分）。满分100表示低风险，低于60分表示高风险。颜色越绿越安全，越红风险越高。</span></div>',
                unsafe_allow_html=True,
            )

            # 风险评分（使用基于持仓稳定后数据的指标）
            risk_score = compute_risk_score(volatility, effective_max_dd, sharpe)
            risk_color = get_risk_color(risk_score)
            risk_label = get_risk_label(risk_score)

            fig_gauge = go.Figure(
                go.Indicator(
                    mode="gauge+number",
                    value=risk_score,
                    number={"suffix": "分", "font": {"size": 40, "color": risk_color}},
                    gauge={
                        "axis": {"range": [0, 100], "tickcolor": "#8b949e", "tickfont": {"size": 10}},
                        "bar": {"color": risk_color},
                        "bgcolor": "#161b22",
                        "steps": [
                            {"range": [0, 40], "color": "rgba(239,68,68,0.15)"},
                            {"range": [40, 70], "color": "rgba(245,158,11,0.15)"},
                            {"range": [70, 100], "color": "rgba(34,197,94,0.15)"},
                        ],
                        "threshold": {"line": {"color": risk_color, "width": 3}, "thickness": 0.8, "value": risk_score},
                    },
                )
            )
            fig_gauge.update_layout(
                height=250,
                plot_bgcolor="#0d1117",
                paper_bgcolor="#0d1117",
                font=dict(color="#c9d1d9"),
                margin=dict(l=30, r=30, t=10, b=10),
            )
            st.plotly_chart(fig_gauge, width="stretch")

            st.markdown(
                f'<div style="text-align:center;color:{risk_color};font-size:16px;font-weight:bold;">'
                f"{risk_label}</div>",
                unsafe_allow_html=True,
            )

        with col_risk_detail:
            st.markdown(
                '<div class="tip-title" style="">风险指标详情<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">展示夏普比率、Sortino比率、Calmar比率、最大回撤、年化波动率、胜率和盈亏比等核心风险指标，悬停可查看指标含义。</span></div>',
                unsafe_allow_html=True,
            )


            risk_metrics = [
                ("夏普比率", sharpe, "衡量风险调整后收益，>1为优秀"),
                ("Sortino比率", ext_risk.get("sortino", np.nan), "仅考虑下行波动的风险调整收益"),
                ("Calmar比率", ext_risk.get("calmar", np.nan), "年化收益 / 最大回撤，越高越好"),
                ("最大回撤", effective_max_dd, "历史最大亏损幅度（持仓稳定后）"),
                ("年化波动率", volatility, "收益率的标准差，越高越不稳定"),
                ("胜率", ext_risk.get("win_rate", np.nan), "盈利天数 / 有盈亏交易天数"),
                ("盈亏比", ext_risk.get("pl_ratio", np.nan), "平均盈利 / 平均亏损，>1为优"),
                ("最大连续盈利", ext_risk.get("max_consec_win", 0), "历史最长连续盈利天数"),
                ("最大连续亏损", ext_risk.get("max_consec_loss", 0), "历史最长连续亏损天数"),
                ("最大回撤持续", ext_risk.get("max_dd_duration", 0), "历史最长回撤恢复天数（净值低于峰值）"),
                ("偏度", ext_risk.get("skewness", np.nan), "收益率分布偏斜，正值为右偏"),
                ("峰度", ext_risk.get("kurtosis", np.nan), "收益率分布尾部厚度，>0为尖峰"),
                (
                    "持仓盈亏比",
                    f"{profit_count}:{loss_count}" if profit_count or loss_count else "N/A",
                    f"盈利{profit_count}只 vs 亏损{loss_count}只",
                ),
                ("数据周期", f"{len(summary)}天" if not summary.empty else "N/A", "历史数据积累天数"),
            ]

            for name, value, desc in risk_metrics:
                if isinstance(value, float) and not np.isnan(value):
                    val_str = f"{value:.3f}" if abs(value) < 1 else f"{value:.2f}"
                elif value is None or (isinstance(value, float) and np.isnan(value)):
                    val_str = '<span style="color:#888;">N/A</span>'
                else:
                    val_str = str(value)

                st.markdown(
                    f'<div style="padding:8px 12px;border-bottom:1px solid #21262d;">'
                    f'<div style="display:flex;justify-content:space-between;">'
                    f'<span style="color:#8b949e;font-size:13px;">{name}</span>'
                    f'<span style="color:#c9d1d9;font-size:13px;font-weight:bold;">{val_str}</span>'
                    f"</div>"
                    f'<div style="font-size:11px;color:#484f58;">{desc}</div>'
                    f"</div>",
                    unsafe_allow_html=True,
                )

        # 回撤曲线（降采样）
        if not summary.empty and len(summary) > 5:
            st.markdown(
                '<div class="tip-title" style="">回撤曲线<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">展示组合从历史最高点到当前市值的回撤幅度(%)。可识别最大回撤区间及其恢复时间，评估组合的抗风险能力。</span></div>',
                unsafe_allow_html=True,
            )
            dd_data = summary[["date", "total_value"]].copy()
            dd_data["drawdown"] = (
                (dd_data["total_value"] - dd_data["total_value"].cummax()) / dd_data["total_value"].cummax() * 100
            )
            dd_chart = downsample(dd_data, max_points=500)

            fig_dd = go.Figure()
            fig_dd.add_trace(
                go.Scatter(
                    x=dd_chart["date"],
                    y=dd_chart["drawdown"],
                    mode="lines",
                    name="回撤",
                    fill="tozeroy",
                    line=dict(color="#ef4444", width=1.5),
                    fillcolor="rgba(239,68,68,0.15)",
                )
            )
            # 标记最大回撤
            _add_min_max_annotations(fig_dd, dd_chart["date"], dd_chart["drawdown"], y_label="回撤")

            fig_dd.update_layout(
                height=200,
                plot_bgcolor="#0d1117",
                paper_bgcolor="#0d1117",
                font=dict(color="#c9d1d9", size=11),
                margin=dict(l=50, r=20, t=10, b=40),
                xaxis=dict(showgrid=False),
                yaxis=dict(title="回撤 (%)", showgrid=True, gridcolor="#21262d"),
            )
            st.plotly_chart(fig_dd, width="stretch")

        # ===== P2: 收益归因分析（Brinson模型） =====
        st.markdown("---")
        st.markdown(
            '<div class="tip-title" style="">收益归因分析（Brinson 模型）<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">使用Brinson归因模型将组合超额收益分解为「配置效应」（超配/低配行业的贡献）和「选股效应」（行业内个股选择的贡献），帮助判断收益来源。</span></div>',
            unsafe_allow_html=True,
        )
        st.caption("将组合收益分解为行业配置效应（超配/低配行业的贡献）和选股效应（行业内个股选择的贡献）")

        attr_result = compute_return_attribution(days=show_days, end_date=selected_date)
        if attr_result and attr_result.get("sector_returns"):
            ar = attr_result

            # 瀑布图数据
            waterfall_labels = ["基准收益"]
            waterfall_values = [ar["benchmark_return"] * 100]
            waterfall_colors = ["#8b949e"]

            # 配置效应
            alloc_total = 0
            for sector, val in sorted(ar["allocation_effect"].items(), key=lambda x: abs(x[1]), reverse=True):
                if abs(val) > 0.001:  # > 0.1% 才显示
                    waterfall_labels.append(f"{sector}\n配置")
                    waterfall_values.append(val * 100)
                    waterfall_colors.append("#22c55e" if val > 0 else "#ef4444")
                    alloc_total += val

            # 选股效应
            sel_total = 0
            for sector, val in sorted(ar["selection_effect"].items(), key=lambda x: abs(x[1]), reverse=True):
                if abs(val) > 0.001:
                    waterfall_labels.append(f"{sector}\n选股")
                    waterfall_values.append(val * 100)
                    waterfall_colors.append("#58a6ff" if val > 0 else "#f59e0b")
                    sel_total += val

            waterfall_labels.append("组合收益")
            waterfall_values.append(ar["total_return"] * 100)
            waterfall_colors.append("#a855f7")

            # 计算瀑布图中间值
            running = 0
            y_data = []
            for i, v in enumerate(waterfall_values):
                if i == 0 or i == len(waterfall_values) - 1:
                    y_data.append(v)
                    running = v
                else:
                    y_data.append(running + v)
                    running += v

            # 底部坐标（从上一个running开始）
            base_data = [0]  # 基准从0开始
            run = waterfall_values[0]
            for i in range(1, len(waterfall_values) - 1):
                base_data.append(run)
                run += waterfall_values[i]
            base_data.append(0)  # 组合收益从0开始

            fig_wf = go.Figure()
            fig_wf.add_trace(
                go.Bar(
                    x=waterfall_labels,
                    y=[
                        v if i == 0 or i == len(waterfall_values) - 1 else abs(v)
                        for i, v in enumerate(waterfall_values)
                    ],
                    base=base_data,
                    marker_color=waterfall_colors,
                    text=[f"{v:+.2f}%" for v in waterfall_values],
                    textposition="outside",
                    textfont=dict(size=9, color="#c9d1d9"),
                    hovertemplate="<b>%{x}</b><br>贡献: %{text}<extra></extra>",
                )
            )
            fig_wf.update_layout(
                height=max(350, len(waterfall_labels) * 22),
                plot_bgcolor="#0d1117",
                paper_bgcolor="#0d1117",
                font=dict(color="#c9d1d9", size=11),
                margin=dict(l=50, r=20, t=10, b=80),
                xaxis=dict(tickangle=45, tickfont=dict(size=8)),
                yaxis=dict(title="收益率 (%)", showgrid=True, gridcolor="#21262d"),
                showlegend=False,
                barmode="relative",
            )
            st.plotly_chart(fig_wf, width="stretch")

            # 归因摘要卡片
            col_attr1, col_attr2, col_attr3 = st.columns(3)
            with col_attr1:
                st.metric("组合收益", f"{ar['total_return']*100:+.2f}%")
            with col_attr2:
                st.metric("基准收益", f"{ar['benchmark_return']*100:+.2f}%")
            with col_attr3:
                alpha = (ar["total_return"] - ar["benchmark_return"]) * 100
                st.metric("超额收益 (Alpha)", f"{alpha:+.2f}%")

            # 行业明细表
            with st.expander("查看行业归因明细", expanded=False):
                attr_rows = []
                for sector in sorted(
                    set(list(ar["sector_weights"].keys()) + list(ar.get("allocation_effect", {}).keys()))
                ):
                    attr_rows.append(
                        {
                            "行业": sector,
                            "组合权重": f"{ar['sector_weights'].get(sector, 0)*100:.1f}%",
                            "行业收益": f"{ar['sector_returns'].get(sector, 0)*100:+.2f}%",
                            "配置效应": f"{ar['allocation_effect'].get(sector, 0)*100:+.3f}%",
                            "选股效应": f"{ar['selection_effect'].get(sector, 0)*100:+.3f}%",
                        }
                    )
                if attr_rows:
                    st.markdown(pd.DataFrame(attr_rows).to_html(index=False, escape=False), unsafe_allow_html=True)
        else:
            st.info("历史数据不足（需要至少250个交易日），暂无法进行收益归因分析")

        # ===== P2b: 多因子归因分析 =====
        st.markdown("---")
        st.markdown(
            '<div class="tip-title" style="">多因子归因分析<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">基于 A 股公开指数构造市场/规模/价值/动量/质量五因子模型，量化各因子对组合收益的贡献。</span></div>',
            unsafe_allow_html=True,
        )
        st.caption("将组合日收益对五因子做 OLS 回归，分解为 Alpha（选股能力）和 Beta（因子暴露）贡献")

        try:
            from src.analysis.factor_attribution import (
                FACTOR_DESCRIPTION,
                FACTOR_NAME_MAP,
                run_full_attribution,
            )

            conn_attr = get_db_connection()
            try:
                attr_full = run_full_attribution(conn_attr, positions, ETF_CATEGORIES, lookback_days=250)
            finally:
                conn_attr.close()

            fa = attr_full.get("factor_attribution", {})
            if fa and "error" not in fa and fa.get("n_obs", 0) >= 30:
                col_fa1, col_fa2, col_fa3 = st.columns(3)
                with col_fa1:
                    alpha_val = fa.get("alpha", 0)
                    st.metric(
                        "Alpha (年化)",
                        f"{alpha_val:+.2f}%",
                        delta=f"贡献占比 {fa.get('alpha_contribution_pct', 0):+.1f}%",
                    )
                with col_fa2:
                    r2 = fa.get("r_squared", 0)
                    st.metric("模型 R\u00b2", f"{r2:.1%}", help="因子模型解释力，越高说明收益越可被因子解释")
                with col_fa3:
                    n_obs = fa.get("n_obs", 0)
                    st.metric("回归区间", f"{n_obs} 个交易日", help=fa.get("regression_period", ""))

                beta_factors = fa.get("beta_factors", {})
                if beta_factors:
                    factor_names = [FACTOR_NAME_MAP.get(k, k) for k in beta_factors.keys()]
                    factor_betas = list(beta_factors.values())
                    factor_colors = ["#58a6ff", "#f59e0b", "#22c55e", "#a855f7", "#ef4444"][: len(factor_names)]
                    fig_beta = go.Figure(
                        go.Bar(
                            orientation="h",
                            y=factor_names,
                            x=factor_betas,
                            marker_color=factor_colors,
                            text=[f"{v:.3f}" for v in factor_betas],
                            textposition="auto",
                            textfont=dict(size=11, color="#c9d1d9"),
                        )
                    )
                    fig_beta.add_vline(x=0, line_dash="dash", line_color="#484f58", opacity=0.6)
                    fig_beta.add_vline(x=1, line_dash="dot", line_color="#6e7681", opacity=0.3)
                    fig_beta.update_layout(
                        xaxis=dict(
                            title="因子暴露度 (Beta)", gridcolor="#21262d", tickfont=dict(size=10, color="#8b949e")
                        ),
                        yaxis=dict(title="", tickfont=dict(size=11, color="#c9d1d9")),
                        paper_bgcolor="#0d1117",
                        plot_bgcolor="#0d1117",
                        height=max(250, 35 * len(factor_names)),
                        margin=dict(l=100, r=30, t=10, b=30),
                        bargap=0.3,
                    )
                    st.plotly_chart(fig_beta, width="stretch")

                contributions = fa.get("factor_contributions", {})
                if contributions:
                    col_pie, col_detail = st.columns([1, 1])
                    with col_pie:
                        pie_labels, pie_values, pie_colors_list = [], [], []
                        color_map_pie = {
                            "Rm_Rf": "#58a6ff",
                            "SMB": "#f59e0b",
                            "HML": "#22c55e",
                            "MOM": "#a855f7",
                            "QMJ": "#ef4444",
                        }
                        for fname, finfo in contributions.items():
                            cp = abs(finfo.get("contribution_pct", 0))
                            if cp > 0.5:
                                pie_labels.append(FACTOR_NAME_MAP.get(fname, fname))
                                pie_values.append(cp)
                                pie_colors_list.append(color_map_pie.get(fname, "#8b949e"))
                        ap = abs(fa.get("alpha_contribution_pct", 0))
                        if ap > 0.5:
                            pie_labels.append("Alpha")
                            pie_values.append(ap)
                            pie_colors_list.append("#ffffff")
                        if pie_labels:
                            fig_pie = go.Figure(
                                go.Pie(
                                    labels=pie_labels,
                                    values=pie_values,
                                    marker_colors=pie_colors_list,
                                    textinfo="label+percent",
                                    textfont=dict(size=11, color="#c9d1d9"),
                                    hole=0.4,
                                )
                            )
                            fig_pie.update_layout(
                                paper_bgcolor="#0d1117",
                                plot_bgcolor="#0d1117",
                                height=300,
                                margin=dict(t=10, b=10, l=10, r=10),
                                showlegend=False,
                            )
                            st.plotly_chart(fig_pie, width="stretch")

                    with col_detail:
                        detail_rows = []
                        for fname, finfo in contributions.items():
                            detail_rows.append(
                                {
                                    "因子": FACTOR_NAME_MAP.get(fname, fname),
                                    "Beta": f"{finfo['beta']:.3f}",
                                    "收益贡献": f"{finfo['contribution']*100:+.2f}%",
                                    "贡献占比": f"{finfo['contribution_pct']:+.1f}%",
                                }
                            )
                        detail_rows.append(
                            {
                                "因子": "Alpha",
                                "Beta": "-",
                                "收益贡献": f"{fa.get('alpha',0):+.2f}%(年化)",
                                "贡献占比": f"{fa.get('alpha_contribution_pct',0):+.1f}%",
                            }
                        )
                        st.markdown(
                            pd.DataFrame(detail_rows).to_html(index=False, escape=False), unsafe_allow_html=True
                        )
            else:
                err_msg = fa.get("error", "数据不足") if fa else "因子归因计算失败"
                st.info(f"多因子归因: {err_msg}")
        except Exception as e:
            st.info(f"多因子归因模块暂不可用: {str(e)[:80]}")

        # ---------- 风险提示面板 ----------
        if not positions.empty:
            st.markdown(
                '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">风险提示<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">基于当前持仓结构和风险指标，自动识别并提示需要关注的风险因素。</span></div>',
                unsafe_allow_html=True,
            )

            warnings = []
            import math
            warnings = get_warnings(positions, effective_max_dd, volatility, sharpe, profit_count, loss_count)

            # 渲染风险提示
            if warnings:
                for icon, title, desc in warnings:
                    # 根据等级设置背景色
                    if "🔴" in icon:
                        bg_color = "rgba(239,68,68,0.08)"
                        border_color = "rgba(239,68,68,0.3)"
                    elif "🟡" in icon:
                        bg_color = "rgba(245,158,11,0.08)"
                        border_color = "rgba(245,158,11,0.3)"
                    else:
                        bg_color = "rgba(34,197,94,0.06)"
                        border_color = "rgba(34,197,94,0.2)"
                    st.markdown(
                        f'<div style="background:{bg_color};border:1px solid {border_color};border-radius:6px;padding:10px 14px;margin-bottom:6px;">'
                        f'<div style="font-size:13px;font-weight:bold;color:#c9d1d9;">{icon} {title}</div>'
                        f'<div style="font-size:12px;color:#8b949e;margin-top:3px;">{desc}</div>'
                        f"</div>",
                        unsafe_allow_html=True,
                    )
            else:
                st.markdown(
                    '<div style="background:rgba(34,197,94,0.06);border:1px solid rgba(34,197,94,0.2);border-radius:6px;padding:12px 14px;">'
                    '<div style="font-size:13px;color:#22c55e;font-weight:bold;">🟢 风险状况良好</div>'
                    '<div style="font-size:12px;color:#8b949e;margin-top:3px;">当前未检测到显著风险因素，继续保持关注。</div>'
                    "</div>",
                    unsafe_allow_html=True,
                )

        # ===== P2c: 风格暴露分析 =====
        st.markdown("---")
        st.markdown(
            '<div class="tip-title" style="">风格暴露分析<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">基于持仓 ETF 的分类标签，加权计算组合在规模、风格、行业三个维度的暴露度。</span></div>',
            unsafe_allow_html=True,
        )

        try:
            from src.analysis.factor_attribution import compute_style_exposure

            style_exp = compute_style_exposure(positions, ETF_CATEGORIES)
            if style_exp:
                col_size, col_style, col_sect = st.columns([1, 1, 1])

                # 规模暴露
                with col_size:
                    st.markdown("**规模维度**")
                    size_exp = style_exp.get("size_exposure", {})
                    if size_exp:
                        fig_size = go.Figure(
                            go.Pie(
                                labels=list(size_exp.keys()),
                                values=list(size_exp.values()),
                                marker_colors=["#58a6ff", "#f59e0b", "#a855f7"],
                                textinfo="label+percent",
                                textfont=dict(size=11, color="#c9d1d9"),
                                hole=0.5,
                            )
                        )
                        fig_size.update_layout(
                            paper_bgcolor="#0d1117",
                            plot_bgcolor="#0d1117",
                            height=220,
                            margin=dict(t=5, b=5, l=5, r=5),
                            showlegend=False,
                        )
                        st.plotly_chart(fig_size, width="stretch")

                # 风格暴露
                with col_style:
                    st.markdown("**风格维度**")
                    style_exp_d = style_exp.get("style_exposure", {})
                    if style_exp_d:
                        fig_sty = go.Figure(
                            go.Pie(
                                labels=list(style_exp_d.keys()),
                                values=list(style_exp_d.values()),
                                marker_colors=["#22c55e", "#ef4444", "#8b949e"],
                                textinfo="label+percent",
                                textfont=dict(size=11, color="#c9d1d9"),
                                hole=0.5,
                            )
                        )
                        fig_sty.update_layout(
                            paper_bgcolor="#0d1117",
                            plot_bgcolor="#0d1117",
                            height=220,
                            margin=dict(t=5, b=5, l=5, r=5),
                            showlegend=False,
                        )
                        st.plotly_chart(fig_sty, width="stretch")

                # 行业暴露
                with col_sect:
                    st.markdown("**行业维度**")
                    sector_exp = style_exp.get("sector_exposure", {})
                    if sector_exp:
                        sec_labels = list(sector_exp.keys())[:8]
                        sec_values = list(sector_exp.values())[:8]
                        fig_sec = go.Figure(
                            go.Bar(
                                orientation="h",
                                y=sec_labels,
                                x=sec_values,
                                marker_color="#58a6ff",
                                text=[f"{v:.1f}%" for v in sec_values],
                                textposition="auto",
                                textfont=dict(size=10, color="#c9d1d9"),
                            )
                        )
                        fig_sec.update_layout(
                            xaxis=dict(title="权重%", gridcolor="#21262d", tickfont=dict(size=9, color="#8b949e")),
                            yaxis=dict(title="", tickfont=dict(size=9, color="#c9d1d9")),
                            paper_bgcolor="#0d1117",
                            plot_bgcolor="#0d1117",
                            height=220,
                            margin=dict(l=60, r=20, t=5, b=25),
                            bargap=0.3,
                        )
                        st.plotly_chart(fig_sec, width="stretch")

                # 风格雷达图
                size_e = style_exp.get("size_exposure", {})
                style_e = style_exp.get("style_exposure", {})
                if size_e or style_e:
                    radar_cats = []
                    radar_vals = []
                    for k, v in size_e.items():
                        radar_cats.append(f"规模-{k}")
                        radar_vals.append(v)
                    for k, v in style_e.items():
                        radar_cats.append(f"风格-{k}")
                        radar_vals.append(v)

                    fig_radar_style = go.Figure(
                        go.Scatterpolar(
                            r=radar_vals,
                            theta=radar_cats,
                            fill="toself",
                            fillcolor="rgba(88,166,255,0.15)",
                            line=dict(color="#58a6ff", width=2),
                            marker=dict(size=6, color="#58a6ff"),
                        )
                    )
                    fig_radar_style.update_layout(
                        polar=dict(
                            radialaxis=dict(
                                visible=True,
                                tickfont=dict(size=9, color="#6e7681"),
                                gridcolor="#21262d",
                                range=[0, max(radar_vals) * 1.3] if radar_vals else [0, 100],
                            ),
                            angularaxis=dict(tickfont=dict(size=10, color="#c9d1d9"), gridcolor="#21262d"),
                            bgcolor="#0d1117",
                        ),
                        paper_bgcolor="#0d1117",
                        plot_bgcolor="#0d1117",
                        height=300,
                        margin=dict(t=10, b=10, l=10, r=10),
                        showlegend=False,
                    )
                    st.plotly_chart(fig_radar_style, width="stretch")
        except Exception as e:
            st.info(f"风格暴露分析暂不可用: {str(e)[:80]}")

        # ===== P2d: 行业轮动分析 =====
        st.markdown("---")
        st.markdown(
            '<div class="tip-title" style="">行业轮动分析<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">追踪各行业指数在不同时间窗口的收益排名变化，衡量市场轮动速度。</span></div>',
            unsafe_allow_html=True,
        )

        try:
            from src.analysis.factor_attribution import compute_sector_rotation

            conn_rot = get_db_connection()
            try:
                rotation = compute_sector_rotation(conn_rot)
            finally:
                conn_rot.close()

            if rotation and "error" not in rotation:
                # 轮动速度指标
                rot_speed = rotation.get("rotation_speed", {})
                if rot_speed:
                    col_rs = st.columns(len(rot_speed))
                    for ci, (period, speed) in enumerate(rot_speed.items()):
                        with col_rs[ci]:
                            st.metric(
                                f"轮动速度 ({period})", f"{speed:.1f}", help=f"行业收益标准差，值越大说明行业分化越明显"
                            )
                    st.caption("轮动速度 = 行业收益率标准差，反映行业分化程度。高轮动速度意味着行业间表现差异大。")

                # 行业排名变化表
                period_returns = rotation.get("sector_period_returns", {})
                if period_returns:
                    periods = list(period_returns.keys())
                    # 取最近两个时段做对比
                    if len(periods) >= 2:
                        p1, p2 = periods[0], periods[1]
                        r1 = period_returns.get(p1, {})
                        r2 = period_returns.get(p2, {})
                        all_sectors = sorted(set(list(r1.keys()) + list(r2.keys())))
                        table_rows = []
                        for sec in all_sectors:
                            ret1 = r1.get(sec, 0)
                            ret2 = r2.get(sec, 0)
                            rank1 = sorted(r1.items(), key=lambda x: -x[1])
                            rank2 = sorted(r2.items(), key=lambda x: -x[1])
                            rk1 = next((i + 1 for i, (k, _) in enumerate(rank1) if k == sec), "-")
                            rk2 = next((i + 1 for i, (k, _) in enumerate(rank2) if k == sec), "-")
                            rank_change = ""
                            if isinstance(rk1, int) and isinstance(rk2, int):
                                diff = rk1 - rk2
                                if diff > 0:
                                    rank_change = f'<span style="color:#22c55e">↑{diff}</span>'
                                elif diff < 0:
                                    rank_change = f'<span style="color:#ef4444">↓{abs(diff)}</span>'
                                else:
                                    rank_change = "-"
                            table_rows.append(
                                {
                                    "行业/指数": sec,
                                    f"{p1}收益": f"{ret1:+.2f}%",
                                    f"{p1}排名": rk1,
                                    f"{p2}收益": f"{ret2:+.2f}%",
                                    f"{p2}排名": rk2,
                                    "排名变化": rank_change,
                                }
                            )
                        if table_rows:
                            st.markdown(
                                pd.DataFrame(table_rows).to_html(index=False, escape=False), unsafe_allow_html=True
                            )
        except Exception as e:
            st.info(f"行业轮动分析暂不可用: {str(e)[:80]}")

        # ========== 告警中心 ==========
        st.markdown("---")
        alert_tab1, alert_tab2 = st.tabs(["🔔 告警中心", "📊 告警统计"])

        with alert_tab1:
            st.markdown(
                '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">' "实时告警监控</div>",
                unsafe_allow_html=True,
            )

            realtime_alerts = []
            if not positions.empty and not summary.empty:
                ls = summary.iloc[-1]
                dr = ls.get("daily_return", 0)
                if dr and not np.isnan(dr) and dr < -3:
                    realtime_alerts.append(
                        {
                            "level": "error",
                            "rule": "单日暴跌",
                            "message": f"组合单日跌幅 {dr:.2f}%，超过3%警戒线",
                            "time": selected_date,
                        }
                    )
                mdd = ls.get("max_drawdown", 0)
                if mdd and not np.isnan(mdd) and abs(mdd) > 15:
                    realtime_alerts.append(
                        {
                            "level": "error",
                            "rule": "深度回撤",
                            "message": f"最大回撤 {abs(mdd):.2f}%，超过15%警戒线",
                            "time": selected_date,
                        }
                    )
                elif mdd and not np.isnan(mdd) and abs(mdd) > 10:
                    realtime_alerts.append(
                        {
                            "level": "warning",
                            "rule": "回撤预警",
                            "message": f"最大回撤 {abs(mdd):.2f}%，超过10%关注线",
                            "time": selected_date,
                        }
                    )
                vol_val = ls.get("volatility", 0)
                if vol_val and not np.isnan(vol_val) and vol_val > 30:
                    realtime_alerts.append(
                        {
                            "level": "warning",
                            "rule": "波动飙升",
                            "message": f"年化波动率 {vol_val:.2f}%，超过30%警戒线",
                            "time": selected_date,
                        }
                    )
                sp = ls.get("sharpe_ratio", 0)
                if sp is not None and not np.isnan(sp) and sp < 0:
                    realtime_alerts.append(
                        {
                            "level": "warning",
                            "rule": "夏普异常",
                            "message": f"夏普比率 {sp:.3f}，风险调整后收益为负",
                            "time": selected_date,
                        }
                    )
                for _, pos in positions.iterrows():
                    pr = pos.get("pnl_rate", 0)
                    if pr < -20:
                        realtime_alerts.append(
                            {
                                "level": "error",
                                "rule": "个股暴跌",
                                "message": f'「{pos["name"]}」亏损 {pr:.2f}%，超过20%止损线',
                                "time": selected_date,
                            }
                        )
                    elif pr < -15:
                        realtime_alerts.append(
                            {
                                "level": "warning",
                                "rule": "个股预警",
                                "message": f'「{pos["name"]}」亏损 {pr:.2f}%，接近止损线',
                                "time": selected_date,
                            }
                        )
                if not positions.empty:
                    total_mv = positions["market_value"].sum()
                    max_w = positions["market_value"].max() / total_mv * 100 if total_mv > 0 else 0
                    if max_w > 30:
                        max_name = positions.loc[positions["market_value"].idxmax(), "name"]
                        realtime_alerts.append(
                            {
                                "level": "warning",
                                "rule": "集中度风险",
                                "message": f"「{max_name}」占比 {max_w:.1f}%，超过30%集中度警戒线",
                                "time": selected_date,
                            }
                        )

            if realtime_alerts:
                level_config = {
                    "error": {
                        "bg": "rgba(239,68,68,0.08)",
                        "border": "rgba(239,68,68,0.3)",
                        "icon": "🔴",
                        "label": "严重",
                    },
                    "warning": {
                        "bg": "rgba(245,158,11,0.08)",
                        "border": "rgba(245,158,11,0.3)",
                        "icon": "🟡",
                        "label": "警告",
                    },
                    "info": {
                        "bg": "rgba(88,166,255,0.06)",
                        "border": "rgba(88,166,255,0.2)",
                        "icon": "🔵",
                        "label": "提示",
                    },
                }
                level_order = {"error": 0, "warning": 1, "info": 2}
                realtime_alerts.sort(key=lambda x: level_order.get(x["level"], 99))
                for alert in realtime_alerts:
                    cfg = level_config.get(alert["level"], level_config["info"])
                    st.markdown(
                        f'<div style="background:{cfg["bg"]};border:1px solid {cfg["border"]};'
                        f'border-radius:6px;padding:8px 12px;margin-bottom:4px;">'
                        f'<div style="display:flex;justify-content:space-between;align-items:center;">'
                        f'<span style="font-size:12px;font-weight:bold;color:#c9d1d9;">'
                        f'{cfg["icon"]} [{cfg["label"]}] {alert["rule"]}</span>'
                        f'<span style="font-size:10px;color:#484f58;">{alert["time"]}</span></div>'
                        f'<div style="font-size:12px;color:#8b949e;margin-top:2px;">{alert["message"]}</div></div>',
                        unsafe_allow_html=True,
                    )
                n_error = sum(1 for a in realtime_alerts if a["level"] == "error")
                n_warning = sum(1 for a in realtime_alerts if a["level"] == "warning")
                st.markdown(
                    f'<div style="font-size:11px;color:#484f58;padding:4px 0;">'
                    f'当前触发: <span style="color:#ef4444;font-weight:bold;">{n_error} 严重</span> / '
                    f'<span style="color:#f59e0b;font-weight:bold;">{n_warning} 警告</span></div>',
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    '<div style="background:rgba(34,197,94,0.06);border:1px solid rgba(34,197,94,0.2);'
                    'border-radius:6px;padding:10px 14px;">'
                    '<div style="font-size:13px;color:#22c55e;font-weight:bold;">🟢 告警状态正常</div>'
                    '<div style="font-size:12px;color:#8b949e;margin-top:3px;">'
                    "当前未触发任何告警规则，所有指标处于安全范围内。</div></div>",
                    unsafe_allow_html=True,
                )

            with st.expander("查看历史告警记录", expanded=False):
                hist_alerts = load_alerts(limit=20)
                if not hist_alerts.empty:
                    for _, ha in hist_alerts.iterrows():
                        ha_level = ha.get("level", "info")
                        ha_cfg = {"error": {"icon": "🔴"}, "warning": {"icon": "🟡"}, "info": {"icon": "🔵"}}.get(
                            ha_level, {"icon": "🔵"}
                        )
                        ack = "✅" if ha.get("acknowledged") else ""
                        st.markdown(
                            f'<div style="font-size:12px;padding:3px 0;color:#8b949e;">'
                            f'{ha_cfg["icon"]} <span style="color:#c9d1d9;">{ha.get("rule_name", "未知")}</span> '
                            f'{ha.get("message", "")} <span style="color:#484f58;font-size:10px;">{ha.get("created_at", "")}</span> {ack}</div>',
                            unsafe_allow_html=True,
                        )
                else:
                    st.caption("暂无历史告警记录")

        with alert_tab2:
            st.markdown(
                '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">'
                "告警规则配置与统计</div>",
                unsafe_allow_html=True,
            )

            rules_display = [
                {"name": "单日暴跌", "condition": "日收益率 < -3%", "level": "严重"},
                {"name": "深度回撤", "condition": "最大回撤 > 15%", "level": "严重"},
                {"name": "回撤预警", "condition": "最大回撤 > 10%", "level": "警告"},
                {"name": "个股暴跌", "condition": "单一ETF亏损 > 20%", "level": "严重"},
                {"name": "个股预警", "condition": "单一ETF亏损 > 15%", "level": "警告"},
                {"name": "波动飙升", "condition": "年化波动率 > 30%", "level": "警告"},
                {"name": "夏普异常", "condition": "夏普比率 < 0", "level": "警告"},
                {"name": "集中度风险", "condition": "单一持仓占比 > 30%", "level": "警告"},
            ]
            html_rules = (
                '<div style="overflow-x:auto;"><table style="width:100%;border-collapse:collapse;font-size:12px;">'
                '<thead><tr style="background:#161b22;">'
                '<th style="padding:6px 10px;color:#8b949e;text-align:left;font-size:11px;">状态</th>'
                '<th style="padding:6px 10px;color:#8b949e;text-align:left;font-size:11px;">规则名称</th>'
                '<th style="padding:6px 10px;color:#8b949e;text-align:left;font-size:11px;">触发条件</th>'
                '<th style="padding:6px 10px;color:#8b949e;text-align:center;font-size:11px;">级别</th>'
                "</tr></thead><tbody>"
            )
            for rule in rules_display:
                triggered = any(a["rule"] == rule["name"] for a in realtime_alerts) if realtime_alerts else False
                status_html = (
                    '<span style="color:#ef4444;">触发</span>'
                    if triggered
                    else '<span style="color:#22c55e;">正常</span>'
                )
                level_color = "#ef4444" if rule["level"] == "严重" else "#f59e0b"
                html_rules += (
                    f'<tr style="border-bottom:1px solid #21262d;">'
                    f'<td style="padding:5px 10px;">{status_html}</td>'
                    f'<td style="padding:5px 10px;color:#c9d1d9;">{rule["name"]}</td>'
                    f'<td style="padding:5px 10px;color:#8b949e;">{rule["condition"]}</td>'
                    f'<td style="padding:5px 10px;text-align:center;color:{level_color};font-weight:bold;">{rule["level"]}</td></tr>'
                )
            html_rules += "</tbody></table></div>"
            st.markdown(html_rules, unsafe_allow_html=True)

            hist_alerts = load_alerts(limit=50)
            if not hist_alerts.empty:
                ac1, ac2, ac3 = st.columns(3)
                with ac1:
                    st.metric("历史告警总数", f"{len(hist_alerts)} 条")
                with ac2:
                    st.metric("严重告警", f"{len(hist_alerts[hist_alerts['level'] == 'error'])} 条")
                with ac3:
                    st.metric("警告告警", f"{len(hist_alerts[hist_alerts['level'] == 'warning'])} 条")

                rule_counts = hist_alerts["rule_name"].value_counts()
                if not rule_counts.empty:
                    fig_alert_dist = go.Figure(
                        go.Bar(
                            y=rule_counts.index,
                            x=rule_counts.values,
                            orientation="h",
                            marker_color="#f59e0b",
                            text=[str(v) for v in rule_counts.values],
                            textposition="outside",
                            textfont=dict(size=10, color="#c9d1d9"),
                        )
                    )
                    fig_alert_dist.update_layout(
                        height=max(200, len(rule_counts) * 30),
                        plot_bgcolor="#0d1117",
                        paper_bgcolor="#0d1117",
                        font=dict(color="#c9d1d9", size=11),
                        margin=dict(l=100, r=40, t=10, b=20),
                        xaxis=dict(showgrid=True, gridcolor="#21262d"),
                        yaxis=dict(showgrid=False, tickfont=dict(size=10)),
                    )
                    st.plotly_chart(fig_alert_dist, width="stretch")

        st.markdown("---")

    # ========== 收益日历 ==========



def _render_tab4_calendar(tab4, positions, summary):
    """Extracted from main() - tab4 renderer"""
    with tab4:
        st.caption("📅 以日历热力图形式展示每月每个交易日的收益情况，支持按年/月切换查看")
        cal_data = load_calendar_data()

        if cal_data.empty:
            st.info("暂无日历数据")
        else:
            years = sorted(cal_data["year"].unique(), reverse=True)
            latest_year = years[0]
            today_str = datetime.now().strftime("%Y-%m-%d")

            # --- 年份选择 ---
            cur_year = st.session_state.get("cal_year", latest_year)
            yr_cols = st.columns(len(years))
            for i, yr in enumerate(years):
                with yr_cols[i]:
                    if st.button(str(yr), key=f"yr_{yr}", type="primary" if cur_year == yr else "secondary"):
                        st.session_state["cal_year"] = yr
                        st.session_state.pop("cal_month", None)
                        st.rerun()

            sel_year = cur_year
            year_df = cal_data[cal_data["year"] == sel_year]

            # --- 年度月度概览（月份可点击切换） ---
            months_in_year = sorted(year_df["month"].unique())
            sel_month = st.session_state.get("cal_month", months_in_year[-1] if months_in_year else 1)
            if sel_month not in months_in_year:
                sel_month = months_in_year[-1] if months_in_year else 1

            yr_monthly = (
                year_df.groupby("month")
                .agg(pnl_sum=("daily_pnl", "sum"), ret_sum=("daily_return", "sum"), days=("day", "count"))
                .reset_index()
            )

            yr_monthly["profit_days"] = (
                year_df[year_df["daily_pnl"] > 0]
                .groupby("month")
                .size()
                .reindex(yr_monthly["month"], fill_value=0)
                .values
            )
            yr_monthly["loss_days"] = (
                year_df[year_df["daily_pnl"] < 0]
                .groupby("month")
                .size()
                .reindex(yr_monthly["month"], fill_value=0)
                .values
            )

            # --- 年度月度概览（月份按钮在表格内） ---
            yr_total_pnl = year_df["daily_pnl"].sum()
            yr_total_ret = year_df["daily_return"].sum()
            yr_total_days = len(year_df)
            yr_profit_days = len(year_df[year_df["daily_pnl"] > 0])
            yr_loss_days = len(year_df[year_df["daily_pnl"] < 0])
            yr_pnl_color = "#22c55e" if yr_total_pnl >= 0 else "#ef4444"
            yr_ret_color = "#22c55e" if yr_total_ret >= 0 else "#ef4444"

            # Header row: label + data headers
            hdr_col1, hdr_col2 = st.columns([1, 5])
            with hdr_col1:
                st.markdown(
                    '<div style="color:#8b949e;font-size:13px;padding:6px 0;border-bottom:1px solid #30363d;text-align:center;">月份</div>',
                    unsafe_allow_html=True,
                )
            with hdr_col2:
                st.markdown(
                    '<div style="display:flex;color:#8b949e;font-size:13px;border-bottom:1px solid #30363d;">'
                    '<div style="flex:1;text-align:right;padding:6px 10px;">月收益</div>'
                    '<div style="flex:1;text-align:right;padding:6px 10px;">月收益率</div>'
                    '<div style="flex:1;text-align:center;padding:6px 10px;">交易日</div>'
                    '<div style="flex:1;text-align:center;padding:6px 10px;">盈利天数</div>'
                    '<div style="flex:1;text-align:center;padding:6px 10px;">亏损天数</div>'
                    "</div>",
                    unsafe_allow_html=True,
                )

            # Data rows: month button + data
            for _, row in yr_monthly.iterrows():
                m = int(row["month"])
                pnl = row["pnl_sum"]
                ret = row["ret_sum"]
                days = int(row["days"])
                profit_d = int(row["profit_days"])
                loss_d = int(row["loss_days"])
                pnl_color = "#22c55e" if pnl >= 0 else "#ef4444"
                ret_color = "#22c55e" if ret >= 0 else "#ef4444"
                is_active = m == sel_month

                row_col1, row_col2 = st.columns([1, 5])
                with row_col1:
                    _b1, _b2, _b3 = st.columns([1, 1, 1])
                    with _b2:
                        if st.button(f"{m}月", key=f"mo_{sel_year}_{m}", type="primary" if is_active else "secondary"):
                            st.session_state["cal_month"] = m
                            st.rerun()
                with row_col2:
                    bg = "background:#161b22;" if is_active else ""
                    st.markdown(
                        f'<div style="display:flex;{bg}border-bottom:1px solid #21262d;">'
                        f'<div style="flex:1;text-align:right;padding:6px 10px;color:{pnl_color};">¥{pnl:,.0f}</div>'
                        f'<div style="flex:1;text-align:right;padding:6px 10px;color:{ret_color};">{ret*100:+.2f}%</div>'
                        f'<div style="flex:1;text-align:center;padding:6px 10px;">{days}天</div>'
                        f'<div style="flex:1;text-align:center;padding:6px 10px;color:#22c55e;">{profit_d}天</div>'
                        f'<div style="flex:1;text-align:center;padding:6px 10px;color:#ef4444;">{loss_d}天</div>'
                        f"</div>",
                        unsafe_allow_html=True,
                    )

            # Yearly total row
            tot_col1, tot_col2 = st.columns([1, 5])
            with tot_col1:
                st.markdown(
                    '<div style="font-weight:bold;text-align:center;padding:8px 0;color:#58a6ff;'
                    'border-top:2px solid #30363d;">全年合计</div>',
                    unsafe_allow_html=True,
                )
            with tot_col2:
                st.markdown(
                    f'<div style="display:flex;font-weight:bold;background:#161b22;border-top:2px solid #30363d;">'
                    f'<div style="flex:1;text-align:right;padding:8px 10px;color:{yr_pnl_color};">¥{yr_total_pnl:,.0f}</div>'
                    f'<div style="flex:1;text-align:right;padding:8px 10px;color:{yr_ret_color};">{yr_total_ret*100:+.2f}%</div>'
                    f'<div style="flex:1;text-align:center;padding:8px 10px;">{yr_total_days}天</div>'
                    f'<div style="flex:1;text-align:center;padding:8px 10px;color:#22c55e;">{yr_profit_days}天</div>'
                    f'<div style="flex:1;text-align:center;padding:8px 10px;color:#ef4444;">{yr_loss_days}天</div>'
                    f"</div>",
                    unsafe_allow_html=True,
                )

            month_df = year_df[year_df["month"] == sel_month]

            # --- 月度汇总 ---
            m_pnl = month_df["daily_pnl"].sum()
            m_return = month_df["daily_return"].sum()
            m_trading = len(month_df)
            m_profit = len(month_df[month_df["daily_pnl"] > 0])
            m_loss = len(month_df[month_df["daily_pnl"] < 0])

            st.markdown("---")
            sum_col1, sum_col2, sum_col3, sum_col4, sum_col5 = st.columns(5)
            with sum_col1:
                st.metric("月收益", f"¥{m_pnl:,.0f}")
            with sum_col2:
                st.metric("月收益率", f"{m_return*100:.2f}%")
            with sum_col3:
                st.metric("交易日", f"{m_trading}天")
            with sum_col4:
                st.metric("盈利天数", f"{m_profit}天")
            with sum_col5:
                st.metric("亏损天数", f"{m_loss}天")

            # --- 月度日历 ---
            st.markdown(f"**{sel_year}年{sel_month}月 日历**")

            # 获取交易日数据字典
            trading_days = {}
            for _, row in month_df.iterrows():
                d = int(row["day"])
                pnl = row["daily_pnl"]
                ret = row["daily_return"]
                dt_str = row["date"].strftime("%Y-%m-%d")
                trading_days[d] = {"pnl": pnl, "ret": ret, "date_str": dt_str}

            # 构建日历HTML
            cal = calendar.Calendar(firstweekday=0)  # 周一开始
            month_days = list(cal.itermonthdays(sel_year, sel_month))

            # 周标题
            week_headers = ["一", "二", "三", "四", "五", "六", "日"]

            cal_html = '<table class="cal-table"><tr>'
            for h in week_headers:
                cal_html += f"<th>{h}</th>"
            cal_html += "</tr><tr>"

            for i, day in enumerate(month_days):
                if day == 0:
                    cal_html += '<td class="cal-non-trading"></td>'
                elif day in trading_days:
                    info = trading_days[day]
                    pnl = info["pnl"]
                    ret = info["ret"]
                    dt_str = info["date_str"]

                    if pnl > 0:
                        td_cls = "cal-trading cal-profit"
                        pnl_cls = "cal-pnl cal-pnl-profit"
                    elif pnl < 0:
                        td_cls = "cal-trading cal-loss"
                        pnl_cls = "cal-pnl cal-pnl-loss"
                    else:
                        td_cls = "cal-trading"
                        pnl_cls = "cal-pnl cal-pnl-zero"

                    today_cls = " cal-today" if dt_str == today_str else ""

                    # 格式化收益金额
                    if abs(pnl) >= 10000:
                        pnl_text = f"{pnl/10000:.1f}万"
                    elif abs(pnl) >= 1000:
                        pnl_text = f"{pnl/1000:.1f}k"
                    else:
                        pnl_text = f"{pnl:.0f}"

                    cal_html += (
                        f'<td class="{td_cls}{today_cls}" title="{dt_str}  收益: ¥{pnl:,.0f}  ({ret:+.2f}%)">'
                        f'<span class="cal-day">{day}</span>'
                        f'<span class="{pnl_cls}">{pnl_text}</span>'
                        f"</td>"
                    )
                else:
                    # 非交易日
                    cal_html += f'<td class="cal-non-trading"><span class="cal-day">{day}</span></td>'

                if (i + 1) % 7 == 0:
                    cal_html += "</tr><tr>"

            # 清理最后可能的多余tr
            cal_html = cal_html.rstrip("<tr>")
            cal_html += "</table>"

            st.markdown(cal_html, unsafe_allow_html=True)

            # --- 每日收益明细表 ---
            with st.expander("查看每日收益明细", expanded=False):
                detail_df = month_df[["date", "daily_pnl", "daily_return"]].copy()
                detail_df.columns = ["日期", "日收益 (¥)", "日收益率 (%)"]
                detail_df["日期"] = detail_df["日期"].dt.strftime("%Y-%m-%d")
                detail_df["日收益 (¥)"] = detail_df["日收益 (¥)"].apply(
                    lambda x: f'<span style="color:{"#22c55e" if x >= 0 else "#ef4444"}">{x:,.2f}</span>'
                )
                detail_df["日收益率 (%)"] = detail_df["日收益率 (%)"].apply(
                    lambda x: f'<span style="color:{"#22c55e" if x >= 0 else "#ef4444"}">{x*100:+.2f}%</span>'
                )
                st.markdown(detail_df.to_html(index=False, escape=False), unsafe_allow_html=True)

            # --- 月度收益热力图 ---
            st.markdown("---")
            st.markdown(
                '<div class="tip-title" style="font-size:14px;border-bottom:none;padding:5px 0;">月度收益热力图<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">以热力图形式展示12个月的月度收益，颜色深浅反映收益高低。</span></div>',
                unsafe_allow_html=True,
            )
            monthly_pivot = compute_monthly_returns()
            if not monthly_pivot.empty:
                # compute_monthly_returns 返回小数形式收益率，乘100转为百分比
                heat_z = monthly_pivot.values * 100
                fig_heat = go.Figure(
                    go.Heatmap(
                        z=heat_z,
                        x=monthly_pivot.columns.tolist(),
                        y=monthly_pivot.index.astype(str).tolist(),
                        text=heat_z,
                        texttemplate="%{text:.2f}%" if abs(heat_z).max() < 100 else "%{text:.1f}%",
                        textfont=dict(size=10),
                        colorscale=[[0, "#ef4444"], [0.5, "#0d1117"], [1, "#22c55e"]],
                        zmin=-abs(heat_z).max(),
                        zmax=abs(heat_z).max(),
                        xgap=2,
                        ygap=2,
                        hovertemplate="%{y}年%{x}<br>收益率: %{z:.2f}%<extra></extra>",
                    )
                )
                fig_heat.update_layout(
                    height=max(250, 40 * len(monthly_pivot)),
                    plot_bgcolor="#0d1117",
                    paper_bgcolor="#0d1117",
                    font=dict(color="#c9d1d9", size=11),
                    margin=dict(l=50, r=20, t=10, b=40),
                    xaxis=dict(title="", showgrid=False, side="top"),
                    yaxis=dict(title="", showgrid=False, autorange="reversed"),
                )
                st.plotly_chart(fig_heat, width="stretch")

            # --- 事件日历：关键日期提醒 ---
            st.markdown("---")
            st.markdown(
                '<div class="tip-title" style="font-size:14px;border-bottom:none;padding:5px 0;">关键日期提醒<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">自动检测持仓中的关键事件日期，如财报季、期权到期日等。</span></div>',
                unsafe_allow_html=True,
            )

            # 1. 财报季提醒
            current_year = datetime.now().year
            current_month = datetime.now().month
            current_day = datetime.now().day

            earnings_periods = [
                {"name": "一季报", "start": (4, 1), "end": (4, 30), "icon": "📊"},
                {"name": "半年报", "start": (7, 1), "end": (8, 31), "icon": "📋"},
                {"name": "三季报", "start": (10, 1), "end": (10, 31), "icon": "📊"},
                {"name": "年报", "start": (1, 1), "end": (4, 30), "icon": "📋"},
            ]

            # 2. 期权到期日（每月第三个周五）
            def get_third_friday(year, month):
                """计算某月的第三个周五"""
                import calendar as cal_mod

                cal = cal_mod.monthcalendar(year, month)
                # 找到有周五的周
                fridays = [week[cal_mod.FRIDAY] for week in cal if week[cal_mod.FRIDAY] != 0]
                return fridays[2] if len(fridays) >= 3 else fridays[-1]

            # 3. 基金分红季
            dividend_months = [1, 6, 12]

            # 4. 系统性风险事件
            events_list = []

            # 财报季
            for ep in earnings_periods:
                s_m, s_d = ep["start"]
                e_m, e_d = ep["end"]
                days_ahead = 0
                if current_year == current_year:
                    if s_m == current_month:
                        days_ahead = s_d - current_day
                    elif s_m > current_month:
                        month_diff = s_m - current_month
                        days_ahead = (month_diff * 30) + (s_d - current_day)

                if days_ahead >= 0 and days_ahead <= 90:
                    urgency = "即将到来" if days_ahead <= 14 else ("本月" if days_ahead <= 30 else f"{days_ahead}天后")
                    events_list.append(
                        {
                            "icon": ep["icon"],
                            "title": f'{ep["name"]}披露期',
                            "date": f"{current_year}-{s_m:02d}-{s_d:02d} ~ {current_year}-{e_m:02d}-{e_d:02d}",
                            "urgency": urgency,
                            "days_ahead": days_ahead,
                            "color": "#f59e0b" if days_ahead <= 30 else "#8b949e",
                            "desc": f"A股上市公司{ep['name']}集中披露窗口",
                        }
                    )

            # 期权到期日（未来3个月）
            for m_offset in range(0, 4):
                evt_month = current_month + m_offset
                evt_year = current_year
                while evt_month > 12:
                    evt_month -= 12
                    evt_year += 1
                try:
                    third_fri = get_third_friday(evt_year, evt_month)
                    evt_date = datetime(evt_year, evt_month, third_fri)
                    delta = (evt_date - datetime.now()).days
                    if 0 <= delta <= 90:
                        urgency = "本周五" if delta <= 7 else ("即将" if delta <= 14 else f"{delta}天后")
                        events_list.append(
                            {
                                "icon": "📅",
                                "title": "股指期权交割日",
                                "date": evt_date.strftime("%Y-%m-%d"),
                                "urgency": urgency,
                                "days_ahead": delta,
                                "color": "#ef4444" if delta <= 7 else "#f59e0b" if delta <= 14 else "#8b949e",
                                "desc": "沪深300/中证1000股指期权到期，注意波动加剧",
                            }
                        )
                except Exception:
                    pass

            # 基金分红提醒
            for m_offset in range(0, 4):
                d_month = current_month + m_offset
                d_year = current_year
                while d_month > 12:
                    d_month -= 12
                    d_year += 1
                if d_month in dividend_months:
                    delta = (datetime(d_year, d_month, 15) - datetime.now()).days
                    if 0 <= delta <= 90:
                        events_list.append(
                            {
                                "icon": "💰",
                                "title": "基金分红季",
                                "date": f"{d_year}-{d_month:02d}",
                                "urgency": f"{delta}天后" if delta > 7 else "即将",
                                "days_ahead": delta,
                                "color": "#22c55e" if delta > 14 else "#f59e0b",
                                "desc": "ETF基金常见分红除息月份，关注持仓基金公告",
                            }
                        )

            # 年底/年初换仓提醒
            if 12 <= current_month <= 12 or 1 <= current_month <= 1:
                events_list.append(
                    {
                        "icon": "🔄",
                        "title": "年度换仓窗口",
                        "date": f"{current_year}-12 ~ {current_year + 1}-01",
                        "urgency": "当前",
                        "days_ahead": 0,
                        "color": "#a855f7",
                        "desc": "年末机构调仓高峰，市场风格可能切换",
                    }
                )

            # Sort by days_ahead
            events_list.sort(key=lambda x: x["days_ahead"])

            # Render events
            if events_list:
                for evt in events_list:
                    st.markdown(
                        f'<div style="background:#161b22;border-radius:6px;padding:10px 14px;margin-bottom:5px;border-left:3px solid {evt["color"]};">'
                        f'<div style="display:flex;justify-content:space-between;align-items:center;">'
                        f'<div style="display:flex;align-items:center;gap:8px;">'
                        f'<span style="font-size:16px;">{evt["icon"]}</span>'
                        f"<div>"
                        f'<div style="font-size:13px;color:#e6edf3;font-weight:bold;">{evt["title"]}</div>'
                        f'<div style="font-size:11px;color:#6e7681;margin-top:2px;">{evt["desc"]}</div>'
                        f"</div></div>"
                        f'<div style="text-align:right;">'
                        f'<div style="font-size:12px;color:{evt["color"]};font-weight:bold;">{evt["urgency"]}</div>'
                        f'<div style="font-size:11px;color:#484f58;">{evt["date"]}</div>'
                        f"</div></div></div>",
                        unsafe_allow_html=True,
                    )
            else:
                st.info("近90天内暂无关键日期事件")

    # ========== Tab6: 技术信号 ==========



def _render_tab6_technical(tab6, technical):
    """Extracted from main() - tab6 renderer"""
    with tab6:
        st.markdown(
            '<div class="tip-title" style="">技术信号总览<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">基于MA均线排列、MACD、KDJ、RSI、布林带位置等技术指标，对持仓品种进行全面信号检测，辅助判断短期走势。</span></div>',
            unsafe_allow_html=True,
        )

        tech_df = load_technical()

        if tech_df is None or tech_df.empty:
            st.info("暂无技术信号数据")
        else:
            # ---------- 信号概览卡片 ----------
            n_total = len(tech_df)
            n_bullish_ma = len(tech_df[tech_df["ma_signal"].isin(["多头排列", "金叉"])])
            n_bearish_ma = len(tech_df[tech_df["ma_signal"].isin(["空头排列", "死叉"])])
            n_overbought = len(tech_df[tech_df["rsi_status"].isin(["超买", "严重超买"])])
            n_oversold = len(tech_df[tech_df["rsi_status"].isin(["超卖", "严重超卖"])])
            n_bull_macd = len(tech_df[tech_df["macd_signal"].isin(["多头", "金叉", "看多"])])
            n_bear_macd = len(tech_df[tech_df["macd_signal"].isin(["空头", "死叉"])])
            n_bull_kdj = len(tech_df[tech_df["kdj_signal"] == "金叉"])
            n_bear_kdj = len(tech_df[tech_df["kdj_signal"] == "死叉"])
            n_strong_up = len(tech_df[tech_df["trend"] == "强势上涨"])
            n_weak_down = len(tech_df[tech_df["trend"] == "下跌"])

            overview_cols = st.columns(6)
            with overview_cols[0]:
                st.markdown(
                    f'<div style="padding:10px;border-radius:8px;background:#161b22;border-left:3px solid #22c55e;">'
                    f'<div style="font-size:11px;color:#8b949e;">MA 多头</div>'
                    f'<div style="font-size:20px;font-weight:bold;color:#22c55e;">{n_bullish_ma}<span style="font-size:12px;color:#484f58;">/{n_total}</span></div>'
                    f"</div>",
                    unsafe_allow_html=True,
                )
            with overview_cols[1]:
                st.markdown(
                    f'<div style="padding:10px;border-radius:8px;background:#161b22;border-left:3px solid #ef4444;">'
                    f'<div style="font-size:11px;color:#8b949e;">MA 空头</div>'
                    f'<div style="font-size:20px;font-weight:bold;color:#ef4444;">{n_bearish_ma}<span style="font-size:12px;color:#484f58;">/{n_total}</span></div>'
                    f"</div>",
                    unsafe_allow_html=True,
                )
            with overview_cols[2]:
                st.markdown(
                    f'<div style="padding:10px;border-radius:8px;background:#161b22;border-left:3px solid #f59e0b;">'
                    f'<div style="font-size:11px;color:#8b949e;">RSI 超买</div>'
                    f'<div style="font-size:20px;font-weight:bold;color:#f59e0b;">{n_overbought}</div>'
                    f"</div>",
                    unsafe_allow_html=True,
                )
            with overview_cols[3]:
                st.markdown(
                    f'<div style="padding:10px;border-radius:8px;background:#161b22;border-left:3px solid #3b82f6;">'
                    f'<div style="font-size:11px;color:#8b949e;">RSI 超卖</div>'
                    f'<div style="font-size:20px;font-weight:bold;color:#3b82f6;">{n_oversold}</div>'
                    f"</div>",
                    unsafe_allow_html=True,
                )
            with overview_cols[4]:
                st.markdown(
                    f'<div style="padding:10px;border-radius:8px;background:#161b22;border-left:3px solid #22c55e;">'
                    f'<div style="font-size:11px;color:#8b949e;">强势上涨</div>'
                    f'<div style="font-size:20px;font-weight:bold;color:#22c55e;">{n_strong_up}</div>'
                    f"</div>",
                    unsafe_allow_html=True,
                )
            with overview_cols[5]:
                st.markdown(
                    f'<div style="padding:10px;border-radius:8px;background:#161b22;border-left:3px solid #ef4444;">'
                    f'<div style="font-size:11px;color:#8b949e;">下跌趋势</div>'
                    f'<div style="font-size:20px;font-weight:bold;color:#ef4444;">{n_weak_down}</div>'
                    f"</div>",
                    unsafe_allow_html=True,
                )

            st.markdown('<div style="height:8px;"></div>', unsafe_allow_html=True)

            # ---------- 信号强度雷达图 ----------
            st.markdown(
                '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">多空信号分布<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">雷达图展示各维度的多空力量对比，越靠近外圈表示该维度多头信号越强。</span></div>',
                unsafe_allow_html=True,
            )

            radar_cols = st.columns([1, 1])
            with radar_cols[0]:
                # 雷达图：多头 vs 空头
                categories = ["MA均线", "MACD", "KDJ", "RSI<br>(超买)", "RSI<br>(超卖)", "趋势<br>(强势)"]
                bull_values = [n_bullish_ma, n_bull_macd, n_bull_kdj, n_overbought, n_oversold, n_strong_up]
                bear_values = [n_bearish_ma, n_bear_macd, n_bear_kdj, n_overbought, n_oversold, n_weak_down]

                fig_radar = go.Figure()
                fig_radar.add_trace(
                    go.Scatterpolar(
                        r=bull_values,
                        theta=categories,
                        fill="toself",
                        fillcolor="rgba(34,197,94,0.15)",
                        line_color="#22c55e",
                        name="多头信号",
                        marker_size=5,
                    )
                )
                fig_radar.add_trace(
                    go.Scatterpolar(
                        r=bear_values,
                        theta=categories,
                        fill="toself",
                        fillcolor="rgba(239,68,68,0.15)",
                        line_color="#ef4444",
                        name="空头信号",
                        marker_size=5,
                    )
                )
                fig_radar.update_layout(
                    polar=dict(
                        radialaxis=dict(
                            visible=True,
                            range=[0, max(max(bull_values), max(bear_values), 1)],
                            gridcolor="#30363d",
                            tickfont=dict(size=9, color="#8b949e"),
                            angle=45,
                        ),
                        bgcolor="#0d1117",
                        angularaxis=dict(gridcolor="#30363d", tickfont=dict(size=11, color="#c9d1d9")),
                    ),
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=1.1,
                        xanchor="center",
                        x=0.5,
                        font=dict(size=11, color="#c9d1d9"),
                    ),
                    paper_bgcolor="#0d1117",
                    plot_bgcolor="#0d1117",
                    height=320,
                    margin=dict(l=40, r=40, t=40, b=40),
                )
                st.plotly_chart(fig_radar, width="stretch")

            with radar_cols[1]:
                # 技术指标信号汇总堆叠柱状图
                indicator_labels = ["MA多头", "MA空头", "MACD多头", "MACD空头", "KDJ金叉", "KDJ死叉", "超买", "超卖"]
                indicator_values = [
                    n_bullish_ma,
                    n_bearish_ma,
                    n_bull_macd,
                    n_bear_macd,
                    n_bull_kdj,
                    n_bear_kdj,
                    n_overbought,
                    n_oversold,
                ]
                bar_colors = ["#22c55e", "#ef4444", "#22c55e", "#ef4444", "#22c55e", "#ef4444", "#f59e0b", "#3b82f6"]

                fig_bar = go.Figure(
                    go.Bar(
                        x=indicator_labels,
                        y=indicator_values,
                        marker_color=bar_colors,
                        text=indicator_values,
                        textposition="auto",
                        textfont=dict(size=12, color="#c9d1d9"),
                        hovertemplate="%{x}: %{y}只<extra></extra>",
                    )
                )
                fig_bar.update_layout(
                    xaxis=dict(tickfont=dict(size=10, color="#c9d1d9"), gridcolor="#21262d"),
                    yaxis=dict(title="持仓数量", tickfont=dict(size=10, color="#8b949e"), gridcolor="#21262d", dtick=1),
                    paper_bgcolor="#0d1117",
                    plot_bgcolor="#0d1117",
                    height=320,
                    margin=dict(l=50, r=20, t=20, b=40),
                    bargap=0.3,
                )
                st.plotly_chart(fig_bar, width="stretch")

            st.markdown('<div style="height:12px;"></div>', unsafe_allow_html=True)

            # ---------- 技术信号详情表 ----------
            st.markdown(
                '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">持仓技术信号详情<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">展示每只持仓品种的完整技术指标信号。颜色标记：绿色=多头/看多，红色=空头/看空，黄色=超买/超卖预警。</span></div>',
                unsafe_allow_html=True,
            )

            # 构建 HTML 表格（st.dataframe 不渲染 HTML 标签）
            def _sig(val, bull, bear, warn=None):
                if warn and val in warn:
                    return f'<span style="color:#f59e0b;font-weight:bold">{val}</span>'
                if val in bull:
                    return f'<span style="color:#22c55e;font-weight:bold">{val}</span>'
                if val in bear:
                    return f'<span style="color:#ef4444;font-weight:bold">{val}</span>'
                return f'<span style="color:#8b949e">{val}</span>'

            def _rsi_c(v):
                if v >= 80:
                    return f'<span style="color:#ef4444;font-weight:bold">{v:.1f}</span>'
                if v >= 70:
                    return f'<span style="color:#f59e0b;font-weight:bold">{v:.1f}</span>'
                if v <= 20:
                    return f'<span style="color:#3b82f6;font-weight:bold">{v:.1f}</span>'
                if v <= 30:
                    return f'<span style="color:#f59e0b">{v:.1f}</span>'
                return f'<span style="color:#c9d1d9">{v:.1f}</span>'

            def _boll_c(v):
                if v >= 80:
                    return f'<span style="color:#ef4444;font-weight:bold">{v:.1f}%</span>'
                if v >= 60:
                    return f'<span style="color:#22c55e">{v:.1f}%</span>'
                if v <= 20:
                    return f'<span style="color:#3b82f6;font-weight:bold">{v:.1f}%</span>'
                if v <= 40:
                    return f'<span style="color:#f59e0b">{v:.1f}%</span>'
                return f'<span style="color:#c9d1d9">{v:.1f}%</span>'

            def _atr_c(v):
                if v >= 3.0:
                    return f'<span style="color:#f59e0b;font-weight:bold">{v:.2f}%</span>'
                if v >= 2.0:
                    return f'<span style="color:#c9d1d9">{v:.2f}%</span>'
                return f'<span style="color:#22c55e">{v:.2f}%</span>'

            tbl = (
                '<div style="overflow-x:auto;"><table style="width:100%;border-collapse:collapse;font-size:13px;">'
                '<thead><tr style="background:#161b22;">'
                '<th style="padding:8px 10px;color:#8b949e;text-align:left;font-size:12px;">代码</th>'
                '<th style="padding:8px 10px;color:#8b949e;text-align:left;font-size:12px;">名称</th>'
                '<th style="padding:8px 10px;color:#8b949e;text-align:center;font-size:12px;">趋势</th>'
                '<th style="padding:8px 10px;color:#8b949e;text-align:center;font-size:12px;">MA信号</th>'
                '<th style="padding:8px 10px;color:#8b949e;text-align:center;font-size:12px;">MACD信号</th>'
                '<th style="padding:8px 10px;color:#8b949e;text-align:center;font-size:12px;">KDJ信号</th>'
                '<th style="padding:8px 10px;color:#8b949e;text-align:center;font-size:12px;">RSI值</th>'
                '<th style="padding:8px 10px;color:#8b949e;text-align:center;font-size:12px;">RSI状态</th>'
                '<th style="padding:8px 10px;color:#8b949e;text-align:center;font-size:12px;">布林位置</th>'
                '<th style="padding:8px 10px;color:#8b949e;text-align:center;font-size:12px;">ATR</th>'
                "</tr></thead><tbody>"
            )
            for _, r in tech_df.iterrows():
                bg = "#161b22" if _ % 2 == 0 else "#0d1117"
                tbl += (
                    f'<tr style="background:{bg};border-bottom:1px solid #21262d;">'
                    f'<td style="padding:7px 10px;color:#c9d1d9;font-family:monospace;">{r["code"]}</td>'
                    f'<td style="padding:7px 10px;color:#c9d1d9;">{r["name"]}</td>'
                    f'<td style="padding:7px 10px;text-align:center;">{_sig(r["trend"], ["强势上涨","温和上涨"], ["下跌"])}</td>'
                    f'<td style="padding:7px 10px;text-align:center;">{_sig(r["ma_signal"], ["多头排列","金叉"], ["空头排列","死叉"])}</td>'
                    f'<td style="padding:7px 10px;text-align:center;">{_sig(r["macd_signal"], ["多头","金叉","看多"], ["空头","死叉"])}</td>'
                    f'<td style="padding:7px 10px;text-align:center;">{_sig(r["kdj_signal"], ["金叉"], ["死叉"])}</td>'
                    f'<td style="padding:7px 10px;text-align:center;">{_rsi_c(r["rsi_value"])}</td>'
                    f'<td style="padding:7px 10px;text-align:center;">{_sig(r["rsi_status"], [], [], ["超买","严重超买"])}</td>'
                    f'<td style="padding:7px 10px;text-align:center;">{_boll_c(r["bollinger_position"])}</td>'
                    f'<td style="padding:7px 10px;text-align:center;">{_atr_c(r["atr_pct"])}</td>'
                    "</tr>"
                )
            tbl += "</tbody></table></div>"
            st.markdown(tbl, unsafe_allow_html=True)

            st.markdown('<div style="height:12px;"></div>', unsafe_allow_html=True)

            # ---------- 布林带位置分布图 ----------
            st.markdown(
                '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">布林带位置分布<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">展示所有持仓品种在布林带中的相对位置(0%=下轨, 50%=中轨, 100%=上轨)。超过100%或低于0%表示突破布林带轨道。</span></div>',
                unsafe_allow_html=True,
            )

            # 按布林位置排序
            boll_df = tech_df[["name", "code", "bollinger_position"]].sort_values("bollinger_position", ascending=True)

            boll_colors = []
            for _, row in boll_df.iterrows():
                bp = row["bollinger_position"]
                if bp >= 80:
                    boll_colors.append("#ef4444")
                elif bp >= 60:
                    boll_colors.append("#22c55e")
                elif bp <= 20:
                    boll_colors.append("#3b82f6")
                elif bp <= 40:
                    boll_colors.append("#f59e0b")
                else:
                    boll_colors.append("#8b949e")

            fig_boll = go.Figure(
                go.Bar(
                    orientation="h",
                    y=boll_df["name"],
                    x=boll_df["bollinger_position"],
                    marker_color=boll_colors,
                    text=[f"{v:.1f}%" for v in boll_df["bollinger_position"]],
                    textposition="auto",
                    textfont=dict(size=10, color="#c9d1d9"),
                    hovertemplate="%{y}: %{x:.1f}%<extra></extra>",
                )
            )
            # 添加参考线
            fig_boll.add_vline(x=0, line_dash="dash", line_color="#3b82f6", opacity=0.5, annotation_text="下轨")
            fig_boll.add_vline(x=50, line_dash="dash", line_color="#8b949e", opacity=0.5, annotation_text="中轨")
            fig_boll.add_vline(x=100, line_dash="dash", line_color="#ef4444", opacity=0.5, annotation_text="上轨")

            fig_boll.update_layout(
                xaxis=dict(
                    title="布林带位置 (%)",
                    range=[
                        min(-10, boll_df["bollinger_position"].min() - 5),
                        max(110, boll_df["bollinger_position"].max() + 5),
                    ],
                    tickfont=dict(size=10, color="#8b949e"),
                    gridcolor="#21262d",
                ),
                yaxis=dict(title="", tickfont=dict(size=10, color="#c9d1d9"), gridcolor="#21262d"),
                paper_bgcolor="#0d1117",
                plot_bgcolor="#0d1117",
                height=max(300, 30 * n_total),
                margin=dict(l=80, r=30, t=30, b=40),
                bargap=0.2,
            )
            st.plotly_chart(fig_boll, width="stretch")

            st.markdown('<div style="height:12px;"></div>', unsafe_allow_html=True)

            # ---------- RSI 分布图 ----------
            st.markdown(
                '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">RSI 相对强弱分布<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">展示所有持仓品种的RSI值分布。RSI>70为超买区间(红色)，RSI<30为超卖区间(蓝色)。</span></div>',
                unsafe_allow_html=True,
            )

            rsi_df = tech_df[["name", "code", "rsi_value", "rsi_status"]].sort_values("rsi_value", ascending=True)

            rsi_bar_colors = []
            for _, row in rsi_df.iterrows():
                r = row["rsi_value"]
                if r >= 80:
                    rsi_bar_colors.append("#ef4444")
                elif r >= 70:
                    rsi_bar_colors.append("#f59e0b")
                elif r <= 20:
                    rsi_bar_colors.append("#3b82f6")
                elif r <= 30:
                    rsi_bar_colors.append("#f59e0b")
                else:
                    rsi_bar_colors.append("#22c55e")

            fig_rsi = go.Figure(
                go.Bar(
                    orientation="h",
                    y=rsi_df["name"],
                    x=rsi_df["rsi_value"],
                    marker_color=rsi_bar_colors,
                    text=[f"{v:.1f}" for v in rsi_df["rsi_value"]],
                    textposition="auto",
                    textfont=dict(size=10, color="#c9d1d9"),
                    hovertemplate="%{y}: RSI=%{x:.1f}<extra></extra>",
                )
            )
            # RSI 参考区域
            fig_rsi.add_vrect(x0=0, x1=30, fillcolor="rgba(59,130,246,0.08)", line_width=0)
            fig_rsi.add_vrect(x0=70, x1=100, fillcolor="rgba(239,68,68,0.08)", line_width=0)
            fig_rsi.add_vline(x=30, line_dash="dash", line_color="#3b82f6", opacity=0.4)
            fig_rsi.add_vline(x=70, line_dash="dash", line_color="#ef4444", opacity=0.4)
            fig_rsi.add_vline(x=50, line_dash="dot", line_color="#8b949e", opacity=0.3)

            fig_rsi.update_layout(
                xaxis=dict(
                    title="RSI 值", range=[0, 100], tickfont=dict(size=10, color="#8b949e"), gridcolor="#21262d"
                ),
                yaxis=dict(title="", tickfont=dict(size=10, color="#c9d1d9"), gridcolor="#21262d"),
                paper_bgcolor="#0d1117",
                plot_bgcolor="#0d1117",
                height=max(300, 30 * n_total),
                margin=dict(l=80, r=30, t=30, b=40),
                bargap=0.2,
            )
            st.plotly_chart(fig_rsi, width="stretch")

    # ========== Tab7: 资讯与评估 ==========



def _render_tab7_news(tab7, positions, summary, technical):
    """Extracted from main() - tab7 renderer"""
    with tab7:
        st.caption("📰 持仓相关市场资讯与综合评估，帮助把握投资时机")

        # ===== 资讯面板 =====
        st.markdown(
            '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">市场资讯<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">展示与持仓板块相关的最新市场新闻，按行业板块分类。</span></div>',
            unsafe_allow_html=True,
        )

        news_categories_map = {
            "医药": "医药板块",
            "金融": "券商板块",
            "军工": "军工板块",
            "新能源": "大盘行情",
            "科技": "AI板块",
            "宽基": "ETF市场",
            "红利": "大盘行情",
            "债券": "大盘行情",
        }
        if not positions.empty:
            held_sectors = set()
            for _, pos in positions.iterrows():
                code = str(pos["code"])
                cat_info = ETF_CATEGORIES.get(code)
                if cat_info:
                    held_sectors.add(cat_info["sector"])

            news_cats_to_load = set()
            for sector in held_sectors:
                cat = news_categories_map.get(sector, "大盘行情")
                news_cats_to_load.add(cat)
            news_cats_to_load.add("大盘行情")
            news_cats_to_load.add("ETF市场")
        else:
            news_cats_to_load = ["大盘行情", "ETF市场"]

        news_df = _load_latest_news(tuple(news_cats_to_load))

        if not news_df.empty:
            # Category filter
            all_cats = sorted(news_df["category"].unique())
            selected_cat = st.selectbox(
                "筛选板块", ["全部"] + all_cats, key="news_cat_filter", label_visibility="collapsed"
            )
            if selected_cat != "全部":
                filtered_news = news_df[news_df["category"] == selected_cat]
            else:
                filtered_news = news_df

            cat_color_map = {
                "大盘行情": "#58a6ff",
                "ETF市场": "#06b6d4",
                "医药板块": "#22c55e",
                "券商板块": "#58a6ff",
                "军工板块": "#ef4444",
                "AI板块": "#a855f7",
                "新能源": "#f59e0b",
            }

            # 每个分类最多显示 5 条，避免单一板块刷屏
            if not filtered_news.empty:
                _display = filtered_news.groupby("category").head(5).reset_index(drop=True)
            else:
                _display = filtered_news
            for _, row in _display.iterrows():
                cat_color = cat_color_map.get(row["category"], "#8b949e")
                summary_text = row.get("summary", "") or ""
                summary_html = (
                    f'<div style="font-size:12px;color:#6e7681;margin-top:4px;line-height:1.5;">{summary_text[:150]}{"..." if len(summary_text) > 150 else ""}</div>'
                    if summary_text
                    else ""
                )
                url_html = (
                    f'<a href="{row["url"]}" target="_blank" style="font-size:11px;color:#58a6ff;">{row["source"]} | {row.get("publish_time", "")[:16]}</a>'
                    if pd.notna(row.get("url")) and row["url"]
                    else f'<span style="font-size:11px;color:#484f58;">{row["source"]}</span>'
                )
                st.markdown(
                    f'<div style="background:#161b22;border-radius:6px;padding:12px 14px;margin-bottom:6px;border-left:3px solid {cat_color};">'
                    f'<div style="display:flex;justify-content:space-between;align-items:center;">'
                    f'<span style="font-size:11px;color:{cat_color};background:{cat_color}15;padding:2px 8px;border-radius:3px;">{row["category"]}</span>'
                    f'<span style="font-size:11px;color:#484f58;">{row["date"]}</span>'
                    f"</div>"
                    f'<div style="font-size:13px;color:#e6edf3;font-weight:bold;margin-top:6px;line-height:1.4;">{row["title"]}</div>'
                    f"{summary_html}"
                    f'<div style="margin-top:6px;">{url_html}</div>'
                    f"</div>",
                    unsafe_allow_html=True,
                )
        else:
            st.info("暂无市场资讯数据，请检查数据采集服务是否正常运行")

        st.markdown("---")

        # ===== 综合评估面板 =====
        st.markdown(
            '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">综合评估<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">基于收益、风险、技术面多维度的综合投资评分。</span></div>',
            unsafe_allow_html=True,
        )

        if not summary.empty and not positions.empty:
            import math

            # 加载技术面信号数据
            conn2 = get_db_connection()
            try:
                held_codes = positions["code"].tolist()[:5]
                if held_codes:
                    tech_df = _load_tech_signals(tuple(held_codes), _full=False)
                else:
                    tech_df = pd.DataFrame()
            except Exception:
                tech_df = pd.DataFrame()
            finally:
                conn2.close()

            # 综合评分计算
            scores = compute_comprehensive_score(
                positions, summary, volatility, effective_max_dd, tech_df
            )
            score_return = scores["score_return"]
            score_risk = scores["score_risk"]
            tech_score = scores["tech_score"]
            score_health = scores["score_health"]
            total_score = scores["total_score"]
            score_color = scores["score_color"]
            score_label = scores["score_label"]
            tech_signals = scores["tech_signals"]


            # 渲染评分
            col_score1, col_score2 = st.columns([1, 2])
            with col_score1:
                fig_score_gauge = go.Figure(
                    go.Indicator(
                        mode="gauge+number",
                        value=total_score,
                        number={"suffix": "分", "font": {"size": 42, "color": score_color}},
                        gauge={
                            "axis": {"range": [0, 100], "tickcolor": "#8b949e", "tickfont": {"size": 10}},
                            "bar": {"color": score_color},
                            "bgcolor": "#161b22",
                            "steps": [
                                {"range": [0, 40], "color": "rgba(239,68,68,0.12)"},
                                {"range": [40, 70], "color": "rgba(245,158,11,0.12)"},
                                {"range": [70, 100], "color": "rgba(34,197,94,0.12)"},
                            ],
                            "threshold": {
                                "line": {"color": score_color, "width": 3},
                                "thickness": 0.8,
                                "value": total_score,
                            },
                        },
                    )
                )
                fig_score_gauge.update_layout(
                    height=220,
                    plot_bgcolor="#0d1117",
                    paper_bgcolor="#0d1117",
                    font=dict(color="#c9d1d9"),
                    margin=dict(l=20, r=20, t=5, b=5),
                )
                st.plotly_chart(fig_score_gauge, width="stretch")
                st.markdown(
                    f'<div style="text-align:center;color:{score_color};font-size:15px;font-weight:bold;">{score_label}</div>',
                    unsafe_allow_html=True,
                )

            with col_score2:
                score_items = [
                    ("收益能力", score_return, 30, "累计收益表现"),
                    ("风险控制", score_risk, 30, "波动率与回撤水平"),
                    ("技术面", tech_score, 25, "均线/MACD/RSI信号"),
                    ("持仓健康", score_health, 15, "分散度与盈亏比"),
                ]
                for name, score, max_s, desc in score_items:
                    pct = score / max_s * 100 if max_s > 0 else 0
                    bar_color = "#22c55e" if pct >= 70 else "#f59e0b" if pct >= 40 else "#ef4444"
                    st.markdown(
                        f'<div style="margin-bottom:8px;">'
                        f'<div style="display:flex;justify-content:space-between;font-size:13px;">'
                        f'<span style="color:#c9d1d9;">{name} <span style="color:#484f58;font-size:11px;">{desc}</span></span>'
                        f'<span style="color:{bar_color};font-weight:bold;">{score}/{max_s}</span>'
                        f"</div>"
                        f'<div style="height:6px;background:#21262d;border-radius:3px;overflow:hidden;margin-top:3px;">'
                        f'<div style="height:100%;width:{pct}%;background:{bar_color};border-radius:3px;transition:width 0.3s;"></div>'
                        f"</div></div>",
                        unsafe_allow_html=True,
                    )

                if tech_signals:
                    with st.expander("技术面信号详情", expanded=False):
                        for sig in tech_signals[:10]:
                            st.markdown(
                                f'<div style="font-size:12px;color:#8b949e;padding:3px 0;">{sig}</div>',
                                unsafe_allow_html=True,
                            )
        else:
            st.info("数据不足，暂无法生成综合评估")

        # ===== 市场情绪仪表盘 =====
        st.markdown("---")
        st.markdown(
            '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">'
            "市场情绪仪表盘"
            '<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span>'
            '<span class="tip-text" style="left: 4px; top: calc(100% + 10px);">'
            "基于持仓ETF的涨跌分布，计算市场宽度、涨跌比、情绪偏向等指标，辅助判断整体市场情绪。"
            "</span></div>",
            unsafe_allow_html=True,
        )
        st.caption("基于持仓ETF的涨跌分布，计算市场宽度、涨跌比、情绪偏向等指标")

        if not positions.empty:
            total_count = len(positions)
            up_count = len(positions[positions["pnl"] > 0])
            dn_count = len(positions[positions["pnl"] < 0])
            flat_count = total_count - up_count - dn_count
            up_ratio = up_count / total_count * 100 if total_count > 0 else 50

            if up_ratio >= 75:
                emotion = ("极度乐观", "#22c55e", "多数持仓上涨，市场情绪高涨，注意短期过热风险")
            elif up_ratio >= 60:
                emotion = ("偏乐观", "#4ade80", "多数持仓上涨，市场情绪偏暖")
            elif up_ratio >= 45:
                emotion = ("中性", "#f59e0b", "涨跌互现，市场情绪中性")
            elif up_ratio >= 30:
                emotion = ("偏悲观", "#fb923c", "多数持仓下跌，市场情绪偏冷，关注企稳信号")
            else:
                emotion = ("极度悲观", "#ef4444", "多数持仓下跌，市场情绪低迷，可能存在超跌反弹机会")

            ec1, ec2, ec3, ec4 = st.columns(4)
            with ec1:
                st.markdown(
                    f'<div style="padding:8px;border-radius:6px;background:#161b22;border-left:3px solid {emotion[1]};text-align:center;">'
                    f'<div style="font-size:10px;color:#8b949e;">市场情绪</div>'
                    f'<div style="font-size:16px;font-weight:bold;color:{emotion[1]};">{emotion[0]}</div>'
                    f"</div>",
                    unsafe_allow_html=True,
                )
            with ec2:
                st.markdown(
                    f'<div style="padding:8px;border-radius:6px;background:#161b22;border-left:3px solid #58a6ff;text-align:center;">'
                    f'<div style="font-size:10px;color:#8b949e;">涨跌比</div>'
                    f'<div style="font-size:16px;font-weight:bold;">'
                    f'<span style="color:#22c55e;">{up_count}</span>'
                    f' / <span style="color:#ef4444;">{dn_count}</span>'
                    f' / <span style="color:#8b949e;">{flat_count}</span>'
                    f"</div></div>",
                    unsafe_allow_html=True,
                )
            with ec3:
                adv_color = "#22c55e" if up_ratio >= 50 else "#ef4444"
                st.markdown(
                    f'<div style="padding:8px;border-radius:6px;background:#161b22;border-left:3px solid {adv_color};text-align:center;">'
                    f'<div style="font-size:10px;color:#8b949e;">上涨占比</div>'
                    f'<div style="font-size:16px;font-weight:bold;color:{adv_color};">{up_ratio:.0f}%</div>'
                    f"</div>",
                    unsafe_allow_html=True,
                )
            with ec4:
                avg_pnl = positions["pnl_rate"].mean() if "pnl_rate" in positions.columns else 0
                avg_c = "#22c55e" if avg_pnl >= 0 else "#ef4444"
                st.markdown(
                    f'<div style="padding:8px;border-radius:6px;background:#161b22;border-left:3px solid {avg_c};text-align:center;">'
                    f'<div style="font-size:10px;color:#8b949e;">平均收益率</div>'
                    f'<div style="font-size:16px;font-weight:bold;color:{avg_c};">{avg_pnl:+.2f}%</div>'
                    f"</div>",
                    unsafe_allow_html=True,
                )

            st.markdown(
                f'<div style="padding:8px 12px;border-radius:6px;background:#161b22;font-size:12px;color:#8b949e;'
                f'border:1px solid #21262d;margin:6px 0;">{emotion[2]}</div>',
                unsafe_allow_html=True,
            )

            st.markdown(
                '<div style="font-size:14px;color:#c9d1d9;font-weight:bold;margin:10px 0 6px 0;">行业涨跌热力图</div>',
                unsafe_allow_html=True,
            )
            sector_pnl = {}
            for _, pos in positions.iterrows():
                code = str(pos["code"])
                cat_info = ETF_CATEGORIES.get(code)
                if cat_info:
                    sector = cat_info["sector"]
                    if sector not in sector_pnl:
                        sector_pnl[sector] = {"total_mv": 0, "total_pnl_rate": 0, "count": 0}
                    sector_pnl[sector]["total_mv"] += pos.get("market_value", 0)
                    sector_pnl[sector]["total_pnl_rate"] += pos.get("pnl_rate", 0) * pos.get("market_value", 0)
                    sector_pnl[sector]["count"] += 1
            if sector_pnl:
                sector_list = sorted(sector_pnl.items(), key=lambda x: x[1]["total_pnl_rate"], reverse=True)
                # 预计算所有行业加权平均收益率，用于动态缩放
                avg_values = [
                    sd["total_pnl_rate"] / sd["total_mv"] if sd["total_mv"] > 0 else 0 for _, sd in sector_list
                ]
                max_val = max(abs(v) for v in avg_values) if avg_values else 1
                html_bars = '<div style="display:flex;flex-direction:column;gap:6px;">'
                for (sector_name, sdata), avg_s in zip(sector_list, avg_values):
                    # pnl_rate 以百分比形式存储（如44.24=44.24%），加权平均后直接为百分比
                    bar_c = "#22c55e" if avg_s >= 0 else "#ef4444"
                    # sqrt缩放：最大值映射到45%，最小3%，避免极端值压制小值
                    bar_width = max(math.sqrt(abs(avg_s)) / math.sqrt(max_val) * 45, 3)
                    color = SECTOR_COLORS.get(sector_name, "#8b949e")
                    if avg_s >= 0:
                        bar_html = f'<div style="position:absolute;top:0;left:50%;width:{bar_width}%;height:100%;background:{bar_c};border-radius:0 3px 3px 0;"></div>'
                    else:
                        bar_html = f'<div style="position:absolute;top:0;right:50%;width:{bar_width}%;height:100%;background:{bar_c};border-radius:3px 0 0 3px;"></div>'
                    html_bars += (
                        f'<div style="display:flex;align-items:center;gap:8px;">'
                        f'<div style="width:50px;font-size:12px;color:{color};font-weight:bold;flex-shrink:0;">{sector_name}</div>'
                        f'<div style="flex:1;position:relative;height:20px;background:#161b22;border-radius:3px;overflow:hidden;">'
                        f'<div style="position:absolute;top:0;left:50%;width:1px;height:100%;background:#484f58;"></div>'
                        f"{bar_html}</div>"
                        f'<div style="width:55px;text-align:right;font-size:12px;color:{bar_c};font-weight:bold;flex-shrink:0;">{avg_s:+.2f}%</div>'
                        f'<div style="width:40px;text-align:right;font-size:10px;color:#484f58;flex-shrink:0;">{sdata["count"]}只</div></div>'
                    )
                html_bars += "</div>"
                st.markdown(html_bars, unsafe_allow_html=True)

            pc1, pc2 = st.columns([1, 2])
            with pc1:
                fig_pie = go.Figure(
                    go.Pie(
                        labels=["上涨", "下跌", "持平"],
                        values=[up_count, dn_count, flat_count],
                        marker_colors=["#22c55e", "#ef4444", "#8b949e"],
                        hole=0.6,
                        textinfo="label+percent",
                        textfont=dict(size=11, color="#c9d1d9"),
                        hovertemplate="%{label}: %{value}只 (%{percent})<extra></extra>",
                    )
                )
                fig_pie.update_layout(
                    height=220,
                    plot_bgcolor="#0d1117",
                    paper_bgcolor="#0d1117",
                    margin=dict(l=10, r=10, t=10, b=10),
                    legend=dict(font=dict(size=10, color="#8b949e"), orientation="h", yanchor="bottom", y=-0.1),
                )
                st.plotly_chart(fig_pie, width="stretch")

            with pc2:
                if "pnl_rate" in positions.columns and not positions.empty:
                    pnl_rates = positions["pnl_rate"].dropna().values
                    fig_pnl_dist = go.Figure()
                    fig_pnl_dist.add_trace(
                        go.Histogram(
                            x=pnl_rates, nbinsx=max(5, min(20, total_count)), opacity=0.85, marker_color="#58a6ff"
                        )
                    )
                    fig_pnl_dist.update_layout(
                        height=220,
                        plot_bgcolor="#0d1117",
                        paper_bgcolor="#0d1117",
                        font=dict(color="#c9d1d9", size=11),
                        margin=dict(l=40, r=20, t=10, b=30),
                        xaxis=dict(title="收益率 (%)", showgrid=True, gridcolor="#21262d"),
                        yaxis=dict(title="数量", showgrid=True, gridcolor="#21262d"),
                        bargap=0.15,
                    )
                    st.plotly_chart(fig_pnl_dist, width="stretch")

    # ========== Tab8: 操作建议 ==========



def _render_tab8_advice(tab8, positions, summary, technical):
    """Extracted from main() - tab8 renderer"""
    with tab8:
        st.caption("💡 基于技术信号和持仓状态，生成具体操作建议")

        if not positions.empty:
            conn = get_db_connection()
            try:
                held_codes = positions["code"].tolist()
                if held_codes:
                    tech_df = _load_tech_signals(tuple(held_codes), _full=True)
                else:
                    tech_df = pd.DataFrame()

            except Exception:
                tech_df = pd.DataFrame()
            finally:
                conn.close()

            suggestions = []
            action_colors = {
                "买入": "#22c55e",
                "持有": "#f59e0b",
                "观望": "#8b949e",
                "卖出": "#ef4444",
                "加仓": "#22c55e",
                "减仓": "#ef4444",
            }

            if not tech_df.empty:
                latest_tech = tech_df.drop_duplicates("code", keep="first")

                for _, pos in positions.iterrows():
                    code = str(pos["code"])
                    name = pos["name"]
                    pnl_rate = pos.get("pnl_rate", 0)
                    mv = pos["market_value"]
                    cat_info = ETF_CATEGORIES.get(code, {})
                    sector = cat_info.get("sector", "未知")

                    tech_row = latest_tech[latest_tech["code"] == code]
                    if tech_row.empty:
                        continue
                    tr = tech_row.iloc[0]

                    # 技术面综合判断
                    buy_signals = 0
                    sell_signals = 0
                    reasons = []

                    # 均线信号
                    if tr.get("ma_signal") == "多头排列":
                        buy_signals += 2
                        reasons.append("均线多头排列")
                    elif tr.get("ma_signal") == "空头排列":
                        sell_signals += 2
                        reasons.append("均线空头排列")
                    elif tr.get("ma_signal") == "金叉":
                        buy_signals += 1
                        reasons.append("均线金叉")
                    elif tr.get("ma_signal") == "死叉":
                        sell_signals += 1
                        reasons.append("均线死叉")

                    # MACD信号
                    if tr.get("macd_signal") == "金叉":
                        buy_signals += 1.5
                        reasons.append("MACD金叉")
                    elif tr.get("macd_signal") == "死叉":
                        sell_signals += 1.5
                        reasons.append("MACD死叉")

                    # RSI信号
                    rsi_val = tr.get("rsi_value", 50)
                    rsi_status = tr.get("rsi_status", "中性")
                    if rsi_status in ("超卖", "偏低"):
                        buy_signals += 1
                        reasons.append(f"RSI偏低({rsi_val:.0f})")
                    elif rsi_status in ("超买", "偏高"):
                        sell_signals += 1
                        reasons.append(f"RSI偏高({rsi_val:.0f})")

                    # KDJ信号
                    kdj = tr.get("kdj_signal", "")
                    if "金叉" in str(kdj):
                        buy_signals += 1
                        reasons.append("KDJ金叉")
                    elif "死叉" in str(kdj):
                        sell_signals += 1
                        reasons.append("KDJ死叉")

                    # 布林带
                    boll_pos = tr.get("bollinger_position", "")
                    if "下轨" in str(boll_pos):
                        buy_signals += 0.5
                        reasons.append("触及布林下轨")
                    elif "上轨" in str(boll_pos):
                        sell_signals += 0.5
                        reasons.append("触及布林上轨")

                    # 趋势
                    trend = tr.get("trend", "")
                    if trend == "上涨":
                        buy_signals += 1
                    elif trend == "下跌":
                        sell_signals += 1

                    # 盈亏状态调整
                    if pnl_rate < -10:
                        sell_signals += 0.5
                        reasons.append(f"亏损较深({pnl_rate:.1f}%)")
                    elif pnl_rate > 20:
                        sell_signals += 0.5
                        reasons.append(f"盈利较多({pnl_rate:+.1f}%)，注意止盈")

                    # 生成建议
                    net_signal = buy_signals - sell_signals
                    if net_signal >= 3:
                        action = "买入"
                        urgency = "强烈建议"
                    elif net_signal >= 1.5:
                        action = "加仓"
                        urgency = "建议"
                    elif net_signal >= -0.5:
                        action = "持有"
                        urgency = "维持"
                    elif net_signal >= -2:
                        action = "观望"
                        urgency = "建议"
                    else:
                        action = "卖出"
                        urgency = "建议"

                    suggestions.append(
                        {
                            "name": name,
                            "code": code,
                            "sector": sector,
                            "action": action,
                            "urgency": urgency,
                            "reasons": reasons,
                            "buy_score": buy_signals,
                            "sell_score": sell_signals,
                            "net_signal": net_signal,
                            "pnl_rate": pnl_rate,
                            "market_value": mv,
                            "trend": trend,
                            "rsi": rsi_val,
                        }
                    )

            # 按净信号排序
            suggestions.sort(key=lambda x: x["net_signal"], reverse=True)

            # ===== 操作建议汇总卡片 =====
            st.markdown(
                '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">建议汇总<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">基于技术指标综合评分，为每只持仓ETF生成操作建议。</span></div>',
                unsafe_allow_html=True,
            )

            action_counts = {}
            for s in suggestions:
                action_counts[s["action"]] = action_counts.get(s["action"], 0) + 1

            summary_html_parts = []
            for action in ["买入", "加仓", "持有", "观望", "卖出"]:
                cnt = action_counts.get(action, 0)
                if cnt > 0:
                    color = action_colors[action]
                    summary_html_parts.append(
                        f'<span style="display:inline-flex;align-items:center;gap:4px;background:{color}15;color:{color};padding:6px 14px;border-radius:6px;margin:0 4px 4px 0;font-size:13px;font-weight:bold;">'
                        f'{action} <span style="font-size:16px;">{cnt}</span>只</span>'
                    )
            st.markdown(
                f'<div style="display:flex;flex-wrap:wrap;gap:4px;padding:8px 0;">{"".join(summary_html_parts)}</div>',
                unsafe_allow_html=True,
            )

            # ===== 建议详情 =====
            st.markdown(
                '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">建议详情</div>',
                unsafe_allow_html=True,
            )

            for s in suggestions:
                action_color = action_colors.get(s["action"], "#8b949e")
                sector_color = SECTOR_COLORS.get(s["sector"], "#8b949e")
                trend_icon = {"上涨": "🟢", "下跌": "🔴", "震荡": "🟡"}.get(s["trend"], "⚪")
                reasons_str = " | ".join(s["reasons"][:5]) if s["reasons"] else "暂无明显信号"

                st.markdown(
                    f'<div style="background:#161b22;border-radius:6px;padding:12px 14px;margin-bottom:6px;border-left:3px solid {action_color};">'
                    f'<div style="display:flex;justify-content:space-between;align-items:center;">'
                    f"<div>"
                    f'<span style="font-size:14px;font-weight:bold;color:#e6edf3;">{s["name"]}</span>'
                    f'<span style="font-size:11px;color:#484f58;margin-left:8px;">{s["code"]}</span>'
                    f'<span style="font-size:11px;color:{sector_color};background:{sector_color}15;padding:1px 6px;border-radius:3px;margin-left:6px;">{s["sector"]}</span>'
                    f"</div>"
                    f'<div style="display:flex;align-items:center;gap:6px;">'
                    f"{trend_icon}"
                    f'<span style="color:{action_color};font-size:13px;font-weight:bold;background:{action_color}15;padding:3px 10px;border-radius:4px;">{s["urgency"]}{s["action"]}</span>'
                    f"</div></div>"
                    f'<div style="font-size:12px;color:#6e7681;margin-top:6px;">信号: {reasons_str}</div>'
                    f'<div style="display:flex;gap:16px;margin-top:4px;font-size:11px;color:#484f58;">'
                    f'<span>多空信号: <b style="color:#22c55e;">{s["buy_score"]:.1f}</b> / <b style="color:#ef4444;">{s["sell_score"]:.1f}</b></span>'
                    f'<span>净信号: <b style="color:{action_color};">{s["net_signal"]:+.1f}</b></span>'
                    f'<span>收益率: <b style="color:{"#22c55e" if s["pnl_rate"] >= 0 else "#ef4444"};">{s["pnl_rate"]:+.2f}%</b></span>'
                    f'<span>RSI: {s["rsi"]:.0f}</span>'
                    f"</div></div>",
                    unsafe_allow_html=True,
                )

            if not suggestions:
                st.info("暂无足够技术数据生成操作建议")
        else:
            st.info("暂无持仓数据")

    # ========== 数据导出 ==========
    st.markdown("---")
    st.markdown(
        '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">数据导出<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">将当前投资组合数据导出为 Excel 专业报告，包含持仓明细、收益汇总、风险分析、技术指标和告警记录。</span></div>',
        unsafe_allow_html=True,
    )

    ec1, ec2 = st.columns(2)
    with ec1:
        if st.button("📊 导出 Excel 报告", width='stretch', type="primary"):
            try:
                from src.report.excel_report import ExcelReportGenerator

                gen = ExcelReportGenerator(str(DATABASE_PATH))
                output = gen.generate()
                st.success(f"报告已生成: {output}")
                with open(output, "rb") as f:
                    st.download_button(
                        label="⬇ 下载 Excel 报告",
                        data=f.read(),
                        file_name=os.path.basename(output),
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        width='stretch',
                    )
            except Exception as e:
                st.error(f"导出失败: {e}")
    with ec2:
        if st.button("📄 导出 HTML 日报", width='stretch'):
            try:
                from src.utils.email_report import EmailReportBuilder

                builder = EmailReportBuilder(str(DATABASE_PATH))
                html = builder.build_daily_report()
                report_path = builder.save_report(html)
                st.success(f"报告已生成: {report_path}")
                with open(report_path, "r", encoding="utf-8") as f:
                    st.download_button(
                        label="⬇ 下载 HTML 日报",
                        data=f.read(),
                        file_name=os.path.basename(report_path),
                        mime="text/html",
                        width='stretch',
                    )
            except Exception as e:
                st.error(f"导出失败: {e}")

    # ========== Tab5: 高级分析（Monte Carlo / 再平衡建议） ==========



def _render_tab5_advanced(tab5, positions, summary, technical):
    """Extracted from main() - tab5 renderer"""
    with tab5:
        st.markdown(
            '<div class="tip-title" style="">高级分析工具<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">包含Monte Carlo模拟（基于历史收益率随机采样预测未来收益区间）和再平衡建议（基于目标权重偏离度生成调仓方案）两种高级分析工具。</span></div>',
            unsafe_allow_html=True,
        )

        # ----- Monte Carlo 模拟 -----
        st.markdown(
            '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">Monte Carlo 模拟（未来收益预测）<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">基于历史日收益率分布进行Bootstrap随机采样，生成大量模拟路径，统计未来市值的概率分布区间（P5/P50/P95）。</span></div>',
            unsafe_allow_html=True,
        )
        st.caption("基于历史日收益率分布进行 Bootstrap 采样，生成未来收益区间预测")

        mc_col1, mc_col2 = st.columns([2, 1])
        with mc_col1:
            mc_days = st.slider("模拟天数", 30, 500, 252, step=30, key="mc_days")
        with mc_col2:
            mc_sims = st.selectbox("模拟路径数", [200, 500, 1000], index=1, key="mc_sims")

        mc_result = run_monte_carlo(days=mc_days, n_simulations=mc_sims, end_date=selected_date)

        if mc_result is not None:
            perc_df = mc_result["percentiles"]

            # 扇形区域图
            fig_mc = go.Figure()

            # 扇形填充区域（从外到内）
            fig_mc.add_trace(
                go.Scatter(
                    x=perc_df["day"],
                    y=perc_df["p95"],
                    mode="lines",
                    name="P95",
                    line=dict(width=0),
                    showlegend=False,
                    hoverinfo="skip",
                )
            )
            fig_mc.add_trace(
                go.Scatter(
                    x=perc_df["day"],
                    y=perc_df["p75"],
                    mode="lines",
                    name="P75",
                    fill="tonexty",
                    fillcolor="rgba(88,166,255,0.08)",
                    line=dict(width=0),
                    showlegend=False,
                    hoverinfo="skip",
                )
            )
            fig_mc.add_trace(
                go.Scatter(
                    x=perc_df["day"],
                    y=perc_df["p25"],
                    mode="lines",
                    name="P25",
                    fill="tonexty",
                    fillcolor="rgba(88,166,255,0.12)",
                    line=dict(width=0),
                    showlegend=False,
                    hoverinfo="skip",
                )
            )
            fig_mc.add_trace(
                go.Scatter(
                    x=perc_df["day"],
                    y=perc_df["p5"],
                    mode="lines",
                    name="P5",
                    fill="tonexty",
                    fillcolor="rgba(88,166,255,0.08)",
                    line=dict(width=0),
                    showlegend=False,
                    hoverinfo="skip",
                )
            )

            # 中位数线
            fig_mc.add_trace(
                go.Scatter(
                    x=perc_df["day"],
                    y=perc_df["p50"],
                    mode="lines",
                    name="中位数 (P50)",
                    line=dict(color="#58a6ff", width=2),
                    hovertemplate="第 %{x} 天<br>中位数: ¥%{y:,.0f}<extra></extra>",
                )
            )

            # 起始值水平线
            fig_mc.add_hline(
                y=mc_result["last_value"],
                line_dash="dash",
                line_color="#f59e0b",
                annotation_text=f"当前 ¥{mc_result['last_value']:,.0f}",
                annotation_position="top right",
                annotation_font=dict(size=10, color="#f59e0b"),
            )

            fig_mc.update_layout(
                height=350,
                plot_bgcolor="#0d1117",
                paper_bgcolor="#0d1117",
                font=dict(color="#c9d1d9", size=11),
                margin=dict(l=60, r=20, t=10, b=40),
                xaxis=dict(title="交易日", showgrid=False),
                yaxis=dict(title="组合市值 (¥)", showgrid=True, gridcolor="#21262d"),
                hovermode="x unified",
                legend=dict(
                    orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(size=10, color="#8b949e")
                ),
            )
            st.plotly_chart(fig_mc, width="stretch")

            # 模拟摘要
            mc_sum1, mc_sum2, mc_sum3, mc_sum4 = st.columns(4)
            with mc_sum1:
                st.metric("当前市值", f"¥{mc_result['last_value']:,.0f}")
            with mc_sum2:
                final_p50 = perc_df["p50"].iloc[-1]
                chg = (final_p50 / mc_result["last_value"] - 1) * 100 if mc_result["last_value"] > 0 else 0
                st.metric(f"P50 ({mc_days}日后)", f"¥{final_p50:,.0f}", delta=f"{chg:+.1f}%")
            with mc_sum3:
                final_p5 = perc_df["p5"].iloc[-1]
                loss = (final_p5 / mc_result["last_value"] - 1) * 100 if mc_result["last_value"] > 0 else 0
                st.metric("P5 (悲观)", f"¥{final_p5:,.0f}", delta=f"{loss:+.1f}%")
            with mc_sum4:
                final_p95 = perc_df["p95"].iloc[-1]
                gain = (final_p95 / mc_result["last_value"] - 1) * 100 if mc_result["last_value"] > 0 else 0
                st.metric("P95 (乐观)", f"¥{final_p95:,.0f}", delta=f"{gain:+.1f}%")

            # VaR 估计
            with st.expander("查看风险价值 (VaR) 估计", expanded=False):
                var_95 = mc_result["last_value"] - perc_df["p5"].iloc[-1]
                cvar_95 = mc_result["last_value"] - np.percentile(mc_result["paths"][:, -1], 5)
                st.markdown(
                    f"**95% VaR（{mc_days}日）:** ¥{var_95:,.0f}\n\n"
                    f"**95% CVaR（条件VaR）:** ¥{cvar_95:,.0f}\n\n"
                    f"*VaR 表示在 95% 置信度下，{mc_days} 个交易日内的最大可能损失。"
                    f"CVaR 是超出 VaR 时的平均损失（尾部风险）。*"
                )
        else:
            st.info("历史数据不足（需要至少30个交易日），暂无法进行 Monte Carlo 模拟")

        st.markdown("---")

        # ----- 压力测试 -----
        st.markdown(
            '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">' "持仓压力测试</div>",
            unsafe_allow_html=True,
        )
        st.caption("基于历史波动率和持仓权重，模拟极端情景下的组合市值变化")

        if not positions.empty and not summary.empty:
            total_mv = positions["market_value"].sum()
            current_weights = {}
            for _, pos in positions.iterrows():
                code = str(pos["code"])
                current_weights[code] = {
                    "weight": pos["market_value"] / total_mv if total_mv > 0 else 0,
                    "name": pos["name"],
                    "beta": pos.get("beta", 1.0) if pd.notna(pos.get("beta")) else 1.0,
                    "sector": ETF_CATEGORIES.get(code, {}).get("sector", "未知"),
                    "mv": pos["market_value"],
                    "pnl_rate": pos.get("pnl_rate", 0),
                }

            scenarios = {
                "温和下跌": {
                    "market": -0.05,
                    "label": "基准跌5%",
                    "color": "#f59e0b",
                    "desc": "市场温和回调，宽基ETF领跌",
                },
                "大幅下跌": {
                    "market": -0.15,
                    "label": "基准跌15%",
                    "color": "#ef4444",
                    "desc": "市场大幅下跌，成长板块承压",
                },
                "极端暴跌": {
                    "market": -0.30,
                    "label": "基准跌30%",
                    "color": "#dc2626",
                    "desc": "类似股灾级别的系统性风险",
                },
                "震荡盘整": {
                    "market": -0.02,
                    "label": "基准±2%",
                    "color": "#8b949e",
                    "desc": "市场窄幅震荡，行业轮动加快",
                },
                "结构牛市": {
                    "market": 0.15,
                    "label": "基准涨15%",
                    "color": "#22c55e",
                    "desc": "市场结构性上涨，科技成长领涨",
                },
            }
            st_cols = st.columns(len(scenarios))
            stress_results = []
            for idx, (sname, sdata) in enumerate(scenarios.items()):
                market_shock = sdata["market"]
                total_impact = 0
                sector_impacts = {}
                for code, wdata in current_weights.items():
                    beta = wdata["beta"] if wdata["beta"] and not np.isnan(wdata["beta"]) else 1.0
                    sector = wdata["sector"]
                    if market_shock < -0.1:
                        sector_adj = {"医药": 0.85, "债券": 0.6, "红利": 0.8, "军工": 0.9}.get(sector, 1.0)
                    elif market_shock > 0.1:
                        sector_adj = {"科技": 1.2, "新能源": 1.15, "军工": 1.1}.get(sector, 1.0)
                    else:
                        sector_adj = 1.0
                    adj_shock = market_shock * beta * sector_adj
                    total_impact += wdata["weight"] * adj_shock
                    if sector not in sector_impacts:
                        sector_impacts[sector] = 0
                    sector_impacts[sector] += wdata["weight"] * adj_shock
                est_loss = total_mv * total_impact
                stress_results.append(
                    {
                        "scenario": sname,
                        "market": sdata["label"],
                        "est_loss": est_loss,
                        "est_value": total_mv + est_loss,
                        "impact_pct": total_impact * 100,
                        "color": sdata["color"],
                        "desc": sdata["desc"],
                        "sector_impacts": sector_impacts,
                    }
                )
                with st_cols[idx]:
                    loss_c = "#22c55e" if est_loss >= 0 else "#ef4444"
                    st.markdown(
                        f'<div style="padding:8px;border-radius:6px;background:#161b22;'
                        f'border-left:3px solid {sdata["color"]};text-align:center;">'
                        f'<div style="font-size:10px;color:#8b949e;">{sname}</div>'
                        f'<div style="font-size:10px;color:#484f58;">{sdata["label"]}</div>'
                        f'<div style="font-size:14px;font-weight:bold;color:{loss_c};margin:4px 0;">'
                        f'{"+" if est_loss >= 0 else ""}\u00a5{est_loss:,.0f}</div>'
                        f'<div style="font-size:11px;color:{loss_c};">{total_impact*100:+.1f}%</div></div>',
                        unsafe_allow_html=True,
                    )

            with st.expander("查看压力测试详情", expanded=False):
                for sr in stress_results:
                    loss_c = "#22c55e" if sr["est_loss"] >= 0 else "#ef4444"
                    st.markdown(
                        f'<div style="margin:8px 0;padding:10px 12px;border-radius:6px;background:#161b22;'
                        f'border-left:3px solid {sr["color"]};">'
                        f'<div style="display:flex;justify-content:space-between;align-items:center;">'
                        f'<span style="font-size:14px;font-weight:bold;color:#c9d1d9;">{sr["scenario"]} '
                        f'<span style="font-size:11px;color:#484f58;">({sr["market"]})</span></span>'
                        f'<span style="font-size:16px;font-weight:bold;color:{loss_c};">'
                        f'{sr["impact_pct"]:+.1f}% ({"+" if sr["est_loss"] >= 0 else ""}\u00a5{sr["est_loss"]:,.0f})</span></div>'
                        f'<div style="font-size:11px;color:#8b949e;margin-top:4px;">{sr["desc"]}</div>'
                        f'<div style="font-size:11px;color:#c9d1d9;margin-top:6px;">'
                        f'预估市值: <b>\u00a5{sr["est_value"]:,.0f}</b> (当前 \u00a5{total_mv:,.0f})</div></div>',
                        unsafe_allow_html=True,
                    )
                    if sr["sector_impacts"]:
                        si_cols = st.columns(min(len(sr["sector_impacts"]), 4))
                        for si_idx, (sec_name, sec_impact) in enumerate(
                            sorted(sr["sector_impacts"].items(), key=lambda x: abs(x[1]), reverse=True)
                        ):
                            si_c = "#22c55e" if sec_impact >= 0 else "#ef4444"
                            sec_color = SECTOR_COLORS.get(sec_name, "#8b949e")
                            with si_cols[si_idx % len(si_cols)]:
                                st.markdown(
                                    f'<div style="text-align:center;padding:4px 0;">'
                                    f'<div style="font-size:10px;color:{sec_color};">{sec_name}</div>'
                                    f'<div style="font-size:12px;font-weight:bold;color:{si_c};">{sec_impact*100:+.1f}%</div></div>',
                                    unsafe_allow_html=True,
                                )
                    st.markdown(
                        '<div style="height:1px;background:#21262d;margin:6px 0;"></div>', unsafe_allow_html=True
                    )
                worst = min(stress_results, key=lambda x: x["est_value"])
                st.markdown(
                    f'<div style="padding:8px 12px;border-radius:6px;background:#2d1215;'
                    f'border:1px solid #ef4444;font-size:12px;color:#c9d1d9;">'
                    f'<b>极端情景预警:</b> 在「{worst["scenario"]}」({worst["market"]})情景下，'
                    f'组合预估损失 <b style="color:#ef4444;">\u00a5{worst["est_loss"]:,.0f} ({worst["impact_pct"]:+.1f}%)</b>，'
                    f'预估市值 <b>\u00a5{worst["est_value"]:,.0f}</b>。</div>',
                    unsafe_allow_html=True,
                )
        else:
            st.info("暂无持仓数据，无法执行压力测试")

        st.markdown("---")

        st.markdown("---")

        # ----- 再平衡建议 -----
        st.markdown(
            '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">再平衡建议<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">基于各行业目标权重与实际权重的偏离度，自动生成调仓方案。超过偏离阈值的行业将给出买入/卖出建议和估算股数。</span></div>',
            unsafe_allow_html=True,
        )
        st.caption("基于目标行业权重与实际权重的偏离，生成调仓方案")

        rb_col1, rb_col2 = st.columns([2, 1])
        with rb_col1:
            st.markdown("*默认目标权重*")
        with rb_col2:
            show_rb = st.toggle("显示再平衡方案", value=True, key="rb_toggle")

        # 目标权重展示
        default_targets = {
            "医药": 0.15,
            "金融": 0.10,
            "军工": 0.10,
            "新能源": 0.15,
            "科技": 0.15,
            "宽基": 0.20,
            "红利": 0.10,
            "债券": 0.05,
        }

        if show_rb:
            rb_result = compute_rebalance_suggestion(threshold=0.03)

            if rb_result is not None:
                rw = rb_result["current_weights"]
                tw = rb_result["target_weights"]
                all_sectors = sorted(set(list(rw.keys()) + list(tw.keys())))

                # 权重对比柱状图
                fig_rb = go.Figure()
                x_labels = all_sectors
                fig_rb.add_trace(
                    go.Bar(
                        name="当前权重",
                        x=x_labels,
                        y=[rw.get(s, 0) * 100 for s in all_sectors],
                        marker_color="#58a6ff",
                        opacity=0.85,
                        hovertemplate="%{x}<br>当前: %{y:.1f}%<extra></extra>",
                    )
                )
                fig_rb.add_trace(
                    go.Bar(
                        name="目标权重",
                        x=x_labels,
                        y=[tw.get(s, 0) * 100 for s in all_sectors],
                        marker_color="#f59e0b",
                        opacity=0.6,
                        hovertemplate="%{x}<br>目标: %{y:.1f}%<extra></extra>",
                    )
                )

                # 偏离线
                deviations = [(rw.get(s, 0) - tw.get(s, 0)) * 100 for s in all_sectors]
                fig_rb.add_trace(
                    go.Scatter(
                        name="偏离",
                        x=x_labels,
                        y=deviations,
                        mode="lines+markers",
                        marker_color="#ef4444",
                        marker_size=6,
                        line=dict(color="#ef4444", width=1.5, dash="dot"),
                        yaxis="y2",
                        hovertemplate="%{x}<br>偏离: %{y:+.1f}%<extra></extra>",
                    )
                )

                fig_rb.update_layout(
                    height=300,
                    barmode="group",
                    plot_bgcolor="#0d1117",
                    paper_bgcolor="#0d1117",
                    font=dict(color="#c9d1d9", size=11),
                    margin=dict(l=40, r=40, t=10, b=40),
                    xaxis=dict(showgrid=False, tickfont=dict(size=10)),
                    yaxis=dict(title="权重 (%)", showgrid=True, gridcolor="#21262d"),
                    yaxis2=dict(title="偏离 (%)", overlaying="y", side="right", showgrid=False, range=[-20, 20]),
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=1.02,
                        xanchor="right",
                        x=1,
                        font=dict(size=10, color="#8b949e"),
                    ),
                )
                st.plotly_chart(fig_rb, width="stretch")

                # 摘要指标
                rb_s1, rb_s2, rb_s3 = st.columns(3)
                with rb_s1:
                    n_suggestions = len(rb_result["suggestions"])
                    st.metric("需调仓行业", f"{n_suggestions} 个")
                with rb_s2:
                    max_dev = max(abs(rw.get(s, 0) - tw.get(s, 0)) * 100 for s in all_sectors)
                    max_sector = max(all_sectors, key=lambda s: abs(rw.get(s, 0) - tw.get(s, 0)))
                    st.metric("最大偏离", f"{max_dev:.1f}%", delta=max_sector)
                with rb_s3:
                    st.metric("组合总市值", f"¥{rb_result['total_value']:,.0f}")

                # 调仓明细
                if rb_result["suggestions"]:
                    with st.expander("查看调仓明细", expanded=False):
                        rb_rows = []
                        for s in rb_result["suggestions"]:
                            rb_rows.append(
                                {
                                    "行业": s["sector"],
                                    "ETF": f"{s['name']}（{s['code']}）",
                                    "方向": s["direction"],
                                    "当前权重": f"{s['current_weight']*100:.1f}%",
                                    "目标权重": f"{s['target_weight']*100:.1f}%",
                                    "偏离": f"{s['diff']*100:+.1f}%",
                                    "调仓金额": f"¥{s['trade_value']:+,.0f}",
                                    "预估股数": f"{s['shares']:+,}",
                                    "现价": f"¥{s['price']:.3f}",
                                }
                            )
                        st.markdown(pd.DataFrame(rb_rows).to_html(index=False, escape=False), unsafe_allow_html=True)
                        st.caption(
                            f"*调仓阈值为 {rb_result['threshold']*100:.0f}%，低于此偏离的行业不触发调仓。股数按整数估算，实际以交易为准。*"
                        )
                else:
                    st.success("当前行业权重分布合理，无需调仓")
            else:
                st.info("暂无持仓数据，无法生成再平衡建议")
        else:
            # 显示目标权重表格
            target_df = pd.DataFrame([{"行业": k, "目标权重": f"{v*100:.0f}%"} for k, v in default_targets.items()])
            st.markdown(target_df.to_html(index=False, escape=False), unsafe_allow_html=True)

    # ========== 技术指标（增强版：点击持仓行查看详情） ==========
    st.markdown(
        '<div class="tip-title" style="margin-top:20px;">🔍 技术指标信号<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">展示各ETF的技术指标信号概览，包括RSI超买超卖、MA均线信号和综合趋势判断(看多/看空/中性)。点击持仓表格中的ETF行可查看完整技术分析面板。</span></div>',
        unsafe_allow_html=True,
    )
    if not technical.empty:
        st.info(
            "💡 点击上方持仓表格中的任意ETF行，即可查看完整的技术分析详情面板（价格走势、RSI/MACD/KDJ指标、收益率分布等）。"
        )
        # 全览信号卡片（精简版）
        trend_map = {
            "bullish": ("看多", "#22c55e"),
            "bearish": ("看空", "#ef4444"),
            "neutral": ("中性", "#f59e0b"),
            None: ("--", "#888"),
        }
        tech_cols = st.columns(min(len(technical), 6))
        for idx, (_, row) in enumerate(technical.iterrows()):
            if idx >= 12:
                break
            with tech_cols[idx % len(tech_cols)]:
                trend_label, trend_color = trend_map.get(row.get("trend"), ("--", "#888"))
                st.markdown(
                    f'<div style="padding:8px;border-radius:6px;background:#161b22;'
                    f'border-left:3px solid {trend_color};margin-bottom:4px;">'
                    f'<div style="font-size:11px;color:#c9d1d9;font-weight:bold;white-space:nowrap;'
                    f'overflow:hidden;text-overflow:ellipsis;">{row.get("name", row.get("code", "未知"))}</div>'
                    f'<div style="font-size:11px;color:{trend_color};">{trend_label}</div>'
                    f'<div style="font-size:10px;color:#8b949e;">RSI: {row.get("rsi_value", 0):.1f} | MA: {row.get("ma_signal", "--")}</div>'
                    f"</div>",
                    unsafe_allow_html=True,
                )

    # ========== 智能建议 ==========
    report_dir = PROJECT_ROOT / "data" / "reports"
    if report_dir.exists():
        report_files = sorted(report_dir.glob("smart_report_*.md"), reverse=True)
        if report_files:
            with st.expander("💡 智能分析建议（最新报告）", expanded=False):
                with open(report_files[0], "r", encoding="utf-8") as f:
                    report_text = f.read()
                st.markdown(report_text[:3000] + ("..." if len(report_text) > 3000 else ""))

    # ========== 数据导出 ==========
    st.markdown("---")
    with st.expander("📥 数据导出", expanded=False):
        col_exp1, col_exp2, col_exp3 = st.columns(3)
        with col_exp1:
            if not positions.empty:
                href_pos, fname_pos = export_positions_csv(positions, f"持仓数据_{selected_date}")
                st.markdown(
                    f'<a href="{href_pos}" download="{fname_pos}" '
                    f'style="display:inline-block;padding:8px 16px;background:#21262d;color:#c9d1d9;'
                    f'border-radius:6px;text-decoration:none;font-size:13px;border:1px solid #30363d;">'
                    f"📋 导出持仓数据 (CSV)</a>",
                    unsafe_allow_html=True,
                )
        with col_exp2:
            if not summary.empty:
                href_sum, fname_sum = export_summary_csv(summary, f"收益数据_{selected_date}")
                st.markdown(
                    f'<a href="{href_sum}" download="{fname_sum}" '
                    f'style="display:inline-block;padding:8px 16px;background:#21262d;color:#c9d1d9;'
                    f'border-radius:6px;text-decoration:none;font-size:13px;border:1px solid #30363d;">'
                    f"📈 导出收益数据 (CSV)</a>",
                    unsafe_allow_html=True,
                )
        with col_exp3:
            if st.button("📸 导出 Dashboard 截图 (PNG)", key="screenshot_btn"):
                with st.spinner("正在截图，请稍候..."):
                    screenshot_path = capture_dashboard_screenshot(port=8501)
                if screenshot_path:
                    st.success(f"截图已保存: {screenshot_path}")
                    # 提供下载链接
                    with open(screenshot_path, "rb") as f:
                        img_bytes = f.read()
                    st.download_button(
                        label="📥 下载截图",
                        data=img_bytes,
                        file_name=f"dashboard_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png",
                        mime="image/png",
                        key="download_screenshot",
                    )
                else:
                    st.error("截图失败，请确认 Dashboard 正在运行")

        # PDF 导出按钮（第4列，新行）
        st.markdown("<br>", unsafe_allow_html=True)
        col_exp4 = st.columns([1, 1, 1])[1]
        with col_exp4:
            if st.button("📄 导出 Dashboard 报告 (PDF)", key="pdf_btn"):
                with st.spinner("正在生成 PDF，请稍候..."):
                    pdf_path = export_dashboard_pdf(port=8501)
                if pdf_path:
                    st.success(f"PDF 已生成: {pdf_path}")
                    with open(pdf_path, "rb") as f:
                        pdf_bytes = f.read()
                    st.download_button(
                        label="📥 下载 PDF 报告",
                        data=pdf_bytes,
                        file_name=f"dashboard_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf",
                        mime="application/pdf",
                        key="download_pdf",
                    )
                else:
                    st.error("PDF 导出失败，请确认 Dashboard 正在运行")

        # 一键综合分析报告
        st.markdown("<br>", unsafe_allow_html=True)
        col_exp5 = st.columns([1, 1, 1])[1]
        with col_exp5:
            if st.button("📋 一键导出综合分析报告 (HTML)", key="report_btn"):
                with st.spinner("正在生成报告..."):
                    report_html = _generate_oneclick_report(
                        positions, summary, technical, selected_date, selected_benchmark
                    )
                if report_html:
                    st.success("报告已生成！")
                    st.download_button(
                        label="📥 下载综合报告",
                        data=report_html.encode("utf-8"),
                        file_name=f"投资组合分析报告_{selected_date}.html",
                        mime="text/html",
                        key="download_report",
                    )
                else:
                    st.error("报告生成失败，数据不足")

    # ========== Tab9: 自定义指标工作台 ==========



def _render_tab9_custom(tab9, positions):
    """Extracted from main() - tab9 renderer"""
    with tab9:
        st.caption("🔬 自定义技术指标组合回测，K线形态识别，量化验证交易策略")

        tab9_sub1, tab9_sub2 = st.tabs(["📊 指标回测", "🕯️ K线形态"])

        # ----- 指标回测子Tab -----
        with tab9_sub1:
            st.markdown(
                '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">指标信号回测<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">选择预置模板或自定义指标条件，对持仓ETF进行历史信号回测。</span></div>',
                unsafe_allow_html=True,
            )

            try:
                from src.analysis.indicator_backtest import (
                    INDICATOR_TEMPLATES,
                    backtest_technical_composite,
                )

                # 模板选择
                template_names = [t["name"] for t in INDICATOR_TEMPLATES]
                selected_tmpl = st.selectbox("选择指标模板", template_names, key="ind_tmpl_sel")
                tmpl = next(t for t in INDICATOR_TEMPLATES if t["name"] == selected_tmpl)
                st.caption(tmpl["description"])

                # 选择ETF
                if not positions.empty:
                    etf_options = {f"{row['name']}({row['code']})": str(row["code"]) for _, row in positions.iterrows()}
                    selected_etf = st.selectbox("选择ETF", list(etf_options.keys()), key="ind_etf_sel")
                    etf_code = etf_options[selected_etf]
                else:
                    st.info("暂无持仓数据")
                    etf_code = None

                # 回测参数
                col_bt1, col_bt2 = st.columns(2)
                with col_bt1:
                    hold_days = st.slider("持有天数", 1, 30, 5, key="ind_hold")
                with col_bt2:
                    lookback = st.selectbox("回溯天数", [90, 180, 250, 500], index=2, key="ind_lookback")

                if etf_code and st.button("🚀 开始回测", key="ind_run_bt", type="primary"):
                    with st.spinner("正在回测..."):
                        conn_bt = get_db_connection()
                        try:
                            result = backtest_technical_composite(conn_bt, etf_code, tmpl["formula"], lookback=lookback)
                        finally:
                            conn_bt.close()

                    if "error" in result:
                        st.warning(result["error"])
                    else:
                        # 结果展示
                        col_r1, col_r2, col_r3, col_r4 = st.columns(4)
                        with col_r1:
                            st.metric("总信号数", result["total_signals"])
                        with col_r2:
                            wr = result["win_rate"]
                            wr_color = "#22c55e" if wr >= 50 else "#ef4444"
                            st.metric("胜率", f"{wr}%", delta_color="normal" if wr >= 50 else "inverse")
                        with col_r3:
                            st.metric("平均收益", f"{result['avg_return_pct']:+.2f}%")
                        with col_r4:
                            pf = result["profit_factor"]
                            st.metric("盈亏比", f"{pf:.2f}" if pf != float("inf") else "∞")

                        st.metric(
                            "最大单次收益",
                            f"{result['max_return_pct']:+.2f}%",
                            delta=f"最大亏损: {result['max_loss_pct']:+.2f}%",
                        )

                        # 信号明细
                        details = result.get("signals_detail", [])
                        if details:
                            st.markdown("**最近信号记录**")
                            detail_df = pd.DataFrame(details)
                            detail_df["return_pct"] = detail_df["return_pct"].apply(
                                lambda x: f'<span style="color:{"#22c55e" if x>0 else "#ef4444"}">{x:+.2f}%</span>'
                            )
                            st.markdown(detail_df.to_html(index=False, escape=False), unsafe_allow_html=True)

                        # 收益分布图
                        if len(details) >= 3:
                            fig_bt = go.Figure(
                                go.Bar(
                                    x=[d["date"][-5:] for d in details],
                                    y=[d["return_pct"] for d in details],
                                    marker_color=["#22c55e" if d["return_pct"] > 0 else "#ef4444" for d in details],
                                    text=[f"{d['return_pct']:+.1f}%" for d in details],
                                    textposition="auto",
                                    textfont=dict(size=9, color="#c9d1d9"),
                                )
                            )
                            fig_bt.add_hline(y=0, line_dash="dash", line_color="#484f58")
                            fig_bt.update_layout(
                                xaxis=dict(
                                    title="信号日期", gridcolor="#21262d", tickfont=dict(size=9, color="#8b949e")
                                ),
                                yaxis=dict(title="收益%", gridcolor="#21262d", tickfont=dict(size=9, color="#8b949e")),
                                paper_bgcolor="#0d1117",
                                plot_bgcolor="#0d1117",
                                height=280,
                                margin=dict(l=40, r=20, t=10, b=30),
                                bargap=0.3,
                            )
                            st.plotly_chart(fig_bt, width="stretch")

            except Exception as e:
                st.info(f"指标回测模块暂不可用: {str(e)[:80]}")

        # ----- K线形态识别子Tab -----
        with tab9_sub2:
            st.markdown(
                '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">K线形态识别<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">自动识别常见K线反转和持续形态，辅助判断市场转折点。</span></div>',
                unsafe_allow_html=True,
            )

            try:
                from src.analysis.candle_patterns import (
                    PATTERN_NAME_MAP,
                    PATTERN_SIGNAL,
                    detect_candle_patterns,
                )

                if not positions.empty:
                    etf_opt2 = {f"{row['name']}({row['code']})": str(row["code"]) for _, row in positions.iterrows()}
                    sel_etf2 = st.selectbox("选择ETF", list(etf_opt2.keys()), key="candle_etf_sel")
                    candle_code = etf_opt2[sel_etf2]
                else:
                    st.info("暂无持仓数据")
                    candle_code = None

                if candle_code:
                    n_candle = st.slider("显示天数", 20, 120, 60, key="candle_days")

                    conn_ck = get_db_connection()
                    try:
                        snaps = pd.read_sql_query(
                            """
                            SELECT date, current_price AS close
                            FROM portfolio_snapshots
                            WHERE code = ?
                            ORDER BY date DESC
                            LIMIT ?
                        """,
                            conn_ck,
                            params=[candle_code, n_candle],
                        )
                    finally:
                        conn_ck.close()

                    if not snaps.empty:
                        snaps = snaps.sort_values("date").reset_index(drop=True)

                        # 基于收盘价合成 OHLC 数据
                        # open = 前一日close（首日 open=close）
                        snaps["open"] = snaps["close"].shift(1).fillna(snaps["close"])
                        # high = max(open, close) * (1 + 微小随机波动)
                        snaps["high"] = snaps[["open", "close"]].max(axis=1) * 1.003
                        # low = min(open, close) * (1 - 微小随机波动)
                        snaps["low"] = snaps[["open", "close"]].min(axis=1) * 0.997
                        # 将 high/low 限制在合理范围
                        snaps["high"] = snaps[["high", "close"]].max(axis=1)
                        snaps["low"] = snaps[["low", "close"]].min(axis=1)

                        ohlc = detect_candle_patterns(snaps)

                        # 筛选有形态的行
                        pattern_rows = ohlc[ohlc["pattern"] != ""]
                        if not pattern_rows.empty:
                            # 统计
                            pat_count = {}
                            for p in pattern_rows["pattern"]:
                                for name in p.split(","):
                                    cn = PATTERN_NAME_MAP.get(name.strip(), name)
                                    pat_count[cn] = pat_count.get(cn, 0) + 1

                            col_pc1, col_pc2 = st.columns([1, 2])
                            with col_pc1:
                                st.markdown("**形态统计**")
                                for pname, cnt in sorted(pat_count.items(), key=lambda x: -x[1]):
                                    sig = PATTERN_SIGNAL.get(pname, "neutral")
                                    icon = "🟢" if sig == "bullish" else ("🔴" if sig == "bearish" else "⚪")
                                    st.markdown(f"{icon} {pname}: **{cnt}** 次")

                            with col_pc2:
                                # K线图
                                fig_k = go.Figure(
                                    data=[
                                        go.Candlestick(
                                            x=ohlc["date"],
                                            open=ohlc["open"],
                                            high=ohlc["high"],
                                            low=ohlc["low"],
                                            close=ohlc["close"],
                                            increasing_line_color="#22c55e",
                                            decreasing_line_color="#ef4444",
                                        )
                                    ]
                                )
                                # 标记形态位置
                                for _, row in pattern_rows.iterrows():
                                    sig = "bullish"
                                    for p in row["pattern"].split(","):
                                        ps = PATTERN_SIGNAL.get(p.strip(), "neutral")
                                        if ps == "bearish":
                                            sig = "bearish"
                                    color = "#22c55e" if sig == "bullish" else "#ef4444"
                                    name_str = PATTERN_NAME_MAP.get(row["pattern"].split(",")[0].strip(), "")
                                    fig_k.add_annotation(
                                        x=row["date"],
                                        y=row["high"] * 1.005,
                                        text=f"▼ {name_str}" if sig == "bearish" else f"▲ {name_str}",
                                        showarrow=False,
                                        font=dict(size=9, color=color),
                                    )

                                fig_k.update_layout(
                                    xaxis=dict(gridcolor="#21262d", tickfont=dict(size=9, color="#8b949e")),
                                    yaxis=dict(gridcolor="#21262d", tickfont=dict(size=9, color="#8b949e")),
                                    paper_bgcolor="#0d1117",
                                    plot_bgcolor="#0d1117",
                                    height=max(350, n_candle * 4),
                                    margin=dict(l=40, r=20, t=10, b=30),
                                    xaxis_rangeslider_visible=False,
                                )
                                st.plotly_chart(fig_k, width="stretch")
                        else:
                            st.info(f"近 {n_candle} 日未检测到经典K线形态")
            except Exception as e:
                st.info(f"K线形态识别暂不可用: {str(e)[:80]}")

    # ========== Tab10: 资金动向 ==========



def _render_tab10_fund_flow(tab10, positions, summary):
    """Extracted from main() - tab10 renderer"""
    with tab10:
        st.caption("💰 行业/ETF资金流向分析，追踪主力资金动态，辅助判断市场热点切换")

        tab10_sub1, tab10_sub2, tab10_sub3 = st.tabs(["📊 行业资金流", "📈 ETF资金流", "💰 主力资金"])

        # ----- 行业资金流 -----
        with tab10_sub1:
            st.markdown(
                '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">行业资金流向<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">各行业板块主力资金净流入/流出排名与趋势。</span></div>',
                unsafe_allow_html=True,
            )

            try:
                conn_ff = get_db_connection()
                try:
                    sector_df = pd.read_sql_query(
                        """
                        SELECT date, name, code, net_inflow
                        FROM fund_flows
                        WHERE category = 'sector'
                        ORDER BY date DESC, net_inflow DESC
                    """,
                        conn_ff,
                    )
                finally:
                    conn_ff.close()

                if not sector_df.empty:
                    # 去除重复行业（同板块不同层级代码数据相同，只保留Ⅱ）
                    sector_df = sector_df[
                        ~sector_df["code"].isin(
                            {"BK1366", "BK1471"}  # 证券Ⅲ→保留Ⅱ(BK0473), 工程咨询服务Ⅲ→保留Ⅱ(BK0726)
                        )
                    ].copy()

                    # 最新日期的行业排名
                    latest_date = sector_df["date"].iloc[0]
                    latest = sector_df[sector_df["date"] == latest_date].head(20)

                    if not latest.empty:
                        fig_sf = go.Figure(
                            go.Bar(
                                orientation="h",
                                y=latest["name"],
                                x=latest["net_inflow"] / 1e8,
                                marker_color=["#22c55e" if v > 0 else "#ef4444" for v in latest["net_inflow"] / 1e8],
                                text=[f"{v/1e8:.1f}亿" for v in latest["net_inflow"]],
                                textposition="auto",
                                textfont=dict(size=9, color="#c9d1d9"),
                            )
                        )
                        fig_sf.update_layout(
                            title=f"<span style='font-size:12px;color:#8b949e'>{latest_date} 行业资金净流入TOP20</span>",
                            xaxis=dict(
                                title="净流入(亿元)", gridcolor="#21262d", tickfont=dict(size=9, color="#8b949e")
                            ),
                            yaxis=dict(title="", tickfont=dict(size=10, color="#c9d1d9")),
                            paper_bgcolor="#0d1117",
                            plot_bgcolor="#0d1117",
                            height=max(400, 22 * len(latest)),
                            margin=dict(l=80, r=30, t=35, b=30),
                            bargap=0.2,
                        )
                        st.plotly_chart(fig_sf, width="stretch")
                        # TOP10行业资金净流入时间趋势
                        if sector_df["date"].nunique() >= 3:
                            # 只选至少有30天数据的行业，避免单日行业被选中导致趋势线无意义
                            days_per_name = sector_df.groupby("name")["date"].nunique()
                            qualified = days_per_name[days_per_name >= 10].index
                            if len(qualified) > 0:
                                trend_df = sector_df[sector_df["name"].isin(qualified)].copy()
                                # 按最近30天累计净流入排序选TOP10
                                recent_cutoff = sorted(trend_df["date"].unique())[
                                    -min(30, trend_df["date"].nunique()) :
                                ]
                                recent_sum = (
                                    trend_df[trend_df["date"].isin(recent_cutoff)].groupby("name")["net_inflow"].sum()
                                )
                                top10_names = recent_sum.nlargest(10).index.tolist()
                                trend_df = trend_df[trend_df["name"].isin(top10_names)].copy()
                                trend_df["net_inflow_yi"] = trend_df["net_inflow"] / 1e8

                                fig_trend = go.Figure()
                                for name in top10_names:
                                    sub = trend_df[trend_df["name"] == name].sort_values("date")
                                    fig_trend.add_trace(
                                        go.Scatter(
                                            x=sub["date"],
                                            y=sub["net_inflow_yi"],
                                            name=name,
                                            mode="lines",
                                            line=dict(width=1.5),
                                        )
                                    )
                                fig_trend.add_hline(y=0, line_dash="dash", line_color="#484f58")
                                fig_trend.update_layout(
                                    title="<span style='font-size:12px;color:#8b949e'>TOP10行业资金净流入趋势(亿元)</span>",
                                    yaxis=dict(
                                        title="净流入(亿元)",
                                        gridcolor="#21262d",
                                        tickfont=dict(size=9, color="#8b949e"),
                                    ),
                                    xaxis=dict(gridcolor="#21262d", tickfont=dict(size=9, color="#8b949e")),
                                    paper_bgcolor="#0d1117",
                                    plot_bgcolor="#0d1117",
                                    height=400,
                                    margin=dict(l=50, r=30, t=50, b=30),
                                    legend=dict(
                                        orientation="h",
                                        yanchor="bottom",
                                        y=1.02,
                                        font=dict(size=9, color="#8b949e"),
                                        groupclick="toggleitem",
                                    ),
                                )
                                st.plotly_chart(fig_trend, width="stretch")
                    # 多日趋势热力图
                    if sector_df["date"].nunique() >= 3:
                        # 取最近30个交易日
                        recent_dates = sorted(sector_df["date"].unique(), reverse=True)[:30]
                        heat_df = sector_df[sector_df["date"].isin(recent_dates)].copy()

                        pivot = heat_df.pivot_table(index="name", columns="date", values="net_inflow", aggfunc="sum")
                        # 过滤数据稀疏行业: 至少覆盖一半日期，避免NaN过多导致热力图失真
                        min_coverage = max(5, len(pivot.columns) // 2)
                        valid_mask = pivot.notna().sum(axis=1) >= min_coverage
                        pivot = pivot.loc[valid_mask]
                        if not pivot.empty:
                            # 按最近5日日均净流入排序，正负各半选取更有对比度
                            daily_avg = pivot.apply(lambda row: row.tail(5).mean(), axis=1).sort_values(ascending=False)
                            top_pos = daily_avg.nlargest(8).index.tolist()
                            top_neg = daily_avg.nsmallest(7).index.tolist()
                            top_names = [n for n in top_pos + top_neg if n in pivot.index]
                            pivot = pivot.loc[top_names]

                            pivot_yi = pivot / 1e8  # 转亿元，保留NaN

                            fig_heat = go.Figure(
                                go.Heatmap(
                                    z=pivot_yi.values,
                                    x=[str(d)[-5:] for d in pivot_yi.columns],
                                    y=pivot_yi.index,
                                    colorscale=[[0, "#ef4444"], [0.5, "#0d1117"], [1, "#22c55e"]],
                                    zmid=0,
                                    text=[[f"{v:.1f}" if pd.notna(v) else "" for v in row] for row in pivot_yi.values],
                                    texttemplate="%{text}",
                                    textfont=dict(size=8),
                                    hovertemplate="%{y}: %{x}<br>净流入: %{z:.1f}亿<extra></extra>",
                                )
                            )
                            fig_heat.update_layout(
                                title="<span style='font-size:12px;color:#8b949e'>近30日行业资金流热力图(亿元)</span>",
                                paper_bgcolor="#0d1117",
                                plot_bgcolor="#0d1117",
                                height=max(350, 30 * len(pivot_yi)),
                                margin=dict(l=100, r=20, t=35, b=30),
                                xaxis=dict(side="bottom", tickangle=45),
                                yaxis=dict(tickfont=dict(size=10)),
                            )
                            st.plotly_chart(fig_heat, width="stretch")
                else:
                    st.info("暂无行业资金流数据，请先运行数据采集任务")
                    if st.button("采集行业资金流", key="fetch_sector_flow"):
                        with st.spinner("正在采集..."):
                            try:
                                from src.data_sources.fund_flow import (
                                    fetch_sector_fund_flow,
                                    save_fund_flows,
                                )

                                conn_f = get_db_connection()
                                try:
                                    sdf = fetch_sector_fund_flow()
                                    if not sdf.empty:
                                        cnt = save_fund_flows(conn_f, sdf)
                                        st.success(f"采集成功，写入 {cnt} 条记录")
                                    else:
                                        st.warning("采集返回空数据")
                                finally:
                                    conn_f.close()
                            except Exception as e:
                                st.error(f"采集失败: {str(e)[:100]}")
                            st.rerun()

            except Exception as e:
                st.info(f"行业资金流模块暂不可用: {str(e)[:80]}")

        # ----- ETF资金流 -----
        with tab10_sub2:
            st.markdown(
                '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">ETF资金流向<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">持仓ETF的主力资金流入流出趋势。</span></div>',
                unsafe_allow_html=True,
            )

            try:
                conn_ef = get_db_connection()
                try:
                    etf_flow = pd.read_sql_query(
                        """
                        SELECT f.date, f.code, f.name, f.net_inflow,
                               ps.current_price AS close
                        FROM fund_flows f
                        LEFT JOIN portfolio_snapshots ps
                            ON f.code = ps.code AND f.date = ps.date
                        WHERE f.category = 'etf'
                        ORDER BY f.date DESC, f.code
                    """,
                        conn_ef,
                    )
                finally:
                    conn_ef.close()

                if not etf_flow.empty:
                    etf_flow = etf_flow.sort_values("date").reset_index(drop=True)
                    etf_list = etf_flow["code"].unique()

                    selected_etf_flow = st.selectbox(
                        "选择ETF",
                        etf_list,
                        format_func=lambda x: etf_flow[etf_flow["code"] == x]["name"].iloc[0],
                        key="etf_flow_sel",
                    )
                    etf_single = etf_flow[etf_flow["code"] == selected_etf_flow]

                    if not etf_single.empty and "close" in etf_single.columns:
                        col_p1, col_p2 = st.columns([3, 1])
                        with col_p1:
                            fig_ef = go.Figure()
                            fig_ef.add_trace(
                                go.Bar(
                                    x=etf_single["date"],
                                    y=etf_single["net_inflow"] / 1e8,
                                    name="主力净流入",
                                    marker_color=[
                                        "#22c55e" if v > 0 else "#ef4444" for v in etf_single["net_inflow"] / 1e8
                                    ],
                                    yaxis="y",
                                )
                            )
                            fig_ef.add_trace(
                                go.Scatter(
                                    x=etf_single["date"],
                                    y=etf_single["close"],
                                    name="收盘价",
                                    mode="lines",
                                    line=dict(color="#58a6ff", width=1.5),
                                    yaxis="y2",
                                )
                            )
                            fig_ef.update_layout(
                                yaxis=dict(
                                    title="净流入(亿元)", gridcolor="#21262d", tickfont=dict(size=9, color="#8b949e")
                                ),
                                yaxis2=dict(
                                    title="收盘价",
                                    overlaying="y",
                                    side="right",
                                    gridcolor="#21262d",
                                    tickfont=dict(size=9, color="#58a6ff"),
                                ),
                                xaxis=dict(gridcolor="#21262d", tickfont=dict(size=9, color="#8b949e")),
                                paper_bgcolor="#0d1117",
                                plot_bgcolor="#0d1117",
                                height=350,
                                margin=dict(l=50, r=50, t=10, b=30),
                                legend=dict(
                                    orientation="h", yanchor="bottom", y=1.02, font=dict(size=10, color="#8b949e")
                                ),
                                showlegend=True,
                            )
                            st.plotly_chart(fig_ef, width="stretch")
                        with col_p2:
                            total_net = etf_single["net_inflow"].sum()
                            st.metric(
                                "累计净流入",
                                f"{total_net/1e8:.1f}亿" if abs(total_net) > 1e8 else f"{total_net/1e4:.0f}万",
                            )
                            flow_up = len(etf_single[etf_single["net_inflow"] > 0])
                            st.metric(
                                "流入天数",
                                f"{flow_up} / {len(etf_single)}",
                                delta=f"{flow_up/len(etf_single)*100:.0f}%",
                            )
                else:
                    st.info("暂无ETF资金流数据")
                    if not positions.empty:
                        if st.button("采集持仓ETF资金流", key="fetch_etf_flow"):
                            with st.spinner("正在采集..."):
                                try:
                                    from src.data_sources.fund_flow import (
                                        check_push2his_available,
                                        fetch_etf_fund_flow,
                                        fetch_etf_fund_flow_batch,
                                        save_fund_flows,
                                    )

                                    conn_f2 = get_db_connection()
                                    try:
                                        if check_push2his_available():
                                            for _, pos in positions.head(5).iterrows():
                                                code = str(pos["code"])
                                                name = pos["name"]
                                                st.caption(f"正在采集 {name}...")
                                                edf = fetch_etf_fund_flow(code, name)
                                                if not edf.empty:
                                                    save_fund_flows(conn_f2, edf)
                                            st.success("采集完成(push2his逐只)")
                                        else:
                                            etf_codes = positions["code"].head(5).astype(str).tolist()
                                            batch_df = fetch_etf_fund_flow_batch(etf_codes)
                                            if not batch_df.empty:
                                                save_fund_flows(conn_f2, batch_df)
                                                st.success(f"采集完成(批量, {len(batch_df)}只)")
                                            else:
                                                st.warning("批量采集无数据返回")
                                    finally:
                                        conn_f2.close()
                                except Exception as e:
                                    st.error(f"采集失败: {str(e)[:100]}")
                                st.rerun()

            except Exception as e:
                st.info(f"ETF资金流模块暂不可用: {str(e)[:80]}")

        # ----- 主力资金净流入（替代已停更的北向资金） -----
        with tab10_sub3:
            st.markdown(
                '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">主力资金<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">A股主力资金净流入趋势（主力=超大单+大单），数据自2025-11-07起，替代已停更的北向资金。</span></div>',
                unsafe_allow_html=True,
            )

            try:
                conn_nf = get_db_connection()
                try:
                    mf_df = pd.read_sql_query(
                        """
                        SELECT date, net_inflow, super_large_inflow, large_inflow,
                               medium_inflow, small_inflow, net_inflow_pct
                        FROM fund_flows
                        WHERE category = 'main_fund'
                        ORDER BY date
                    """,
                        conn_nf,
                    )
                finally:
                    conn_nf.close()

                if not mf_df.empty:
                    col_n1, col_n2, col_n3 = st.columns(3)
                    latest_mf = mf_df.iloc[-1]
                    with col_n1:
                        val = latest_mf["net_inflow"] / 1e8
                        st.metric(
                            "最新主力净流入", f"{val:.1f}亿", delta=f"{val/1e4:.2f}万亿" if abs(val) > 10000 else None
                        )
                    with col_n2:
                        val5 = mf_df.tail(5)["net_inflow"].sum() / 1e8
                        st.metric("近5日累计", f"{val5:.1f}亿")
                    with col_n3:
                        val20 = mf_df.tail(20)["net_inflow"].sum() / 1e8
                        st.metric("近20日累计", f"{val20:.1f}亿")

                    fig_mf = go.Figure()
                    fig_mf.add_trace(
                        go.Bar(
                            x=mf_df["date"],
                            y=mf_df["net_inflow"] / 1e8,
                            name="主力净流入(亿)",
                            marker_color=["#22c55e" if v > 0 else "#ef4444" for v in mf_df["net_inflow"] / 1e8],
                        )
                    )
                    fig_mf.add_trace(
                        go.Scatter(
                            x=mf_df["date"],
                            y=(mf_df["net_inflow"] / 1e8).cumsum(),
                            name="累计净流入(亿)",
                            mode="lines",
                            line=dict(color="#f59e0b", width=2),
                            yaxis="y2",
                        )
                    )
                    fig_mf.add_hline(y=0, line_dash="dash", line_color="#484f58")
                    fig_mf.update_layout(
                        yaxis=dict(title="日净流入(亿)", gridcolor="#21262d", tickfont=dict(size=9, color="#8b949e")),
                        yaxis2=dict(
                            title="累计(亿)",
                            overlaying="y",
                            side="right",
                            gridcolor="#21262d",
                            tickfont=dict(size=9, color="#f59e0b"),
                        ),
                        xaxis=dict(gridcolor="#21262d", tickfont=dict(size=9, color="#8b949e")),
                        paper_bgcolor="#0d1117",
                        plot_bgcolor="#0d1117",
                        height=350,
                        margin=dict(l=50, r=50, t=10, b=30),
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, font=dict(size=10, color="#8b949e")),
                    )
                    st.plotly_chart(fig_mf, width="stretch")

                    # ----- 持仓ETF合计主力资金净流入 -----
                    st.markdown(
                        '<div class="tip-title" style="font-size:14px;border-bottom:none;padding:3px 0;">持仓ETF合计资金流</div>',
                        unsafe_allow_html=True,
                    )

                    try:
                        conn_ef2 = get_db_connection()
                        try:
                            etf_total = pd.read_sql_query(
                                """
                                SELECT f.date, SUM(f.net_inflow) as total_net_inflow,
                                       COUNT(DISTINCT f.code) as etf_count
                                FROM fund_flows f
                                WHERE f.category = 'etf'
                                  AND f.date >= date('now', '-90 days')
                                GROUP BY f.date
                                ORDER BY f.date
                            """,
                                conn_ef2,
                            )
                        finally:
                            conn_ef2.close()

                        if not etf_total.empty:
                            col_e1, col_e2, col_e3 = st.columns(3)
                            latest_et = etf_total.iloc[-1]
                            with col_e1:
                                st.metric("最新ETF合计净流入", f"{latest_et['total_net_inflow']/1e8:.1f}亿")
                            with col_e2:
                                st.metric("覆盖ETF数", f"{latest_et['etf_count']}只")
                            with col_e3:
                                ev5 = etf_total.tail(5)["total_net_inflow"].sum() / 1e8
                                st.metric("近5日ETF累计", f"{ev5:.1f}亿")

                            fig_etf_total = go.Figure()
                            fig_etf_total.add_trace(
                                go.Bar(
                                    x=etf_total["date"],
                                    y=etf_total["total_net_inflow"] / 1e8,
                                    name="ETF合计净流入(亿)",
                                    marker_color=[
                                        "#22c55e" if v > 0 else "#ef4444" for v in etf_total["total_net_inflow"] / 1e8
                                    ],
                                )
                            )
                            fig_etf_total.add_trace(
                                go.Scatter(
                                    x=etf_total["date"],
                                    y=(etf_total["total_net_inflow"] / 1e8).cumsum(),
                                    name="累计净流入(亿)",
                                    mode="lines",
                                    line=dict(color="#58a6ff", width=2),
                                    yaxis="y2",
                                )
                            )
                            fig_etf_total.add_hline(y=0, line_dash="dash", line_color="#484f58")
                            fig_etf_total.update_layout(
                                yaxis=dict(
                                    title="日净流入(亿)", gridcolor="#21262d", tickfont=dict(size=9, color="#8b949e")
                                ),
                                yaxis2=dict(
                                    title="累计(亿)",
                                    overlaying="y",
                                    side="right",
                                    gridcolor="#21262d",
                                    tickfont=dict(size=9, color="#58a6ff"),
                                ),
                                xaxis=dict(gridcolor="#21262d", tickfont=dict(size=9, color="#8b949e")),
                                paper_bgcolor="#0d1117",
                                plot_bgcolor="#0d1117",
                                height=350,
                                margin=dict(l=50, r=50, t=10, b=30),
                                legend=dict(
                                    orientation="h", yanchor="bottom", y=1.02, font=dict(size=10, color="#8b949e")
                                ),
                            )
                            st.plotly_chart(fig_etf_total, width="stretch")
                        else:
                            st.info("暂无ETF资金流数据")
                    except Exception as e2:
                        st.caption(f"ETF合计资金流: {str(e2)[:60]}")
                else:
                    st.info("暂无主力资金数据")
                    if st.button("采集主力资金数据", key="fetch_main_fund"):
                        with st.spinner("正在采集..."):
                            try:
                                from src.data_sources.fund_flow import (
                                    fetch_main_fund_flow,
                                    save_fund_flows,
                                )

                                conn_f3 = get_db_connection()
                                try:
                                    mdf = fetch_main_fund_flow(days=120)
                                    if not mdf.empty:
                                        cnt = save_fund_flows(conn_f3, mdf)
                                        st.success(f"采集成功，写入 {cnt} 条记录")
                                finally:
                                    conn_f3.close()
                            except Exception as e:
                                st.error(f"采集失败: {str(e)[:100]}")
                            st.rerun()

            except Exception as e:
                st.info(f"主力资金模块暂不可用: {str(e)[:80]}")

        # ========== Tab11: 黄金市场分析 ==========


def main():
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

    # 标题
    st.markdown('<div class="main-header">📊 投资组合跟踪分析系统</div>', unsafe_allow_html=True)

    # 获取数据
    available_dates = get_available_dates()
    if not available_dates:
        st.warning("暂无数据，请先运行 run_analysis.py")
        return

    # 侧边栏
    with st.sidebar:
        st.markdown("### 🔧 控制面板")

        selected_date = st.selectbox(
            "选择日期",
            available_dates,
            index=0,
            format_func=lambda x: f"{x} {'(最新)' if x == available_dates[0] else ''}",
        )

        # 快捷预设
        preset = st.radio(
            "时间范围", ["3个月", "6个月", "1年", "2年", "5年", "全部", "自定义"], horizontal=True, index=2
        )
        preset_days = {"3个月": 60, "6个月": 120, "1年": 250, "2年": 500, "5年": 1250, "全部": 4000}
        if preset == "自定义":
            show_days = st.slider("自定义天数", min_value=10, max_value=4000, value=250, step=10)
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

    # ========== 图表行1: 净值曲线 + 收益分布 ==========
    tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9, tab10, tab11 = st.tabs(
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
        ]
    )

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

    _render_tab1_body(tab1, positions, summary, selected_date, show_days, selected_benchmark, rolling_data, effective_max_dd)

    _render_tab2_position(tab2, positions, summary, selected_date)

    _render_tab3_risk(tab3, positions, summary, technical, selected_date, ext_risk)

    _render_tab4_calendar(tab4, positions, summary)

    _render_tab6_technical(tab6, technical)

    _render_tab7_news(tab7, positions, summary, technical)

    _render_tab8_advice(tab8, positions, summary, technical)

    _render_tab5_advanced(tab5, positions, summary, technical)

    _render_tab9_custom(tab9, positions)

    _render_tab10_fund_flow(tab10, positions, summary)

if __name__ == "__main__":
    main()
