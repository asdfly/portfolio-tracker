#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Portfolio Tracker 主入口 — 精简版（从 2521 行拆分）

原 dashboard.py 拆分后保留：主流程编排、概览渲染、Tab 注册、
格式化工具、综合评分、导出功能、ETF 详情面板。

拆分出的模块：
- data_loader.py — 数据加载与计算引擎
- sidebar.py — 侧边栏 UI 与自定义样式
"""

from components.ui import render_chart, render_empty_state
import io
import base64
from datetime import datetime

import numpy as np
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import calendar
import sqlite3

from PIL import Image

from config.settings import CACHE_TTL, DATABASE_PATH, DOWNSAMPLE_MAX_POINTS, ETF_CATEGORIES, INDEX_CODES, SECTOR_COLORS
from src.utils.database import get_db_connection
from data_loader import (
    _ensure_indexes, get_db_connection,
    load_positions, load_summary, load_index_quotes, load_technical,
    load_alerts, load_execution_logs, get_available_dates, load_calendar_data,
    compute_extended_risk_metrics, compute_monthly_returns, compute_rolling_metrics,
    load_correlation_matrix, load_etf_detail, load_etf_price_history,
    load_benchmark_comparison, load_sector_weights, run_monte_carlo,
    compute_return_attribution, compute_rebalance_suggestion,
    _cleanse_daily_returns, _load_latest_news, _load_tech_signals,
)
from sidebar import _inject_custom_css, _render_sidebar

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


def downsample(df, date_col="date", max_points=DOWNSAMPLE_MAX_POINTS):
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


@st.cache_data(ttl=CACHE_TTL['short'], show_spinner=False)


@st.cache_data(ttl=CACHE_TTL['short'], show_spinner=False)


@st.cache_data(ttl=CACHE_TTL['short'], show_spinner=False)


@st.cache_data(ttl=CACHE_TTL['short'], show_spinner=False)


@st.cache_data(ttl=CACHE_TTL['short'], show_spinner=False)


@st.cache_data(ttl=CACHE_TTL['short'], show_spinner=False)


@st.cache_data(ttl=CACHE_TTL['medium'], show_spinner=False)


@st.cache_data(ttl=CACHE_TTL['medium'], show_spinner=False)


@st.cache_data(ttl=CACHE_TTL['medium'], show_spinner=False)




@st.cache_data(ttl=CACHE_TTL['medium'], show_spinner=False)


@st.cache_data(ttl=CACHE_TTL['medium'], show_spinner=False)


# ==================== P1: 持仓相关性矩阵 ====================
@st.cache_data(ttl=CACHE_TTL['medium'], show_spinner=False)


# ==================== P1: 单只ETF详情数据 ====================
@st.cache_data(ttl=CACHE_TTL['short'], show_spinner=False)


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
            if len(df) > DOWNSAMPLE_MAX_POINTS:
                step = max(1, len(df) // DOWNSAMPLE_MAX_POINTS)
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
            render_chart(fig)

        with col_tech:
            st.markdown(
                '<div class="tip-title" style="font-size:14px;border-bottom:none;padding:5px 0;">技术指标<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">展示该ETF的RSI、MACD、KDJ、布林带等常用技术指标信号。</span></div>',
                unsafe_allow_html=True,
            )
            if not detail_df.empty:
                latest = detail_df.iloc[-1]

                trend_map = {
                    "强势上涨": ("看多", "#22c55e"),
                    "温和上涨": ("偏多", "#22c55e"),
                    "震荡整理": ("中性", "#f59e0b"),
                    "震荡中性": ("中性", "#f59e0b"),
                    "下跌": ("看空", "#ef4444"),
                    "强势下跌": ("看空", "#ef4444"),
                    None: ("--", "#888"),
                }
                _trend = str(latest.get("trend", ""))
                if "上涨" in _trend:
                    trend_label, trend_color = ("看多", "#22c55e")
                elif "下跌" in _trend:
                    trend_label, trend_color = ("看空", "#ef4444")
                else:
                    trend_label, trend_color = trend_map.get(_trend if _trend else None, ("--", "#888"))

                # 技术指标卡片
                indicators = [
                    ("趋势", trend_label, trend_color),
                    (
                        "RSI",
                        f"{latest.get('rsi_value', '--'):.1f}" if pd.notna(latest.get("rsi_value")) else "--",
                        (
                            "#22c55e"
                            if latest.get("rsi_status") in ("超卖", "偏低")
                            else "#ef4444" if latest.get("rsi_status") in ("超买", "偏高") else "#c9d1d9"
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
                render_chart(fig_hist)


@st.cache_data(ttl=CACHE_TTL['short'], show_spinner=False)


# ==================== P1: 多基准指数对比数据 ====================
@st.cache_data(ttl=CACHE_TTL['medium'], show_spinner=False)


# ==================== 样式工具 ====================


@st.cache_data(ttl=CACHE_TTL['short'], show_spinner=False)


@st.cache_data(ttl=CACHE_TTL['medium'], show_spinner=False)


@st.cache_data(ttl=CACHE_TTL['medium'], show_spinner=False)


@st.cache_data(ttl=CACHE_TTL['medium'], show_spinner=False)


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
    # 收益评分 (30分) — 使用 corrected daily_return 累积净值法，避免 total_value 跳变影响
    port_daily = (summary["daily_return"] / 100).dropna() if "daily_return" in summary.columns else summary["total_value"].pct_change().dropna()
    total_ret = ((1 + port_daily).prod() - 1) if len(port_daily) > 0 else 0
    ann_ret = port_daily.mean() * 252 * 100 if len(port_daily) > 0 else 0
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

    # 使用 portfolio_summary 预存的 corrected daily_return（已校正持仓变化），避免 total_value 跳变影响
    port_daily = (summary["daily_return"] / 100).dropna() if "daily_return" in summary.columns else summary["total_value"].pct_change().dropna()
    ann_ret = port_daily.mean() * 252 * 100 if len(port_daily) > 0 else 0
    ann_vol = port_daily.std() * math.sqrt(252) * 100 if len(port_daily) > 1 else 0
    sharpe = (port_daily.mean() / port_daily.std() * math.sqrt(252)) if port_daily.std() > 0 else 0
    # 使用预存的 max_drawdown（基于 corrected daily_return 累积序列）
    max_dd = summary["max_drawdown"].min() if "max_drawdown" in summary.columns else ((summary["total_value"] - summary["total_value"].cummax()) / summary["total_value"].cummax() * 100).min()

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


@st.cache_data(ttl=CACHE_TTL['medium'], show_spinner=False)


@st.cache_data(ttl=CACHE_TTL['short'], show_spinner=False)



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
        sharpe_color = "#22c55e" if (sharpe and sharpe > 0.5) else "#f59e0b" if sharpe else "#888"
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

        # 实际交易胜率（从 trade_records）
        trade_wr_text = ""
        try:
            trade_stats = pd.read_sql_query(
                "SELECT COUNT(*) as total_fee_trades, "
                "SUM(CASE WHEN action='证券买入' THEN 1 ELSE 0 END) as buy_n, "
                "SUM(CASE WHEN action='证券卖出' THEN 1 ELSE 0 END) as sell_n, "
                "SUM(commission + stamp_tax) as total_fee "
                "FROM trade_records WHERE action IN ('证券买入','证券卖出')",
                get_db_connection())
            if not trade_stats.empty and trade_stats.iloc[0]['total_fee_trades'] > 0:
                bn = int(trade_stats.iloc[0]['buy_n'])
                sn = int(trade_stats.iloc[0]['sell_n'])
                tf = trade_stats.iloc[0]['total_fee']
                trade_wr_text = (
                    f'<span style="color:#8b949e;">实际交易: <b>{bn}买/{sn}卖</b> '
                    f'<span style="color:#484f58;font-size:11px;">费用 ¥{tf:,.0f}</span></span>'
                )
        except Exception:
            pass

        trade_span = f" {trade_wr_text}" if trade_wr_text else ""

        st.markdown(
            f'<div style="display:flex;gap:20px;flex-wrap:wrap;padding:8px 4px;margin-bottom:4px;font-size:13px;">'
            f'<span style="color:#8b949e;">胜率: <b style="color:{wr_color};">{wr:.1f}%</b> <span style="color:#484f58;font-size:11px;">({pc}盈/{lc}亏)</span></span>'
            f'<span style="color:#8b949e;">最大持仓: <b style="color:{wt_color};">{max_pos["name"]}</b> <span style="color:#484f58;font-size:11px;">{max_wt:.1f}%</span></span>'
            f'<span style="color:#8b949e;">技术信号: <b style="color:{sig_color};">{buy_sig}多 / {sell_sig}空</b></span>'
            f"{trade_span}"
            f"</div>"
            f'<div style="padding:2px 4px 8px;">{sector_tags}</div>',
            unsafe_allow_html=True,
        )



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
    st.session_state["selected_date"] = selected_date
    st.session_state["show_days"] = show_days
    st.session_state["selected_benchmark"] = selected_benchmark
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

    if render_empty_state(positions, f"{selected_date} 无持仓数据", warn=True): return

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
    # Tab 注册表: (标签名, 模块路径, 渲染函数名)
    # 新增 Tab 只需在此列表末尾追加一项即可
    TAB_REGISTRY = [
        ("📈 净值走势",   "tabs.tab1_net_value",        "render_tab1"),
        ("📊 持仓分布",   "tabs.tab2_position",         "render_tab2"),
        ("⚠️ 风险分析",   "tabs.tab3_risk",             "render_tab3"),
        ("📅 收益日历",   "tabs.tab4_calendar",         "render_tab4"),
        ("💠 高级分析",   "tabs.tab5_advanced",         "render_tab5"),
        ("📡 技术信号",   "tabs.tab6_technical",        "render_tab6"),
        ("📰 资讯与评估", "tabs.tab7_news",             "render_tab7"),
        ("💡 操作建议",   "tabs.tab8_advice",           "render_tab8"),
        ("🔬 自定义指标", "tabs.tab9_custom",           "render_tab9"),
        ("💰 资金动向",   "tabs.tab10_fund_flow",       "render_tab10"),
        ("🥇 黄金市场",   "tabs.tab11_gold",            "render_tab11"),
        ("🌐 宏观市场",   "tabs.tab12_macro",           "render_tab12"),
        ("📊 数据质量",   "tabs.tab13_data_quality",     "render_tab13"),
        ("📋 市场事件",   "tabs.tab14_market_events",   "render_tab14"),
        ("🔁 交易复盘",   "tabs.tab15_trade_review",   "render_tab15"),
    ]
    tab_objects = st.tabs([label for label, _, _ in TAB_REGISTRY])

    _render_quick_stats(positions, profit_count, loss_count, technical)
    for tab_obj, (_, module_path, func_name) in zip(tab_objects, TAB_REGISTRY):
        mod = __import__(module_path, fromlist=[func_name])
        with tab_obj:
            getattr(mod, func_name)()

if __name__ == "__main__":
    main()
