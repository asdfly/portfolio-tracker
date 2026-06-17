"""
Tab1: 净值走势
"""

from components.ui import render_chart, render_empty_state
import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
from src.utils.chart_utils import downsample, _add_min_max_annotations, _fmt, _fmt_cell
from config.settings import BENCHMARK_NAME_TO_CODE, CHART_DAYS, DOWNSAMPLE_MAX_POINTS, INDEX_CODES
from src.utils.database import get_db_connection
import sqlite3
from data_loader import load_positions, load_summary, load_benchmark_comparison, compute_rolling_metrics





def _resolve_benchmark_code(name_or_code):
    """将基准名称或代码统一转为代码格式"""
    if name_or_code in INDEX_CODES:
        return name_or_code  # 已经是code格式如 "sh000300"
    return BENCHMARK_NAME_TO_CODE.get(name_or_code, name_or_code)
def load_benchmark_comparison(code, days=CHART_DAYS["default"], end_date=None):
    """加载指定基准指数行情，用于净值曲线对比（委托到 data_loader）"""
    import data_loader as _dl
    return _dl.load_benchmark_comparison(code, days=days, end_date=end_date)


def compute_rolling_metrics(window=60, end_date=None):
    """计算滚动夏普比率和滚动波动率（委托到 data_loader）"""
    import data_loader as _dl
    return _dl.compute_rolling_metrics(window=window, end_date=end_date)


def _render_basic_metrics(positions, summary, index_quotes, selected_date, selected_benchmark, technical, volatility, max_dd, sharpe, cal_data, tech_signals, show_days):
    col_left, col_right = st.columns([2, 1])

    with col_left:
        st.markdown(
            '<div class="tip-title" style="">组合净值走势<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">以组合总市值为基准归一化到100，展示组合净值随时间的变化趋势，同时叠加基准指数走势进行对比。</span></div>',
            unsafe_allow_html=True,
        )
        if not summary.empty and len(summary) > 1:
            # 计算累计净值（基准100）
            # 过滤到 show_days 范围，确保与基准指数对齐
            plot_end = selected_date if selected_date else summary["date"].iloc[-1]
            plot_start_idx = max(0, len(summary) - show_days - 30)  # 多取30天确保日期重叠
            summary_plot = summary.iloc[plot_start_idx:].copy()
            # 使用 corrected daily_return 累积净值法（基准100），避免 total_value 跳变导致净值突增
            if "daily_return" in summary_plot.columns:
                nav_returns = (summary_plot["daily_return"] / 100).fillna(0)
                summary_plot["nav"] = (1 + nav_returns).cumprod() * 100
            else:
                base_value = summary_plot.iloc[0]["total_value"]
                summary_plot["nav"] = summary_plot["total_value"] / base_value * 100

            # 降采样用于图表渲染
            chart_data = downsample(summary_plot, max_points=DOWNSAMPLE_MAX_POINTS)

            # 基准指数对比（使用侧边栏选择的基准）
            bench_name = INDEX_CODES.get(selected_benchmark, selected_benchmark)
            bench_code = _resolve_benchmark_code(selected_benchmark)
            bench_df = load_benchmark_comparison(bench_code, show_days + 10, selected_date)
            if not bench_df.empty:
                bench_base = bench_df.iloc[0]["close"]
                bench_plot = bench_df.copy()
                bench_plot["nav"] = bench_plot["close"] / bench_base * 100
                bench_chart = downsample(bench_plot, max_points=DOWNSAMPLE_MAX_POINTS)

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
            render_chart(fig)

    with col_right:
        st.markdown(
            '<div class="tip-title" style="">日收益率分布<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">统计选定时间范围内每日收益率(%)的频率分布。橙色虚线为均值，黄色区间为±1个标准差范围，绿色虚线为±2个标准差。</span></div>',
            unsafe_allow_html=True,
        )
        if not summary.empty and "total_value" in summary.columns and len(summary) > 5:
            # 使用show_days范围内的数据计算日收益率，与净值走势图对齐
            ret_start_idx = max(0, len(summary) - show_days)
            summary_ret = summary.iloc[ret_start_idx:].copy()
            # 使用预存的 corrected daily_return，避免 total_value 跳变影响
            daily_rets = (summary_ret["daily_return"].dropna() * 100).values if "daily_return" in summary_ret.columns else (summary_ret["total_value"].pct_change().dropna() * 100).values
            if len(daily_rets) > 0:
                std_ret = np.std(daily_rets, ddof=1)
                mean_ret = np.mean(daily_rets)

                # 裁剪极端值用于直方图显示（保留±5σ范围），避免异常值压缩正常分布
                clip_bound = min(std_ret * 5, 20)
                rets_clipped = np.clip(daily_rets, -clip_bound, clip_bound)

                fig_hist = go.Figure()
                fig_hist.add_trace(
                    go.Histogram(
                        x=rets_clipped,
                        nbinsx=40,
                        marker_color="#58a6ff",
                        marker_line_color="#0d1117",
                        marker_line_width=0.5,
                        opacity=0.85,
                        name="日收益率",
                    )
                )
                # 均值线（橙色虚线）
                fig_hist.add_vline(
                    x=mean_ret, line_dash="dash", line_color="#f59e0b",
                    annotation_text=f"均值 {mean_ret:.3f}%",
                    annotation_position="top left",
                )
                # ±1σ 标注（黄色虚线 + 半透明区间）
                fig_hist.add_vline(x=mean_ret - std_ret, line_dash="dot", line_color="#eab308", opacity=0.7)
                fig_hist.add_vline(x=mean_ret + std_ret, line_dash="dot", line_color="#eab308", opacity=0.7)
                fig_hist.add_vrect(
                    x0=mean_ret - std_ret, x1=mean_ret + std_ret,
                    fillcolor="#eab308", opacity=0.08, line_width=0,
                )
                # ±2σ 标注（绿色虚线 + 半透明区间）
                fig_hist.add_vline(x=mean_ret - 2 * std_ret, line_dash="dot", line_color="#22c55e", opacity=0.7)
                fig_hist.add_vline(x=mean_ret + 2 * std_ret, line_dash="dot", line_color="#22c55e", opacity=0.7)
                fig_hist.add_vrect(
                    x0=mean_ret - 2 * std_ret, x1=mean_ret + 2 * std_ret,
                    fillcolor="#22c55e", opacity=0.05, line_width=0,
                )

                fig_hist.update_layout(
                    height=200,
                    plot_bgcolor="#0d1117",
                    paper_bgcolor="#0d1117",
                    font=dict(color="#c9d1d9", size=11),
                    margin=dict(l=50, r=20, t=10, b=40),
                    xaxis=dict(title="日收益率 (%)", showgrid=True, gridcolor="#21262d"),
                    yaxis=dict(title="天数", showgrid=True, gridcolor="#21262d"),
                    showlegend=False,
                )
                render_chart(fig_hist)

                # 补充统计指标
                median_ret = np.median(daily_rets)
                pos_ratio = (daily_rets > 0).sum() / len(daily_rets) * 100
                st.caption(
                    f"中位数: {median_ret:.3f}% | 标准差: {std_ret:.3f}% | "
                    f"盈利率: {pos_ratio:.1f}% | 数据范围: {summary_ret['date'].iloc[0]} ~ {summary_ret['date'].iloc[-1]} ({len(daily_rets)}个交易日)"
                )

    # 日收益柱状图（降采样）
    st.markdown(
        '<div class="tip-title" style="">每日盈亏<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">展示每个交易日的盈亏金额(元)。绿色柱体表示盈利日，红色柱体表示亏损日，可直观观察收益的连续性和波动幅度。</span></div>',
        unsafe_allow_html=True,
    )
    if not summary.empty and "total_value" in summary.columns and len(summary) > 1:
        bar_data = downsample(summary_ret[["date"]].copy(), max_points=DOWNSAMPLE_MAX_POINTS)
        bar_data["daily_pnl"] = summary_ret["daily_pnl"].values if "daily_pnl" in summary_ret.columns else summary_ret["total_value"].diff().values
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
        render_chart(fig_bar)



def _render_rolling_charts(summary, selected_date, show_days):
    # ---------- 滚动指标图表 ----------
    r1, r2 = st.columns([1, 3])
    with r1:
        rolling_window = st.selectbox(
            "滚动窗口", options=[60, 120, 250], format_func=lambda x: f"{x}日", index=0, key="rolling_window"
        )
    rolling_data = compute_rolling_metrics(window=rolling_window, end_date=selected_date)
    if not rolling_data.empty and len(rolling_data) > 5:
        # 限定显示范围与主图对齐
        rolling_show_start = max(0, len(summary) - show_days - 30)
        rolling_start_date = pd.Timestamp(summary["date"].iloc[rolling_show_start])
        rolling_data = rolling_data[rolling_data["date"] >= rolling_start_date].copy()
        if len(rolling_data) > 5:
            st.markdown(
                f'<div class="tip-title">'
                f"滚动风险指标（{rolling_window}日窗口）"
                f'<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span>'
                f'<span class="tip-text" style="left: 4px; top: calc(100% + 10px);">'
                f"使用{rolling_window}日滚动窗口计算的夏普比率和年化波动率。滚动夏普比率反映近期风险调整收益的稳定程度；滚动波动率反映近期市场波动水平的演变。</span>"
                f"</div>",
                unsafe_allow_html=True,
            )
            rolling_chart = downsample(rolling_data, max_points=DOWNSAMPLE_MAX_POINTS)

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
            render_chart(fig_roll)



def _render_benchmark_comparison(summary, selected_benchmark, selected_date, show_days):
    # ---------- 基准对比表 ----------
    # 基准对比详情：使用 show_days 范围内的数据
    if not summary.empty and len(summary) > 1:
        range_start_idx = max(0, len(summary) - show_days)
        summary_for_compare = summary.iloc[range_start_idx:].copy()
        bench_code_raw = _resolve_benchmark_code(selected_benchmark)
        bench_df_raw = load_benchmark_comparison(bench_code_raw, show_days, selected_date)
        if not bench_df_raw.empty:
            st.markdown(
                '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">基准对比详情<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">数值化展示组合与基准指数的收益对比，包括累计收益率、年化收益率、夏普比率、最大回撤、年化波动率和超额收益(Alpha)。</span></div>',
                unsafe_allow_html=True,
            )
            import math

            # 组合指标（使用 show_days 范围内的数据）
            port_start_val = summary_for_compare.iloc[0]["total_value"]
            port_end_val = summary_for_compare.iloc[-1]["total_value"]
            port_total_ret = (port_end_val / port_start_val - 1) * 100 if port_start_val > 0 else 0
            # 使用 portfolio_summary 预存的 daily_return（已校正持仓变化），避免 total_value 跳变影响
            port_daily = (summary_for_compare["daily_return"] / 100).dropna()
            port_ann_ret = port_daily.mean() * 252 * 100 if len(port_daily) > 0 else 0
            port_vol = port_daily.std() * math.sqrt(252) * 100 if len(port_daily) > 1 else 0
            port_sharpe = (
                (port_daily.mean() / port_daily.std() * math.sqrt(252))
                if port_daily.std() > 0
                else 0
            )
            # 使用 portfolio_summary 预存的 max_drawdown（基于 corrected daily_return 累积序列）
            port_drawdown = summary_for_compare["max_drawdown"].min()
            # 基准指标
            bench_start = bench_df_raw.iloc[0]["close"]
            bench_end = bench_df_raw.iloc[-1]["close"]
            bench_total_ret = (bench_end / bench_start - 1) * 100 if bench_start > 0 else 0
            bench_daily_ret = bench_df_raw["close"].pct_change().dropna()
            bench_ann_ret = bench_daily_ret.mean() * 252 * 100 if len(bench_daily_ret) > 0 else 0
            bench_vol = bench_daily_ret.std() * math.sqrt(252) * 100 if len(bench_daily_ret) > 1 else 0
            bench_sharpe = (
                (bench_daily_ret.mean() / bench_daily_ret.std() * math.sqrt(252))
                if bench_daily_ret.std() > 0
                else 0
            )
            bench_cummax = bench_df_raw["close"].cummax()
            bench_drawdown = ((bench_df_raw["close"] - bench_cummax) / bench_cummax * 100).min()
            # 对齐日期计算超额收益
            merged = summary_for_compare[["date", "total_value", "daily_return"]].merge(bench_df_raw[["date", "close"]], on="date", how="inner")
            if not merged.empty:
                excess_ret = (
                    (merged["total_value"].iloc[-1] / merged["total_value"].iloc[0] - 1)
                    - (merged["close"].iloc[-1] / merged["close"].iloc[0] - 1)
                ) * 100
                # 日超额收益率序列：组合侧使用预存的 corrected daily_return
                port_daily_aligned = (merged["daily_return"] / 100).dropna()
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



def _calc_range_metrics(range_data):
    """纯数据函数：计算区间指标（累计/年化收益、波动率、夏普、回撤等）"""
    import math as _math
    r_start_val = range_data.iloc[0]["total_value"]
    r_end_val = range_data.iloc[-1]["total_value"]
    r_cum_ret = (r_end_val / r_start_val - 1) * 100 if r_start_val > 0 else 0
    # 使用预存的 corrected daily_return（已校正持仓变化），避免 total_value 跳变影响
    r_daily = (range_data["daily_return"] / 100).dropna() if "daily_return" in range_data.columns else range_data["total_value"].pct_change().dropna()
    n_days = len(r_daily)
    r_ann_ret = (r_daily.mean() * 252 * 100) if n_days > 0 else 0
    r_vol = (r_daily.std() * _math.sqrt(252) * 100) if n_days > 1 else 0
    r_sharpe = (
        (r_daily.mean() / r_daily.std() * _math.sqrt(252)) if r_daily.std() > 0 else 0
    )
    # 使用预存的 max_drawdown（基于 corrected daily_return 累积序列）
    r_dd = range_data["max_drawdown"].min() if "max_drawdown" in range_data.columns else ((range_data["total_value"] - range_data["total_value"].cummax()) / range_data["total_value"].cummax() * 100).min()
    r_best_day = r_daily.max() if len(r_daily) > 0 else 0
    r_worst_day = r_daily.min() if len(r_daily) > 0 else 0
    r_up_days = (r_daily > 0).sum()
    r_dn_days = (r_daily < 0).sum()
    r_wr = (r_up_days / n_days * 100) if n_days > 0 else 0
    avg_win = r_daily[r_daily > 0].mean() if r_up_days > 0 else 0
    avg_loss = abs(r_daily[r_daily < 0].mean()) if r_dn_days > 0 else 0.0001
    r_pnl_ratio = avg_win / avg_loss if avg_loss > 0 else 0
    return {
        "cum_ret": r_cum_ret, "ann_ret": r_ann_ret, "vol": r_vol,
        "sharpe": r_sharpe, "dd": r_dd, "best_day": r_best_day,
        "worst_day": r_worst_day, "wr": r_wr, "pnl_ratio": r_pnl_ratio, "n_days": n_days,
    }

def _render_benchmark_comparison_tab1(summary, selected_date, show_days):
    """Tab1 子块：多基准叠加对比图表 + 收益排行卡片"""
    st.markdown(
        '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">'
        "多基准叠加对比"
        '<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span>'
        '<span class="tip-text" style="left: 4px; top: calc(100% + 10px);">'
        "将投资组合与多个基准指数归一化到同一起点，直观比较不同时间段的相对走势强弱。"
        "</span></div>",
        unsafe_allow_html=True,
    )

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
        plot_end2 = selected_date if selected_date else summary["date"].iloc[-1]
        ps_idx = max(0, len(summary) - show_days - 30)
        summary_plot = summary.iloc[ps_idx:].copy()
        base_value = summary_plot.iloc[0]["total_value"]
        summary_plot["nav"] = summary_plot["total_value"] / base_value * 100
        chart_data = downsample(summary_plot, max_points=DOWNSAMPLE_MAX_POINTS)

        fig_multi = go.Figure()
        fig_multi.add_trace(
            go.Scatter(
                x=chart_data["date"], y=chart_data["nav"], mode="lines",
                name="投资组合", line=dict(color="#58a6ff", width=2.5),
            )
        )
        _add_min_max_annotations(fig_multi, chart_data["date"], chart_data["nav"], y_label="净值")

        bench_colors = ["#f59e0b", "#22c55e", "#ef4444", "#a855f7", "#06b6d4"]
        bench_stats = []

        for idx, bcode in enumerate(selected_benches):
            bname = bench_options.get(bcode, bcode)
            bdf = load_benchmark_comparison(bcode, show_days + 10, selected_date)
            if not bdf.empty:
                b_base = bdf.iloc[0]["close"]
                b_plot = bdf.copy()
                b_plot["nav"] = b_plot["close"] / b_base * 100
                b_chart = downsample(b_plot, max_points=DOWNSAMPLE_MAX_POINTS)
                fig_multi.add_trace(
                    go.Scatter(
                        x=b_chart["date"], y=b_chart["nav"], mode="lines",
                        name=bname,
                        line=dict(color=bench_colors[idx % len(bench_colors)], width=1.2, dash="dash"),
                    )
                )
                b_start = bdf.iloc[0]["close"]
                b_end = bdf.iloc[-1]["close"]
                b_ret = (b_end / b_start - 1) * 100 if b_start > 0 else 0
                bench_stats.append({"基准": bname, "累计收益": f"{b_ret:+.2f}%"})

        fig_multi.update_layout(
            height=350, plot_bgcolor="#0d1117", paper_bgcolor="#0d1117",
            font=dict(color="#c9d1d9", size=11),
            margin=dict(l=50, r=20, t=10, b=40),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(size=10, color="#8b949e")),
            xaxis=dict(showgrid=False),
            yaxis=dict(title="净值 (基准100)", showgrid=True, gridcolor="#21262d"),
            hovermode="x unified",
        )
        render_chart(fig_multi)

        if bench_stats:
            port_end = summary_plot.iloc[-1]["total_value"]
            port_ret = (port_end / base_value - 1) * 100 if base_value > 0 else 0
            all_items = [{"基准": "投资组合", "累计收益": f"{port_ret:+.2f}%"}] + bench_stats
            all_items.sort(key=lambda x: float(x["累计收益"].replace("%", "").replace("+", "")), reverse=True)
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
                        f'<div style="font-size:14px;font-weight:bold;color:{c};">{item["累计收益"]}</div></div>',
                        unsafe_allow_html=True,
                    )

def _render_range_analysis_tab2(summary, selected_date, selected_benchmark, show_days):
    """Tab2 子块：区间收益分析（日期选择 + 指标计算委托）"""
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
            start_dt = st.selectbox("起始日期", all_dates_list, index=min(len(all_dates_list) - 1, 60), key="range_start")
        with date_range[1]:
            end_dt = st.selectbox("结束日期", all_dates_list, index=0, key="range_end")

        if start_dt < end_dt:
            mask = (summary["date"] >= start_dt) & (summary["date"] <= end_dt)
            range_data = summary[mask].copy()
            if len(range_data) > 1:
                r = _calc_range_metrics(range_data)
                bench_code = _resolve_benchmark_code(selected_benchmark)
                bname = INDEX_CODES.get(bench_code, bench_code)
                bdf = load_benchmark_comparison(bench_code, show_days, selected_date)
                b_stats = {}
                if not bdf.empty:
                    import math as _math
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
                            "sharpe": (b_daily.mean() / b_daily.std() * _math.sqrt(252)) if b_daily.std() > 0 else 0,
                            "dd": ((b_range["close"] - b_range["close"].cummax()) / b_range["close"].cummax() * 100).min(),
                        }
                _render_range_metrics_cards(r, b_stats, bname, start_dt, end_dt)
            else:
                st.info("所选区间内交易日不足，请调整日期范围")
        else:
            st.warning("起始日期须早于结束日期")

def _render_range_metrics_cards(r, b_stats, bname, start_dt, end_dt):
    """区间指标卡片 + 详细对比表渲染"""

    def _fmt(v, suffix="", dec=2, inv=False):
        try:
            fv = float(v)
        except (ValueError, TypeError):
            return '<span style="color:#8b949e;">--</span>'
        c = "#22c55e" if (fv >= 0 and not inv) or (fv < 0 and inv) else "#ef4444"
        if abs(fv) < 0.005:
            c = "#c9d1d9"
        return f'<span style="color:{c};font-weight:bold;">{fv:+.{dec}f}{suffix}</span>'

    ic1, ic2, ic3, ic4 = st.columns(4)
    with ic1:
        c = "#22c55e" if r["cum_ret"] >= 0 else "#ef4444"
        st.markdown(f'<div style="padding:8px;border-radius:6px;background:#161b22;border-left:3px solid {c};"><div style="font-size:10px;color:#8b949e;">累计收益</div><div style="font-size:16px;font-weight:bold;color:{c};">{r["cum_ret"]:+.2f}%</div></div>', unsafe_allow_html=True)
    with ic2:
        c2 = "#22c55e" if r["sharpe"] >= 0.5 else "#f59e0b" if r["sharpe"] >= 0 else "#ef4444"
        st.markdown(f'<div style="padding:8px;border-radius:6px;background:#161b22;border-left:3px solid {c2};"><div style="font-size:10px;color:#8b949e;">区间夏普</div><div style="font-size:16px;font-weight:bold;color:{c2};">{r["sharpe"]:.3f}</div></div>', unsafe_allow_html=True)
    with ic3:
        c3 = "#ef4444" if abs(r["dd"]) > 15 else "#f59e0b" if abs(r["dd"]) > 8 else "#22c55e"
        st.markdown(f'<div style="padding:8px;border-radius:6px;background:#161b22;border-left:3px solid {c3};"><div style="font-size:10px;color:#8b949e;">最大回撤</div><div style="font-size:16px;font-weight:bold;color:{c3};">{r["dd"]:.2f}%</div></div>', unsafe_allow_html=True)
    with ic4:
        c4 = "#22c55e" if r["wr"] >= 60 else "#f59e0b" if r["wr"] >= 45 else "#ef4444"
        st.markdown(f'<div style="padding:8px;border-radius:6px;background:#161b22;border-left:3px solid {c4};"><div style="font-size:10px;color:#8b949e;">胜率 / 盈亏比</div><div style="font-size:14px;font-weight:bold;color:{c4};">{r["wr"]:.0f}% / {r["pnl_ratio"]:.2f}</div></div>', unsafe_allow_html=True)

    b_cum_s = _fmt(b_stats.get("cum_ret", 0), "%")
    b_ann_s = _fmt(b_stats.get("ann_ret", 0), "%")
    b_sh_s = f'<span style="color:#8b949e;">{b_stats.get("sharpe", 0):.3f}</span>'
    b_dd_s = f'<span style="color:#ef4444;">{b_stats.get("dd", 0):.2f}%</span>'
    b_vol_s = f'<span style="color:#8b949e;">{b_stats.get("vol", 0):.2f}%</span>'
    alpha = r["cum_ret"] - b_stats.get("cum_ret", 0)
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
    <td style="padding:6px 10px;text-align:center;">{_fmt(r["cum_ret"], "%")}</td>
    <td style="padding:6px 10px;text-align:center;">{b_cum_s}</td>
    <td style="padding:6px 10px;text-align:center;font-weight:bold;color:{alpha_c};">{alpha:+.2f}%</td>
    </tr>
    <tr style="background:#161b22;border-bottom:1px solid #21262d;">
    <td style="padding:6px 10px;color:#c9d1d9;">年化收益率</td>
    <td style="padding:6px 10px;text-align:center;">{_fmt(r["ann_ret"], "%")}</td>
    <td style="padding:6px 10px;text-align:center;">{b_ann_s}</td>
    <td style="padding:6px 10px;text-align:center;">{_fmt(r["ann_ret"] - b_stats.get("ann_ret", 0), "%")}</td>
    </tr>
    <tr style="border-bottom:1px solid #21262d;">
    <td style="padding:6px 10px;color:#c9d1d9;">夏普比率</td>
    <td style="padding:6px 10px;text-align:center;"><span style="color:#8b949e;">{r["sharpe"]:.3f}</span></td>
    <td style="padding:6px 10px;text-align:center;">{b_sh_s}</td>
    <td style="padding:6px 10px;text-align:center;">{_fmt(r["sharpe"] - b_stats.get("sharpe", 0), dec=3)}</td>
    </tr>
    <tr style="background:#161b22;border-bottom:1px solid #21262d;">
    <td style="padding:6px 10px;color:#c9d1d9;">最大回撤</td>
    <td style="padding:6px 10px;text-align:center;"><span style="color:#ef4444;">{r["dd"]:.2f}%</span></td>
    <td style="padding:6px 10px;text-align:center;">{b_dd_s}</td>
    <td style="padding:6px 10px;text-align:center;">{_fmt(r["dd"] - b_stats.get("dd", 0), "%", inv=True)}</td>
    </tr>
    <tr style="border-bottom:1px solid #21262d;">
    <td style="padding:6px 10px;color:#c9d1d9;">年化波动率</td>
    <td style="padding:6px 10px;text-align:center;"><span style="color:#8b949e;">{r["vol"]:.2f}%</span></td>
    <td style="padding:6px 10px;text-align:center;">{b_vol_s}</td>
    <td style="padding:6px 10px;text-align:center;">{_fmt(r["vol"] - b_stats.get("vol", 0), "%", inv=True)}</td>
    </tr>
    <tr style="background:#161b22;border-bottom:1px solid #21262d;">
    <td style="padding:6px 10px;color:#c9d1d9;">胜率</td>
    <td style="padding:6px 10px;text-align:center;"><span style="color:#8b949e;">{r["wr"]:.1f}%</span></td>
    <td style="padding:6px 10px;text-align:center;" colspan="2"><span style="color:#484f58;">--</span></td>
    </tr>
    <tr style="border-bottom:1px solid #21262d;">
    <td style="padding:6px 10px;color:#c9d1d9;">盈亏比</td>
    <td style="padding:6px 10px;text-align:center;"><span style="color:#8b949e;">{r["pnl_ratio"]:.2f}</span></td>
    <td style="padding:6px 10px;text-align:center;" colspan="2"><span style="color:#484f58;">--</span></td>
    </tr>
    <tr style="background:#161b22;">
    <td style="padding:6px 10px;color:#c9d1d9;">最佳/最差单日</td>
    <td style="padding:6px 10px;text-align:center;">{_fmt(r["best_day"], "%")} / {_fmt(r["worst_day"], "%")}</td>
    <td style="padding:6px 10px;text-align:center;" colspan="2"><span style="color:#484f58;">--</span></td>
    </tr>
    </tbody></table></div>"""
    st.markdown(html_range, unsafe_allow_html=True)
    st.caption(f"*区间: {start_dt} ~ {end_dt}，共 {r['n_days']} 个交易日*")

def _render_multi_benchmark_analysis(summary, selected_date, selected_benchmark, show_days):
    """编排函数：多基准对比 & 区间分析"""
    st.markdown("---")
    compare_tab1, compare_tab2 = st.tabs(["📊 多基准对比", "📅 区间收益分析"])
    with compare_tab1:
        _render_benchmark_comparison_tab1(summary, selected_date, show_days)
    with compare_tab2:
        _render_range_analysis_tab2(summary, selected_date, selected_benchmark, show_days)

def _render_annual_returns(summary):
    # --- 年度收益对比条形图 ---
    st.markdown("---")
    st.markdown(
        '<div class="tip-title" style="font-size:14px;border-bottom:none;padding:5px 0;">'
        '年度收益对比<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span>'
        '<span class="tip-text" style="left: 4px; top: calc(100% + 10px);">'
        '各年度累计收益率对比，颜色反映涨跌方向，帮助识别组合的年度表现规律。</span></div>',
        unsafe_allow_html=True,
    )

    if not summary.empty and len(summary) > 1:
        summary_annual = summary.copy()
        summary_annual["date"] = pd.to_datetime(summary_annual["date"])
        summary_annual["year"] = summary_annual["date"].dt.year
        yearly_returns = summary_annual.groupby("year")[["total_value"]].apply(
            lambda g: g.iloc[-1, 0] / g.iloc[0, 0] - 1
        ) * 100
        if not yearly_returns.empty:
            bar_colors = ["#22c55e" if v >= 0 else "#ef4444" for v in yearly_returns.values]
            fig_annual = go.Figure()
            fig_annual.add_trace(go.Bar(
                x=yearly_returns.index.astype(str),
                y=yearly_returns.values,
                marker_color=bar_colors,
                text=[f"{v:+.1f}%" for v in yearly_returns.values],
                textposition="auto",
                textfont=dict(size=11, color="#c9d1d9"),
                hovertemplate="%{x}年<br>收益率: %{y:+.2f}%<extra></extra>",
            ))
            fig_annual.add_hline(y=0, line_dash="dash", line_color="#8b949e", line_width=1)
            fig_annual.update_layout(
                height=280,
                plot_bgcolor="#0d1117", paper_bgcolor="#0d1117",
                font=dict(color="#c9d1d9", size=11),
                margin=dict(l=40, r=20, t=10, b=30),
                xaxis=dict(title="", showgrid=False),
                yaxis=dict(title="年度收益率 (%)", showgrid=True, gridcolor="#21262d"),
                showlegend=False,
            )
            render_chart(fig_annual)


def render_tab1():
    selected_date = st.session_state.get("selected_date", "")
    selected_benchmark = st.session_state.get("selected_benchmark", "sh000300")
    show_days = st.session_state.get('show_days', 250)
    positions = load_positions(selected_date)
    summary = load_summary(show_days, selected_date)
    """渲染Tab1: 净值走势 - 编排入口"""
    if summary.empty:
        st.info("暂无汇总数据，请先运行数据收集脚本。")
        st.code("python backfill_full_history.py")
        return
    technical = pd.DataFrame()
    volatility = None
    max_dd = None
    sharpe = None
    cal_data = pd.DataFrame()
    tech_signals = pd.DataFrame()
    show_days = st.session_state.get('show_days', 250)

    st.caption("展示组合净值走势与基准对比、日收益率分布、每日盈亏及滚动风险指标")

    _render_basic_metrics(positions, summary, {}, selected_date, selected_benchmark, technical, volatility, max_dd, sharpe, cal_data, tech_signals, show_days)
    _render_rolling_charts(summary, selected_date, show_days)
    _render_benchmark_comparison(summary, selected_benchmark, selected_date, show_days)
    _render_multi_benchmark_analysis(summary, selected_date, selected_benchmark, show_days)
    _render_annual_returns(summary)


    


