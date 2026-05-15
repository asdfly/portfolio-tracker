"""
图表与工具函数（从 dashboard.py 提取的纯函数）
"""

import numpy as np
import pandas as pd
import plotly.graph_objects as go


def downsample(df, date_col="date", max_points=500):
    """将时间序列降采样到max_points个点，保留边界值"""
    n = len(df)
    if n <= max_points:
        return df
    step = max(1, (n - 2) // (max_points - 2))
    indices = list(range(0, n, step))
    if indices[-1] != n - 1:
        indices.append(n - 1)
    if indices[0] != 0:
        indices.insert(0, 0)
    indices = sorted(set(indices))
    return df.iloc[indices].reset_index(drop=True)


def _add_min_max_annotations(fig, x_data, y_data, row=None, col=None, y_label=None, date_format="%m-%d"):
    """在时间轴图表中标记最大值和最小值的位置及数值"""
    x_arr = np.array(x_data)
    y_arr = np.array(y_data, dtype=float)
    valid = ~np.isnan(y_arr)
    x_arr, y_arr = x_arr[valid], y_arr[valid]
    if len(x_arr) < 2:
        return
    max_idx = np.argmax(y_arr)
    min_idx = np.argmin(y_arr)
    max_x, max_y = x_arr[max_idx], y_arr[max_idx]
    min_x, min_y = x_arr[min_idx], y_arr[min_idx]

    def fmt_val(v):
        if abs(v) >= 1000:
            return f"{v:,.0f}"
        elif abs(v) >= 1:
            return f"{v:.2f}"
        else:
            return f"{v:.4f}"

    max_text = f"Max {fmt_val(max_y)}"
    min_text = f"Min {fmt_val(min_y)}"

    scatter_kwargs = dict(mode="markers+text", hoverinfo="skip", showlegend=False)
    marker_max = dict(color="#22c55e", size=8, symbol="triangle-down")
    marker_min = dict(color="#ef4444", size=8, symbol="triangle-up")
    font_max = dict(size=9, color="#22c55e")
    font_min = dict(size=9, color="#ef4444")

    trace_kwargs = {}
    if row is not None and col is not None:
        trace_kwargs["row"] = row
        trace_kwargs["col"] = col

    fig.add_trace(
        go.Scatter(x=[max_x], y=[max_y], marker=marker_max,
                   text=[max_text], textposition="top center",
                   textfont=font_max, **scatter_kwargs),
        **trace_kwargs
    )
    fig.add_trace(
        go.Scatter(x=[min_x], y=[min_y], marker=marker_min,
                   text=[min_text], textposition="bottom center",
                   textfont=font_min, **scatter_kwargs),
        **trace_kwargs
    )


def _cleanse_daily_returns(df, return_col="daily_return", threshold=5.0, max_tail=500):
    """清洗日收益率数据：过滤异常值 + 截断早期高波动区间"""
    original_count = len(df)
    mask = df[return_col].abs() <= threshold
    filtered_df = df[mask].copy()
    filtered_count = original_count - len(filtered_df)
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
    return tailed_df, stats


def _fmt(v, suffix="", dec=2, inv=False):
    """格式化数值并着色（HTML span）"""
    try:
        fv = float(v)
    except:
        return '<span style="color:#8b949e;">--</span>'
    c = "#22c55e" if (fv >= 0 and not inv) or (fv < 0 and inv) else "#ef4444"
    if abs(fv) < 0.005:
        c = "#c9d1d9"
    return f'<span style="color:{c};font-weight:bold;">{fv:+.{dec}f}{suffix}</span>'


def _fmt_cell(val, suffix="", decimals=2, invert_color=False):
    """格式化数值并着色（HTML span，用于表格单元格）"""
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


def _sig(val, bull, bear, warn=None):
    """技术信号颜色标记"""
    if warn and val in warn:
        return f'<span style="color:#f59e0b;font-weight:bold">{val}</span>'
    if val in bull:
        return f'<span style="color:#22c55e;font-weight:bold">{val}</span>'
    if val in bear:
        return f'<span style="color:#ef4444;font-weight:bold">{val}</span>'
    return f'<span style="color:#8b949e">{val}</span>'


def _rsi_c(v):
    """RSI指标颜色标记"""
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
    """布林带位置颜色标记"""
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
    """ATR颜色标记"""
    if v >= 3.0:
        return f'<span style="color:#f59e0b;font-weight:bold">{v:.2f}%</span>'
    if v >= 2.0:
        return f'<span style="color:#c9d1d9">{v:.2f}%</span>'
    return f'<span style="color:#22c55e">{v:.2f}%</span>'
