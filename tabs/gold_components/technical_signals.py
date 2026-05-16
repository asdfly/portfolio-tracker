"""
黄金技术信号面板：MACD + RSI + Bollinger Bands 三合一技术分析
"""

import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np

from tabs.gold_components.gold_utils import (
    fetch_sge_hist, calc_bollinger, calc_macd, calc_rsi,
    DARK_BG, DARK_FONT_COLOR, GRID_COLOR,
)


@st.cache_data(ttl=3600)
def _get_gold_data(symbol, n_days):
    """获取并截取黄金K线数据"""
    df = fetch_sge_hist(symbol=symbol)
    if df is None or df.empty:
        return None
    cutoff = pd.Timestamp.now() - pd.Timedelta(days=n_days)
    df = df[df["date"] >= cutoff].copy()
    if df.empty:
        return None
    return df

def _compute_signals(df):
    """计算MACD、RSI、Bollinger指标，并返回综合信号"""
    close = df["close"].astype(float)
    boll = calc_bollinger(close)
    macd = calc_macd(close)
    rsi = calc_rsi(close)

    last_close = close.iloc[-1]
    last_boll = boll.iloc[-1]
    last_macd = macd.iloc[-1]
    last_rsi = rsi.iloc[-1]

    bullish = 0
    bearish = 0

    # Bollinger
    if last_close <= last_boll["lower"]:
        bullish += 2
    elif last_close <= last_boll["middle"]:
        bullish += 1
    if last_close >= last_boll["upper"]:
        bearish += 2
    elif last_close >= last_boll["middle"]:
        bearish += 1

    # MACD
    if last_macd["macd"] > last_macd["signal"]:
        bullish += 1
    elif last_macd["macd"] < last_macd["signal"]:
        bearish += 1

    # MACD柱状图翻转
    if len(macd) >= 2:
        prev_hist = macd["hist"].iloc[-2]
        curr_hist = last_macd["hist"]
        if prev_hist < 0 and curr_hist > 0:
            bullish += 1
        elif prev_hist > 0 and curr_hist < 0:
            bearish += 1

    # RSI
    if last_rsi < 30:
        bullish += 2
    elif last_rsi < 40:
        bullish += 1
    if last_rsi > 70:
        bearish += 2
    elif last_rsi > 60:
        bearish += 1

    if bullish > bearish + 1:
        signal = "\U0001f7e2 \u591a\u5934\u504f\u5f3a"
        signal_color = "#66BB6A"
    elif bearish > bullish + 1:
        signal = "\U0001f534 \u7a7a\u5934\u504f\u5f3a"
        signal_color = "#EF5350"
    elif bullish > bearish:
        signal = "\U0001f7e2 \u504f\u591a\u5934"
        signal_color = "#A5D6A7"
    elif bearish > bullish:
        signal = "\U0001f534 \u504f\u7a7a\u5934"
        signal_color = "#EF9A9A"
    else:
        signal = "\u26aa \u4e2d\u6027"
        signal_color = "#BDBDBD"

    return boll, macd, rsi, signal, signal_color



def render_technical_signals():
    """渲染技术信号面板子Tab"""
    ctrl = st.columns([2, 2, 1])
    with ctrl[0]:
        symbol = st.selectbox("品种", ["Au99.99", "Au99.95", "Au(T+D)", "mAu(T+D)"], key="tech_symbol")
    with ctrl[1]:
        period = st.selectbox("周期", ["近3月", "近6月", "近1年", "近3年"], key="tech_period")
    with ctrl[2]:
        show_volume = st.checkbox("成交量", value=False, key="tech_volume")

    period_map = {"近3月": 90, "近6月": 180, "近1年": 365, "近3年": 1095}
    n_days = period_map.get(period, 365)

    df = _get_gold_data(symbol, n_days)
    if df is None:
        st.warning("暂无数据")
        return

    boll, macd, rsi, signal, signal_color = _compute_signals(df)

    latest_close = float(df["close"].iloc[-1])
    latest_rsi = float(rsi.iloc[-1])
    latest_macd_val = float(macd["macd"].iloc[-1])
    boll_width = float(boll["upper"].iloc[-1] - boll["lower"].iloc[-1])
    boll_lower = float(boll["lower"].iloc[-1])
    boll_pct = (latest_close - boll_lower) / max(boll_width, 0.01) * 100

    st.markdown(
        f'<div style="display:flex;gap:12px;margin-bottom:12px;">'
        f'<div style="flex:1;background:#252540;padding:12px;border-radius:8px;border-left:4px solid {signal_color};">'
        f'<div style="font-size:12px;color:#999;">综合信号</div>'
        f'<div style="font-size:18px;font-weight:bold;color:{signal_color};">{signal}</div></div>'
        f'<div style="flex:1;background:#252540;padding:12px;border-radius:8px;">'
        f'<div style="font-size:12px;color:#999;">RSI(14)</div>'
        f'<div style="font-size:18px;font-weight:bold;">{latest_rsi:.1f}</div></div>'
        f'<div style="flex:1;background:#252540;padding:12px;border-radius:8px;">'
        f'<div style="font-size:12px;color:#999;">MACD</div>'
        f'<div style="font-size:18px;font-weight:bold;">{latest_macd_val:.2f}</div></div>'
        f'<div style="flex:1;background:#252540;padding:12px;border-radius:8px;">'
        f'<div style="font-size:12px;color:#999;">Bollinger位置</div>'
        f'<div style="font-size:18px;font-weight:bold;">{boll_pct:.0f}%</div></div>'
        f'</div>',
        unsafe_allow_html=True,
    )

    # 三合一图表
    fig = make_subplots(
        rows=3, cols=1, shared_xaxes=True,
        vertical_spacing=0.05,
        row_heights=[0.5, 0.25, 0.25],
    )

    fig.add_trace(go.Candlestick(
        x=df["date"], open=df["open"], high=df["high"],
        low=df["low"], close=df["close"], name="K线",
        increasing_line_color="#ef5350", decreasing_line_color="#26a69a",
    ), row=1, col=1)
    fig.add_trace(go.Scatter(
        x=df["date"], y=boll["upper"], mode="lines",
        name="Boll上轨", line=dict(color="#FF9800", width=1, dash="dash"),
    ), row=1, col=1)
    fig.add_trace(go.Scatter(
        x=df["date"], y=boll["middle"], mode="lines",
        name="Boll中轨", line=dict(color="#FFD54F", width=1),
    ), row=1, col=1)
    fig.add_trace(go.Scatter(
        x=df["date"], y=boll["lower"], mode="lines",
        name="Boll下轨", line=dict(color="#FF9800", width=1, dash="dash"),
        fill="tonexty", fillcolor="rgba(255,152,0,0.08)",
    ), row=1, col=1)

    macd_colors = ["#66BB6A" if v >= 0 else "#EF5350" for v in macd["hist"]]
    fig.add_trace(go.Bar(
        x=df["date"], y=macd["hist"], name="MACD柱",
        marker_color=macd_colors, opacity=0.7,
    ), row=2, col=1)
    fig.add_trace(go.Scatter(
        x=df["date"], y=macd["macd"], mode="lines",
        name="MACD", line=dict(color="#42A5F5", width=1.5),
    ), row=2, col=1)
    fig.add_trace(go.Scatter(
        x=df["date"], y=macd["signal"], mode="lines",
        name="Signal", line=dict(color="#FF7043", width=1.5, dash="dash"),
    ), row=2, col=1)

    fig.add_trace(go.Scatter(
        x=df["date"], y=rsi, mode="lines",
        name="RSI(14)", line=dict(color="#AB47BC", width=1.5),
    ), row=3, col=1)
    fig.add_trace(go.Scatter(
        x=[df["date"].iloc[0], df["date"].iloc[-1]], y=[70, 70],
        mode="lines", name="超买(70)",
        line=dict(color="#EF5350", width=1, dash="dot"),
    ), row=3, col=1)
    fig.add_trace(go.Scatter(
        x=[df["date"].iloc[0], df["date"].iloc[-1]], y=[30, 30],
        mode="lines", name="超卖(30)",
        line=dict(color="#66BB6A", width=1, dash="dot"),
    ), row=3, col=1)

    fig.update_layout(
        height=700,
        plot_bgcolor=DARK_BG, paper_bgcolor=DARK_BG,
        font=dict(color=DARK_FONT_COLOR),
        xaxis_rangeslider_visible=False,
        xaxis=dict(gridcolor=GRID_COLOR),
        xaxis3=dict(gridcolor=GRID_COLOR),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, font=dict(size=10)),
        margin=dict(l=50, r=30, t=30, b=30),
    )
    fig.update_yaxes(gridcolor=GRID_COLOR, row=1, col=1)
    fig.update_yaxes(gridcolor=GRID_COLOR, row=2, col=1)
    fig.update_yaxes(gridcolor=GRID_COLOR, row=3, col=1, range=[0, 100])

    st.plotly_chart(fig, use_container_width=True)

    with st.expander("技术指标解读", expanded=False):
        col_a, col_b = st.columns(2)
        with col_a:
            st.markdown("**Bollinger Bands**")
            st.markdown(f"- 上轨: {boll['upper'].iloc[-1]:.2f}")
            st.markdown(f"- 中轨: {boll['middle'].iloc[-1]:.2f}")
            st.markdown(f"- 下轨: {boll['lower'].iloc[-1]:.2f}")
            st.markdown(f"- 带宽: {boll_width:.2f}")
            st.markdown(f"- 价格位置: {boll_pct:.0f}%")
        with col_b:
            st.markdown("**MACD (12,26,9)**")
            st.markdown(f"- DIF: {latest_macd_val:.2f}")
            st.markdown(f"- DEA: {macd['signal'].iloc[-1]:.2f}")
            st.markdown(f"- 柱: {macd['hist'].iloc[-1]:.2f}")
            st.markdown("")
            st.markdown(f"**RSI (14)**: {latest_rsi:.1f}")
            if latest_rsi > 70:
                st.markdown("- 警告: 超买区域")
            elif latest_rsi < 30:
                st.markdown("- 机会: 超卖区域")
            else:
                st.markdown("- 中性区域")


def render_technical_signals():
    """渲染技术信号面板子Tab"""
    ctrl = st.columns([2, 2, 1])
    with ctrl[0]:
        symbol = st.selectbox("品种", ["Au99.99", "Au99.95", "Au(T+D)", "mAu(T+D)"], key="tech_symbol")
    with ctrl[1]:
        period = st.selectbox("周期", ["近3月", "近6月", "近1年", "近3年"], key="tech_period")
    with ctrl[2]:
        show_volume = st.checkbox("成交量", value=False, key="tech_volume")

    period_map = {"近3月": 90, "近6月": 180, "近1年": 365, "近3年": 1095}
    n_days = period_map.get(period, 365)

    df = _get_gold_data(symbol, n_days)
    if df is None:
        st.warning("暂无数据")
        return

    boll, macd, rsi, signal, signal_color = _compute_signals(df)
    latest_close = float(df["close"].iloc[-1])
    latest_rsi = float(rsi.iloc[-1])
    latest_macd_val = float(macd["macd"].iloc[-1])
    boll_width = float(boll["upper"].iloc[-1] - boll["lower"].iloc[-1])
    boll_lower = float(boll["lower"].iloc[-1])
    boll_pct = (latest_close - boll_lower) / max(boll_width, 0.01) * 100

    st.markdown(
        f'<div style="display:flex;gap:12px;margin-bottom:12px;">'
        f'<div style="flex:1;background:#252540;padding:12px;border-radius:8px;border-left:4px solid {signal_color};">'
        f'<div style="font-size:12px;color:#999;">综合信号</div>'
        f'<div style="font-size:18px;font-weight:bold;color:{signal_color};">{signal}</div></div>'
        f'<div style="flex:1;background:#252540;padding:12px;border-radius:8px;">'
        f'<div style="font-size:12px;color:#999;">RSI(14)</div>'
        f'<div style="font-size:18px;font-weight:bold;">{latest_rsi:.1f}</div></div>'
        f'<div style="flex:1;background:#252540;padding:12px;border-radius:8px;">'
        f'<div style="font-size:12px;color:#999;">MACD</div>'
        f'<div style="font-size:18px;font-weight:bold;">{latest_macd_val:.2f}</div></div>'
        f'<div style="flex:1;background:#252540;padding:12px;border-radius:8px;">'
        f'<div style="font-size:12px;color:#999;">Bollinger位置</div>'
        f'<div style="font-size:18px;font-weight:bold;">{boll_pct:.0f}%</div></div>'
        f'</div>',
        unsafe_allow_html=True,
    )

    fig = make_subplots(rows=3, cols=1, shared_xaxes=True, vertical_spacing=0.05, row_heights=[0.5, 0.25, 0.25])

    fig.add_trace(go.Candlestick(
        x=df["date"], open=df["open"], high=df["high"],
        low=df["low"], close=df["close"], name="K线",
        increasing_line_color="#ef5350", decreasing_line_color="#26a69a",
    ), row=1, col=1)
    fig.add_trace(go.Scatter(x=df["date"], y=boll["upper"], mode="lines", name="Boll上轨", line=dict(color="#FF9800", width=1, dash="dash")), row=1, col=1)
    fig.add_trace(go.Scatter(x=df["date"], y=boll["middle"], mode="lines", name="Boll中轨", line=dict(color="#FFD54F", width=1)), row=1, col=1)
    fig.add_trace(go.Scatter(x=df["date"], y=boll["lower"], mode="lines", name="Boll下轨", line=dict(color="#FF9800", width=1, dash="dash"), fill="tonexty", fillcolor="rgba(255,152,0,0.08)"), row=1, col=1)

    macd_colors = ["#66BB6A" if v >= 0 else "#EF5350" for v in macd["hist"]]
    fig.add_trace(go.Bar(x=df["date"], y=macd["hist"], name="MACD柱", marker_color=macd_colors, opacity=0.7), row=2, col=1)
    fig.add_trace(go.Scatter(x=df["date"], y=macd["macd"], mode="lines", name="MACD", line=dict(color="#42A5F5", width=1.5)), row=2, col=1)
    fig.add_trace(go.Scatter(x=df["date"], y=macd["signal"], mode="lines", name="Signal", line=dict(color="#FF7043", width=1.5, dash="dash")), row=2, col=1)

    fig.add_trace(go.Scatter(x=df["date"], y=rsi, mode="lines", name="RSI(14)", line=dict(color="#AB47BC", width=1.5)), row=3, col=1)
    fig.add_trace(go.Scatter(x=[df["date"].iloc[0], df["date"].iloc[-1]], y=[70, 70], mode="lines", name="超买(70)", line=dict(color="#EF5350", width=1, dash="dot")), row=3, col=1)
    fig.add_trace(go.Scatter(x=[df["date"].iloc[0], df["date"].iloc[-1]], y=[30, 30], mode="lines", name="超卖(30)", line=dict(color="#66BB6A", width=1, dash="dot")), row=3, col=1)

    fig.update_layout(
        height=700, plot_bgcolor=DARK_BG, paper_bgcolor=DARK_BG,
        font=dict(color=DARK_FONT_COLOR), xaxis_rangeslider_visible=False,
        xaxis=dict(gridcolor=GRID_COLOR), xaxis3=dict(gridcolor=GRID_COLOR),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, font=dict(size=10)),
        margin=dict(l=50, r=30, t=30, b=30),
    )
    fig.update_yaxes(gridcolor=GRID_COLOR, row=1, col=1)
    fig.update_yaxes(gridcolor=GRID_COLOR, row=2, col=1)
    fig.update_yaxes(gridcolor=GRID_COLOR, row=3, col=1, range=[0, 100])
    st.plotly_chart(fig, use_container_width=True)

    with st.expander("技术指标解读", expanded=False):
        col_a, col_b = st.columns(2)
        with col_a:
            st.markdown("**Bollinger Bands**")
            st.markdown(f"- 上轨: {boll['upper'].iloc[-1]:.2f}")
            st.markdown(f"- 中轨: {boll['middle'].iloc[-1]:.2f}")
            st.markdown(f"- 下轨: {boll['lower'].iloc[-1]:.2f}")
            st.markdown(f"- 带宽: {boll_width:.2f}")
            st.markdown(f"- 价格位置: {boll_pct:.0f}%")
        with col_b:
            st.markdown("**MACD (12,26,9)**")
            st.markdown(f"- DIF: {latest_macd_val:.2f}")
            st.markdown(f"- DEA: {macd['signal'].iloc[-1]:.2f}")
            st.markdown(f"- 柱: {macd['hist'].iloc[-1]:.2f}")
            st.markdown("")
            st.markdown(f"**RSI (14)**: {latest_rsi:.1f}")
            if latest_rsi > 70:
                st.markdown("- 警告: 超买区域")
            elif latest_rsi < 30:
                st.markdown("- 机会: 超卖区域")
            else:
                st.markdown("- 中性区域")
