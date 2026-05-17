"""央行购金全球趋势追踪（Phase 4.1）"""
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from .gold_utils import fetch_china_reserve_data, fetch_global_etf_holdings


def _render_reserve_cards(df_reserve, df_etf):
    if df_reserve is None or df_reserve.empty:
        return
    latest = df_reserve.iloc[-1]
    gold_res = latest.get("gold_reserve", 0)
    fx_res = latest.get("fx_reserve", 0)
    gold_ratio = (gold_res / fx_res * 100) if fx_res and fx_res > 0 else 0
    gold_mom = latest.get("gold_reserve_mom", 0)
    recent_12 = df_reserve.tail(12)
    total_increase = recent_12["gold_reserve"].diff().dropna().sum()
    etf_latest = df_etf["total_holdings"].iloc[-1] if df_etf is not None and not df_etf.empty else 0
    etf_change = df_etf["change"].iloc[-1] if df_etf is not None and not df_etf.empty else 0
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("黄金储备（万吨）", f"{gold_res:,.1f}", delta=f"{gold_mom:+.1f}%" if pd.notna(gold_mom) else None)
    with c2:
        st.metric("占外汇储备", f"{gold_ratio:.1f}%")
    with c3:
        st.metric("近12月累计增持", f"{total_increase:+,.1f} 万吨")
    with c4:
        st.metric("全球ETF持仓（吨）", f"{etf_latest:,.1f}", delta=f"{etf_change:+.2f} 吨" if pd.notna(etf_change) else None)


def _render_china_reserve_trend(df):
    if df is None or df.empty:
        st.info("暂无中国黄金储备数据"); return
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    recent = df[df["month"] >= df["month"].max() - pd.DateOffset(years=5)].copy()
    fig.add_trace(go.Scatter(x=recent["month"], y=recent["gold_reserve"], mode="lines+markers", name="黄金储备（万吨）",
        fill="tozeroy", fillcolor="rgba(250,204,21,0.15)", line=dict(color="#facc15", width=2), marker=dict(size=3),
        hovertemplate="%{x|%Y-%m}<br>储备: %{y:.1f} 万吨<extra></extra>"), secondary_y=False)
    mc = recent["gold_reserve"].diff()
    colors = ["#22c55e" if v >= 0 else "#ef4444" for v in mc.fillna(0)]
    fig.add_trace(go.Bar(x=recent["month"], y=mc, name="月度增持/减持", marker_color=colors, opacity=0.7,
        hovertemplate="%{x|%Y-%m}<br>变动: %{y:+.1f} 万吨<extra></extra>"), secondary_y=True)
    fig.update_layout(title="中国黄金储备月度趋势", height=380, template="plotly_dark",
        margin=dict(l=50, r=50, t=40, b=30), legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        xaxis=dict(rangeslider=dict(visible=False)))
    fig.update_yaxes(title_text="储备量（万吨）", secondary_y=False, side="left")
    fig.update_yaxes(title_text="增持/减持（万吨）", secondary_y=True, side="right")
    st.plotly_chart(fig, use_container_width=True)


def _render_reserve_ratio(df):
    if df is None or df.empty:
        return
    dc = df.dropna(subset=["gold_reserve", "fx_reserve"]).copy()
    dc["gold_ratio"] = dc["gold_reserve"] / dc["fx_reserve"] * 100
    recent = dc[dc["month"] >= dc["month"].max() - pd.DateOffset(years=10)]
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=recent["month"], y=recent["gold_ratio"], mode="lines+markers", name="黄金占比",
        line=dict(color="#f59e0b", width=2), marker=dict(size=3), fill="tozeroy", fillcolor="rgba(245,158,11,0.1)",
        hovertemplate="%{x|%Y-%m}<br>占比: %{y:.2f}%<extra></extra>"))
    avg = recent["gold_ratio"].mean()
    fig.add_hline(y=avg, line_dash="dash", line_color="#8b949e", annotation_text=f"均值 {avg:.1f}%")
    fig.update_layout(title="黄金储备占外汇储备比例（%）", height=320, template="plotly_dark",
        margin=dict(l=50, r=50, t=40, b=30), xaxis=dict(rangeslider=dict(visible=False)))
    st.plotly_chart(fig, use_container_width=True)


def _render_global_etf_trend(df):
    if df is None or df.empty:
        st.info("暂无全球黄金ETF持仓数据"); return
    recent = df[df["date"] >= df["date"].max() - pd.DateOffset(years=2)].copy()
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Scatter(x=recent["date"], y=recent["total_holdings"], mode="lines", name="全球黄金ETF持仓（吨）",
        fill="tozeroy", fillcolor="rgba(88,166,255,0.1)", line=dict(color="#58a6ff", width=2),
        hovertemplate="%{x|%Y-%m-%d}<br>持仓: %{y:.1f} 吨<extra></extra>"), secondary_y=False)
    colors = ["#22c55e" if v >= 0 else "#ef4444" for v in recent["change"].fillna(0)]
    fig.add_trace(go.Bar(x=recent["date"], y=recent["change"], name="日度增减", marker_color=colors, opacity=0.5,
        hovertemplate="%{x|%Y-%m-%d}<br>变动: %{y:+.2f} 吨<extra></extra>"), secondary_y=True)
    fig.update_layout(title="全球黄金ETF持仓趋势（近2年）", height=380, template="plotly_dark",
        margin=dict(l=50, r=50, t=40, b=30), legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        xaxis=dict(rangeslider=dict(visible=False)))
    fig.update_yaxes(title_text="持仓量（吨）", secondary_y=False, side="left")
    fig.update_yaxes(title_text="日增减（吨）", secondary_y=True, side="right")
    st.plotly_chart(fig, use_container_width=True)


def _render_reserve_vs_etf(df_reserve, df_etf):
    if df_reserve is None or df_reserve.empty or df_etf is None or df_etf.empty:
        return
    em = df_etf.set_index("date").resample("ME")["total_holdings"].mean().reset_index()
    em["month"] = em["date"].dt.to_period("M").dt.to_timestamp()
    dm = df_reserve[["month", "gold_reserve"]].merge(em[["month", "total_holdings"]], on="month", how="inner")
    if dm is None or dm.empty:
        return
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Scatter(x=dm["month"], y=dm["gold_reserve"], mode="lines+markers", name="中国黄金储备（万吨）",
        line=dict(color="#facc15", width=2), marker=dict(size=4), hovertemplate="%{x|%Y-%m}<br>储备: %{y:.1f}<extra></extra>"),
        secondary_y=False)
    fig.add_trace(go.Scatter(x=dm["month"], y=dm["total_holdings"], mode="lines+markers", name="全球ETF持仓（吨）",
        line=dict(color="#58a6ff", width=2), marker=dict(size=4), hovertemplate="%{x|%Y-%m}<br>ETF: %{y:.0f}<extra></extra>"),
        secondary_y=True)
    fig.update_layout(title="中国央行储备 vs 全球黄金ETF持仓", height=320, template="plotly_dark",
        margin=dict(l=50, r=50, t=40, b=30), legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        xaxis=dict(rangeslider=dict(visible=False)))
    fig.update_yaxes(title_text="储备（万吨）", secondary_y=False, side="left")
    fig.update_yaxes(title_text="ETF持仓（吨）", secondary_y=True, side="right")
    st.plotly_chart(fig, use_container_width=True)


def render_central_bank_trends():
    st.markdown("#### 央行购金全球趋势")
    with st.spinner("加载央行储备数据..."):
        df_reserve = fetch_china_reserve_data()
    with st.spinner("加载全球ETF持仓数据..."):
        df_etf = fetch_global_etf_holdings()
    _render_reserve_cards(df_reserve, df_etf)
    st.markdown("---")
    _render_china_reserve_trend(df_reserve)
    cl, cr = st.columns(2)
    with cl:
        _render_reserve_ratio(df_reserve)
    with cr:
        _render_reserve_vs_etf(df_reserve, df_etf)
    _render_global_etf_trend(df_etf)
