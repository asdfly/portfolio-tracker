"""黄金供需平衡分析（Phase 4.2）"""
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from .gold_utils import fetch_comex_inventory, fetch_global_etf_holdings, fetch_sge_benchmark


def _render_supply_demand_cards(df_comex, df_etf, df_sge):
    c1, c2, c3, c4 = st.columns(4)
    if not df_comex.empty:
        cv = df_comex["inventory_ton"].iloc[-1]
        c1y = df_comex["inventory_ton"].iloc[-252] if len(df_comex) > 252 else df_comex["inventory_ton"].iloc[0]
        cc = (cv - c1y) / c1y * 100 if c1y else 0
        with c1:
            st.metric("COMEX库存（吨）", f"{cv:,.1f}", delta=f"{cc:+.1f}%")
    if not df_etf.empty:
        ev = df_etf["total_holdings"].iloc[-1]
        e1y = df_etf["total_holdings"].iloc[-252] if len(df_etf) > 252 else df_etf["total_holdings"].iloc[0]
        ec = (ev - e1y) / e1y * 100 if e1y else 0
        with c2:
            st.metric("全球ETF持仓（吨）", f"{ev:,.1f}", delta=f"{ec:+.1f}%")
    if not df_etf.empty and len(df_etf) >= 30:
        n30 = df_etf["change"].iloc[-30:].sum()
        with c3:
            st.metric("ETF近30日净流入", f"{n30:+,.1f} 吨")
    if not df_sge.empty:
        sp = df_sge["close"].iloc[-1]
        with c4:
            st.metric("上海金基准价", f"¥{sp:,.2f}/g")


def _render_comex_inventory_trend(df):
    if df.empty:
        st.info("暂无COMEX库存数据"); return
    recent = df[df["date"] >= df["date"].max() - pd.DateOffset(years=2)].copy()
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=recent["date"], y=recent["inventory_ton"], mode="lines",
        name="COMEX黄金库存（吨）", fill="tozeroy", fillcolor="rgba(168,85,247,0.1)",
        line=dict(color="#a855f7", width=2), hovertemplate="%{x|%Y-%m-%d}<br>库存: %{y:,.1f} 吨<extra></extra>"))
    fig.update_layout(title="COMEX黄金库存趋势（近2年）", height=350, template="plotly_dark",
        margin=dict(l=50, r=50, t=40, b=30), xaxis=dict(rangeslider=dict(visible=False)))
    st.plotly_chart(fig, use_container_width=True)


def _render_etf_monthly_flow(df):
    if df.empty:
        st.info("暂无ETF持仓数据"); return
    df = df.copy()
    df["month"] = df["date"].dt.to_period("M")
    monthly = df.groupby("month")["change"].sum().reset_index()
    monthly["month_str"] = monthly["month"].astype(str)
    recent = monthly.tail(24)
    colors = ["#22c55e" if v >= 0 else "#ef4444" for v in recent["change"]]
    fig = go.Figure()
    fig.add_trace(go.Bar(x=recent["month_str"], y=recent["change"], marker_color=colors, opacity=0.8,
        hovertemplate="%{x}<br>净流入: %{y:+.1f} 吨<extra></extra>"))
    fig.update_layout(title="全球黄金ETF月度净流入（吨）", height=350, template="plotly_dark",
        margin=dict(l=50, r=50, t=40, b=30), xaxis=dict(tickangle=-45))
    st.plotly_chart(fig, use_container_width=True)


def _render_etf_vs_price(df_etf, df_sge):
    if df_etf.empty or df_sge.empty:
        return
    de = df_etf.copy()
    de["month"] = de["date"].dt.to_period("M").astype(str)
    em = de.groupby("month")["change"].sum().reset_index()
    em.columns = ["month", "etf_net_flow"]
    ds = df_sge.copy()
    ds["month"] = pd.to_datetime(ds["date"]).dt.to_period("M").astype(str)
    sm = ds.groupby("month")["close"].mean().reset_index()
    sm.columns = ["month", "sge_price"]
    dm = em.merge(sm, on="month", how="inner").tail(24)
    if dm.empty:
        return
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    colors = ["#22c55e" if v >= 0 else "#ef4444" for v in dm["etf_net_flow"]]
    fig.add_trace(go.Bar(x=dm["month"], y=dm["etf_net_flow"], name="ETF月度净流入（吨）",
        marker_color=colors, opacity=0.7, hovertemplate="%{x}<br>净流入: %{y:+.1f} 吨<extra></extra>"),
        secondary_y=False)
    fig.add_trace(go.Scatter(x=dm["month"], y=dm["sge_price"], mode="lines+markers",
        name="上海金均价（¥/g）", line=dict(color="#facc15", width=2), marker=dict(size=5),
        hovertemplate="%{x}<br>均价: ¥%{y:.2f}<extra></extra>"), secondary_y=True)
    fig.update_layout(title="ETF资金流向 vs 上海金价格", height=350, template="plotly_dark",
        margin=dict(l=50, r=50, t=40, b=30),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        xaxis=dict(tickangle=-45))
    fig.update_yaxes(title_text="净流入（吨）", secondary_y=False, side="left")
    fig.update_yaxes(title_text="¥/g", secondary_y=True, side="right")
    st.plotly_chart(fig, use_container_width=True)


def render_supply_demand():
    st.markdown("#### 供需平衡分析")
    with st.spinner("加载COMEX库存数据..."):
        df_comex = fetch_comex_inventory()
    with st.spinner("加载全球ETF持仓数据..."):
        df_etf = fetch_global_etf_holdings()
    with st.spinner("加载上海金基准价..."):
        df_sge = fetch_sge_benchmark()
    _render_supply_demand_cards(df_comex, df_etf, df_sge)
    st.markdown("---")
    cl, cr = st.columns(2)
    with cl:
        _render_comex_inventory_trend(df_comex)
    with cr:
        _render_etf_monthly_flow(df_etf)
    _render_etf_vs_price(df_etf, df_sge)
