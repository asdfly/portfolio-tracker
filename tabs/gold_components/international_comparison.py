"""国际金价对比分析（Phase 4.3）

数据源：
- 上海金基准价（spot_golden_benchmark_sge）：人民币计价，日频
- 全球黄金ETF总价值（macro_cons_gold）：美元计价，日频，作为国际金价代理
- 外汇投机情绪（macro_fx_sentiment）：XAUUSD + USDX 多空比，可选加载
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from .gold_utils import fetch_sge_benchmark, fetch_global_etf_holdings


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_fx_sentiment_daily(start_date="20250601", end_date=""):
    """获取金十外汇投机情绪日频数据（XAUUSD + USDX）

    macro_fx_sentiment 只支持单日查询，逐日采集后取每日均值。
    """
    import akshare as ak
    if not end_date:
        end_date = pd.Timestamp.now().strftime("%Y%m%d")
    start_dt = pd.Timestamp(start_date)
    end_dt = pd.Timestamp(end_date)
    dates = pd.date_range(start=start_dt, end=end_dt, freq="B")
    records = []
    for d in dates:
        ds = d.strftime("%Y%m%d")
        try:
            df = ak.macro_fx_sentiment(start_date=ds, end_date=ds)
            if df is not None and not df.empty:
                row = {"date": d}
                if "XAUUSD" in df.columns:
                    row["xauusd_sentiment"] = df["XAUUSD"].mean()
                if "USDX" in df.columns:
                    row["usdx_sentiment"] = df["USDX"].mean()
                records.append(row)
        except Exception:
            continue
    if not records:
        return pd.DataFrame()
    return pd.DataFrame(records)


def _render_intl_cards(df_etf, df_sge, df_sentiment):
    """顶部指标卡片"""
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        if df_etf is not None and not df_etf.empty:
            tv = df_etf["total_value"].iloc[-1]
            tv_prev = df_etf["total_value"].iloc[-2] if len(df_etf) > 1 else None
            delta = None
            if tv_prev and tv_prev > 0:
                delta = f"{(tv - tv_prev) / tv_prev * 100:+.2f}%"
            st.metric("ETF总价值（亿美元）", f"{tv:,.1f}", delta=delta)
        else:
            st.metric("ETF总价值", "N/A")
    with c2:
        if df_etf is not None and not df_etf.empty:
            th = df_etf["total_holdings"].iloc[-1]
            th_prev = df_etf["total_holdings"].iloc[-2] if len(df_etf) > 1 else None
            delta = None
            if th_prev and th_prev > 0:
                delta = f"{(th - th_prev):+.1f} 吨"
            st.metric("全球ETF持仓（吨）", f"{th:,.1f}", delta=delta)
        else:
            st.metric("全球ETF持仓", "N/A")
    with c3:
        if df_sge is not None and not df_sge.empty:
            lp = df_sge["close"].iloc[-1]
            st.metric("上海金基准价", f"¥{lp:,.2f}/g")
        else:
            st.metric("上海金基准价", "N/A")
    with c4:
        if df_sentiment is not None and not df_sentiment.empty and "xauusd_sentiment" in df_sentiment.columns:
            latest = df_sentiment["xauusd_sentiment"].iloc[-1]
            direction = "偏多" if latest > 55 else ("偏空" if latest < 45 else "中性")
            st.metric("XAUUSD情绪指数", f"{latest:.1f}", delta=direction)
        else:
            st.metric("XAUUSD情绪指数", "N/A")


def _render_etf_holdings_trend(df_etf):
    """全球黄金ETF持仓趋势"""
    if df_etf is None or df_etf.empty:
        st.info("暂无ETF持仓数据")
        return
    recent = df_etf[df_etf["date"] >= df_etf["date"].max() - pd.DateOffset(years=2)].copy()
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Scatter(
        x=recent["date"], y=recent["total_holdings"],
        mode="lines", name="持仓量（吨）",
        fill="tozeroy", fillcolor="rgba(88,166,255,0.1)",
        line=dict(color="#58a6ff", width=2),
        hovertemplate="%{x|%Y-%m-%d}<br>持仓: %{y:,.0f} 吨<extra></extra>",
    ), secondary_y=False)
    colors = ["#22c55e" if v >= 0 else "#ef4444" for v in recent["change"].fillna(0)]
    fig.add_trace(go.Bar(
        x=recent["date"], y=recent["change"],
        name="日增减（吨）", marker_color=colors, opacity=0.5,
        hovertemplate="%{x|%Y-%m-%d}<br>增减: %{y:+.1f} 吨<extra></extra>",
    ), secondary_y=True)
    fig.update_layout(
        title="全球黄金ETF持仓趋势（近2年）",
        height=350, template="plotly_dark",
        margin=dict(l=50, r=50, t=40, b=30),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        xaxis=dict(rangeslider=dict(visible=False)),
    )
    fig.update_yaxes(title_text="持仓（吨）", secondary_y=False, side="left")
    fig.update_yaxes(title_text="日增减（吨）", secondary_y=True, side="right")
    st.plotly_chart(fig, width='stretch')

def _render_etf_value_vs_sge_price(df_etf, df_sge):
    """ETF总价值（国际金价代理） vs 上海金基准价"""
    if df_etf is None or df_etf.empty or df_sge is None or df_sge.empty:
        st.info("国际金价或上海金数据不足，无法对比")
        return
    de = df_etf.copy()
    de["month"] = de["date"].dt.to_period("M")
    etf_m = de.groupby("month").agg(
        etf_value=("total_value", "mean"),
        etf_holdings=("total_holdings", "mean"),
    ).reset_index()
    etf_m["month_str"] = etf_m["month"].astype(str)
    ds = df_sge.copy()
    ds["date"] = pd.to_datetime(ds["date"], errors="coerce")
    ds["month"] = ds["date"].dt.to_period("M")
    sge_m = ds.groupby("month")["close"].mean().reset_index()
    sge_m.columns = ["month", "sge_price"]
    sge_m["month_str"] = sge_m["month"].astype(str)
    merged = etf_m.merge(sge_m, on="month_str", how="inner").tail(24)
    if merged is None or merged.empty:
        st.info("无重叠月度数据")
        return
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Scatter(
        x=merged["month_str"], y=merged["etf_value"],
        mode="lines+markers", name="ETF总价值（亿美元）",
        line=dict(color="#58a6ff", width=2), marker=dict(size=4),
        hovertemplate="%{x}<br>ETF价值: %{y:,.0f} 亿美元<extra></extra>",
    ), secondary_y=False)
    fig.add_trace(go.Scatter(
        x=merged["month_str"], y=merged["sge_price"],
        mode="lines+markers", name="上海金均价（¥/g）",
        line=dict(color="#facc15", width=2), marker=dict(size=4),
        hovertemplate="%{x}<br>上海金: ¥%{y:.2f}/g<extra></extra>",
    ), secondary_y=True)
    fig.update_layout(
        title="国际金价（ETF总价值） vs 上海金基准价",
        height=380, template="plotly_dark",
        margin=dict(l=50, r=50, t=40, b=30),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        xaxis=dict(tickangle=-45),
    )
    fig.update_yaxes(title_text="亿美元", secondary_y=False, side="left")
    fig.update_yaxes(title_text="¥/g", secondary_y=True, side="right")
    st.plotly_chart(fig, width='stretch')


def _render_premium_discount(df_etf, df_sge):
    """上海金 vs 国际金价溢价/折价分析

    用ETF总价值/总持仓推算国际金价（美元/盎司），再转为美元/克，
    反推隐含汇率，用中位数汇率计算溢价。
    """
    if df_etf is None or df_etf.empty or df_sge is None or df_sge.empty:
        return
    de = df_etf.copy()
    de = de[(de["total_value"] > 0) & (de["total_holdings"] > 0)].copy()
    de["est_gold_usd_oz"] = de["total_value"] * 1e8 / (de["total_holdings"] * 32150.72)  # 亿美元->美元
    de["est_gold_usd_g"] = de["est_gold_usd_oz"] / 31.1035
    ds = df_sge.copy()
    ds["date"] = pd.to_datetime(ds["date"], errors="coerce")
    ds = ds.dropna(subset=["date"]).sort_values("date")
    merged = de[["date", "est_gold_usd_g", "total_value"]].merge(
        ds[["date", "close"]], on="date", how="inner"
    ).dropna().tail(120)
    if merged is None or merged.empty:
        st.info("数据不足，无法计算溢价")
        return
    merged["est_cnyusd"] = merged["close"] / merged["est_gold_usd_g"]
    median_rate = merged["est_cnyusd"].median()
    merged["intl_cny"] = merged["est_gold_usd_g"] * median_rate
    merged["premium"] = merged["close"] - merged["intl_cny"]
    merged["premium_pct"] = merged["premium"] / merged["intl_cny"] * 100
    latest = merged.iloc[-1]
    avg_prem = merged["premium_pct"].mean()
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    colors = ["#22c55e" if v >= 0 else "#ef4444" for v in merged["premium_pct"]]
    fig.add_trace(go.Bar(
        x=merged["date"], y=merged["premium_pct"],
        name="溢价率%", marker_color=colors, opacity=0.7,
        hovertemplate="%{x|%Y-%m-%d}<br>溢价: %{y:+.2f}%<extra></extra>",
    ), secondary_y=False)
    fig.add_trace(go.Scatter(
        x=merged["date"], y=merged["close"],
        mode="lines", name="上海金（¥/g）",
        line=dict(color="#facc15", width=1.5),
        hovertemplate="%{x|%Y-%m-%d}<br>\u00a5%{y:.2f}<extra></extra>",
    ), secondary_y=True)
    fig.add_hline(y=0, line_dash="dash", line_color="#8b949e", secondary_y=False)
    fig.add_hline(y=avg_prem, line_dash="dot", line_color="#58a6ff",
                  annotation_text=f"均值 {avg_prem:+.2f}%", secondary_y=False)
    fig.update_layout(
        title=f"上海金 vs 国际金价溢价分析（隐含汇率 {median_rate:.2f}）",
        height=350, template="plotly_dark",
        margin=dict(l=50, r=50, t=40, b=30),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        xaxis=dict(rangeslider=dict(visible=False)),
    )
    fig.update_yaxes(title_text="溢价率%", secondary_y=False, side="left")
    fig.update_yaxes(title_text="\u00a5/g", secondary_y=True, side="right")
    st.plotly_chart(fig, width='stretch')
    c1, c2, c3 = st.columns(3)
    c1.metric("当前溢价率", f"{latest['premium_pct']:+.2f}%")
    c2.metric("平均溢价率", f"{avg_prem:+.2f}%")
    pct_rank = (merged["premium_pct"] < latest["premium_pct"]).sum() / len(merged) * 100
    c3.metric("溢价分位数", f"{pct_rank:.0f}%", "历史百分位")


def _render_sentiment_analysis(df_sentiment):
    """XAUUSD / USDX 投机情绪分析"""
    if df_sentiment is None or df_sentiment.empty:
        st.info("暂无外汇投机情绪数据")
        return
    if "xauusd_sentiment" not in df_sentiment.columns:
        st.info("投机情绪数据缺少XAUUSD字段")
        return
    recent = df_sentiment.tail(90).copy()
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=recent["date"], y=recent["xauusd_sentiment"],
        mode="lines+markers", name="XAUUSD 多空比",
        line=dict(color="#facc15", width=2), marker=dict(size=3),
        hovertemplate="%{x|%Y-%m-%d}<br>XAUUSD: %{y:.1f}<extra></extra>",
    ))
    fig.add_hline(y=50, line_dash="dash", line_color="#8b949e", annotation_text="中性 50")
    fig.add_hline(y=60, line_dash="dot", line_color="#ef4444", annotation_text="偏多 60")
    fig.add_hline(y=40, line_dash="dot", line_color="#22c55e", annotation_text="偏空 40")
    fig.update_layout(
        title="XAUUSD 投机情绪（多空比，<40偏空 >60偏多）",
        height=300, template="plotly_dark",
        margin=dict(l=50, r=50, t=40, b=30),
        xaxis=dict(rangeslider=dict(visible=False)),
        yaxis=dict(range=[30, 70]),
    )
    st.plotly_chart(fig, width='stretch')
    if "usdx_sentiment" in df_sentiment.columns:
        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(
            x=recent["date"], y=recent["usdx_sentiment"],
            mode="lines+markers", name="USDX 多空比",
            line=dict(color="#a78bfa", width=2), marker=dict(size=3),
            hovertemplate="%{x|%Y-%m-%d}<br>USDX: %{y:.1f}<extra></extra>",
        ))
        fig2.add_hline(y=50, line_dash="dash", line_color="#8b949e", annotation_text="中性 50")
        fig2.update_layout(
            title="美元指数（USDX）投机情绪",
            height=250, template="plotly_dark",
            margin=dict(l=50, r=50, t=40, b=30),
            xaxis=dict(rangeslider=dict(visible=False)),
            yaxis=dict(range=[30, 70]),
        )
        st.plotly_chart(fig2, width='stretch')



def _render_sentiment_analysis(df_sentiment):
    """XAUUSD / USDX 投机情绪分析"""
    if df_sentiment is None or df_sentiment.empty:
        st.info("暂无外汇投机情绪数据")
        return
    if "xauusd_sentiment" not in df_sentiment.columns:
        st.info("投机情绪数据缺少XAUUSD字段")
        return
    recent = df_sentiment.tail(90).copy()
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=recent["date"], y=recent["xauusd_sentiment"],
        mode="lines+markers", name="XAUUSD 多空比",
        line=dict(color="#facc15", width=2), marker=dict(size=3),
        hovertemplate="%{x|%Y-%m-%d}<br>XAUUSD: %{y:.1f}<extra></extra>",
    ))
    fig.add_hline(y=50, line_dash="dash", line_color="#8b949e", annotation_text="中性 50")
    fig.add_hline(y=60, line_dash="dot", line_color="#ef4444", annotation_text="偏多 60")
    fig.add_hline(y=40, line_dash="dot", line_color="#22c55e", annotation_text="偏空 40")
    fig.update_layout(
        title="XAUUSD 投机情绪（多空比）",
        height=300, template="plotly_dark",
        margin=dict(l=50, r=50, t=40, b=30),
        xaxis=dict(rangeslider=dict(visible=False)),
        yaxis=dict(range=[30, 70]),
    )
    st.plotly_chart(fig, width='stretch')
    if "usdx_sentiment" in df_sentiment.columns:
        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(
            x=recent["date"], y=recent["usdx_sentiment"],
            mode="lines+markers", name="USDX 多空比",
            line=dict(color="#a78bfa", width=2), marker=dict(size=3),
            hovertemplate="%{x|%Y-%m-%d}<br>USDX: %{y:.1f}<extra></extra>",
        ))
        fig2.add_hline(y=50, line_dash="dash", line_color="#8b949e", annotation_text="中性 50")
        fig2.update_layout(
            title="美元指数（USDX）投机情绪",
            height=250, template="plotly_dark",
            margin=dict(l=50, r=50, t=40, b=30),
            xaxis=dict(rangeslider=dict(visible=False)),
            yaxis=dict(range=[30, 70]),
        )
        st.plotly_chart(fig2, width='stretch')


def render_international_comparison():
    """国际金价对比分析主入口"""

    st.markdown(
        '<div class="tip-title" style="font-size:14px;border-bottom:none;padding:5px 0;">国际金价对比<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">对比不同市场的黄金价格（伦敦金、纽约金、上海金），分析价差和汇率影响。</span></div>',
        unsafe_allow_html=True,
    )
    st.markdown("#### 国际金价对比")
    # 优先使用预加载数据
    from tabs.gold_components.gold_preloader import get_preloaded
    df_etf = get_preloaded("global_etf_holdings")
    df_sge = get_preloaded("sge_benchmark")
    # 缺失的数据源按需回退
    if df_etf is None:
        with st.spinner("加载全球ETF持仓数据..."):
            df_etf = fetch_global_etf_holdings()
    if df_sge is None:
        with st.spinner("加载上海金基准价..."):
            df_sge = fetch_sge_benchmark()
    load_sentiment = st.checkbox("加载投机情绪数据（较慢）", value=False, key="intl_load_sentiment")
    df_sentiment = pd.DataFrame()
    if load_sentiment:
        with st.spinner("正在采集外汇投机情绪数据（约需30秒）..."):
            df_sentiment = fetch_fx_sentiment_daily(start_date="20250601")
    _render_intl_cards(df_etf, df_sge, df_sentiment)
    st.markdown("---")
    cl, cr = st.columns(2)
    with cl:
        _render_etf_holdings_trend(df_etf)
    with cr:
        _render_premium_discount(df_etf, df_sge)
    _render_etf_value_vs_sge_price(df_etf, df_sge)
    if df_sentiment is not None and not df_sentiment.empty:
        _render_sentiment_analysis(df_sentiment)
    with st.expander("数据来源与计算说明"):
        st.markdown(
            "**国际金价推算方法：**\n"
            "- 数据源：世界黄金协会全球黄金ETF持仓统计（akshare: macro_cons_gold）\n"
            "- 推算公式：金价($/oz) = ETF总价值(亿美元) * 1亿 / (ETF总持仓(吨) * 32150.72)\n"
            "- 转换为人民币：国际金价(元/g) = 金价($/oz) / 31.1035 * 隐含汇率\n\n"
            "**溢价定义：**\n"
            "- 隐含汇率 = 上海金基准价(元/g) / 推算国际金价($/g)\n"
            "- 溢价 = 上海金基准价 - 推算国际金价 * 中位数隐含汇率\n\n"
            "**局限说明：**\n"
            "- 国际金价为推算值，非直接行情，可能与LBMA金价存在偏差\n"
            "- 投机情绪数据来源：金十数据 macro_fx_sentiment（需逐日采集，较慢）"
        )
