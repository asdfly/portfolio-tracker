"""
黄金定价因子相关性分析模块
"""

import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np

from tabs.gold_components.gold_utils import (
    fetch_sge_hist, fetch_bond_yields,
    DARK_BG, DARK_FONT_COLOR, GRID_COLOR,
)


@st.cache_data(ttl=3600)
def _fetch_factor_gold(n_days):
    """获取金价数据"""
    gold_df = fetch_sge_hist("Au99.99")
    if gold_df is None or gold_df.empty:
        return None
    cutoff = pd.Timestamp.now() - pd.Timedelta(days=n_days)
    gold_df = gold_df[gold_df["date"] >= cutoff].copy()
    if gold_df.empty:
        return None
    gold_df["date"] = pd.to_datetime(gold_df["date"])
    return gold_df[["date", "close"]].dropna().rename(columns={"close": "gold_price"})


@st.cache_data(ttl=3600)
def _fetch_factor_bonds():
    """获取中美国债收益率"""
    bond_df = fetch_bond_yields()
    if bond_df is None or bond_df.empty:
        return None
    bond_df["date"] = pd.to_datetime(bond_df["date"])
    bond_daily = bond_df[["date", "cn_10y", "us_10y"]].dropna().set_index("date")
    bond_daily["spread"] = bond_daily["cn_10y"] - bond_daily["us_10y"]
    return bond_daily


@st.cache_data(ttl=3600)
def _load_all_factors(n_days):
    """并发加载所有因子数据并对齐"""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    # 并发获取金价和国债
    gold_daily = _fetch_factor_gold(n_days)
    if gold_daily is None:
        return None
    gold_daily = gold_daily.set_index("date")

    bond_daily = None
    with ThreadPoolExecutor(max_workers=2) as pool:
        future_bond = pool.submit(_fetch_factor_bonds)
        bond_daily = future_bond.result()

    merged = gold_daily.copy()
    if bond_daily is not None:
        merged = merged.join(bond_daily, how="outer")
    merged = merged.sort_index().dropna(subset=["gold_price"])

    return {"daily": merged}


def render_correlation():
    """渲染定价因子相关性分析子Tab"""
    period = st.selectbox("分析周期", ["近1年", "近2年", "近3年"], key="corr_period")
    period_map = {"近1年": 365, "近2年": 730, "近3年": 1095}
    n_days = period_map.get(period, 365)

    with st.spinner("加载因子数据..."):
        data = _load_all_factors(n_days)

    if data is None:
        st.warning("金价数据不可用")
        return

    daily = data["daily"]

    # 相关系数计算
    corr_cols = [c for c in ["gold_price", "usdcny", "cn_10y", "us_10y", "spread"] if c in daily.columns]
    if len(corr_cols) < 2:
        st.warning("因子数据不足，无法计算相关性")
        return

    corr_df = daily[corr_cols].dropna()
    corr_matrix = corr_df.corr()

    # 中文标签
    label_map = {
        "gold_price": "Au99.99金价",
        "usdcny": "美元/离岸人民币",
        "cn_10y": "中国10Y国债",
        "us_10y": "美国10Y国债",
        "spread": "中美利差",
    }
    labels = [label_map.get(c, c) for c in corr_matrix.columns]

    # === 热力图 ===
    fig_heat = go.Figure(go.Heatmap(
        z=corr_matrix.values,
        x=labels, y=labels,
        text=corr_matrix.values.round(3),
        texttemplate="%{text}",
        textfont=dict(size=12),
        colorscale="RdBu_r",
        zmin=-1, zmax=1,
        colorbar=dict(title="Pearson相关系数"),
    ))
    fig_heat.update_layout(
        title=dict(text="定价因子相关系数矩阵", font=dict(size=14)),
        height=450,
        plot_bgcolor=DARK_BG, paper_bgcolor=DARK_BG,
        font=dict(color=DARK_FONT_COLOR),
        margin=dict(l=120, r=30, t=50, b=30),
        xaxis=dict(showgrid=False),
        yaxis=dict(showgrid=False),
    )
    st.plotly_chart(fig_heat, use_container_width=True)

    # === 关键相关性摘要卡片 ===
    if "usdcny" in corr_matrix.columns:
        r_usd = corr_matrix.loc["gold_price", "usdcny"]
        usd_desc = "负相关" if r_usd < 0 else "正相关"
        usd_str = f"美元走强 → 人民币金价{'下跌' if r_usd < 0 else '上涨'}（r={r_usd:.3f}）"
    else:
        usd_str = "无USD/CNY数据"

    if "spread" in corr_matrix.columns:
        r_spread = corr_matrix.loc["gold_price", "spread"]
        spread_str = f"中美利差与金价（r={r_spread:.3f}）"
    else:
        spread_str = "无中美利差数据"

    if "us_10y" in corr_matrix.columns:
        r_us10y = corr_matrix.loc["gold_price", "us_10y"]
        us10y_str = f"美国10Y国债收益率（r={r_us10y:.3f}）"
    else:
        us10y_str = "无美国国债数据"

    st.markdown(
        f'<div style="display:flex;gap:10px;margin:10px 0;">'
        f'<div style="flex:1;background:#252540;padding:10px;border-radius:6px;">'
        f'<div style="font-size:11px;color:#999;">USD/CNY</div>'
        f'<div style="font-size:13px;">{usd_str}</div></div>'
        f'<div style="flex:1;background:#252540;padding:10px;border-radius:6px;">'
        f'<div style="font-size:11px;color:#999;">中美利差</div>'
        f'<div style="font-size:13px;">{spread_str}</div></div>'
        f'<div style="flex:1;background:#252540;padding:10px;border-radius:6px;">'
        f'<div style="font-size:11px;color:#999;">美国10Y</div>'
        f'<div style="font-size:13px;">{us10y_str}</div></div>'
        f'</div>',
        unsafe_allow_html=True,
    )

    # === 双轴趋势对比图 ===
    factor_choice = st.selectbox(
        "叠加因子", ["美元/离岸人民币", "中美利差", "美国10Y国债", "中国10Y国债"],
        key="corr_factor",
    )
    factor_col_map = {
        "美元/离岸人民币": "usdcny",
        "中美利差": "spread",
        "美国10Y国债": "us_10y",
        "中国10Y国债": "cn_10y",
    }
    factor_col = factor_col_map.get(factor_choice)
    if factor_col and factor_col in daily.columns:
        pair = daily[["gold_price", factor_col]].dropna()
        if not pair.empty:
            fig2 = make_subplots(specs=[[{"secondary_y": True}]])
            fig2.add_trace(go.Scatter(
                x=pair.index, y=pair["gold_price"], mode="lines",
                name="Au99.99", line=dict(color="#FFD700", width=2),
            ), secondary_y=False)
            fig2.add_trace(go.Scatter(
                x=pair.index, y=pair[factor_col], mode="lines",
                name=factor_choice, line=dict(color="#42A5F5", width=1.5),
            ), secondary_y=True)
            fig2.update_layout(
                title=dict(text=f"金价 vs {factor_choice}", font=dict(size=14)),
                height=400,
                plot_bgcolor=DARK_BG, paper_bgcolor=DARK_BG,
                font=dict(color=DARK_FONT_COLOR),
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
                margin=dict(l=50, r=60, t=40, b=30),
                xaxis=dict(gridcolor=GRID_COLOR),
            )
            fig2.update_yaxes(gridcolor=GRID_COLOR, title_text="Au99.99", secondary_y=False)
            fig2.update_yaxes(gridcolor=GRID_COLOR, title_text=factor_choice, secondary_y=True)
            st.plotly_chart(fig2, use_container_width=True)

    # === 散点图 ===
    if factor_col and factor_col in daily.columns:
        scatter_df = daily[["gold_price", factor_col]].dropna()
        if len(scatter_df) > 20:
            x_vals = scatter_df[factor_col].values
            y_vals = scatter_df["gold_price"].values
            z = np.polyfit(x_vals, y_vals, 1)
            p = np.poly1d(z)
            x_fit = np.linspace(x_vals.min(), x_vals.max(), 100)
            y_fit = p(x_fit)
            r_scatter = np.corrcoef(x_vals, y_vals)[0, 1]

            fig3 = go.Figure()
            fig3.add_trace(go.Scatter(
                x=x_vals, y=y_vals, mode="markers",
                name="数据点", marker=dict(color="#42A5F5", size=4, opacity=0.5),
            ))
            fig3.add_trace(go.Scatter(
                x=x_fit, y=y_fit, mode="lines",
                name=f"拟合线 (r={r_scatter:.3f})",
                line=dict(color="#FF7043", width=2, dash="dash"),
            ))
            fig3.update_layout(
                title=dict(text=f"金价 vs {factor_choice} 散点分布", font=dict(size=14)),
                height=380,
                plot_bgcolor=DARK_BG, paper_bgcolor=DARK_BG,
                font=dict(color=DARK_FONT_COLOR),
                xaxis=dict(title=factor_choice, gridcolor=GRID_COLOR),
                yaxis=dict(title="Au99.99", gridcolor=GRID_COLOR),
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
                margin=dict(l=50, r=30, t=40, b=30),
            )
            st.plotly_chart(fig3, use_container_width=True)
