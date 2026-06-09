"""Tab3 归因分析子模块 — Brinson 归因、多因子归因"""

from components.ui import render_chart
import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from src.utils.chart_utils import downsample
from config.settings import CHART_DAYS, DOWNSAMPLE_MAX_POINTS, ETF_CATEGORIES
from src.utils.database import get_db_connection
from data_loader import load_positions, compute_return_attribution
def _render_brinson_attribution(show_days, selected_date):
    """Extracted from render_tab3."""
    # ===== P2: 收益归因分析（Brinson模型） =====
    st.markdown("---")
    st.markdown(
        '<div class="tip-title" style="">收益归因分析（Brinson 模型）<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">使用Brinson归因模型将组合超额收益分解为「配置效应」（超配/低配行业的贡献）和「选股效应」（行业内个股选择的贡献），帮助判断收益来源。</span></div>',
        unsafe_allow_html=True,
    )


    attr_result = compute_return_attribution(days=min(show_days, 500), end_date=selected_date)
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
        render_chart(fig_wf)

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




def _render_multi_factor_attribution(positions):
    """Extracted from render_tab3."""
    # ===== P2b: 多因子归因分析 =====
    st.markdown("---")
    st.markdown(
        '<div class="tip-title" style="">多因子归因分析<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">基于 A 股公开指数构造市场/规模/价值/动量/质量五因子模型，量化各因子对组合收益的贡献。</span></div>',
        unsafe_allow_html=True,
    )


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
                render_chart(fig_beta)

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
                        render_chart(fig_pie)

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


