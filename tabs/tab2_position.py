from components.ui import render_chart, render_empty_state
import sqlite3
"""
Tab2: 持仓分布
"""

import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from config.settings import CHART_DAYS, DATABASE_PATH, ETF_CATEGORIES, SECTOR_COLORS
from tabs._helpers import _render_etf_detail_panel
from src.utils.database import get_db_connection
from data_loader import load_positions, load_summary, load_etf_fundamental, load_etf_industry_alloc, load_etf_top_holdings


def load_correlation_matrix(days=CHART_DAYS["default"], end_date=None):
    """load_correlation_matrix（委托到 data_loader）"""
    import data_loader as _dl
    return _dl.load_correlation_matrix(days=days, end_date=end_date)

def load_sector_weights(days=CHART_DAYS["default"], end_date=None):
    """load_sector_weights（委托到 data_loader）"""
    import data_loader as _dl
    return _dl.load_sector_weights(days=days, end_date=end_date)

def _render_etf_filter(positions, summary, technical, selected_date):
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
            render_chart(fig_pie)

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
            # F10 扩展列：折价率和资金流向
            if "discount_rate" in positions.columns:
                display_df["折价率"] = positions["discount_rate"].apply(
                    lambda x: f"{x:+.2f}%" if pd.notna(x) else "--")
            if "main_net_inflow" in positions.columns:
                display_df["主力净流入"] = positions["main_net_inflow"].apply(
                    lambda x: (f"¥{x/1e8:+.2f}亿" if abs(x) >= 1e8 else f"¥{x/1e4:+.0f}万")
                    if pd.notna(x) else "--")
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
                        if "上涨" in str(trend):
                            parts.append('<span style="color:#22c55e;">↑</span>')
                        elif "下跌" in str(trend):
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
            for idx, (orig_idx, row_data) in enumerate(display_df.iterrows()):
                pos_row = positions.iloc[idx]
                pnl_c = "#22c55e" if pos_row["pnl"] >= 0 else "#ef4444"
                zebra = "background:#161b22;" if idx % 2 == 0 else ""
                html_rows.append(
                    f'<tr style="{zebra}">'
                    f'<td style="padding:5px 8px;color:#c9d1d9;border-bottom:1px solid #21262d;white-space:nowrap;">{row_data["名称"]}</td>'
                    f'<td style="padding:5px 8px;color:#8b949e;border-bottom:1px solid #21262d;">{row_data["代码"]}</td>'
                    f'<td style="padding:5px 8px;text-align:right;color:#c9d1d9;border-bottom:1px solid #21262d;">{row_data["持仓量"]}</td>'
                    f'<td style="padding:5px 8px;text-align:right;color:#c9d1d9;border-bottom:1px solid #21262d;">{row_data["成本价"]}</td>'
                    f'<td style="padding:5px 8px;text-align:right;color:#c9d1d9;border-bottom:1px solid #21262d;">{row_data["现价"]}</td>'
                    f'<td style="padding:5px 8px;text-align:right;color:#c9d1d9;border-bottom:1px solid #21262d;">{row_data["市值"]}</td>'
                    f'<td style="padding:5px 8px;text-align:right;color:{pnl_c};border-bottom:1px solid #21262d;">{row_data["盈亏"]}</td>'
                    f'<td style="padding:5px 8px;text-align:right;color:{pnl_c};border-bottom:1px solid #21262d;">{row_data["收益率%"]}</td>'
                    f'<td style="padding:5px 8px;text-align:right;color:#c9d1d9;border-bottom:1px solid #21262d;font-size:11px;">{row_data["折价率"]}</td>'
                    f'<td style="padding:5px 8px;text-align:right;color:#c9d1d9;border-bottom:1px solid #21262d;font-size:11px;">{row_data["主力净流入"]}</td>'
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
                f'<th style="padding:6px 8px;color:#8b949e;text-align:right;font-size:11px;">折价率</th>'
                f'<th style="padding:6px 8px;color:#8b949e;text-align:right;font-size:11px;">主力净流入</th>'
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
                    f'<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">{row["name"]}（{row["code"]}）详细分析<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">查看该ETF的价格走势、技术指标及持仓收益等详细分析信息。</span></div>',
                    unsafe_allow_html=True,
                )
                _render_etf_detail_panel(row, selected_date, summary.iloc[-1]["total_value"])
                _render_etf_f10_panel(str(row["code"]), row["name"])

def _render_sector_weights(positions, summary, selected_date):
    # ===== 行业权重堆叠面积图 =====
    st.markdown(
        '<div class="tip-title" style="">行业权重变化趋势<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">以堆叠面积图展示各行业ETF在组合中的权重占比随时间的变化，可观察仓位配置的调整趋势。</span></div>',
        unsafe_allow_html=True,
    )
    sector_weight_df, sector_colors = load_sector_weights(days=len(summary), end_date=selected_date)
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
        render_chart(fig_sector)

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

def _render_correlation_matrix(positions, selected_date):
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
        render_chart(fig_corr)
        st.caption(f"基于最近250个交易日的市值日收益率计算 | 数据截至 {selected_date}")
    else:
        st.info("持仓数据不足，暂无法计算相关性矩阵")

def _render_deep_analysis(positions, summary):
    if not positions.empty:
        st.markdown("---")
        st.markdown(
            '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">持仓集中度与风险贡献<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">基于HHI指数衡量持仓集中度，基于Beta和市值权重分解各ETF对组合风险的贡献度。</span></div>',
            unsafe_allow_html=True,
        )

        col_hhi, col_beta = st.columns(2)

        with col_hhi:
            # HHI指数计算
            total_mv = positions["market_value"].sum()
            if total_mv > 0:
                weights = (positions["market_value"] / total_mv)
                hhi = (weights ** 2).sum()
                hhi_max = 1.0  # 完全集中
                # 有效持仓数 = 1/HHI
                effective_n = 1 / hhi if hhi > 0 else len(positions)

                # HHI评级
                if hhi <= 0.15:
                    hhi_grade, hhi_color = "高度分散", "#22c55e"
                elif hhi <= 0.25:
                    hhi_grade, hhi_color = "适度集中", "#f59e0b"
                else:
                    hhi_grade, hhi_color = "高度集中", "#ef4444"

                st.metric("HHI指数", f"{hhi:.4f}", delta=f"{hhi_grade}")
                st.metric("有效持仓数", f"{effective_n:.1f}只", delta=f"共{len(positions)}只")

                # 个股权重分布条形图
                pos_sorted = positions.sort_values("market_value", ascending=True)
                fig_hhi = go.Figure(go.Bar(
                    y=pos_sorted["name"],
                    x=pos_sorted["market_value"] / total_mv * 100,
                    orientation="h",
                    marker_color="#58a6ff",
                    text=[f"{v:.1f}%" for v in pos_sorted["market_value"] / total_mv * 100],
                    textposition="outside",
                    textfont=dict(size=9, color="#c9d1d9"),
                ))
                fig_hhi.update_layout(
                    xaxis=dict(title="权重%", range=[0, max(pos_sorted["market_value"] / total_mv * 100) * 1.3],
                               gridcolor="#21262d", tickfont=dict(size=9, color="#8b949e")),
                    yaxis=dict(tickfont=dict(size=10, color="#c9d1d9")),
                    paper_bgcolor="#0d1117", plot_bgcolor="#0d1117",
                    height=max(200, 28 * len(pos_sorted)),
                    margin=dict(l=80, r=40, t=10, b=30),
                    showlegend=False,
                )
                render_chart(fig_hhi)

        with col_beta:
            # Beta贡献分析
            if "beta" in positions.columns and positions["beta"].notna().any():
                positions_b = positions.dropna(subset=["beta"]).copy()
                if not positions_b.empty and total_mv > 0:
                    positions_b["weight"] = positions_b["market_value"] / total_mv
                    positions_b["beta_contribution"] = positions_b["weight"] * positions_b["beta"]
                    portfolio_beta = positions_b["beta_contribution"].sum()

                    st.metric("组合加权Beta", f"{portfolio_beta:.3f}")

                    # Beta贡献条形图
                    beta_sorted = positions_b.sort_values("beta_contribution", ascending=True)
                    fig_beta = go.Figure(go.Bar(
                        y=beta_sorted["name"],
                        x=beta_sorted["beta_contribution"],
                        orientation="h",
                        marker_color=["#22c55e" if v <= 1 else "#f59e0b" if v <= 1.5 else "#ef4444"
                                      for v in beta_sorted["beta_contribution"]],
                        text=[f"{v:.3f}" for v in beta_sorted["beta_contribution"]],
                        textposition="outside",
                        textfont=dict(size=9, color="#c9d1d9"),
                    ))
                    fig_beta.update_layout(
                        xaxis=dict(title="Beta贡献(权重×Beta)", gridcolor="#21262d",
                                   tickfont=dict(size=9, color="#8b949e")),
                        yaxis=dict(tickfont=dict(size=10, color="#c9d1d9")),
                        paper_bgcolor="#0d1117", plot_bgcolor="#0d1117",
                        height=max(200, 28 * len(beta_sorted)),
                        margin=dict(l=80, r=40, t=10, b=30),
                        showlegend=False,
                    )
                    render_chart(fig_beta)
                else:
                    st.caption("暂无有效Beta数据")
            else:
                st.caption("暂无Beta数据（需技术分析模块计算）")

def _render_cumulative_pnl(positions):
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
        render_chart(fig_pnl)
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


def _render_etf_f10_panel(code, name):
    """渲染 ETF F10 基本面面板：实时行情指标、行业配置、重仓股、指数估值"""
    # 加载 F10 数据
    fund_df = load_etf_fundamental()
    industry_df = load_etf_industry_alloc(code)
    holdings_df = load_etf_top_holdings(code, top_n=10)

    # 获取该 ETF 的 fundamental 行
    fund_row = None
    if not fund_df.empty and "code" in fund_df.columns:
        match = fund_df[fund_df["code"] == code]
        if not match.empty:
            fund_row = match.iloc[0]

    st.markdown("---")
    st.markdown(
        '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">'
        f'{name} 基本面数据 (F10)'
        '<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span>'
        '<span class="tip-text" style="left: 4px; top: calc(100% + 10px);">'
        '展示该ETF的实时行情指标（折价率/资金流向/换手率）、行业配置、重仓股及追踪指数估值。'
        '</span></div>',
        unsafe_allow_html=True,
    )

    # ===== 第一行：实时行情指标卡片 =====
    if fund_row is not None:
        _render_f10_metrics(fund_row)
    else:
        st.info("暂无该 ETF 的基本面数据，请在每日分析时运行数据采集。")

    # ===== 第二行：行业配置 + 重仓股 =====
    col_ind, col_hol = st.columns([1, 1])

    with col_ind:
        _render_f10_industry_alloc(code, industry_df)

    with col_hol:
        _render_f10_top_holdings(code, holdings_df)

    # ===== 第三行：指数估值（仅宽基 ETF） =====
    if fund_row is not None:
        _render_f10_index_valuation(fund_row)

    # ===== 第四行：持仓交易历史 =====
    _render_trade_history_panel(code, name)


def _render_f10_metrics(fund_row):
    """渲染 F10 实时行情指标卡片"""
    cols = st.columns(6)
    metrics = []

    # IOPV
    iopv = fund_row.get("iopv")
    if pd.notna(iopv) and iopv != 0:
        metrics.append(("IOPV", f"{iopv:.4f}", "#c9d1d9"))
    else:
        metrics.append(("IOPV", "--", "#484f58"))

    # 折价率
    dr = fund_row.get("discount_rate")
    if pd.notna(dr):
        dr_color = "#22c55e" if dr < 0 else "#ef4444" if dr > 0 else "#c9d1d9"
        metrics.append(("折价率", f"{dr:+.2f}%", dr_color))
    else:
        metrics.append(("折价率", "--", "#484f58"))

    # 主力净流入
    inflow = fund_row.get("main_net_inflow")
    if pd.notna(inflow):
        inf_color = "#22c55e" if inflow > 0 else "#ef4444"
        if abs(inflow) >= 1e8:
            inf_str = f"{'+' if inflow > 0 else ''}{inflow/1e8:.2f}亿"
        elif abs(inflow) >= 1e4:
            inf_str = f"{'+' if inflow > 0 else ''}{inflow/1e4:.0f}万"
        else:
            inf_str = f"{'+' if inflow > 0 else ''}{inflow:,.0f}"
        metrics.append(("主力净流入", inf_str, inf_color))
    else:
        metrics.append(("主力净流入", "--", "#484f58"))

    # 换手率
    tr = fund_row.get("turnover_rate")
    if pd.notna(tr):
        metrics.append(("换手率", f"{tr:.2f}%", "#c9d1d9"))
    else:
        metrics.append(("换手率", "--", "#484f58"))

    # 量比
    vr = fund_row.get("volume_ratio")
    if pd.notna(vr):
        vr_color = "#22c55e" if vr > 1.5 else "#ef4444" if vr < 0.5 else "#c9d1d9"
        metrics.append(("量比", f"{vr:.2f}", vr_color))
    else:
        metrics.append(("量比", "--", "#484f58"))

    # 份额（亿份）
    shares = fund_row.get("shares")
    if pd.notna(shares) and shares > 0:
        shares_str = f"{shares/1e8:.2f}亿" if shares >= 1e8 else f"{shares/1e4:.0f}万"
        metrics.append(("份额", shares_str, "#c9d1d9"))
    else:
        metrics.append(("份额", "--", "#484f58"))

    for i, (label, value, color) in enumerate(metrics):
        with cols[i]:
            st.markdown(
                f'<div style="text-align:center;padding:8px 4px;">'
                f'<div style="font-size:10px;color:#8b949e;margin-bottom:4px;">{label}</div>'
                f'<div style="font-size:14px;font-weight:bold;color:{color};">{value}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )


def _render_f10_industry_alloc(code, industry_df):
    """渲染行业配置饼图"""
    st.markdown(
        '<div class="tip-title" style="font-size:14px;border-bottom:none;padding:5px 0;">'
        '行业配置<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span>'
        '<span class="tip-text" style="left: 4px; top: calc(100% + 10px);">'
        '基于最新持仓报告期的行业分布，展示该ETF在各行业的配置权重。'
        '</span></div>',
        unsafe_allow_html=True,
    )

    if industry_df is None or industry_df.empty:
        render_empty_state("暂无行业配置数据")
        return

    # 合并小行业（<1%）为"其他"
    df = industry_df.copy()
    small = df[df["weight_pct"] < 1]
    if not small.empty:
        df = df[df["weight_pct"] >= 1].copy()
        other_row = pd.DataFrame({
            "industry": ["其他"],
            "weight_pct": [small["weight_pct"].sum()],
            "market_value": [small["market_value"].sum()]
        })
        df = pd.concat([df, other_row], ignore_index=True)
    df = df.sort_values("weight_pct", ascending=True)

    fig = go.Figure(
        go.Pie(
            labels=df["industry"],
            values=df["weight_pct"],
            hole=0.35,
            textinfo="label+percent",
            textfont=dict(size=9),
            marker=dict(
                colors=[
                    "#58a6ff", "#22c55e", "#f59e0b", "#ef4444", "#a855f7",
                    "#06b6d4", "#f97316", "#ec4899", "#84cc16", "#6366f1",
                    "#14b8a6", "#e11d48", "#8b5cf6", "#0ea5e9", "#d946ef",
                ],
            ),
            hovertemplate="<b>%{label}</b><br>权重: %{value:.2f}%<extra></extra>",
        )
    )
    fig.update_layout(
        height=320,
        plot_bgcolor="#0d1117",
        paper_bgcolor="#0d1117",
        font=dict(color="#c9d1d9"),
        margin=dict(l=10, r=10, t=10, b=10),
        showlegend=False,
    )
    render_chart(fig)

    # 摘要行
    top3 = industry_df.nlargest(3, "weight_pct")
    top3_str = "、".join([f"{r['industry']} {r['weight_pct']:.1f}%" for _, r in top3.iterrows()])
    report_date = industry_df["report_date"].iloc[0] if "report_date" in industry_df.columns else ""
    st.caption(f"前三大: {top3_str} | 报告期: {report_date}")


def _render_f10_top_holdings(code, holdings_df):
    """渲染重仓股表格"""
    st.markdown(
        '<div class="tip-title" style="font-size:14px;border-bottom:none;padding:5px 0;">'
        '前十大重仓股<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span>'
        '<span class="tip-text" style="left: 4px; top: calc(100% + 10px);">'
        '展示该ETF最新的前十大重仓股票及其权重，反映基金的实际投资方向。'
        '</span></div>',
        unsafe_allow_html=True,
    )

    if holdings_df is None or holdings_df.empty:
        render_empty_state("暂无重仓股数据")
        return

    # HTML 表格
    html_rows = []
    for idx, (_, row) in enumerate(holdings_df.iterrows()):
        zebra = "background:#161b22;" if idx % 2 == 0 else ""
        wp = row.get("weight_pct", 0)
        mv = row.get("market_value", 0)
        wp_str = f"{wp:.2f}%"
        # 市值格式化
        if pd.notna(mv) and mv > 0:
            if mv >= 1e8:
                mv_str = f"¥{mv/1e8:.2f}亿"
            elif mv >= 1e4:
                mv_str = f"¥{mv/1e4:.0f}万"
            else:
                mv_str = f"¥{mv:,.0f}"
        else:
            mv_str = "--"

        html_rows.append(
            f'<tr style="{zebra}">'
            f'<td style="padding:4px 8px;color:#8b949e;border-bottom:1px solid #21262d;text-align:center;font-size:11px;">{idx+1}</td>'
            f'<td style="padding:4px 8px;color:#c9d1d9;border-bottom:1px solid #21262d;">{row.get("stock_name","")}</td>'
            f'<td style="padding:4px 8px;color:#8b949e;border-bottom:1px solid #21262d;text-align:center;">{row.get("stock_code","")}</td>'
            f'<td style="padding:4px 8px;color:#c9d1d9;border-bottom:1px solid #21262d;text-align:right;font-weight:bold;">{wp_str}</td>'
            f'<td style="padding:4px 8px;color:#c9d1d9;border-bottom:1px solid #21262d;text-align:right;">{mv_str}</td>'
            f'</tr>'
        )

    quarter = holdings_df["quarter"].iloc[0] if "quarter" in holdings_df.columns else ""
    st.markdown(
        f'<div style="overflow-x:auto;"><table style="width:100%;border-collapse:collapse;font-size:12px;">'
        f'<thead><tr style="background:#0d1117;">'
        f'<th style="padding:6px 8px;color:#8b949e;text-align:center;font-size:11px;width:30px;">#</th>'
        f'<th style="padding:6px 8px;color:#8b949e;text-align:left;font-size:11px;">股票名称</th>'
        f'<th style="padding:6px 8px;color:#8b949e;text-align:center;font-size:11px;">代码</th>'
        f'<th style="padding:6px 8px;color:#8b949e;text-align:right;font-size:11px;">权重</th>'
        f'<th style="padding:6px 8px;color:#8b949e;text-align:right;font-size:11px;">市值</th>'
        f'</tr></thead><tbody>{"".join(html_rows)}</tbody></table></div>',
        unsafe_allow_html=True,
    )
    st.caption(f"报告期: {quarter}")


def _render_f10_index_valuation(fund_row):
    """渲染追踪指数估值（仅宽基 ETF）"""
    index_code = fund_row.get("index_code")
    index_name = fund_row.get("index_name")
    pe1 = fund_row.get("pe1")
    pe2 = fund_row.get("pe2")
    dy1 = fund_row.get("div_yield1")
    dy2 = fund_row.get("div_yield2")

    if pd.isna(index_code) or not index_code:
        return  # 非宽基 ETF，无指数估值

    st.markdown("---")
    st.markdown(
        '<div class="tip-title" style="font-size:14px;border-bottom:none;padding:5px 0;">'
        f'追踪指数估值: {index_name}'
        '<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span>'
        '<span class="tip-text" style="left: 4px; top: calc(100% + 10px);">'
        '展示该ETF追踪指数的市盈率(PE-TTM/PE-PB)和股息率，用于判断指数整体估值水平。'
        '</span></div>',
        unsafe_allow_html=True,
    )

    cols = st.columns(4)
    items = []

    # PE-TTM
    if pd.notna(pe1) and pe1 > 0:
        pe1_color = "#ef4444" if pe1 > 20 else "#f59e0b" if pe1 > 14 else "#22c55e"
        items.append(("PE-TTM", f"{pe1:.2f}", pe1_color))
    else:
        items.append(("PE-TTM", "--", "#484f58"))

    # PE-PB
    if pd.notna(pe2) and pe2 > 0:
        pe2_color = "#ef4444" if pe2 > 20 else "#f59e0b" if pe2 > 14 else "#22c55e"
        items.append(("PE(市净率)", f"{pe2:.2f}", pe2_color))
    else:
        items.append(("PE(市净率)", "--", "#484f58"))

    # 股息率1
    if pd.notna(dy1) and dy1 > 0:
        items.append(("股息率", f"{dy1:.2f}%", "#22c55e"))
    else:
        items.append(("股息率", "--", "#484f58"))

    # 股息率2
    if pd.notna(dy2) and dy2 > 0:
        items.append(("股息率(等权)", f"{dy2:.2f}%", "#22c55e"))
    else:
        items.append(("股息率(等权)", "--", "#484f58"))

    for i, (label, value, color) in enumerate(items):
        with cols[i]:
            st.markdown(
                f'<div style="text-align:center;padding:10px 8px;background:#161b22;border-radius:6px;">'
                f'<div style="font-size:10px;color:#8b949e;margin-bottom:4px;">{label}</div>'
                f'<div style="font-size:18px;font-weight:bold;color:{color};">{value}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )


def _render_trade_history_panel(code, name):
    """渲染该 ETF 的交易历史面板（来自 trade_records）"""
    conn = get_db_connection()
    try:
        df = pd.read_sql_query(
            "SELECT date, action, quantity, price, amount, commission, stamp_tax, change_amount "
            "FROM trade_records WHERE code = ? AND action IN ('证券买入','证券卖出') "
            "ORDER BY date",
            conn, params=(code,)
        )
    except (sqlite3.OperationalError, pd.errors.DatabaseError):
        df = pd.DataFrame()
    finally:
        conn.close()

    if df.empty:
        return

    st.markdown("---")
    st.markdown(
        '<div class="tip-title" style="font-size:14px;border-bottom:none;padding:5px 0;">'
        f'{name} 交易历史'
        '<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span>'
        '<span class="tip-text" style="left: 4px; top: calc(100% + 10px);">'
        '该ETF在交易记录中的全部买卖明细。'
        '</span></div>',
        unsafe_allow_html=True,
    )

    # 汇总统计
    buys = df[df['action'] == '证券买入']
    sells = df[df['action'] == '证券卖出']
    buy_amt = buys['amount'].sum() if not buys.empty else 0
    sell_amt = sells['amount'].sum() if not sells.empty else 0
    total_fee = (df['commission'].sum() + df['stamp_tax'].sum())

    tc1, tc2, tc3 = st.columns(3)
    tc1.metric("买入", f"¥{buy_amt:,.0f}", delta=f"{len(buys)} 笔")
    tc2.metric("卖出", f"¥{sell_amt:,.0f}", delta=f"{len(sells)} 笔")
    tc3.metric("交易费用", f"¥{total_fee:,.2f}")

    display = df.rename(columns={
        'date': '日期', 'action': '操作', 'quantity': '数量',
        'price': '价格', 'amount': '金额', 'commission': '佣金',
        'stamp_tax': '印花税', 'change_amount': '发生额'
    })
    st.dataframe(display.reset_index(drop=True), use_container_width=True, hide_index=True)


def render_tab2():
    selected_date = st.session_state.get("selected_date", "")
    selected_benchmark = st.session_state.get("selected_benchmark", "sh000300")
    positions = load_positions(selected_date)
    # 加载 ETF 基本面数据，merge 到 positions 用于表格展示
    fund_df = load_etf_fundamental()
    if not fund_df.empty and not positions.empty:
        fund_subset = fund_df[["code", "iopv", "discount_rate", "main_net_inflow",
                               "main_net_inflow_pct", "turnover_rate", "volume_ratio", "shares"]].copy()
        fund_subset["code"] = fund_subset["code"].astype(str)
        positions = positions.copy()
        positions["code"] = positions["code"].astype(str)
        positions = positions.merge(fund_subset, on="code", how="left")
    show_days = st.session_state.get("show_days", 250)
    summary = load_summary(show_days, selected_date)
    """渲染Tab2: 持仓分布"""
    technical = pd.DataFrame()
    volatility = None
    max_dd = None
    sharpe = None
    cal_data = pd.DataFrame()
    tech_signals = pd.DataFrame()

    st.caption("📊 展示持仓分布饼图、持仓明细表格、行业权重变化趋势及持仓相关性矩阵")

    _render_etf_filter(positions, summary, technical, selected_date)
    st.markdown("---")
    _render_sector_weights(positions, summary, selected_date)
    st.markdown("---")
    _render_correlation_matrix(positions, selected_date)
    _render_deep_analysis(positions, summary)
    _render_cumulative_pnl(positions)

