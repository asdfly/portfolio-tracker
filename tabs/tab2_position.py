"""
Tab2: 持仓分布
"""

import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from config.settings import ETF_CATEGORIES, SECTOR_COLORS, DATABASE_PATH
from tabs._helpers import _render_etf_detail_panel
from src.utils.database import get_db_connection


def load_correlation_matrix(days=250, end_date=None):
    """计算持仓ETF之间的皮尔逊相关系数矩阵（基于各ETF市值变动）"""
    conn = get_db_connection()
    if end_date:
        query = """
            SELECT date, code, market_value 
            FROM portfolio_snapshots 
            WHERE date <= ? 
            ORDER BY date DESC
        """
        df = pd.read_sql_query(query, conn, params=(end_date,))
    else:
        query = """
            SELECT date, code, market_value 
            FROM portfolio_snapshots 
            ORDER BY date DESC
        """
        df = pd.read_sql_query(query, conn)
    if df.empty:
        return pd.DataFrame(), []

    # 取最近N个交易日
    dates = df["date"].unique()[:days]
    df = df[df["date"].isin(dates)]

    # 构建透视表：行=日期, 列=code, 值=market_value
    pivot = df.pivot_table(index="date", columns="code", values="market_value", aggfunc="first")

    # 只保留有足够数据的ETF（至少80%的交易日有数据）
    min_count = int(len(pivot) * 0.8)
    valid_cols = pivot.columns[pivot.notna().sum() >= min_count]
    pivot = pivot[valid_cols]

    if pivot.shape[1] < 2:
        return pd.DataFrame(), []

    # 计算日收益率
    returns = pivot.pct_change().dropna()

    # 计算相关系数矩阵
    corr = returns.corr()

    # 获取ETF名称
    conn = get_db_connection()
    names = {}
    for code in corr.columns:
        row = conn.execute(
            "SELECT name FROM portfolio_snapshots WHERE code = ? ORDER BY date DESC LIMIT 1", (code,)
        ).fetchone()
        names[code] = row[0] if row else code
    # 简化名称（取前4个字 + "..."）
    short_names = {}
    for code, name in names.items():
        if len(name) > 6:
            short_names[code] = name[:6] + ".."
        else:
            short_names[code] = name

    return corr, short_names



def load_sector_weights(days=250, end_date=None):
    """加载按行业聚合的持仓权重历史（堆叠面积图数据源）"""
    query = """
        SELECT ps.date, ps.code, ps.market_value, ps.quantity, ps.current_price
        FROM portfolio_snapshots ps
        WHERE ps.date IN (
            SELECT DISTINCT date FROM portfolio_snapshots
            ORDER BY date DESC
            LIMIT ?
        )
    """
    if end_date:
        query += " AND ps.date <= ?"
    query += " ORDER BY ps.date, ps.code"

    try:
        conn = get_db_connection()
        params = [days]
        if end_date:
            params.append(end_date)
        df = pd.read_sql_query(query, conn, params=params)
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"load_sector_weights 查询失败: {e}")
        return pd.DataFrame(), {}

    if df.empty:
        return pd.DataFrame(), {}

    # 按行业分类
    df["sector"] = df["code"].map(lambda c: ETF_CATEGORIES.get(c, {}).get("sector", "其他"))

    # 每日各行业总市值
    pivot = df.pivot_table(index="date", columns="sector", values="market_value", aggfunc="sum", fill_value=0)
    # 计算每日权重百分比
    daily_total = pivot.sum(axis=1)
    weight_df = pivot.div(daily_total, axis=0) * 100

    # 确定显示顺序（按最新日期的权重降序）
    if not weight_df.empty:
        latest = weight_df.iloc[-1].sort_values(ascending=False)
        weight_df = weight_df[latest.index]

    # 扇区颜色映射
    sector_color_map = {}
    for sector in weight_df.columns:
        sector_color_map[sector] = SECTOR_COLORS.get(sector, "#6b7280")

    return weight_df, sector_color_map



def render_tab2(positions, summary, index_quotes, selected_date, selected_benchmark, **kwargs):
    # 从kwargs获取额外的变量
    technical = kwargs.get('technical', pd.DataFrame())
    volatility = kwargs.get('volatility', None)
    max_dd = kwargs.get('max_dd', None)
    sharpe = kwargs.get('sharpe', None)
    cal_data = kwargs.get('cal_data', pd.DataFrame())
    tech_signals = kwargs.get('tech_signals', pd.DataFrame())

    """渲染Tab2: 持仓分布"""
    
    st.caption("📊 展示持仓分布饼图、持仓明细表格、行业权重变化趋势及持仓相关性矩阵")

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
            st.plotly_chart(fig_pie, width="stretch")

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
        st.plotly_chart(fig_sector, width="stretch")

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

    st.markdown("---")
    # ===== 相关性矩阵热力图 =====
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
        st.plotly_chart(fig_corr, width="stretch")
        st.caption(f"基于最近250个交易日的市值日收益率计算 | 数据截至 {selected_date}")
    else:
        st.info("持仓数据不足，暂无法计算相关性矩阵")

    # ===== 深度分析: 持仓集中度 + Beta贡献 =====
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
                st.plotly_chart(fig_hhi, width="stretch")

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
                    st.plotly_chart(fig_beta, width="stretch")
                else:
                    st.caption("暂无有效Beta数据")
            else:
                st.caption("暂无Beta数据（需技术分析模块计算）")

    # ---------- 累计盈亏柱状图 ----------
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
        st.plotly_chart(fig_pnl, width="stretch")
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

    


