"""
Tab4: 收益日历
"""

import streamlit as st
from datetime import datetime
import calendar
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from src.utils.database import get_db_connection


def compute_monthly_returns():
    """计算月度收益率矩阵（年份 x 月份，含年度合计列和汇总行）"""
    conn = get_db_connection()
    query = "SELECT date, daily_return, total_value FROM portfolio_summary ORDER BY date"
    df = pd.read_sql_query(query, conn)
    if df.empty:
        return pd.DataFrame()
    df["date"] = pd.to_datetime(df["date"])
    # daily_return 在数据库中以百分比形式存储，改用 total_value.pct_change()
    df["daily_return"] = df["total_value"].pct_change()
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    # 使用月首末日 total_value 计算正确的月度收益率
    monthly = df.groupby(["year", "month"]).agg(
        first_value=("total_value", "first"),
        last_value=("total_value", "last"),
    ).reset_index()
    monthly["monthly_return"] = monthly["last_value"] / monthly["first_value"] - 1
    pivot = monthly.pivot(index="year", columns="month", values="monthly_return")
    pivot.columns = [f"{m}月" for m in pivot.columns]
    # 年度合计列
    yearly = df.groupby("year").agg(first_value=("total_value", "first"), last_value=("total_value", "last")).reset_index()
    yearly["yearly_return"] = yearly["last_value"] / yearly["first_value"] - 1
    pivot = pivot.merge(yearly[["year", "yearly_return"]].rename(columns={"yearly_return": "年累计"}), left_index=True, right_on="year", how="left").set_index("year")
    # 汇总行（各年份同月收益率均值，年累计为年均复合收益率）
    summary_row = pivot.drop(columns=["年累计"]).mean(axis=0)
    summary_row["年累计"] = (1 + pivot["年累计"]).prod() ** (1 / len(pivot)) - 1
    summary_row.name = "月均"
    pivot = pd.concat([pivot, summary_row.to_frame().T])
    return pivot



def load_calendar_data():
    """加载全部日历收益数据（年/月/日汇总）"""
    conn = get_db_connection()
    query = "SELECT date, daily_pnl, daily_return, total_value FROM portfolio_summary ORDER BY date"
    df = pd.read_sql_query(query, conn)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    # daily_return 在数据库中以百分比形式存储，改用 total_value.pct_change()
    df["daily_return"] = df["total_value"].pct_change()
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    df["day"] = df["date"].dt.day
    return df



def render_tab4(positions, summary, index_quotes, selected_date, selected_benchmark, **kwargs):
    # 从kwargs获取额外的变量
    technical = kwargs.get('technical', pd.DataFrame())
    volatility = kwargs.get('volatility', None)
    max_dd = kwargs.get('max_dd', None)
    sharpe = kwargs.get('sharpe', None)
    cal_data = kwargs.get('cal_data', pd.DataFrame())
    tech_signals = kwargs.get('tech_signals', pd.DataFrame())

    """渲染Tab4: 收益日历"""
    
    st.caption("📅 以日历热力图形式展示每月每个交易日的收益情况，支持按年/月切换查看")
    cal_data = load_calendar_data()

    if cal_data.empty:
        st.info("暂无日历数据")
    else:
        years = sorted(cal_data["year"].unique(), reverse=True)
        latest_year = years[0]
        today_str = datetime.now().strftime("%Y-%m-%d")

        # --- 年份选择 ---
        cur_year = st.session_state.get("cal_year", latest_year)
        yr_cols = st.columns(len(years))
        for i, yr in enumerate(years):
            with yr_cols[i]:
                if st.button(str(yr), key=f"yr_{yr}", type="primary" if cur_year == yr else "secondary"):
                    st.session_state["cal_year"] = yr
                    st.session_state.pop("cal_month", None)
                    st.rerun()

        sel_year = cur_year
        year_df = cal_data[cal_data["year"] == sel_year]

        # --- 年度月度概览（月份可点击切换） ---
        months_in_year = sorted(year_df["month"].unique())
        sel_month = st.session_state.get("cal_month", months_in_year[-1] if months_in_year else 1)
        if sel_month not in months_in_year:
            sel_month = months_in_year[-1] if months_in_year else 1

        # 使用月首末日计算正确的月度收益率
        month_returns = year_df.groupby("month").agg(
            pnl_sum=("daily_pnl", "sum"),
            first_value=("total_value", "first"),
            last_value=("total_value", "last"),
            days=("day", "count"),
        ).reset_index()
        month_returns["ret_sum"] = month_returns["last_value"] / month_returns["first_value"] - 1
        yr_monthly = month_returns

        yr_monthly["profit_days"] = (
            year_df[year_df["daily_pnl"] > 0]
            .groupby("month")
            .size()
            .reindex(yr_monthly["month"], fill_value=0)
            .values
        )
        yr_monthly["loss_days"] = (
            year_df[year_df["daily_pnl"] < 0]
            .groupby("month")
            .size()
            .reindex(yr_monthly["month"], fill_value=0)
            .values
        )

        # --- 年度月度概览（月份按钮在表格内） ---
        yr_total_pnl = year_df["daily_pnl"].sum()
        yr_total_ret = year_df["total_value"].iloc[-1] / year_df["total_value"].iloc[0] - 1
        yr_total_days = len(year_df)
        yr_profit_days = len(year_df[year_df["daily_pnl"] > 0])
        yr_loss_days = len(year_df[year_df["daily_pnl"] < 0])
        yr_pnl_color = "#22c55e" if yr_total_pnl >= 0 else "#ef4444"
        yr_ret_color = "#22c55e" if yr_total_ret >= 0 else "#ef4444"

        # Header row: label + data headers
        hdr_col1, hdr_col2 = st.columns([1, 5])
        with hdr_col1:
            st.markdown(
                '<div style="color:#8b949e;font-size:13px;padding:6px 0;border-bottom:1px solid #30363d;text-align:center;">月份</div>',
                unsafe_allow_html=True,
            )
        with hdr_col2:
            st.markdown(
                '<div style="display:flex;color:#8b949e;font-size:13px;border-bottom:1px solid #30363d;">'
                '<div style="flex:1;text-align:right;padding:6px 10px;">月收益</div>'
                '<div style="flex:1;text-align:right;padding:6px 10px;">月收益率</div>'
                '<div style="flex:1;text-align:center;padding:6px 10px;">交易日</div>'
                '<div style="flex:1;text-align:center;padding:6px 10px;">盈利天数</div>'
                '<div style="flex:1;text-align:center;padding:6px 10px;">亏损天数</div>'
                "</div>",
                unsafe_allow_html=True,
            )

        # Data rows: month button + data
        for _, row in yr_monthly.iterrows():
            m = int(row["month"])
            pnl = row["pnl_sum"]
            ret = row["ret_sum"]
            days = int(row["days"])
            profit_d = int(row["profit_days"])
            loss_d = int(row["loss_days"])
            pnl_color = "#22c55e" if pnl >= 0 else "#ef4444"
            ret_color = "#22c55e" if ret >= 0 else "#ef4444"
            is_active = m == sel_month

            row_col1, row_col2 = st.columns([1, 5])
            with row_col1:
                _b1, _b2, _b3 = st.columns([1, 1, 1])
                with _b2:
                    if st.button(f"{m}月", key=f"mo_{sel_year}_{m}", type="primary" if is_active else "secondary"):
                        st.session_state["cal_month"] = m
                        st.rerun()
            with row_col2:
                bg = "background:#161b22;" if is_active else ""
                st.markdown(
                    f'<div style="display:flex;{bg}border-bottom:1px solid #21262d;">'
                    f'<div style="flex:1;text-align:right;padding:6px 10px;color:{pnl_color};">¥{pnl:,.0f}</div>'
                    f'<div style="flex:1;text-align:right;padding:6px 10px;color:{ret_color};">{ret*100:+.2f}%</div>'
                    f'<div style="flex:1;text-align:center;padding:6px 10px;">{days}天</div>'
                    f'<div style="flex:1;text-align:center;padding:6px 10px;color:#22c55e;">{profit_d}天</div>'
                    f'<div style="flex:1;text-align:center;padding:6px 10px;color:#ef4444;">{loss_d}天</div>'
                    f"</div>",
                    unsafe_allow_html=True,
                )

        # Yearly total row
        tot_col1, tot_col2 = st.columns([1, 5])
        with tot_col1:
            st.markdown(
                '<div style="font-weight:bold;text-align:center;padding:8px 0;color:#58a6ff;'
                'border-top:2px solid #30363d;">全年合计</div>',
                unsafe_allow_html=True,
            )
        with tot_col2:
            st.markdown(
                f'<div style="display:flex;font-weight:bold;background:#161b22;border-top:2px solid #30363d;">'
                f'<div style="flex:1;text-align:right;padding:8px 10px;color:{yr_pnl_color};">¥{yr_total_pnl:,.0f}</div>'
                f'<div style="flex:1;text-align:right;padding:8px 10px;color:{yr_ret_color};">{yr_total_ret*100:+.2f}%</div>'
                f'<div style="flex:1;text-align:center;padding:8px 10px;">{yr_total_days}天</div>'
                f'<div style="flex:1;text-align:center;padding:8px 10px;color:#22c55e;">{yr_profit_days}天</div>'
                f'<div style="flex:1;text-align:center;padding:8px 10px;color:#ef4444;">{yr_loss_days}天</div>'
                f"</div>",
                unsafe_allow_html=True,
            )

        month_df = year_df[year_df["month"] == sel_month]

        # --- 月度汇总 ---
        m_pnl = month_df["daily_pnl"].sum()
        m_return = month_df["total_value"].iloc[-1] / month_df["total_value"].iloc[0] - 1 if len(month_df) > 0 else 0
        m_trading = len(month_df)
        m_profit = len(month_df[month_df["daily_pnl"] > 0])
        m_loss = len(month_df[month_df["daily_pnl"] < 0])

        st.markdown("---")
        sum_col1, sum_col2, sum_col3, sum_col4, sum_col5 = st.columns(5)
        with sum_col1:
            st.metric("月收益", f"¥{m_pnl:,.0f}")
        with sum_col2:
            st.metric("月收益率", f"{m_return*100:.2f}%")
        with sum_col3:
            st.metric("交易日", f"{m_trading}天")
        with sum_col4:
            st.metric("盈利天数", f"{m_profit}天")
        with sum_col5:
            st.metric("亏损天数", f"{m_loss}天")

        # --- 月度日历 ---
        st.markdown(f"**{sel_year}年{sel_month}月 日历**")

        # 获取交易日数据字典
        trading_days = {}
        for _, row in month_df.iterrows():
            d = int(row["day"])
            pnl = row["daily_pnl"]
            ret = row["daily_return"]
            dt_str = row["date"].strftime("%Y-%m-%d")
            trading_days[d] = {"pnl": pnl, "ret": ret, "date_str": dt_str}

        # 构建日历HTML
        cal = calendar.Calendar(firstweekday=0)  # 周一开始
        month_days = list(cal.itermonthdays(sel_year, sel_month))

        # 周标题
        week_headers = ["一", "二", "三", "四", "五", "六", "日"]

        st.markdown("""<style>
        .cal-table { width: 100%; border-collapse: collapse; }
        .cal-table th { color: #8b949e; font-size: 12px; padding: 8px 4px; text-align: center; border-bottom: 1px solid #30363d; }
        .cal-table td { padding: 4px; text-align: center; border-radius: 4px; min-height: 48px; vertical-align: top; }
        .cal-non-trading { color: #30363d; }
        .cal-trading { background: #161b22; }
        .cal-profit { background: rgba(34,197,94,0.15); color: #22c55e; }
        .cal-loss { background: rgba(239,68,68,0.15); color: #ef4444; }
        .cal-today { outline: 2px solid #58a6ff; outline-offset: -2px; }
        .cal-day { display: block; font-size: 14px; font-weight: bold; color: #c9d1d9; }
        .cal-pnl { display: block; font-size: 10px; margin-top: 2px; }
        .cal-pnl-profit { color: #22c55e; }
        .cal-pnl-loss { color: #ef4444; }
        .cal-pnl-zero { color: #484f58; }
        </style>""", unsafe_allow_html=True)

        cal_html = '<table class="cal-table"><tr>'
        for h in week_headers:
            cal_html += f"<th>{h}</th>"
        cal_html += "</tr><tr>"

        for i, day in enumerate(month_days):
            if day == 0:
                cal_html += '<td class="cal-non-trading"></td>'
            elif day in trading_days:
                info = trading_days[day]
                pnl = info["pnl"]
                ret = info["ret"]
                dt_str = info["date_str"]

                if pnl > 0:
                    td_cls = "cal-trading cal-profit"
                    pnl_cls = "cal-pnl cal-pnl-profit"
                elif pnl < 0:
                    td_cls = "cal-trading cal-loss"
                    pnl_cls = "cal-pnl cal-pnl-loss"
                else:
                    td_cls = "cal-trading"
                    pnl_cls = "cal-pnl cal-pnl-zero"

                today_cls = " cal-today" if dt_str == today_str else ""

                # 格式化收益金额
                if abs(pnl) >= 10000:
                    pnl_text = f"{pnl/10000:.1f}万"
                elif abs(pnl) >= 1000:
                    pnl_text = f"{pnl/1000:.1f}k"
                else:
                    pnl_text = f"{pnl:.0f}"

                cal_html += (
                    f'<td class="{td_cls}{today_cls}" title="{dt_str}  收益: ¥{pnl:,.0f}  ({ret:+.2f}%)">'
                    f'<span class="cal-day">{day}</span>'
                    f'<span class="{pnl_cls}">{pnl_text}</span>'
                    f"</td>"
                )
            else:
                # 非交易日
                cal_html += f'<td class="cal-non-trading"><span class="cal-day">{day}</span></td>'

            if (i + 1) % 7 == 0:
                cal_html += "</tr><tr>"

        # 清理最后可能的多余tr
        cal_html = cal_html.rstrip("<tr>")
        cal_html += "</table>"

        st.markdown(cal_html, unsafe_allow_html=True)

        # --- 每日收益明细表 ---
        with st.expander("查看每日收益明细", expanded=False):
            detail_df = month_df[["date", "daily_pnl", "daily_return"]].copy()
            detail_df.columns = ["日期", "日收益 (¥)", "日收益率 (%)"]
            detail_df["日期"] = detail_df["日期"].dt.strftime("%Y-%m-%d")
            detail_df["日收益 (¥)"] = detail_df["日收益 (¥)"].apply(
                lambda x: f'<span style="color:{"#22c55e" if x >= 0 else "#ef4444"}">{x:,.2f}</span>'
            )
            detail_df["日收益率 (%)"] = detail_df["日收益率 (%)"].apply(
                lambda x: f'<span style="color:{"#22c55e" if x >= 0 else "#ef4444"}">{x*100:+.2f}%</span>'
            )
            st.markdown(detail_df.to_html(index=False, escape=False), unsafe_allow_html=True)

        # --- 月度收益热力图 ---
        st.markdown("---")
        st.markdown(
            '<div class="tip-title" style="font-size:14px;border-bottom:none;padding:5px 0;">月度收益热力图<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">以热力图形式展示12个月的月度收益，颜色深浅反映收益高低。</span></div>',
            unsafe_allow_html=True,
        )
        monthly_pivot = compute_monthly_returns()
        if not monthly_pivot.empty:
            # compute_monthly_returns 返回小数形式收益率，乘100转为百分比
            heat_z = monthly_pivot.values * 100
            # 使用百分位数限制极端值，避免个别异常月压缩整体色阶
            valid_z = heat_z[~np.isnan(heat_z)]
            z_cap = max(np.percentile(np.abs(valid_z), 98) if len(valid_z) > 0 else 50, 20)

            fig_heat = go.Figure(
                go.Heatmap(
                    z=heat_z,
                    x=monthly_pivot.columns.tolist(),
                    y=monthly_pivot.index.astype(str).tolist(),
                    text=heat_z,
                    texttemplate="%{text:.2f}%%",
                    textfont=dict(size=10),
                    colorscale=[[0, "#ef4444"], [0.5, "#0d1117"], [1, "#22c55e"]],
                    zmin=-z_cap,
                    zmax=z_cap,
                    xgap=2,
                    ygap=2,
                    hovertemplate="%{y}年%{x}<br>收益率: %{z:.2f}%%<extra></extra>",
                )
            )
            fig_heat.update_layout(
                height=max(250, 40 * len(monthly_pivot)),
                plot_bgcolor="#0d1117",
                paper_bgcolor="#0d1117",
                font=dict(color="#c9d1d9", size=11),
                margin=dict(l=50, r=20, t=10, b=40),
                xaxis=dict(title="", showgrid=False, side="top"),
                yaxis=dict(title="", showgrid=False, autorange="reversed"),
            )
            st.plotly_chart(fig_heat, width="stretch")

        # --- 年化收益走势图（Phase 5B新增）---
        st.markdown("---")
        st.markdown(
            '<div class="tip-title" style="font-size:14px;border-bottom:none;padding:5px 0;">年化收益走势<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">组合累计收益率与年化收益率趋势。</span></div>',
            unsafe_allow_html=True,
        )
        all_cal = load_calendar_data()
        if not all_cal.empty:
            yearly_cumret = all_cal.groupby("year").apply(
                lambda g: g["total_value"].iloc[-1] / g["total_value"].iloc[0] - 1,
                include_groups=False
            )
            cumret_by_date = all_cal.groupby("date").agg(
                first_v=("total_value", "first"), last_v=("total_value", "last")
            ).reset_index()
            cumret_by_date["cum_return"] = cumret_by_date["last_v"] / cumret_by_date["first_v"].iloc[0] - 1
            # Annualized return (CAGR)
            cumret_by_date["years_elapsed"] = (cumret_by_date["date"] - cumret_by_date["date"].iloc[0]).dt.days / 365.25
            cumret_by_date["ann_return"] = (1 + cumret_by_date["cum_return"]) ** (1 / cumret_by_date["years_elapsed"].clip(lower=0.01)) - 1

            fig_cum = go.Figure()
            fig_cum.add_trace(go.Scatter(
                x=cumret_by_date["date"], y=cumret_by_date["cum_return"] * 100,
                mode="lines", name="累计收益率",
                line=dict(color="#58a6ff", width=2),
                fill="tozeroy", fillcolor="rgba(88,166,255,0.08)",
            ))
            fig_cum.add_trace(go.Scatter(
                x=cumret_by_date["date"], y=cumret_by_date["ann_return"] * 100,
                mode="lines", name="年化收益率",
                line=dict(color="#f59e0b", width=1.5, dash="dot"),
                yaxis="y2",
            ))
            fig_cum.update_layout(
                height=280,
                plot_bgcolor="#0d1117", paper_bgcolor="#0d1117",
                font=dict(color="#c9d1d9", size=11),
                margin=dict(l=60, r=60, t=10, b=30),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1,
                            font=dict(size=10, color="#8b949e")),
                xaxis=dict(title="", showgrid=True, gridcolor="#21262d"),
                yaxis=dict(title="累计收益率 (%)", showgrid=True, gridcolor="#21262d"),
                yaxis2=dict(title="年化收益率 (%)", overlaying="y", side="right",
                            gridcolor="rgba(255,255,255,0.05)"),
            )
            st.plotly_chart(fig_cum, width="stretch")

        # --- 月度收益分布箱线图（Phase 5B新增）---
        st.markdown("---")
        st.markdown(
            '<div class="tip-title" style="font-size:14px;border-bottom:none;padding:5px 0;">月度收益分布<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">各月份日收益率分布箱线图，展示收益波动范围。</span></div>',
            unsafe_allow_html=True,
        )
        if not all_cal.empty and "daily_return" in all_cal.columns:
            all_cal["month_label"] = all_cal["month"].apply(lambda m: f"{m}月")
            month_order = [f"{m}月" for m in range(1, 13)]
            monthly_groups = [all_cal.loc[all_cal["month"] == m, "daily_return"].dropna() * 100 for m in range(1, 13)]
            valid_groups = [(label, grp) for label, grp in zip(month_order, monthly_groups) if len(grp) > 0]

            if valid_groups:
                fig_box = go.Figure()
                for label, grp in valid_groups:
                    fig_box.add_trace(go.Box(
                        y=grp.values, name=label,
                        marker_color="#58a6ff",
                        line_color="#58a6ff",
                        fillcolor="rgba(88,166,255,0.15)",
                        boxmean="sd",
                    ))
                fig_box.update_layout(
                    height=300,
                    plot_bgcolor="#0d1117", paper_bgcolor="#0d1117",
                    font=dict(color="#c9d1d9", size=11),
                    margin=dict(l=40, r=20, t=10, b=30),
                    xaxis=dict(title="", showgrid=False),
                    yaxis=dict(title="日收益率 (%)", showgrid=True, gridcolor="#21262d"),
                    showlegend=False,
                    boxmode="group",
                )
                st.plotly_chart(fig_box, width="stretch")

                # --- 事件日历：关键日期提醒 ---
        st.markdown("---")
        st.markdown(
            '<div class="tip-title" style="font-size:14px;border-bottom:none;padding:5px 0;">关键日期提醒<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">自动检测持仓中的关键事件日期，如财报季、期权到期日等。</span></div>',
            unsafe_allow_html=True,
        )

        # 1. 财报季提醒
        current_year = datetime.now().year
        current_month = datetime.now().month
        current_day = datetime.now().day

        earnings_periods = [
            {"name": "一季报", "start": (4, 1), "end": (4, 30), "icon": "📊"},
            {"name": "半年报", "start": (7, 1), "end": (8, 31), "icon": "📋"},
            {"name": "三季报", "start": (10, 1), "end": (10, 31), "icon": "📊"},
            {"name": "年报", "start": (1, 1), "end": (4, 30), "icon": "📋"},
        ]

        # 2. 期权到期日（每月第三个周五）
        def get_third_friday(year, month):
            """计算某月的第三个周五"""
            import calendar as cal_mod

            cal = cal_mod.monthcalendar(year, month)
            # 找到有周五的周
            fridays = [week[cal_mod.FRIDAY] for week in cal if week[cal_mod.FRIDAY] != 0]
            return fridays[2] if len(fridays) >= 3 else fridays[-1]

        # 3. 基金分红季
        dividend_months = [1, 6, 12]

        # 4. 系统性风险事件
        events_list = []

        # 财报季
        for ep in earnings_periods:
            s_m, s_d = ep["start"]
            e_m, e_d = ep["end"]
            days_ahead = 0
            if current_year == current_year:
                if s_m == current_month:
                    days_ahead = s_d - current_day
                elif s_m > current_month:
                    month_diff = s_m - current_month
                    days_ahead = (month_diff * 30) + (s_d - current_day)

            if days_ahead >= 0 and days_ahead <= 90:
                urgency = "即将到来" if days_ahead <= 14 else ("本月" if days_ahead <= 30 else f"{days_ahead}天后")
                events_list.append(
                    {
                        "icon": ep["icon"],
                        "title": f'{ep["name"]}披露期',
                        "date": f"{current_year}-{s_m:02d}-{s_d:02d} ~ {current_year}-{e_m:02d}-{e_d:02d}",
                        "urgency": urgency,
                        "days_ahead": days_ahead,
                        "color": "#f59e0b" if days_ahead <= 30 else "#8b949e",
                        "desc": f"A股上市公司{ep['name']}集中披露窗口",
                    }
                )

        # 期权到期日（未来3个月）
        for m_offset in range(0, 4):
            evt_month = current_month + m_offset
            evt_year = current_year
            while evt_month > 12:
                evt_month -= 12
                evt_year += 1
            try:
                third_fri = get_third_friday(evt_year, evt_month)
                evt_date = datetime(evt_year, evt_month, third_fri)
                delta = (evt_date - datetime.now()).days
                if 0 <= delta <= 90:
                    urgency = "本周五" if delta <= 7 else ("即将" if delta <= 14 else f"{delta}天后")
                    events_list.append(
                        {
                            "icon": "📅",
                            "title": "股指期权交割日",
                            "date": evt_date.strftime("%Y-%m-%d"),
                            "urgency": urgency,
                            "days_ahead": delta,
                            "color": "#ef4444" if delta <= 7 else "#f59e0b" if delta <= 14 else "#8b949e",
                            "desc": "沪深300/中证1000股指期权到期，注意波动加剧",
                        }
                    )
            except Exception:
                pass

        # 基金分红提醒
        for m_offset in range(0, 4):
            d_month = current_month + m_offset
            d_year = current_year
            while d_month > 12:
                d_month -= 12
                d_year += 1
            if d_month in dividend_months:
                delta = (datetime(d_year, d_month, 15) - datetime.now()).days
                if 0 <= delta <= 90:
                    events_list.append(
                        {
                            "icon": "💰",
                            "title": "基金分红季",
                            "date": f"{d_year}-{d_month:02d}",
                            "urgency": f"{delta}天后" if delta > 7 else "即将",
                            "days_ahead": delta,
                            "color": "#22c55e" if delta > 14 else "#f59e0b",
                            "desc": "ETF基金常见分红除息月份，关注持仓基金公告",
                        }
                    )

        # 年底/年初换仓提醒
        if 12 <= current_month <= 12 or 1 <= current_month <= 1:
            events_list.append(
                {
                    "icon": "🔄",
                    "title": "年度换仓窗口",
                    "date": f"{current_year}-12 ~ {current_year + 1}-01",
                    "urgency": "当前",
                    "days_ahead": 0,
                    "color": "#a855f7",
                    "desc": "年末机构调仓高峰，市场风格可能切换",
                }
            )

        # Sort by days_ahead
        events_list.sort(key=lambda x: x["days_ahead"])

        # Render events
        if events_list:
            for evt in events_list:
                st.markdown(
                    f'<div style="background:#161b22;border-radius:6px;padding:10px 14px;margin-bottom:5px;border-left:3px solid {evt["color"]};">'
                    f'<div style="display:flex;justify-content:space-between;align-items:center;">'
                    f'<div style="display:flex;align-items:center;gap:8px;">'
                    f'<span style="font-size:16px;">{evt["icon"]}</span>'
                    f"<div>"
                    f'<div style="font-size:13px;color:#e6edf3;font-weight:bold;">{evt["title"]}</div>'
                    f'<div style="font-size:11px;color:#6e7681;margin-top:2px;">{evt["desc"]}</div>'
                    f"</div></div>"
                    f'<div style="text-align:right;">'
                    f'<div style="font-size:12px;color:{evt["color"]};font-weight:bold;">{evt["urgency"]}</div>'
                    f'<div style="font-size:11px;color:#484f58;">{evt["date"]}</div>'
                    f"</div></div></div>",
                    unsafe_allow_html=True,
                )
        else:
            st.info("近90天内暂无关键日期事件")

# ========== Tab6: 技术信号 ==========
