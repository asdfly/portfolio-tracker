"""
Tab3: 风险分析
"""

import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from src.utils.chart_utils import downsample, _add_min_max_annotations
from config.settings import ETF_CATEGORIES
from src.utils.database import get_db_connection


def compute_extended_risk_metrics(end_date=None, min_date="2025-08-01"):
    """计算扩展风险指标（基于持仓稳定后的日收益率）
    
    Args:
        end_date: 截止日期，None表示最新
        min_date: 起始日期，默认2025-08-01（全部ETF覆盖日），
                  因为回填脚本用当前quantity×历史price，早期持仓少时
                  total_value极低导致风险指标严重失真
    """
    conn = get_db_connection()
    query = "SELECT date, daily_return, daily_pnl, total_value FROM portfolio_summary ORDER BY date"
    df = pd.read_sql_query(query, conn)
    if df.empty or len(df) < 10:
        return {}
    df["date"] = pd.to_datetime(df["date"])
    if min_date:
        df = df[df["date"] >= pd.Timestamp(min_date)]
    if end_date:
        df = df[df["date"] <= pd.Timestamp(end_date)]
    if len(df) < 10:
        return {}

    # daily_return 在数据库中以百分比形式存储，改用 total_value.pct_change() 获取正确的小数日收益率
    returns = df["total_value"].pct_change().dropna()
    pnls = df["daily_pnl"]

    # Sortino Ratio (downside deviation)
    neg_returns = returns[returns < 0]
    downside_std = neg_returns.std() * np.sqrt(252) if len(neg_returns) > 1 else np.nan
    n_years = len(returns) / 252
    annual_return = (1 + returns).prod() ** (1 / n_years) - 1
    annual_std = returns.std() * np.sqrt(252)
    # Sharpe & Sortino use arithmetic mean for consistency with annual_std
    mean_return = returns.mean() * 252
    sortino = mean_return / downside_std if downside_std and downside_std > 0 else np.nan

    # Max Drawdown Duration (最大回撤持续时间)
    max_dd_duration = 0
    current_dd_duration = 0
    if "total_value" in df.columns:
        cummax = df["total_value"].cummax()
        in_drawdown = df["total_value"] < cummax
        for is_dd in in_drawdown:
            if is_dd:
                current_dd_duration += 1
                max_dd_duration = max(max_dd_duration, current_dd_duration)
            else:
                current_dd_duration = 0

    # Calmar Ratio (annual return / max drawdown)
    cummax = df["total_value"].cummax() if "total_value" in df.columns else None
    if cummax is not None:
        dd = (df["total_value"] - cummax) / cummax * 100
        max_dd_abs = abs(dd.min())
        calmar = annual_return * 100 / max_dd_abs if max_dd_abs > 0 else np.nan
    else:
        calmar = np.nan

    # Win rate
    win_days = len(pnls[pnls > 0])
    total_days = len(pnls[pnls != 0])
    win_rate = win_days / total_days * 100 if total_days > 0 else np.nan

    # Profit/Loss ratio
    avg_win = pnls[pnls > 0].mean() if win_days > 0 else 0
    avg_loss = abs(pnls[pnls < 0].mean()) if len(pnls[pnls < 0]) > 0 else 1
    pl_ratio = avg_win / avg_loss if avg_loss > 0 else np.nan

    # Max consecutive win/loss days
    max_consec_win, max_consec_loss = 0, 0
    consec_win, consec_loss = 0, 0
    for p in pnls:
        if p > 0:
            consec_win += 1
            consec_loss = 0
            max_consec_win = max(max_consec_win, consec_win)
        elif p < 0:
            consec_loss += 1
            consec_win = 0
            max_consec_loss = max(max_consec_loss, consec_loss)
        else:
            consec_win, consec_loss = 0, 0

    # Skewness & Kurtosis
    skewness = returns.skew()
    kurtosis = returns.kurtosis()

    # 计算最大回撤并返回
    cummax_val = df["total_value"].cummax()
    max_drawdown_val = float(((df["total_value"] - cummax_val) / cummax_val * 100).min()) if len(df) > 0 else np.nan

    return {
        "sortino": sortino,
        "calmar": calmar,
        "win_rate": win_rate,
        "pl_ratio": pl_ratio,
        "max_consec_win": max_consec_win,
        "max_consec_loss": max_consec_loss,
        "max_dd_duration": max_dd_duration,
        "max_drawdown": max_drawdown_val,
        "skewness": skewness,
        "kurtosis": kurtosis,
        "annual_return": annual_return,
        "annual_std": annual_std,
    }



def compute_return_attribution(days=250, end_date=None):
    """Brinson 收益归因：将组合收益分解为行业配置效应和选股效应

    使用基准指数（沪深300）的行业权重作为参考基准。

    Returns:
        dict: {
            'total_return': float,       # 组合总收益率
            'benchmark_return': float,   # 基准总收益率
            'allocation_effect': dict,   # 行业配置效应 {sector: value}
            'selection_effect': dict,    # 选股效应 {sector: value}
            'sector_returns': dict,      # 各行业实际收益率
            'sector_weights': dict,      # 组合各行业权重
            'bench_weights': dict,       # 基准各行业权重（近似）
        }
    """
    conn = get_db_connection()

    # 获取组合持仓快照
    query_snap = """
        SELECT ps.date, ps.code, ps.market_value, ps.pnl_rate
        FROM portfolio_snapshots ps
        WHERE ps.date = (SELECT MAX(date) FROM portfolio_snapshots WHERE date <= :end)
        AND ps.market_value > 0
    """
    if end_date:
        df_snap = pd.read_sql(query_snap, conn, params={"end": end_date})
    else:
        df_snap = pd.read_sql(query_snap, conn, params={"end": "9999-12-31"})

    if df_snap.empty:
        return None

    # 获取N天前快照
    query_prev = """
        SELECT ps.code, ps.market_value as prev_mv
        FROM portfolio_snapshots ps
        WHERE ps.date = (
            SELECT DISTINCT date FROM portfolio_snapshots 
            WHERE date <= :end 
            ORDER BY date DESC 
            LIMIT 1 OFFSET :skip
        )
        AND ps.market_value > 0
    """
    skip = days
    if end_date:
        df_prev = pd.read_sql(query_prev, conn, params={"end": end_date, "skip": skip})
    else:
        df_prev = pd.read_sql(query_prev, conn, params={"end": "9999-12-31", "skip": skip})

    if df_prev.empty:
        return None

    # 行业分类
    def get_sector(code):
        clean = code.replace("sh", "").replace("sz", "")
        cat = ETF_CATEGORIES.get(clean, {})
        return cat.get("sector", "其他")

    # 当前快照按行业聚合
    df_snap["sector"] = df_snap["code"].apply(get_sector)
    total_mv = df_snap["market_value"].sum()
    sector_weights = {}
    for sector, grp in df_snap.groupby("sector"):
        sector_weights[sector] = float(grp["market_value"].sum() / total_mv)

    # 计算各行业收益率
    df_prev["sector"] = df_prev["code"].apply(get_sector)

    # 计算每只ETF的N日收益率
    current_mv = df_snap.set_index("code")["market_value"]
    prev_mv = df_prev.set_index("code")["prev_mv"]

    # 匹配代码
    common_codes = current_mv.index.intersection(prev_mv.index)
    if len(common_codes) == 0:
        return None

    etf_returns = current_mv[common_codes] / prev_mv[common_codes] - 1
    etf_returns_df = etf_returns.reset_index()
    etf_returns_df.columns = ["code", "return"]
    etf_returns_df["sector"] = etf_returns_df["code"].apply(get_sector)

    # 各行业加权收益率
    sector_returns = {}
    for sector, grp in etf_returns_df.groupby("sector"):
        sector_returns[sector] = float(grp["return"].mean())

    # 基准行业权重（近似：均匀分布，实际应用中应从指数成分获取）
    n_sectors = len(sector_weights)
    bench_weights = {s: 1.0 / max(n_sectors, 1) for s in sector_weights}

    # 组合总收益率
    total_return = float(df_snap["market_value"].sum() / df_prev["prev_mv"].sum() - 1)

    # 基准收益率
    conn3 = get_db_connection()
    query_bench = "SELECT close FROM index_quotes WHERE code='sh000300' ORDER BY date DESC LIMIT 1"
    query_bench_prev = "SELECT close FROM index_quotes WHERE code='sh000300' ORDER BY date DESC LIMIT 1 OFFSET ?"
    bench_now = pd.read_sql(query_bench, conn3)
    bench_prev = pd.read_sql(query_bench_prev, conn3, params=(days,))
    conn3.close()

    benchmark_return = 0.0
    if not bench_now.empty and not bench_prev.empty:
        benchmark_return = float(bench_now["close"].iloc[0] / bench_prev["close"].iloc[0] - 1)

    # Brinson 分解
    all_sectors = set(list(sector_weights.keys()) + list(bench_weights.keys()))
    allocation_effect = {}
    selection_effect = {}

    for s in all_sectors:
        w_p = sector_weights.get(s, 0)  # 组合权重
        w_b = bench_weights.get(s, 0)  # 基准权重
        r_p = sector_returns.get(s, 0)  # 行业组合收益
        # 行业基准收益（简化模型：使用等权组合收益率近似）
        r_b = sector_returns.get(s, 0)  # 与 r_p 相同，选股效应为0（简化）

        allocation_effect[s] = (w_p - w_b) * r_b
        selection_effect[s] = w_p * (r_p - r_b)

    return {
        "total_return": total_return,
        "benchmark_return": benchmark_return,
        "allocation_effect": allocation_effect,
        "selection_effect": selection_effect,
        "sector_returns": sector_returns,
        "sector_weights": sector_weights,
        "bench_weights": bench_weights,
    }



def load_alerts(limit=10):
    """加载告警"""
    conn = get_db_connection()
    query = "SELECT * FROM alerts ORDER BY created_at DESC LIMIT ?"
    df = pd.read_sql_query(query, conn, params=(limit,))
    return df



def render_tab3(positions, summary, index_quotes, selected_date, selected_benchmark, **kwargs):
    show_days = kwargs.get("show_days", len(summary) if not summary.empty else 250)
    # 从positions计算盈亏计数，从summary最新行获取风险指标
    profit_count = int((positions["pnl"] > 0).sum()) if not positions.empty else 0
    loss_count = int((positions["pnl"] < 0).sum()) if not positions.empty else 0
    if not summary.empty:
        latest = summary.iloc[-1]
        volatility = latest.get("volatility", None)
        max_dd = latest.get("max_drawdown", None)
    else:
        volatility = None
        max_dd = None
    # 从kwargs获取额外的变量
    technical = kwargs.get('technical', pd.DataFrame())
    sharpe = kwargs.get('sharpe', None)
    cal_data = kwargs.get('cal_data', pd.DataFrame())
    tech_signals = kwargs.get('tech_signals', pd.DataFrame())

    """渲染Tab3: 风险分析"""
    
    st.caption("⚠️ 展示风险评分仪表盘、风险指标详情、回撤曲线及Brinson收益归因分析")
    col_risk_gauge, col_risk_detail = st.columns([1, 1])

    with col_risk_gauge:
        st.markdown(
            '<div class="tip-title" style="">风险指标仪表盘<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">综合年化波动率和最大回撤计算风险评分（0-100分）。满分100表示低风险，低于60分表示高风险。颜色越绿越安全，越红风险越高。</span></div>',
            unsafe_allow_html=True,
        )

        # 风险评分
        risk_score = 100
        if volatility and not np.isnan(volatility):
            if volatility > 30:
                risk_score -= 30
            elif volatility > 20:
                risk_score -= 15
            elif volatility > 15:
                risk_score -= 5
        if max_dd and not np.isnan(max_dd):
            if abs(max_dd) > 15:
                risk_score -= 30
            elif abs(max_dd) > 10:
                risk_score -= 20
            elif abs(max_dd) > 5:
                risk_score -= 10
        if sharpe and not np.isnan(sharpe):
            if sharpe < 0:
                risk_score -= 20
            elif sharpe < 0.5:
                risk_score -= 10

        risk_score = max(0, min(100, risk_score))
        risk_color = "#22c55e" if risk_score >= 70 else "#f59e0b" if risk_score >= 40 else "#ef4444"
        risk_label = "低风险" if risk_score >= 70 else "中等风险" if risk_score >= 40 else "高风险"

        fig_gauge = go.Figure(
            go.Indicator(
                mode="gauge+number",
                value=risk_score,
                number={"suffix": "分", "font": {"size": 40, "color": risk_color}},
                gauge={
                    "axis": {"range": [0, 100], "tickcolor": "#8b949e", "tickfont": {"size": 10}},
                    "bar": {"color": risk_color},
                    "bgcolor": "#161b22",
                    "steps": [
                        {"range": [0, 40], "color": "rgba(239,68,68,0.15)"},
                        {"range": [40, 70], "color": "rgba(245,158,11,0.15)"},
                        {"range": [70, 100], "color": "rgba(34,197,94,0.15)"},
                    ],
                    "threshold": {"line": {"color": risk_color, "width": 3}, "thickness": 0.8, "value": risk_score},
                },
            )
        )
        fig_gauge.update_layout(
            height=250,
            plot_bgcolor="#0d1117",
            paper_bgcolor="#0d1117",
            font=dict(color="#c9d1d9"),
            margin=dict(l=30, r=30, t=10, b=10),
        )
        st.plotly_chart(fig_gauge, width="stretch")

        st.markdown(
            f'<div style="text-align:center;color:{risk_color};font-size:16px;font-weight:bold;">'
            f"{risk_label}</div>",
            unsafe_allow_html=True,
        )

    with col_risk_detail:
        st.markdown(
            '<div class="tip-title" style="">风险指标详情<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">展示夏普比率、Sortino比率、Calmar比率、最大回撤、年化波动率、胜率和盈亏比等核心风险指标，悬停可查看指标含义。</span></div>',
            unsafe_allow_html=True,
        )

        # 计算扩展风险指标
        ext_risk = compute_extended_risk_metrics(end_date=selected_date)

        risk_metrics = [
            ("夏普比率", sharpe, "衡量风险调整后收益，>1为优秀"),
            ("Sortino比率", ext_risk.get("sortino", np.nan), "仅考虑下行波动的风险调整收益"),
            ("Calmar比率", ext_risk.get("calmar", np.nan), "年化收益 / 最大回撤，越高越好"),
            ("最大回撤", ext_risk.get("max_drawdown", max_dd), "历史最大亏损幅度"),
            ("年化波动率", ext_risk.get("annual_std", volatility), "收益率的标准差，越高越不稳定"),
            ("胜率", ext_risk.get("win_rate", np.nan), "盈利天数 / 有盈亏交易天数"),
            ("盈亏比", ext_risk.get("pl_ratio", np.nan), "平均盈利 / 平均亏损，>1为优"),
            ("最大连续盈利", ext_risk.get("max_consec_win", 0), "历史最长连续盈利天数"),
            ("最大连续亏损", ext_risk.get("max_consec_loss", 0), "历史最长连续亏损天数"),
            ("最大回撤持续", ext_risk.get("max_dd_duration", 0), "历史最长回撤恢复天数（净值低于峰值）"),
            ("偏度", ext_risk.get("skewness", np.nan), "收益率分布偏斜，正值为右偏"),
            ("峰度", ext_risk.get("kurtosis", np.nan), "收益率分布尾部厚度，>0为尖峰"),
            (
                "持仓盈亏比",
                f"{profit_count}:{loss_count}" if profit_count or loss_count else "N/A",
                f"盈利{profit_count}只 vs 亏损{loss_count}只",
            ),
            ("数据周期", f"{len(summary)}天" if not summary.empty else "N/A", "历史数据积累天数"),
        ]

        for name, value, desc in risk_metrics:
            if isinstance(value, float) and not np.isnan(value):
                val_str = f"{value:.3f}" if abs(value) < 1 else f"{value:.2f}"
            elif value is None or (isinstance(value, float) and np.isnan(value)):
                val_str = '<span style="color:#888;">N/A</span>'
            else:
                val_str = str(value)

            st.markdown(
                f'<div style="padding:8px 12px;border-bottom:1px solid #21262d;">'
                f'<div style="display:flex;justify-content:space-between;">'
                f'<span style="color:#8b949e;font-size:13px;">{name}</span>'
                f'<span style="color:#c9d1d9;font-size:13px;font-weight:bold;">{val_str}</span>'
                f"</div>"
                f'<div style="font-size:11px;color:#484f58;">{desc}</div>'
                f"</div>",
                unsafe_allow_html=True,
            )

    # 回撤曲线（降采样）
    if not summary.empty and len(summary) > 5:
        st.markdown(
            '<div class="tip-title" style="">回撤曲线<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">展示组合从历史最高点到当前市值的回撤幅度(%)。可识别最大回撤区间及其恢复时间，评估组合的抗风险能力。</span></div>',
            unsafe_allow_html=True,
        )
        dd_data = summary[["date", "total_value"]].copy()
        dd_data["drawdown"] = (
            (dd_data["total_value"] - dd_data["total_value"].cummax()) / dd_data["total_value"].cummax() * 100
        )
        dd_chart = downsample(dd_data, max_points=500)

        fig_dd = go.Figure()
        fig_dd.add_trace(
            go.Scatter(
                x=dd_chart["date"],
                y=dd_chart["drawdown"],
                mode="lines",
                name="回撤",
                fill="tozeroy",
                line=dict(color="#ef4444", width=1.5),
                fillcolor="rgba(239,68,68,0.15)",
            )
        )
        # 标记最大回撤
        _add_min_max_annotations(fig_dd, dd_chart["date"], dd_chart["drawdown"], y_label="回撤")

        fig_dd.update_layout(
            height=200,
            plot_bgcolor="#0d1117",
            paper_bgcolor="#0d1117",
            font=dict(color="#c9d1d9", size=11),
            margin=dict(l=50, r=20, t=10, b=40),
            xaxis=dict(showgrid=False),
            yaxis=dict(title="回撤 (%)", showgrid=True, gridcolor="#21262d"),
        )
        st.plotly_chart(fig_dd, width="stretch")

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
        st.plotly_chart(fig_wf, width="stretch")

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
                st.plotly_chart(fig_beta, width="stretch")

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
                        st.plotly_chart(fig_pie, width="stretch")

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

    # ---------- 风险提示面板 ----------
    if not positions.empty:
        st.markdown(
            '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">风险提示<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">基于当前持仓结构和风险指标，自动识别并提示需要关注的风险因素。</span></div>',
            unsafe_allow_html=True,
        )

        warnings = []
        import math

        # 1. 集中度风险 - 单一持仓占比过高
        if not positions.empty:
            total_mv = positions["market_value"].sum()
            max_pos = positions.loc[positions["market_value"].idxmax()]
            max_weight = max_pos["market_value"] / total_mv * 100 if total_mv > 0 else 0
            if max_weight > 30:
                warnings.append(
                    (
                        "🔴",
                        "集中度风险",
                        f'「{max_pos["name"]}」占比 {max_weight:.1f}%，超过30%阈值，建议适当分散降低单一持仓集中度',
                    )
                )
            elif max_weight > 20:
                warnings.append(
                    ("🟡", "集中度风险", f'「{max_pos["name"]}」占比 {max_weight:.1f}%，接近30%警戒线，需关注')
                )

            # 前3大持仓集中度
            top3_weight = positions.nlargest(3, "market_value")["market_value"].sum() / total_mv * 100
            if top3_weight > 60:
                warnings.append(("🟡", "集中度风险", f"前3大持仓合计占比 {top3_weight:.1f}%，集中度偏高"))

        # 2. Beta 系统性风险
        beta_available = positions[positions["beta"].notna() & (positions["beta"] > 0)]
        if not beta_available.empty:
            port_beta = (
                (beta_available["beta"] * beta_available["market_value"]).sum()
                / beta_available["market_value"].sum()
                if beta_available["market_value"].sum() > 0
                else 1.0
            )
            if port_beta > 1.2:
                warnings.append(("🟡", "Beta风险", f"组合加权Beta为 {port_beta:.2f}，高于市场1.0，系统性风险偏高"))
            elif port_beta < 0.8:
                warnings.append(
                    ("🔵", "Beta风险", f"组合加权Beta为 {port_beta:.2f}，低于市场1.0，防御性较强但可能错失上涨行情")
                )

        # 3. 回撤风险
        if max_dd and not np.isnan(max_dd):
            dd_pct = abs(max_dd)
            if dd_pct > 15:
                warnings.append(("🔴", "回撤风险", f"历史最大回撤 {dd_pct:.2f}%，超过15%警戒线，注意控制下行风险"))
            elif dd_pct > 10:
                warnings.append(("🟡", "回撤风险", f"历史最大回撤 {dd_pct:.2f}%，处于较高水平"))
            elif dd_pct > 5:
                warnings.append(("🔵", "回撤风险", f"历史最大回撤 {dd_pct:.2f}%，处于正常波动范围"))

        # 4. 波动率风险
        if volatility and not np.isnan(volatility):
            if volatility > 25:
                warnings.append(("🟡", "波动率风险", f"年化波动率 {volatility:.2f}%，组合波动较大，注意风险管理"))
            elif volatility < 8:
                warnings.append(("🔵", "波动率风险", f"年化波动率 {volatility:.2f}%，组合波动较低"))

        # 5. 胜率风险
        if profit_count is not None and loss_count is not None and (profit_count + loss_count) > 0:
            wr = profit_count / (profit_count + loss_count) * 100
            if wr < 40:
                warnings.append(("🟡", "胜率偏低", f"当前胜率 {wr:.1f}%，持仓中盈利标的占比较低"))
            elif wr > 70:
                warnings.append(("🟢", "胜率优异", f"当前胜率 {wr:.1f}%，持仓中大部分标的处于盈利状态"))

        # 6. 亏损标的预警
        loss_positions = positions[positions["pnl"] < 0]
        if not loss_positions.empty:
            max_loss = loss_positions.loc[loss_positions["pnl_rate"].idxmin()]
            if max_loss["pnl_rate"] < -15:
                warnings.append(
                    ("🔴", "个股预警", f'「{max_loss["name"]}」亏损 {max_loss["pnl_rate"]:.2f}%，建议关注止损')
                )
            elif len(loss_positions) > len(positions) * 0.5:
                warnings.append(
                    (
                        "🟡",
                        "持仓预警",
                        f"亏损标的有 {len(loss_positions)} 只，占比 {len(loss_positions)/len(positions)*100:.0f}%",
                    )
                )

        # 7. 总盈亏趋势
        total_pnl = positions["pnl"].sum()
        if total_pnl < 0:
            warnings.append(("🟡", "组合亏损", f"当前总盈亏 ¥{total_pnl:,.0f}，整体处于浮亏状态"))

        # 渲染风险提示
        if warnings:
            for icon, title, desc in warnings:
                # 根据等级设置背景色
                if "🔴" in icon:
                    bg_color = "rgba(239,68,68,0.08)"
                    border_color = "rgba(239,68,68,0.3)"
                elif "🟡" in icon:
                    bg_color = "rgba(245,158,11,0.08)"
                    border_color = "rgba(245,158,11,0.3)"
                else:
                    bg_color = "rgba(34,197,94,0.06)"
                    border_color = "rgba(34,197,94,0.2)"
                st.markdown(
                    f'<div style="background:{bg_color};border:1px solid {border_color};border-radius:6px;padding:10px 14px;margin-bottom:6px;">'
                    f'<div style="font-size:13px;font-weight:bold;color:#c9d1d9;">{icon} {title}</div>'
                    f'<div style="font-size:12px;color:#8b949e;margin-top:3px;">{desc}</div>'
                    f"</div>",
                    unsafe_allow_html=True,
                )
        else:
            st.markdown(
                '<div style="background:rgba(34,197,94,0.06);border:1px solid rgba(34,197,94,0.2);border-radius:6px;padding:12px 14px;">'
                '<div style="font-size:13px;color:#22c55e;font-weight:bold;">🟢 风险状况良好</div>'
                '<div style="font-size:12px;color:#8b949e;margin-top:3px;">当前未检测到显著风险因素，继续保持关注。</div>'
                "</div>",
                unsafe_allow_html=True,
            )

    # ===== P2c: 风格暴露分析 =====
    st.markdown("---")
    st.markdown(
        '<div class="tip-title" style="">风格暴露分析<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">基于持仓 ETF 的分类标签，加权计算组合在规模、风格、行业三个维度的暴露度。</span></div>',
        unsafe_allow_html=True,
    )

    try:
        from src.analysis.factor_attribution import compute_style_exposure

        style_exp = compute_style_exposure(positions, ETF_CATEGORIES)
        if style_exp:
            col_size, col_style, col_sect = st.columns([1, 1, 1])

            # 规模暴露
            with col_size:
                st.markdown(
                    '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">规模维度<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">基于持仓ETF的市值规模分类，计算大盘/中盘/小盘风格的暴露占比。</span></div>',
                    unsafe_allow_html=True,
                )
                size_exp = style_exp.get("size_exposure", {})
                if size_exp:
                    fig_size = go.Figure(
                        go.Pie(
                            labels=list(size_exp.keys()),
                            values=list(size_exp.values()),
                            marker_colors=["#58a6ff", "#f59e0b", "#a855f7"],
                            textinfo="label+percent",
                            textfont=dict(size=11, color="#c9d1d9"),
                            hole=0.5,
                        )
                    )
                    fig_size.update_layout(
                        paper_bgcolor="#0d1117",
                        plot_bgcolor="#0d1117",
                        height=220,
                        margin=dict(t=5, b=5, l=5, r=5),
                        showlegend=False,
                    )
                    st.plotly_chart(fig_size, width="stretch")

            # 风格暴露
            with col_style:
                st.markdown(
                    '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">风格维度<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">基于持仓ETF的风格标签，计算成长/价值/均衡风格的暴露占比。</span></div>',
                    unsafe_allow_html=True,
                )
                style_exp_d = style_exp.get("style_exposure", {})
                if style_exp_d:
                    fig_sty = go.Figure(
                        go.Pie(
                            labels=list(style_exp_d.keys()),
                            values=list(style_exp_d.values()),
                            marker_colors=["#22c55e", "#ef4444", "#8b949e"],
                            textinfo="label+percent",
                            textfont=dict(size=11, color="#c9d1d9"),
                            hole=0.5,
                        )
                    )
                    fig_sty.update_layout(
                        paper_bgcolor="#0d1117",
                        plot_bgcolor="#0d1117",
                        height=220,
                        margin=dict(t=5, b=5, l=5, r=5),
                        showlegend=False,
                    )
                    st.plotly_chart(fig_sty, width="stretch")

            # 行业暴露
            with col_sect:
                st.markdown(
                    '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">行业维度<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">基于持仓ETF的行业分类，计算各行业的暴露权重，展示组合的行业集中度。</span></div>',
                    unsafe_allow_html=True,
                )
                sector_exp = style_exp.get("sector_exposure", {})
                if sector_exp:
                    sec_labels = list(sector_exp.keys())[:8]
                    sec_values = list(sector_exp.values())[:8]
                    fig_sec = go.Figure(
                        go.Bar(
                            orientation="h",
                            y=sec_labels,
                            x=sec_values,
                            marker_color="#58a6ff",
                            text=[f"{v:.1f}%" for v in sec_values],
                            textposition="auto",
                            textfont=dict(size=10, color="#c9d1d9"),
                        )
                    )
                    fig_sec.update_layout(
                        xaxis=dict(title="权重%", gridcolor="#21262d", tickfont=dict(size=9, color="#8b949e")),
                        yaxis=dict(title="", tickfont=dict(size=9, color="#c9d1d9")),
                        paper_bgcolor="#0d1117",
                        plot_bgcolor="#0d1117",
                        height=220,
                        margin=dict(l=60, r=20, t=5, b=25),
                        bargap=0.3,
                    )
                    st.plotly_chart(fig_sec, width="stretch")

            # 风格雷达图
            size_e = style_exp.get("size_exposure", {})
            style_e = style_exp.get("style_exposure", {})
            if size_e or style_e:
                radar_cats = []
                radar_vals = []
                for k, v in size_e.items():
                    radar_cats.append(f"规模-{k}")
                    radar_vals.append(v)
                for k, v in style_e.items():
                    radar_cats.append(f"风格-{k}")
                    radar_vals.append(v)

                fig_radar_style = go.Figure(
                    go.Scatterpolar(
                        r=radar_vals,
                        theta=radar_cats,
                        fill="toself",
                        fillcolor="rgba(88,166,255,0.15)",
                        line=dict(color="#58a6ff", width=2),
                        marker=dict(size=6, color="#58a6ff"),
                    )
                )
                fig_radar_style.update_layout(
                    polar=dict(
                        radialaxis=dict(
                            visible=True,
                            tickfont=dict(size=9, color="#6e7681"),
                            gridcolor="#21262d",
                            range=[0, max(radar_vals) * 1.3] if radar_vals else [0, 100],
                        ),
                        angularaxis=dict(tickfont=dict(size=10, color="#c9d1d9"), gridcolor="#21262d"),
                        bgcolor="#0d1117",
                    ),
                    paper_bgcolor="#0d1117",
                    plot_bgcolor="#0d1117",
                    height=300,
                    margin=dict(t=10, b=10, l=10, r=10),
                    showlegend=False,
                )
                st.plotly_chart(fig_radar_style, width="stretch")
    except Exception as e:
        st.info(f"风格暴露分析暂不可用: {str(e)[:80]}")

    # ===== P2d: 行业轮动分析 =====
    st.markdown("---")
    st.markdown(
        '<div class="tip-title" style="">行业轮动分析<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">追踪各行业指数在不同时间窗口的收益排名变化，衡量市场轮动速度。</span></div>',
        unsafe_allow_html=True,
    )

    try:
        from src.analysis.factor_attribution import compute_sector_rotation

        conn_rot = get_db_connection()
        try:
            rotation = compute_sector_rotation(conn_rot)
        finally:
            conn_rot.close()

        if rotation and "error" not in rotation:
            # 轮动速度指标
            rot_speed = rotation.get("rotation_speed", {})
            if rot_speed:
                col_rs = st.columns(len(rot_speed))
                for ci, (period, speed) in enumerate(rot_speed.items()):
                    with col_rs[ci]:
                        st.metric(
                            f"轮动速度 ({period})", f"{speed:.1f}", help=f"行业收益标准差，值越大说明行业分化越明显"
                        )
                st.caption("轮动速度 = 行业收益率标准差，反映行业分化程度。高轮动速度意味着行业间表现差异大。")

            # 行业排名变化表
            period_returns = rotation.get("sector_period_returns", {})
            if period_returns:
                periods = list(period_returns.keys())
                # 取最近两个时段做对比
                if len(periods) >= 2:
                    p1, p2 = periods[0], periods[1]
                    r1 = period_returns.get(p1, {})
                    r2 = period_returns.get(p2, {})
                    all_sectors = sorted(set(list(r1.keys()) + list(r2.keys())))
                    table_rows = []
                    for sec in all_sectors:
                        ret1 = r1.get(sec, 0)
                        ret2 = r2.get(sec, 0)
                        rank1 = sorted(r1.items(), key=lambda x: -x[1])
                        rank2 = sorted(r2.items(), key=lambda x: -x[1])
                        rk1 = next((i + 1 for i, (k, _) in enumerate(rank1) if k == sec), "-")
                        rk2 = next((i + 1 for i, (k, _) in enumerate(rank2) if k == sec), "-")
                        rank_change = ""
                        if isinstance(rk1, int) and isinstance(rk2, int):
                            diff = rk1 - rk2
                            if diff > 0:
                                rank_change = f'<span style="color:#22c55e">↑{diff}</span>'
                            elif diff < 0:
                                rank_change = f'<span style="color:#ef4444">↓{abs(diff)}</span>'
                            else:
                                rank_change = "-"
                        table_rows.append(
                            {
                                "行业/指数": sec,
                                f"{p1}收益": f"{ret1:+.2f}%",
                                f"{p1}排名": rk1,
                                f"{p2}收益": f"{ret2:+.2f}%",
                                f"{p2}排名": rk2,
                                "排名变化": rank_change,
                            }
                        )
                    if table_rows:
                        st.markdown(
                            pd.DataFrame(table_rows).to_html(index=False, escape=False), unsafe_allow_html=True
                        )
    except Exception as e:
        st.info(f"行业轮动分析暂不可用: {str(e)[:80]}")

    # ========== 告警中心 ==========
    st.markdown("---")
    alert_tab1, alert_tab2 = st.tabs(["🔔 告警中心", "📊 告警统计"])

    with alert_tab1:
        st.markdown(
            '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">' "实时告警监控<span class=\"tip-arrow\" style=\"left: 4px; top: calc(100% + 5px);\"></span><span class=\"tip-text\" style=\"left: 4px; top: calc(100% + 10px);\">基于持仓数据实时检测组合异常波动，自动触发暴跌、回撤、集中度等风险告警。</span></div>",
            unsafe_allow_html=True,
        )

        realtime_alerts = []
        if not positions.empty and not summary.empty:
            ls = summary.iloc[-1]
            dr = ls.get("daily_return", 0)
            if dr and not np.isnan(dr) and dr < -3:
                realtime_alerts.append(
                    {
                        "level": "error",
                        "rule": "单日暴跌",
                        "message": f"组合单日跌幅 {dr:.2f}%，超过3%警戒线",
                        "time": selected_date,
                    }
                )
            mdd = ls.get("max_drawdown", 0)
            if mdd and not np.isnan(mdd) and abs(mdd) > 15:
                realtime_alerts.append(
                    {
                        "level": "error",
                        "rule": "深度回撤",
                        "message": f"最大回撤 {abs(mdd):.2f}%，超过15%警戒线",
                        "time": selected_date,
                    }
                )
            elif mdd and not np.isnan(mdd) and abs(mdd) > 10:
                realtime_alerts.append(
                    {
                        "level": "warning",
                        "rule": "回撤预警",
                        "message": f"最大回撤 {abs(mdd):.2f}%，超过10%关注线",
                        "time": selected_date,
                    }
                )
            vol_val = ls.get("volatility", 0)
            if vol_val and not np.isnan(vol_val) and vol_val > 30:
                realtime_alerts.append(
                    {
                        "level": "warning",
                        "rule": "波动飙升",
                        "message": f"年化波动率 {vol_val:.2f}%，超过30%警戒线",
                        "time": selected_date,
                    }
                )
            sp = ls.get("sharpe_ratio", 0)
            if sp is not None and not np.isnan(sp) and sp < 0:
                realtime_alerts.append(
                    {
                        "level": "warning",
                        "rule": "夏普异常",
                        "message": f"夏普比率 {sp:.3f}，风险调整后收益为负",
                        "time": selected_date,
                    }
                )
            for _, pos in positions.iterrows():
                pr = pos.get("pnl_rate", 0)
                if pr < -20:
                    realtime_alerts.append(
                        {
                            "level": "error",
                            "rule": "个股暴跌",
                            "message": f'「{pos["name"]}」亏损 {pr:.2f}%，超过20%止损线',
                            "time": selected_date,
                        }
                    )
                elif pr < -15:
                    realtime_alerts.append(
                        {
                            "level": "warning",
                            "rule": "个股预警",
                            "message": f'「{pos["name"]}」亏损 {pr:.2f}%，接近止损线',
                            "time": selected_date,
                        }
                    )
            if not positions.empty:
                total_mv = positions["market_value"].sum()
                max_w = positions["market_value"].max() / total_mv * 100 if total_mv > 0 else 0
                if max_w > 30:
                    max_name = positions.loc[positions["market_value"].idxmax(), "name"]
                    realtime_alerts.append(
                        {
                            "level": "warning",
                            "rule": "集中度风险",
                            "message": f"「{max_name}」占比 {max_w:.1f}%，超过30%集中度警戒线",
                            "time": selected_date,
                        }
                    )

        if realtime_alerts:
            level_config = {
                "error": {
                    "bg": "rgba(239,68,68,0.08)",
                    "border": "rgba(239,68,68,0.3)",
                    "icon": "🔴",
                    "label": "严重",
                },
                "warning": {
                    "bg": "rgba(245,158,11,0.08)",
                    "border": "rgba(245,158,11,0.3)",
                    "icon": "🟡",
                    "label": "警告",
                },
                "info": {
                    "bg": "rgba(88,166,255,0.06)",
                    "border": "rgba(88,166,255,0.2)",
                    "icon": "🔵",
                    "label": "提示",
                },
            }
            level_order = {"error": 0, "warning": 1, "info": 2}
            realtime_alerts.sort(key=lambda x: level_order.get(x["level"], 99))
            for alert in realtime_alerts:
                cfg = level_config.get(alert["level"], level_config["info"])
                st.markdown(
                    f'<div style="background:{cfg["bg"]};border:1px solid {cfg["border"]};'
                    f'border-radius:6px;padding:8px 12px;margin-bottom:4px;">'
                    f'<div style="display:flex;justify-content:space-between;align-items:center;">'
                    f'<span style="font-size:12px;font-weight:bold;color:#c9d1d9;">'
                    f'{cfg["icon"]} [{cfg["label"]}] {alert["rule"]}</span>'
                    f'<span style="font-size:10px;color:#484f58;">{alert["time"]}</span></div>'
                    f'<div style="font-size:12px;color:#8b949e;margin-top:2px;">{alert["message"]}</div></div>',
                    unsafe_allow_html=True,
                )
            n_error = sum(1 for a in realtime_alerts if a["level"] == "error")
            n_warning = sum(1 for a in realtime_alerts if a["level"] == "warning")
            st.markdown(
                f'<div style="font-size:11px;color:#484f58;padding:4px 0;">'
                f'当前触发: <span style="color:#ef4444;font-weight:bold;">{n_error} 严重</span> / '
                f'<span style="color:#f59e0b;font-weight:bold;">{n_warning} 警告</span></div>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                '<div style="background:rgba(34,197,94,0.06);border:1px solid rgba(34,197,94,0.2);'
                'border-radius:6px;padding:10px 14px;">'
                '<div style="font-size:13px;color:#22c55e;font-weight:bold;">🟢 告警状态正常</div>'
                '<div style="font-size:12px;color:#8b949e;margin-top:3px;">'
                "当前未触发任何告警规则，所有指标处于安全范围内。</div></div>",
                unsafe_allow_html=True,
            )

        with st.expander("查看历史告警记录", expanded=False):
            hist_alerts = load_alerts(limit=20)
            if not hist_alerts.empty:
                for _, ha in hist_alerts.iterrows():
                    ha_level = ha.get("level", "info")
                    ha_cfg = {"error": {"icon": "🔴"}, "warning": {"icon": "🟡"}, "info": {"icon": "🔵"}}.get(
                        ha_level, {"icon": "🔵"}
                    )
                    ack = "✅" if ha.get("acknowledged") else ""
                    st.markdown(
                        f'<div style="font-size:12px;padding:3px 0;color:#8b949e;">'
                        f'{ha_cfg["icon"]} <span style="color:#c9d1d9;">{ha.get("rule_name", "未知")}</span> '
                        f'{ha.get("message", "")} <span style="color:#484f58;font-size:10px;">{ha.get("created_at", "")}</span> {ack}</div>',
                        unsafe_allow_html=True,
                    )
            else:
                st.caption("暂无历史告警记录")

    with alert_tab2:
        st.markdown(
            '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">告警规则配置与统计<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">展示全部8条内置告警规则及当前触发状态，支持按严重级别筛选查看。</span></div>',
            unsafe_allow_html=True,
        )

        rules_display = [
            {"name": "单日暴跌", "condition": "日收益率 < -3%", "level": "严重"},
            {"name": "深度回撤", "condition": "最大回撤 > 15%", "level": "严重"},
            {"name": "回撤预警", "condition": "最大回撤 > 10%", "level": "警告"},
            {"name": "个股暴跌", "condition": "单一ETF亏损 > 20%", "level": "严重"},
            {"name": "个股预警", "condition": "单一ETF亏损 > 15%", "level": "警告"},
            {"name": "波动飙升", "condition": "年化波动率 > 30%", "level": "警告"},
            {"name": "夏普异常", "condition": "夏普比率 < 0", "level": "警告"},
            {"name": "集中度风险", "condition": "单一持仓占比 > 30%", "level": "警告"},
        ]
        html_rules = (
            '<div style="overflow-x:auto;"><table style="width:100%;border-collapse:collapse;font-size:12px;">'
            '<thead><tr style="background:#161b22;">'
            '<th style="padding:6px 10px;color:#8b949e;text-align:left;font-size:11px;">状态</th>'
            '<th style="padding:6px 10px;color:#8b949e;text-align:left;font-size:11px;">规则名称</th>'
            '<th style="padding:6px 10px;color:#8b949e;text-align:left;font-size:11px;">触发条件</th>'
            '<th style="padding:6px 10px;color:#8b949e;text-align:center;font-size:11px;">级别</th>'
            "</tr></thead><tbody>"
        )
        for rule in rules_display:
            triggered = any(a["rule"] == rule["name"] for a in realtime_alerts) if realtime_alerts else False
            status_html = (
                '<span style="color:#ef4444;">触发</span>'
                if triggered
                else '<span style="color:#22c55e;">正常</span>'
            )
            level_color = "#ef4444" if rule["level"] == "严重" else "#f59e0b"
            html_rules += (
                f'<tr style="border-bottom:1px solid #21262d;">'
                f'<td style="padding:5px 10px;">{status_html}</td>'
                f'<td style="padding:5px 10px;color:#c9d1d9;">{rule["name"]}</td>'
                f'<td style="padding:5px 10px;color:#8b949e;">{rule["condition"]}</td>'
                f'<td style="padding:5px 10px;text-align:center;color:{level_color};font-weight:bold;">{rule["level"]}</td></tr>'
            )
        html_rules += "</tbody></table></div>"
        st.markdown(html_rules, unsafe_allow_html=True)

        hist_alerts = load_alerts(limit=50)
        if not hist_alerts.empty:
            ac1, ac2, ac3 = st.columns(3)
            with ac1:
                st.metric("历史告警总数", f"{len(hist_alerts)} 条")
            with ac2:
                st.metric("严重告警", f"{len(hist_alerts[hist_alerts['level'] == 'error'])} 条")
            with ac3:
                st.metric("警告告警", f"{len(hist_alerts[hist_alerts['level'] == 'warning'])} 条")

            rule_counts = hist_alerts["rule_name"].value_counts()
            if not rule_counts.empty:
                fig_alert_dist = go.Figure(
                    go.Bar(
                        y=rule_counts.index,
                        x=rule_counts.values,
                        orientation="h",
                        marker_color="#f59e0b",
                        text=[str(v) for v in rule_counts.values],
                        textposition="outside",
                        textfont=dict(size=10, color="#c9d1d9"),
                    )
                )
                fig_alert_dist.update_layout(
                    height=max(200, len(rule_counts) * 30),
                    plot_bgcolor="#0d1117",
                    paper_bgcolor="#0d1117",
                    font=dict(color="#c9d1d9", size=11),
                    margin=dict(l=100, r=40, t=10, b=20),
                    xaxis=dict(showgrid=True, gridcolor="#21262d"),
                    yaxis=dict(showgrid=False, tickfont=dict(size=10)),
                )
                st.plotly_chart(fig_alert_dist, width="stretch")

