"""Tab3 告警中心子模块 — 实时告警检测、告警仪表盘、趋势分析"""

from components.ui import render_chart
import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from data_loader import load_alerts


# ==================== 常量区 ====================

ALERT_LEVEL_ORDER = {"error": 0, "warning": 1, "info": 2}

ALERT_LEVEL_CONFIG = {
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

ALERT_RULES = [
    {"name": "单日暴跌", "condition": "日收益率 < -3%", "level": "严重"},
    {"name": "深度回撤", "condition": "最大回撤 > 15%", "level": "严重"},
    {"name": "回撤预警", "condition": "最大回撤 > 10%", "level": "警告"},
    {"name": "个股暴跌", "condition": "单一ETF亏损 > 20%", "level": "严重"},
    {"name": "个股预警", "condition": "单一ETF亏损 > 15%", "level": "警告"},
    {"name": "波动飙升", "condition": "年化波动率 > 30%", "level": "警告"},
    {"name": "夏普异常", "condition": "夏普比率 < 0", "level": "警告"},
    {"name": "集中度风险", "condition": "单一持仓占比 > 30%", "level": "警告"},
]

HIST_LEVEL_ICONS = {"error": {"icon": "🔴"}, "warning": {"icon": "🟡"}, "info": {"icon": "🔵"}}


# ==================== 纯数据函数 ====================

def _detect_realtime_alerts(positions, summary, selected_date):
    """检测实时告警规则，返回告警列表（纯数据，无UI）。

    覆盖 6 条规则：单日暴跌、深度回撤/回撤预警、波动飙升、夏普异常、
    个股暴跌/个股预警、集中度风险。
    """
    alerts = []
    if positions.empty or summary.empty:
        return alerts

    ls = summary.iloc[-1]

    # 规则1: 单日暴跌
    dr = ls.get("daily_return", 0)
    if dr and not np.isnan(dr) and dr < -3:
        alerts.append({
            "level": "error", "rule": "单日暴跌",
            "message": f"组合单日跌幅 {dr:.2f}%，超过3%警戒线",
            "time": selected_date,
        })

    # 规则2/3: 深度回撤 / 回撤预警
    mdd = ls.get("max_drawdown", 0)
    if mdd and not np.isnan(mdd) and abs(mdd) > 15:
        alerts.append({
            "level": "error", "rule": "深度回撤",
            "message": f"最大回撤 {abs(mdd):.2f}%，超过15%警戒线",
            "time": selected_date,
        })
    elif mdd and not np.isnan(mdd) and abs(mdd) > 10:
        alerts.append({
            "level": "warning", "rule": "回撤预警",
            "message": f"最大回撤 {abs(mdd):.2f}%，超过10%关注线",
            "time": selected_date,
        })

    # 规则4: 波动飙升
    vol_val = ls.get("volatility", 0)
    if vol_val and not np.isnan(vol_val) and vol_val > 30:
        alerts.append({
            "level": "warning", "rule": "波动飙升",
            "message": f"年化波动率 {vol_val:.2f}%，超过30%警戒线",
            "time": selected_date,
        })

    # 规则5: 夏普异常
    sp = ls.get("sharpe_ratio", 0)
    if sp is not None and not np.isnan(sp) and sp < 0:
        alerts.append({
            "level": "warning", "rule": "夏普异常",
            "message": f"夏普比率 {sp:.3f}，风险调整后收益为负",
            "time": selected_date,
        })

    # 规则6/7: 个股暴跌 / 个股预警
    for _, pos in positions.iterrows():
        pr = pos.get("pnl_rate", 0)
        if pr < -20:
            alerts.append({
                "level": "error", "rule": "个股暴跌",
                "message": f'「{pos["name"]}」亏损 {pr:.2f}%，超过20%止损线',
                "time": selected_date,
            })
        elif pr < -15:
            alerts.append({
                "level": "warning", "rule": "个股预警",
                "message": f'「{pos["name"]}」亏损 {pr:.2f}%，接近止损线',
                "time": selected_date,
            })

    # 规则8: 集中度风险
    total_mv = positions["market_value"].sum()
    if total_mv > 0:
        max_w = positions["market_value"].max() / total_mv * 100
        if max_w > 30:
            max_name = positions.loc[positions["market_value"].idxmax(), "name"]
            alerts.append({
                "level": "warning", "rule": "集中度风险",
                "message": f"「{max_name}」占比 {max_w:.1f}%，超过30%集中度警戒线",
                "time": selected_date,
            })

    alerts.sort(key=lambda x: ALERT_LEVEL_ORDER.get(x["level"], 99))
    return alerts


# ==================== UI 子组件 ====================

def _render_alert_banner(alert):
    """渲染单条告警横幅。"""
    cfg = ALERT_LEVEL_CONFIG.get(alert["level"], ALERT_LEVEL_CONFIG["info"])
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


def _render_alert_summary(realtime_alerts):
    """渲染告警统计摘要。"""
    n_error = sum(1 for a in realtime_alerts if a["level"] == "error")
    n_warning = sum(1 for a in realtime_alerts if a["level"] == "warning")
    st.markdown(
        f'<div style="font-size:11px;color:#484f58;padding:4px 0;">'
        f'当前触发: <span style="color:#ef4444;font-weight:bold;">{n_error} 严重</span> / '
        f'<span style="color:#f59e0b;font-weight:bold;">{n_warning} 警告</span></div>',
        unsafe_allow_html=True,
    )


def _render_all_clear():
    """渲染无告警状态。"""
    st.markdown(
        '<div style="background:rgba(34,197,94,0.06);border:1px solid rgba(34,197,94,0.2);'
        'border-radius:6px;padding:10px 14px;">'
        '<div style="font-size:13px;color:#22c55e;font-weight:bold;">🟢 告警状态正常</div>'
        '<div style="font-size:12px;color:#8b949e;margin-top:3px;">'
        "当前未触发任何告警规则，所有指标处于安全范围内。</div></div>",
        unsafe_allow_html=True,
    )


def _render_historical_alerts(limit=20):
    """统一历史记录 expander，消除重复代码。"""
    with st.expander("查看历史告警记录", expanded=False):
        hist_alerts = load_alerts(limit=limit)
        if not hist_alerts.empty:
            for _, ha in hist_alerts.iterrows():
                ha_level = ha.get("level", "info")
                ha_cfg = HIST_LEVEL_ICONS.get(ha_level, {"icon": "🔵"})
                ack = "✅" if ha.get("acknowledged") else ""
                st.markdown(
                    f'<div style="font-size:12px;padding:3px 0;color:#8b949e;">'
                    f'{ha_cfg["icon"]} <span style="color:#c9d1d9;">{ha.get("rule_name", "未知")}</span> '
                    f'{ha.get("message", "")} <span style="color:#484f58;font-size:10px;">{ha.get("created_at", "")}</span> {ack}</div>',
                    unsafe_allow_html=True,
                )
        else:
            st.caption("暂无历史告警记录")


# ==================== Tab 内容渲染 ====================

def _render_alert_tab_realtime(positions, summary, selected_date):
    """告警中心 Tab 内容。"""
    st.markdown(
        '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">'
        "实时告警监控<span class=\"tip-arrow\" style=\"left: 4px; top: calc(100% + 5px);\"></span>"
        "<span class=\"tip-text\" style=\"left: 4px; top: calc(100% + 10px);\">"
        "基于持仓数据实时检测组合异常波动，自动触发暴跌、回撤、集中度等风险告警。</span></div>",
        unsafe_allow_html=True,
    )

    realtime_alerts = _detect_realtime_alerts(positions, summary, selected_date)

    if realtime_alerts:
        for alert in realtime_alerts:
            _render_alert_banner(alert)
        _render_alert_summary(realtime_alerts)
    else:
        _render_all_clear()
        _render_alert_gauge_dashboard(positions, summary)

    _render_historical_alerts(limit=20)


def _render_alert_tab_statistics(realtime_alerts):
    """告警统计 Tab 内容。"""
    st.markdown(
        '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">'
        '告警规则配置与统计<span class=\"tip-arrow\" style=\"left: 4px; top: calc(100% + 5px);\"></span>'
        '<span class=\"tip-text\" style=\"left: 4px; top: calc(100% + 10px);\">'
        '展示全部8条内置告警规则及当前触发状态，支持按严重级别筛选查看。</span></div>',
        unsafe_allow_html=True,
    )

    # 规则状态表
    html_rules = (
        '<div style="overflow-x:auto;"><table style="width:100%;border-collapse:collapse;font-size:12px;">'
        '<thead><tr style="background:#161b22;">'
        '<th style="padding:6px 10px;color:#8b949e;text-align:left;font-size:11px;">状态</th>'
        '<th style="padding:6px 10px;color:#8b949e;text-align:left;font-size:11px;">规则名称</th>'
        '<th style="padding:6px 10px;color:#8b949e;text-align:left;font-size:11px;">触发条件</th>'
        '<th style="padding:6px 10px;color:#8b949e;text-align:center;font-size:11px;">级别</th>'
        "</tr></thead><tbody>"
    )
    for rule in ALERT_RULES:
        triggered = any(a["rule"] == rule["name"] for a in realtime_alerts) if realtime_alerts else False
        status_html = (
            '<span style="color:#ef4444;">触发</span>' if triggered
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

    # 历史统计
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
                plot_bgcolor="#0d1117", paper_bgcolor="#0d1117",
                font=dict(color="#c9d1d9", size=11),
                margin=dict(l=100, r=40, t=10, b=20),
                xaxis=dict(showgrid=True, gridcolor="#21262d"),
                yaxis=dict(showgrid=False, tickfont=dict(size=10)),
            )
            render_chart(fig_alert_dist)
        _render_alert_trend_analysis(hist_alerts)


# ==================== 编排函数 ====================

def _render_alert_center(positions, summary, selected_date):
    """告警中心编排函数 — 委托给子函数。"""
    st.markdown("---")
    alert_tab1, alert_tab2 = st.tabs(["🔔 告警中心", "📊 告警统计"])

    with alert_tab1:
        _render_alert_tab_realtime(positions, summary, selected_date)

    with alert_tab2:
        realtime_alerts = _detect_realtime_alerts(positions, summary, selected_date)
        _render_alert_tab_statistics(realtime_alerts)


# ==================== 仪表盘 & 趋势（大函数，保持独立） ====================

def _render_alert_gauge_dashboard(positions, summary):
    """Phase 8B: 告警阈值仪表盘 & 健康评分"""
    st.markdown("---")
    st.markdown(
        '<div class="tip-title" style="font-size:13px;border-bottom:none;padding:3px 0;">'
        '指标阈值监控<span class="tip-arrow" style="left:4px;top:calc(100% + 5px);"></span>'
        '<span class="tip-text" style="left:4px;top:calc(100% + 10px);">'
        '展示各关键指标当前值与告警阈值的距离，绿色=安全，黄色=接近阈值，红色=已触发。</span></div>',
        unsafe_allow_html=True,
    )

    gauge_metrics = []
    if not positions.empty and not summary.empty:
        ls = summary.iloc[-1]
        dr = ls.get("daily_return", 0) or 0
        mdd = abs(ls.get("max_drawdown", 0) or 0)
        vol = ls.get("volatility", 0) or 0
        sp = ls.get("sharpe_ratio", 0) or 0
        total_mv = positions["market_value"].sum()
        max_w = positions["market_value"].max() / total_mv * 100 if total_mv > 0 else 0
        gauge_metrics = [
            {"name": "日收益率", "value": dr, "warn": -3, "error": -5, "unit": "%", "lower_is_worse": True},
            {"name": "最大回撤", "value": mdd, "warn": 10, "error": 15, "unit": "%", "lower_is_worse": False},
            {"name": "年化波动率", "value": vol, "warn": 30, "error": 40, "unit": "%", "lower_is_worse": False},
            {"name": "夏普比率", "value": sp, "warn": 0, "error": -0.5, "unit": "", "lower_is_worse": True},
            {"name": "集中度", "value": max_w, "warn": 30, "error": 40, "unit": "%", "lower_is_worse": False},
        ]

    if gauge_metrics:
        gc1, gc2, gc3, gc4, gc5 = st.columns(5)
        health_score = 100
        for idx_g, gm in enumerate(gauge_metrics):
            val = gm["value"]
            if gm["lower_is_worse"]:
                if val <= gm["error"]:
                    status, color = "严重", "#ef4444"; health_score -= 20
                elif val <= gm["warn"]:
                    status, color = "警告", "#f59e0b"; health_score -= 8
                else:
                    status, color = "正常", "#22c55e"
            else:
                if val >= gm["error"]:
                    status, color = "严重", "#ef4444"; health_score -= 20
                elif val >= gm["warn"]:
                    status, color = "警告", "#f59e0b"; health_score -= 8
                else:
                    status, color = "正常", "#22c55e"
            fig_gauge = go.Figure(go.Indicator(
                mode="gauge+number",
                value=abs(val),
                domain={"x": [0, 1], "y": [0, 1]},
                title={"text": f'{gm["name"]} ({status})', "font": {"size": 11, "color": color}},
                number={"suffix": gm["unit"], "font": {"size": 14, "color": color}},
                gauge={
                    "axis": {"range": [0, max(gm["error"] * 1.5, 1)], "tickfont": {"size": 8, "color": "#8b949e"}},
                    "bar": {"color": color},
                    "threshold": {"line": {"color": "#f59e0b", "width": 2}, "thickness": 0.75, "value": gm["warn"]},
                    "bgcolor": "#0d1117", "borderwidth": 0,
                },
            ))
            fig_gauge.update_layout(
                height=160, margin=dict(l=10, r=10, t=40, b=10),
                plot_bgcolor="#0d1117", paper_bgcolor="#0d1117",
                font=dict(color="#c9d1d9"),
            )
            with [gc1, gc2, gc3, gc4, gc5][idx_g]:
                render_chart(fig_gauge)

        health_score = max(0, min(100, health_score))
        hc = "#22c55e" if health_score >= 80 else "#f59e0b" if health_score >= 60 else "#ef4444"
        st.markdown(
            f'<div style="text-align:center;padding:12px;border-radius:8px;'
            f'background:{"rgba(34,197,94,0.06)" if health_score >= 80 else "rgba(245,158,11,0.06)" if health_score >= 60 else "rgba(239,68,68,0.06)"};'
            f'border:1px solid {hc}30;">'
            f'<span style="font-size:28px;font-weight:bold;color:{hc};">{health_score}</span>'
            f'<span style="font-size:14px;color:#8b949e;margin-left:8px;">/100 告警健康评分</span>'
            f'<div style="font-size:11px;color:#484f58;margin-top:4px;">'
            f'{"组合整体风险可控" if health_score >= 80 else "存在风险预警，建议关注" if health_score >= 60 else "多项指标触发告警，需立即关注"}</div></div>',
            unsafe_allow_html=True,
        )


def _render_alert_trend_analysis(hist_alerts):
    """Phase 8A: 告警趋势增强"""
    if not hist_alerts.empty and "created_at" in hist_alerts.columns:
        hist_alerts["created_at"] = pd.to_datetime(hist_alerts["created_at"], errors="coerce")
        hist_valid = hist_alerts.dropna(subset=["created_at"])

        if not hist_valid.empty:
            atc1, atc2 = st.columns(2)

            with atc1:
                st.markdown(
                    '<div class="tip-title" style="font-size:13px;border-bottom:none;padding:3px 0;">'
                    '告警时间线<span class="tip-arrow" style="left:4px;top:calc(100% + 5px);"></span>'
                    '<span class="tip-text" style="left:4px;top:calc(100% + 10px);">'
                    '按时间展示历史告警事件，红色为严重，橙色为警告。</span></div>',
                    unsafe_allow_html=True,
                )
                level_colors = {"error": "#ef4444", "warning": "#f59e0b", "info": "#58a6ff"}
                fig_timeline = go.Figure()
                for lvl, clr in level_colors.items():
                    subset = hist_valid[hist_valid["level"] == lvl]
                    if not subset.empty:
                        fig_timeline.add_trace(go.Scatter(
                            x=subset["created_at"],
                            y=[lvl] * len(subset),
                            mode="markers",
                            name={"error": "严重", "warning": "警告", "info": "提示"}.get(lvl, lvl),
                            marker=dict(size=10, color=clr, symbol="circle"),
                            hovertemplate="%{x|%Y-%m-%d %H:%M}<br>%{text}<extra></extra>",
                            text=subset["message"].str[:60],
                            showlegend=True,
                        ))
                fig_timeline.update_layout(
                    height=200,
                    plot_bgcolor="#0d1117", paper_bgcolor="#0d1117",
                    font=dict(color="#c9d1d9", size=10),
                    margin=dict(l=50, r=20, t=10, b=30),
                    xaxis=dict(showgrid=True, gridcolor="#21262d", tickfont=dict(size=9)),
                    yaxis=dict(showgrid=False, tickfont=dict(size=10)),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02,
                                font=dict(size=9, color="#8b949e")),
                )
                render_chart(fig_timeline)

            with atc2:
                st.markdown(
                    '<div class="tip-title" style="font-size:13px;border-bottom:none;padding:3px 0;">'
                    '告警频率热力图<span class="tip-arrow" style="left:4px;top:calc(100% + 5px);"></span>'
                    '<span class="tip-text" style="left:4px;top:calc(100% + 10px);">'
                    '展示告警在不同星期和时段的分布密度，帮助识别高风险时段。</span></div>',
                    unsafe_allow_html=True,
                )
                hist_valid_copy = hist_valid.copy()
                hist_valid_copy["dow"] = hist_valid_copy["created_at"].dt.dayofweek
                hist_valid_copy["hour"] = hist_valid_copy["created_at"].dt.hour
                dow_labels = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
                hour_bins = [0, 6, 12, 18, 24]
                hour_labels = ["凌晨", "上午", "下午", "晚间"]
                hist_valid_copy["hour_bin"] = pd.cut(hist_valid_copy["hour"], bins=hour_bins, labels=hour_labels, right=False)
                heatmap_data = hist_valid_copy.groupby(["dow", "hour_bin"], observed=False).size().unstack(fill_value=0)
                heatmap_data.index = [dow_labels[i] if i < len(dow_labels) else str(i) for i in heatmap_data.index]
                if not heatmap_data.empty:
                    fig_heatmap = go.Figure(go.Heatmap(
                        z=heatmap_data.values,
                        x=list(heatmap_data.columns),
                        y=list(heatmap_data.index),
                        colorscale=[[0, "#0d1117"], [0.5, "#1a2332"], [1, "#f59e0b"]],
                        showscale=True,
                        text=heatmap_data.values,
                        texttemplate="%{text}",
                        textfont=dict(size=11, color="#c9d1d9"),
                        hovertemplate="%{y} %{x}: %{z} 次告警<extra></extra>",
                    ))
                    fig_heatmap.update_layout(
                        height=200,
                        plot_bgcolor="#0d1117", paper_bgcolor="#0d1117",
                        font=dict(color="#c9d1d9", size=10),
                        margin=dict(l=50, r=40, t=10, b=30),
                        xaxis=dict(showgrid=False, tickfont=dict(size=10)),
                        yaxis=dict(showgrid=False, tickfont=dict(size=10)),
                    )
                    render_chart(fig_heatmap)

    # 规则触发趋势（按周统计堆叠面积图）
    if not hist_alerts.empty and "created_at" in hist_alerts.columns:
        hist_valid2 = hist_alerts.dropna(subset=["created_at"]).copy()
        if len(hist_valid2) >= 2:
            hist_valid2["week"] = hist_valid2["created_at"].dt.isocalendar().week.astype(int)
            hist_valid2["year"] = hist_valid2["created_at"].dt.year
            hist_valid2["week_label"] = hist_valid2["year"].astype(str) + "W" + hist_valid2["week"].astype(str).str.zfill(2)
            weekly = hist_valid2.groupby(["week_label", "rule_name"]).size().unstack(fill_value=0)
            if len(weekly) >= 2:
                st.markdown(
                    '<div class="tip-title" style="font-size:13px;border-bottom:none;padding:3px 0;">'
                    '规则触发趋势<span class="tip-arrow" style="left:4px;top:calc(100% + 5px);"></span>'
                    '<span class="tip-text" style="left:4px;top:calc(100% + 10px);">'
                    '按周统计各告警规则的触发次数，堆叠面积图展示趋势变化。</span></div>',
                    unsafe_allow_html=True,
                )
                trend_colors = ["#ef4444", "#f59e0b", "#58a6ff", "#22c55e", "#a855f7", "#ec4899", "#14b8a6", "#6366f1"]
                fig_trend = go.Figure()
                for ci, col in enumerate(weekly.columns):
                    fig_trend.add_trace(go.Scatter(
                        x=weekly.index, y=weekly[col],
                        mode="lines", stackgroup="one",
                        name=col, line=dict(width=0),
                        fillcolor=trend_colors[ci % len(trend_colors)],
                        hovertemplate="%{x}: %{y} 次<extra></extra>",
                    ))
                fig_trend.update_layout(
                    height=200,
                    plot_bgcolor="#0d1117", paper_bgcolor="#0d1117",
                    font=dict(color="#c9d1d9", size=10),
                    margin=dict(l=50, r=20, t=10, b=30),
                    xaxis=dict(showgrid=True, gridcolor="#21262d", tickfont=dict(size=9)),
                    yaxis=dict(showgrid=True, gridcolor="#21262d", title=dict(text="触发次数", font=dict(size=10))),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02,
                                font=dict(size=9, color="#8b949e")),
                )
                render_chart(fig_trend)
