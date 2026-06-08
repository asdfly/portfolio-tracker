#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""侧边栏与自定义样式模块 — 从 dashboard.py 拆分

包含侧边栏 UI 渲染和自定义 CSS 注入。
"""

from datetime import datetime

import pandas as pd
import streamlit as st

from config.settings import INDEX_CODES
from data_loader import load_execution_logs

def _inject_custom_css():
    """Inject custom dark-theme CSS styles."""
    # 自定义CSS
    st.markdown(
        """
        <style>
        .stApp { background-color: #0d1117; }
        .main-header {
            font-size: 28px; font-weight: bold; color: #58a6ff;
            text-align: center; padding: 20px 0 10px 0;
        }
        .sub-header {
            font-size: 14px; color: #8b949e; text-align: center; padding-bottom: 15px;
        }
        .section-title {
            font-size: 18px; font-weight: bold; color: #c9d1d9;
            padding: 10px 0 5px 0; border-bottom: 1px solid #30363d;
        }
        .tip-title {
            font-size: 18px; font-weight: bold; color: #c9d1d9;
            padding: 10px 0 5px 0; border-bottom: 1px solid #30363d;
            display: inline-block; cursor: help;
        }
        .tip-title::after {
            content: ' ℹ';
            font-size: 11px; color: #58a6ff; font-weight: normal;
        }
        .tip-title .tip-text {
            visibility: hidden; opacity: 0;
            position: absolute; z-index: 999;
            background: #1c2333; color: #c9d1d9;
            border: 1px solid #30363d; border-radius: 6px;
            padding: 8px 12px; font-size: 12px; font-weight: normal;
            line-height: 1.5; width: max-content; max-width: 360px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.4);
            transition: opacity 0.2s, visibility 0.2s;
            margin-top: 6px; margin-left: 0;
        }
        .tip-title:hover .tip-text {
            visibility: visible; opacity: 1;
        }
        .tip-title .tip-arrow {
            visibility: hidden; opacity: 0;
            position: absolute; z-index: 999;
            border-left: 6px solid transparent;
            border-right: 6px solid transparent;
            border-bottom: 6px solid #30363d;
            transition: opacity 0.2s, visibility 0.2s;
        }
        .tip-title:hover .tip-arrow {
            visibility: visible; opacity: 1;
        }


        .cal-table { border-collapse: collapse; margin: 0 auto; }
        .cal-table th { padding: 6px 8px; font-size: 12px; color: #8b949e; font-weight: normal; }
        .cal-table td {
            width: 50px; height: 44px; text-align: center; vertical-align: middle;
            border: 1px solid #21262d; border-radius: 4px; cursor: default;
            position: relative; padding: 2px;
        }
        .cal-table td.cal-today { border: 2px solid #58a6ff; }
        .cal-table td.cal-non-trading {
            background: #0d1117; color: #30363d;
        }
        .cal-table td.cal-trading {
            background: #161b22;
        }
        .cal-table td.cal-profit { background: #0d2818; }
        .cal-table td.cal-loss { background: #2d1215; }
        .cal-day { font-size: 12px; color: #c9d1d9; }
        .cal-pnl { font-size: 10px; display: block; line-height: 1.2; }
        .cal-pnl-profit { color: #22c55e; }
        .cal-pnl-loss { color: #ef4444; }
        .cal-pnl-zero { color: #484f58; }
        .yr-pill {
            display: inline-block; padding: 4px 14px; margin: 2px;
            border-radius: 14px; font-size: 13px; cursor: pointer;
            background: #21262d; color: #c9d1d9; border: 1px solid #30363d;
        }
        .yr-pill.active { background: #1f6feb; color: #ffffff; border-color: #1f6feb; }
        .mo-pill {
            display: inline-block; padding: 3px 12px; margin: 2px;
            border-radius: 12px; font-size: 12px; cursor: pointer;
            background: #161b22; color: #8b949e; border: 1px solid #21262d;
        }
        .mo-pill.active { background: #238636; color: #ffffff; border-color: #238636; }
        .cal-summary {
            display: inline-block; padding: 4px 12px; margin: 2px 6px;
            border-radius: 6px; font-size: 12px; background: #161b22;
        }
        .cal-summary-profit { color: #22c55e; }
        .cal-summary-loss { color: #ef4444; }

        /* 主标签栏换行 */
        .stTabs [data-baseweb="tab-list"] {
            display: flex; flex-wrap: wrap; gap: 2px 4px;
            max-width: 100%; overflow: visible;
        }
        .stTabs [data-baseweb="tab"] {
            flex: 0 0 auto !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _render_sidebar(available_dates):

    # ===== 数据新鲜度提示 =====
    latest_avail = available_dates[0] if available_dates else None
    if latest_avail:
        from datetime import datetime
        try:
            last_dt = datetime.strptime(str(latest_avail), "%Y-%m-%d")
            gap = (datetime.now() - last_dt).days
            if gap <= 1:
                st.sidebar.success(f"数据已更新至 {latest_avail}")
            elif gap <= 3:
                st.sidebar.warning(f"数据截止 {latest_avail}（{gap}天前）")
            else:
                st.sidebar.error(f"数据截止 {latest_avail}（{gap}天前），请检查采集服务")
        except Exception:
            st.sidebar.caption(f"数据截止: {latest_avail}")

    # Render sidebar controls. Returns (selected_date, show_days, selected_benchmark).
    with st.sidebar:
        st.markdown("### 🔧 控制面板")

        selected_date = st.selectbox(
            "选择日期",
            available_dates,
            index=0,
            format_func=lambda x: f"{x} {'(最新)' if x == available_dates[0] else ''}",
            key="select_date",
        )

        # 快捷预设
        preset = st.radio(
            "时间范围", ["3个月", "6个月", "1年", "2年", "5年", "全部", "自定义"], horizontal=True, index=2,
            key="radio_preset",
        )
        preset_days = {"3个月": 60, "6个月": 120, "1年": 250, "2年": 500, "5年": 1250, "全部": 4000}
        if preset == "自定义":
            show_days = st.slider("自定义天数", min_value=10, max_value=4000, value=250, step=10, key="slider_custom_days")
        else:
            show_days = preset_days[preset]

        st.markdown("---")
        st.markdown("### 📋 系统信息")

        logs = load_execution_logs(5)
        if not logs.empty:
            for _, log in logs.iterrows():
                status_icon = "✅" if log["status"] == "success" else "❌" if log["status"] == "failed" else "⏳"
                st.markdown(f"{status_icon} `{log['task_name']}` - {log['status']}")
                if pd.notna(log.get("duration_seconds")):
                    st.caption(f"  耗时: {log['duration_seconds']:.1f}s")

        # 基准指数选择（P1改进）
        st.markdown("### 📌 基准指数")
        benchmark_options = {k: v for k, v in INDEX_CODES.items()}
        # 默认选中沪深300
        default_bench = "sh000300"
        benchmark_keys = list(benchmark_options.keys())
        default_idx = benchmark_keys.index(default_bench) if default_bench in benchmark_keys else 0
        selected_benchmark = st.selectbox(
            "对比基准",
            options=benchmark_keys,
            index=default_idx,
            format_func=lambda x: benchmark_options[x],
            key="benchmark_select",
        )

        st.markdown("---")
        st.markdown(f"*数据更新: {available_dates[0]}*")

        st.markdown("---")
        st.markdown("### 📊 快速指标")
        st.markdown('<span style="font-size:11px;color:#484f58;">↓ 详见下方概览指标条</span>', unsafe_allow_html=True)

    return selected_date, show_days, selected_benchmark

