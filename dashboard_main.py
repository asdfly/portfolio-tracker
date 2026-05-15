#!/usr/bin/env python3
"""
投资组合智能分析系统 - 主入口
使用模块化结构的Streamlit Dashboard
"""

import streamlit as st

# 页面配置必须在所有其他Streamlit命令之前
st.set_page_config(
    page_title="投资组合智能分析系统",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

import pandas as pd
import numpy as np
import sqlite3
from pathlib import Path
import sys

# 添加项目根目录到Python路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# 导入配置
from config.settings import DATABASE_PATH, INDEX_CODES, ETF_CATEGORIES, BENCHMARK_NAME_TO_CODE
from src.utils.database import get_db_connection

# 导入组件
from components.layouts import create_sidebar, create_header, create_footer
from components.metrics import display_metric_card, display_metric_row

# 导入Tab模块
from tabs import (
    render_tab1, render_tab2, render_tab3, render_tab4, render_tab5,
    render_tab6, render_tab7, render_tab8, render_tab9, render_tab10, render_tab11
)



@st.cache_data(ttl=300, show_spinner=False)
def load_positions():
    """加载持仓数据"""
    conn = get_db_connection()
    try:
        query = """
        SELECT code, name, quantity, cost_price, current_price, market_value, pnl,
                 CASE WHEN cost_price > 0 THEN ROUND((current_price - cost_price) / cost_price * 100, 2) ELSE 0 END AS pnl_rate,
                 NULL AS beta
        FROM portfolio_snapshots
        WHERE date = (SELECT MAX(date) FROM portfolio_snapshots)
        """
        return pd.read_sql_query(query, conn)
    except Exception as e:
        st.error(f"加载持仓数据失败: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=300, show_spinner=False)
def load_summary():
    """加载汇总数据"""
    conn = get_db_connection()
    try:
        query = """
        SELECT date, total_value, total_pnl, daily_return
        FROM portfolio_summary
        ORDER BY date
        """
        return pd.read_sql_query(query, conn)
    except Exception as e:
        st.error(f"加载汇总数据失败: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=300, show_spinner=False)
def load_index_quotes(benchmark_code):
    """加载指数行情数据"""
    conn = get_db_connection()
    try:
        query = """
        SELECT date, code, close, change_pct, volume
        FROM index_quotes
        WHERE code = ?
        ORDER BY date
        """
        return pd.read_sql_query(query, conn, params=[benchmark_code])
    except Exception as e:
        st.error(f"加载指数数据失败: {e}")
        return pd.DataFrame()
    
def main():
    """主函数"""
    
    # 注入自定义CSS样式（tip提示系统）
    # 方式1: st.markdown内联注入
    st.markdown("""<style>
        .tip-title {
            font-size: 18px !important; font-weight: bold !important; color: #c9d1d9 !important;
            padding: 10px 0 5px 0 !important; border-bottom: 1px solid #30363d !important;
            display: inline-block !important; cursor: help !important;
            position: relative !important;
        }
        .tip-title::after {
            content: ' \u2139' !important;
            font-size: 11px !important; color: #58a6ff !important; font-weight: normal !important;
        }
        .tip-title .tip-text {
            visibility: hidden !important; opacity: 0 !important;
            position: absolute !important; z-index: 99999 !important;
            background: #1c2333 !important; color: #c9d1d9 !important;
            border: 1px solid #30363d !important; border-radius: 6px !important;
            padding: 8px 12px !important; font-size: 12px !important; font-weight: normal !important;
            line-height: 1.5 !important; width: max-content !important; max-width: 360px !important;
            box-shadow: 0 4px 12px rgba(0,0,0,0.4) !important;
            transition: opacity 0.2s, visibility 0.2s !important;
            margin-top: 6px !important; margin-left: 0 !important;
            pointer-events: none !important;
        }
        .tip-title:hover .tip-text {
            visibility: visible !important; opacity: 1 !important;
        }
        .tip-title .tip-arrow {
            visibility: hidden !important; opacity: 0 !important;
            position: absolute !important; z-index: 99999 !important;
            border-left: 6px solid transparent !important;
            border-right: 6px solid transparent !important;
            border-bottom: 6px solid #30363d !important;
            transition: opacity 0.2s, visibility 0.2s !important;
        }
        .tip-title:hover .tip-arrow {
            visibility: visible !important; opacity: 1 !important;
        }
    </style>""", unsafe_allow_html=True)

    # 创建头部
    create_header()
    
    # 创建侧边栏
    selected_date, selected_benchmark = create_sidebar()
    
    # 加载数据
    positions = load_positions()
    summary = load_summary()
    
    # 获取基准指数代码
    benchmark_code = BENCHMARK_NAME_TO_CODE.get(selected_benchmark, INDEX_CODES.get(selected_benchmark, "sh000300"))
    index_quotes = load_index_quotes(benchmark_code)
    
    # 创建Tab容器
    tab_names = [
        "📈 净值走势", "📊 持仓分布", "⚠️ 风险分析", "📅 收益日历",
        "🔬 高级分析", "📉 技术信号", "📰 资讯与评估", "💡 操作建议",
        "🛠️ 自定义指标", "💰 资金动向", "🥇 黄金市场"
    ]
    
    tabs = st.tabs(tab_names)
    
    # 渲染各个Tab
    with tabs[0]:
        render_tab1(positions, summary, index_quotes, selected_date, selected_benchmark)
    
    with tabs[1]:
        render_tab2(positions, summary, index_quotes, selected_date, selected_benchmark)
    
    with tabs[2]:
        render_tab3(positions, summary, index_quotes, selected_date, selected_benchmark)
    
    with tabs[3]:
        render_tab4(positions, summary, index_quotes, selected_date, selected_benchmark)
    
    with tabs[4]:
        render_tab5(positions, summary, index_quotes, selected_date, selected_benchmark)
    
    with tabs[5]:
        render_tab6(positions, summary, index_quotes, selected_date, selected_benchmark)
    
    with tabs[6]:
        render_tab7(positions, summary, index_quotes, selected_date, selected_benchmark)
    
    with tabs[7]:
        render_tab8(positions, summary, index_quotes, selected_date, selected_benchmark)
    
    with tabs[8]:
        render_tab9(positions, summary, index_quotes, selected_date, selected_benchmark)
    
    with tabs[9]:
        render_tab10(positions, summary, index_quotes, selected_date, selected_benchmark)
    
    with tabs[10]:
        render_tab11(positions, summary, index_quotes, selected_date, selected_benchmark)
    
    # 创建底部
    create_footer()

if __name__ == "__main__":
    main()
