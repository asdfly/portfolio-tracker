"""
布局组件库
提供页面布局函数
"""

import streamlit as st
from datetime import date, datetime


@st.cache_data(ttl=600)
def _get_available_dates():
    """获取所有有数据的交易日期列表（缓存10分钟）"""
    import sqlite3
    from config.settings import DATABASE_PATH

    conn = sqlite3.connect(str(DATABASE_PATH))
    cur = conn.cursor()
    cur.execute(
        "SELECT DISTINCT date FROM portfolio_summary "
        "WHERE date IS NOT NULL ORDER BY date DESC"
    )
    rows = cur.fetchall()
    conn.close()

    date_set = set()
    for (d,) in rows:
        try:
            dt = datetime.strptime(d, "%Y-%m-%d").date()
            date_set.add(dt)
        except ValueError:
            pass
    return date_set


def create_sidebar():
    """创建侧边栏"""
    available_date_set = _get_available_dates()

    with st.sidebar:
        st.title("Portfolio Tracker")
        st.markdown("---")

        st.subheader("数据选择")

        if not available_date_set:
            st.warning("暂无数据，请先运行数据采集")
            return None, "沪深300"

        min_date = min(available_date_set)
        max_date = max(available_date_set)

        if "selected_date" not in st.session_state:
            st.session_state.selected_date = max_date

        selected_date = st.date_input(
            "选择日期",
            value=st.session_state.selected_date,
            min_value=min_date,
            max_value=max_date,
            help="仅可选择已有数据的交易日",
            key="date_input_widget",
        )

        if selected_date not in available_date_set:
            valid = sorted([d for d in available_date_set if d <= selected_date], reverse=True)
            if not valid:
                valid = sorted([d for d in available_date_set if d >= selected_date])
            selected_date = valid[0] if valid else max_date
            st.session_state.selected_date = selected_date
            st.error("该日期无数据，已自动切换到最近交易日")
        else:
            st.session_state.selected_date = selected_date

        weekday_names = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
        weekday = weekday_names[selected_date.weekday()]
        date_str = selected_date.strftime("%Y-%m-%d")
        st.caption(
            f"当前查看: **{date_str}**  {weekday}  "
            f"({len(available_date_set)}个交易日可用)"
        )

        st.subheader("基准选择")
        benchmark_options = ["沪深300", "中证500", "创业板指", "上证指数"]
        selected_benchmark = st.selectbox(
            "选择基准指数",
            benchmark_options,
            index=0,
        )

        st.markdown("---")

        st.subheader("快速操作")
        if st.button("刷新数据"):
            st.rerun()

        if st.button("生成报告"):
            st.info("报告生成功能开发中...")

        return date_str, selected_benchmark


def create_header():
    """创建页面头部"""
    st.markdown("""
    <div style='text-align: center; padding: 1rem 0;'>
        <h1 style='color: #1f77b4; margin-bottom: 0.5rem;'>
            投资组合智能分析系统
        </h1>
        <p style='color: #666; font-size: 1.1rem;'>
            基于Python + Streamlit的自动化投资组合跟踪分析平台
        </p>
    </div>
    """, unsafe_allow_html=True)


def create_footer():
    """创建页面底部"""
    st.markdown("""
    ---
    <div style='text-align: center; color: #666; font-size: 0.9rem;'>
        <p>Portfolio Tracker (c) 2024 | 基于Streamlit构建</p>
    </div>
    """, unsafe_allow_html=True)


def create_tabs_container(tab_names):
    """创建Tab容器"""
    return st.tabs(tab_names)
