"""
指标组件库
提供指标卡片和指标行的显示函数
"""

import streamlit as st


def display_metric_card(title, value, delta=None, delta_color="normal"):
    """显示指标卡片"""
    col1, col2 = st.columns([3, 1])
    with col1:
        st.metric(
            label=title,
            value=value,
            delta=delta,
            delta_color=delta_color
        )


def display_metric_row(metrics, columns=None):
    """显示指标行"""
    if columns is None:
        columns = len(metrics)
    
    cols = st.columns(columns)
    for i, (title, value, delta) in enumerate(metrics):
        with cols[i % columns]:
            st.metric(
                label=title,
                value=value,
                delta=delta
            )


def display_metric_comparison(metrics, columns=None):
    """显示指标对比"""
    if columns is None:
        columns = len(metrics)
    
    cols = st.columns(columns)
    for i, metric in enumerate(metrics):
        with cols[i % columns]:
            if len(metric) == 3:
                title, value, delta = metric
                st.metric(label=title, value=value, delta=delta)
            else:
                title, value = metric
                st.metric(label=title, value=value)
