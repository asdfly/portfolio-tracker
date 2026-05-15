"""
图表组件库
提供各种图表的创建函数
"""

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np


def create_line_chart(data, x_col, y_col, title, **kwargs):
    """创建折线图"""
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=data[x_col],
        y=data[y_col],
        mode='lines+markers',
        name=title
    ))
    fig.update_layout(
        title=title,
        xaxis_title=x_col,
        yaxis_title=y_col,
        **kwargs
    )
    return fig


def create_bar_chart(data, x_col, y_col, title, **kwargs):
    """创建柱状图"""
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=data[x_col],
        y=data[y_col],
        name=title
    ))
    fig.update_layout(
        title=title,
        xaxis_title=x_col,
        yaxis_title=y_col,
        **kwargs
    )
    return fig


def create_pie_chart(data, names_col, values_col, title, **kwargs):
    """创建饼图"""
    fig = go.Figure()
    fig.add_trace(go.Pie(
        labels=data[names_col],
        values=data[values_col],
        name=title
    ))
    fig.update_layout(
        title=title,
        **kwargs
    )
    return fig


def create_heatmap(data, title, **kwargs):
    """创建热力图"""
    fig = go.Figure()
    fig.add_trace(go.Heatmap(
        z=data.values,
        x=data.columns,
        y=data.index,
        colorscale='RdYlGn'
    ))
    fig.update_layout(
        title=title,
        **kwargs
    )
    return fig


def create_radar_chart(data, categories, values, title, **kwargs):
    """创建雷达图"""
    fig = go.Figure()
    fig.add_trace(go.Scatterpolar(
        r=values,
        theta=categories,
        fill='toself',
        name=title
    ))
    fig.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, max(values) * 1.1]
            )
        ),
        title=title,
        **kwargs
    )
    return fig
