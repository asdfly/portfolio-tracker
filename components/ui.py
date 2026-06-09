"""
UI 组件库 — 标准化渲染函数
将高频重复的 Streamlit UI 模式封装为工具函数，统一风格、减少样板代码。

组件清单：
- render_chart: 标准化 plotly 图表渲染（默认 width="stretch"）
- render_empty_state: 空数据守卫（df.empty / None 检查 + st.info + return）
"""

import streamlit as st


def render_chart(fig, width="stretch"):
    """标准化 plotly 图表渲染。

    默认 width='stretch' 与项目内绝大多数调用一致（117 处），
    仅需传 fig 即可，避免逐行重复 width 参数。

    Args:
        fig: plotly.graph_objects.Figure 实例
        width: Streamlit st.plotly_chart 的 width 参数，默认 "stretch"
    """
    st.plotly_chart(fig, width=width)


def render_empty_state(data, message, *, warn=False):
    """空数据守卫：检查 data 是否为空/None，若是则显示提示并返回 True。

    将项目中高频的 "if df.empty: st.info(msg); return" 三行模式
    压缩为一行调用，调用方写法变为::

        if render_empty_state(df, "暂无数据"):
            return

    对于 ``if X is None or X.empty`` 的场景也适用，因为 None
    在布尔上下文中为 False，函数内部统一处理。

    Args:
        data: pandas DataFrame 或 None
        message: 空态时显示的 st.info/st.warning 提示文案
        warn: True 时使用 st.warning 代替 st.info

    Returns:
        True 表示数据为空（调用方应 return），False 表示数据正常
    """
    if data is None or (hasattr(data, 'empty') and data.empty):
        (st.warning if warn else st.info)(message)
        return True
    return False
