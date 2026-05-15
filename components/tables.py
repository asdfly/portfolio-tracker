"""
表格组件库
提供表格显示函数
"""

import streamlit as st
import pandas as pd


def display_dataframe(df, use_container_width=True, hide_index=False):
    """显示DataFrame"""
    st.dataframe(
        df,
        use_container_width=use_container_width,
        hide_index=hide_index
    )


def display_portfolio_table(portfolio_data):
    """显示持仓表格"""
    if portfolio_data.empty:
        st.warning("暂无持仓数据")
        return
    
    # 格式化显示
    display_df = portfolio_data.copy()
    
    # 格式化数值列
    numeric_cols = ['quantity', 'cost_price', 'current_price', 'market_value', 'pnl']
    for col in numeric_cols:
        if col in display_df.columns:
            if col == 'pnl':
                display_df[col] = display_df[col].apply(lambda x: f"{x:,.2f}" if pd.notnull(x) else "")
            else:
                display_df[col] = display_df[col].apply(lambda x: f"{x:,.2f}" if pd.notnull(x) else "")
    
    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True
    )


def display_alert_table(alerts_data):
    """显示告警表格"""
    if alerts_data.empty:
        st.success("暂无告警")
        return
    
    # 按级别排序
    level_order = {'critical': 0, 'error': 1, 'warning': 2, 'info': 3}
    if 'level' in alerts_data.columns:
        alerts_data = alerts_data.copy()
        alerts_data['level_order'] = alerts_data['level'].map(level_order)
        alerts_data = alerts_data.sort_values('level_order')
        alerts_data = alerts_data.drop('level_order', axis=1)
    
    st.dataframe(
        alerts_data,
        use_container_width=True,
        hide_index=True
    )
