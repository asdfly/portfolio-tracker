"""
Dashboard UI组件库
提供可复用的UI组件
"""

from .charts import (
    create_line_chart,
    create_bar_chart,
    create_pie_chart,
    create_heatmap,
    create_radar_chart
)

from .metrics import (
    display_metric_card,
    display_metric_row,
    display_metric_comparison
)

from .tables import (
    display_dataframe,
    display_portfolio_table,
    display_alert_table
)

from .layouts import (
    create_sidebar,
    create_header,
    create_footer,
    create_tabs_container
)

__all__ = [
    # Charts
    'create_line_chart', 'create_bar_chart', 'create_pie_chart',
    'create_heatmap', 'create_radar_chart',
    
    # Metrics
    'display_metric_card', 'display_metric_row', 'display_metric_comparison',
    
    # Tables
    'display_dataframe', 'display_portfolio_table', 'display_alert_table',
    
    # Layouts
    'create_sidebar', 'create_header', 'create_footer', 'create_tabs_container'
]
