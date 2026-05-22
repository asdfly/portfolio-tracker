"""
Dashboard Tab模块
包含所有Tab的独立实现
"""

from .tab1_net_value import render_tab1
from .tab2_position import render_tab2
from .tab3_risk import render_tab3
from .tab4_calendar import render_tab4
from .tab5_advanced import render_tab5
from .tab6_technical import render_tab6
from .tab7_news import render_tab7
from .tab8_advice import render_tab8
from .tab9_custom import render_tab9
from .tab10_fund_flow import render_tab10
from .tab11_gold import render_tab11
from .tab12_macro import render_tab12
from .tab13_data_quality import render_tab13
from .tab14_market_events import render_tab14

__all__ = [
    'render_tab1', 'render_tab2', 'render_tab3', 'render_tab4',
    'render_tab5', 'render_tab6', 'render_tab7', 'render_tab8',
    'render_tab9', 'render_tab10', 'render_tab11',
    'render_tab12', 'render_tab13', 'render_tab14'
]
