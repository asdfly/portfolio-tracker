"""L4 - 集成冒烟测试"""
import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def test_full_import_chain():
    """dashboard_main -> tabs -> chart_utils -> config 全链路导入无错"""
    import config.settings
    import src.utils.chart_utils
    import src.utils.database
    import tabs
    import dashboard as dashboard_main
    # 验证关键对象存在
    assert hasattr(dashboard_main, "main") or hasattr(dashboard_main, "load_positions"),         "dashboard_main 缺少入口函数"


def test_load_positions_real():
    """通过 database 实际查询 portfolio_snapshots"""
    import pandas as pd
    from src.utils.database import get_db_connection
    conn = get_db_connection()
    df = pd.read_sql_query("SELECT * FROM portfolio_snapshots", conn)
    assert isinstance(df, pd.DataFrame)
    # 当前数据库有4行数据
    assert len(df) > 0, "portfolio_snapshots 应有数据"


def test_load_summary_real():
    """通过 database 实际查询 portfolio_summary"""
    import pandas as pd
    from src.utils.database import get_db_connection
    conn = get_db_connection()
    df = pd.read_sql_query("SELECT * FROM portfolio_summary", conn)
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0, "portfolio_summary 应有数据"


def test_render_tab1_real_data():
    """用真实数据库数据调用 render_tab1"""
    from unittest.mock import MagicMock, patch
    import pandas as pd
    from src.utils.database import get_db_connection

    # mock streamlit
    mock_st = MagicMock()
    mock_st.columns = MagicMock(return_value=[MagicMock(), MagicMock(), MagicMock()])
    mock_st.session_state = {}

    # 加载真实数据
    conn = get_db_connection()
    positions = pd.read_sql_query(
        "SELECT code,name,quantity,cost_price,current_price,market_value,pnl,"
        "CASE WHEN cost_price>0 THEN ROUND((current_price-cost_price)/cost_price*100,2) ELSE 0 END AS pnl_rate,"
        "0.0 AS beta FROM portfolio_snapshots "
        "WHERE date=(SELECT MAX(date) FROM portfolio_snapshots)", conn)
    summary = pd.read_sql_query(
        "SELECT date,total_value,total_pnl,daily_return FROM portfolio_summary ORDER BY date", conn)
    index_quotes = pd.read_sql_query("SELECT * FROM index_quotes LIMIT 10", conn)
    selected_date = str(summary["date"].iloc[-1]) if len(summary) > 0 else "2024-01-01"

    with patch.dict("sys.modules", {"streamlit": mock_st}):
        from tabs.tab1_net_value import render_tab1
        render_tab1(positions, summary, index_quotes,
                     selected_date=selected_date, selected_benchmark="sh000300")
