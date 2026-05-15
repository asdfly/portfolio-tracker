"""测试公共 fixture"""
import os
import sys
import pytest
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


@pytest.fixture
def db_connection():
    """提供数据库连接（不手动 close）"""
    from src.utils.database import get_db_connection
    conn = get_db_connection()
    yield conn


@pytest.fixture
def sample_positions():
    """标准持仓 DataFrame（9列）"""
    import pandas as pd
    return pd.DataFrame({
        "code": ["510300", "510500", "159915"],
        "name": ["沪深300ETF", "中证500ETF", "创业板ETF"],
        "quantity": [1000, 500, 800],
        "cost_price": [4.5, 6.2, 2.1],
        "current_price": [4.8, 6.0, 2.3],
        "market_value": [4800.0, 3000.0, 1840.0],
        "pnl": [300.0, -100.0, 160.0],
        "pnl_rate": [6.67, -3.23, 9.52],
        "beta": [0.95, 1.05, 1.15],
    })


@pytest.fixture
def sample_summary():
    """标准汇总 DataFrame（4列，30行）"""
    import pandas as pd, numpy as np
    np.random.seed(42)
    dates = pd.date_range("2024-01-01", periods=30, freq="D").strftime("%Y-%m-%d")
    return pd.DataFrame({
        "date": dates,
        "total_value": np.random.normal(100000, 5000, 30).round(2),
        "total_pnl": np.random.normal(1000, 500, 30).round(2),
        "daily_return": np.random.normal(0.001, 0.02, 30).round(6),
    })


@pytest.fixture
def sample_index_quotes():
    """标准指数行情 DataFrame（7列，10行）"""
    import pandas as pd, numpy as np
    np.random.seed(42)
    dates = pd.date_range("2024-01-20", periods=10, freq="D")
    rows = []
    for d in dates:
        rows.append({"date": d.strftime("%Y-%m-%d"), "code": "sh000300", "name": "沪深300",
                      "close": round(3500 + np.random.randn() * 30, 2),
                      "change_pct": round(np.random.randn() * 1.5, 2),
                      "volume": int(np.random.rand() * 1e8),
                      "amount": int(np.random.rand() * 1e10)})
    return pd.DataFrame(rows)
