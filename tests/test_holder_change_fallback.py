"""Unit tests for fetch_holder_change_data 的健壮性（治盲区 #4/#5）。

核心验证点：
- detail 源返回空 / 抛异常时，**诚实地返回空 DataFrame（真缺失）**，
  而非回退一个无股票代码列的 summary 源（后者会触发 stock_holder_change
  的 NOT NULL(code) 约束被 save_market_events 静默丢弃 —— 盲区#5）。
- detail 源正常时，返回含 code 列的标准化行。

用 unittest.mock.patch 替换 akshare 端点，避免真实网络调用与源端抖动。
"""
import pandas as pd
from unittest.mock import patch

from src.data_sources.market_events import fetch_holder_change_data


def _detail_df():
    """构造一个合法的 detail 源返回（含股票代码列）。"""
    return pd.DataFrame({
        "股东名称": ["股东A"],
        "股东类型": ["个人"],
        "股票代码": ["600000"],
        "股票简称": ["浦发银行"],
        "报告期": ["2026-09-30"],
        "期末持股-数量": [100],
        "期末持股-数量变化": [10],
        "期末持股-数量变化比例": [0.1],
        "期末持股-持股变动": ["增持"],
        "期末持股-流通市值": [1e8],
        "公告日": ["2026-10-08"],
    })


def test_detail_empty_returns_empty_not_broken_summary():
    """detail 空 → 返回空（真缺失），不得返回无 code 列的 summary 行。"""
    summary_fake = pd.DataFrame({"股东名称": ["h1"]})  # 无股票代码列
    with patch("akshare.stock_gdfx_free_holding_detail_em",
               return_value=pd.DataFrame()), \
         patch("akshare.stock_gdfx_free_holding_change_em",
               return_value=summary_fake):
        df = fetch_holder_change_data("20261008")
    assert df.empty, "detail 空时必须诚实返回空，而非假兜底 summary 行"


def test_detail_exception_returns_empty():
    """detail 抛异常 → 返回空（真缺失），不得回退 summary。"""
    summary_fake = pd.DataFrame({"股东名称": ["h1"]})
    with patch("akshare.stock_gdfx_free_holding_detail_em",
               side_effect=RuntimeError("源端抖动")), \
         patch("akshare.stock_gdfx_free_holding_change_em",
               return_value=summary_fake):
        df = fetch_holder_change_data("20261008")
    assert df.empty, "detail 异常时必须诚实返回空"


def test_detail_valid_returns_rows_with_code():
    """detail 正常 → 返回含 code 列的标准化行。"""
    with patch("akshare.stock_gdfx_free_holding_detail_em",
               return_value=_detail_df()):
        df = fetch_holder_change_data("20261008")
    assert not df.empty
    assert "code" in df.columns, "detail 行必须含 code 列以便落 stock_holder_change"
    assert df.iloc[0]["code"] == "600000"
    assert df.iloc[0]["holder_name"] == "股东A"
    assert df.iloc[0]["date"] == "2026-10-08"
