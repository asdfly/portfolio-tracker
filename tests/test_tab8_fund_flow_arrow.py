"""tab8 盘后「资金流向变化」表：数值列不得被 as-of 标注污染成 object dtype。

背景（2026-10-14 实测）：
    原实现把 as-of 日期直接拼进数值列 —— `today_val = f"{today_val} (as-of ...)"`，
    使该列成为 int/str 混合的 object dtype。pyarrow 按首行推断 int64，遇到字符串行
    抛 ArrowInvalid("Could not convert '11823 (as-of 2026-10-13)' ...")，Streamlit
    只能降级重试（"Serialization of dataframe to Arrow table was unsuccessful"）。

修复取向：
    数值列保持纯数值，口径（当日 / as-of 日期 / 无数据）单独走字符串列承载。
    本测试锁死两件事：① 表可无损转 Arrow；② 口径信息一个都不能丢。
"""
from unittest.mock import MagicMock, patch

import pandas as pd
import pyarrow as pa
import pytest


def _render_and_capture(report):
    """调用 tab8 盘后面板，捕获所有传给 st.dataframe 的数据。"""
    from tabs.tab8_advice import _render_post_market_panel

    frames = []
    with patch("data_loader.load_post_market_report", return_value=report), \
         patch("tabs.tab8_advice.st") as mock_st:
        mock_st.dataframe = lambda df, *a, **k: frames.append(df)
        mock_st.session_state = MagicMock()
        mock_st.session_state.get = lambda key, default="": default
        for m in ["caption", "info", "markdown", "metric", "button", "download_button",
                  "success", "error", "bar_chart", "plotly_chart", "warning"]:
            setattr(mock_st, m, MagicMock())
        mock_st.expander = MagicMock()
        mock_st.expander.return_value.__enter__ = lambda *a: None
        mock_st.expander.return_value.__exit__ = lambda *a: None
        mock_st.columns = lambda spec: [MagicMock() for _ in range(
            len(spec) if isinstance(spec, list) else spec)]
        _render_post_market_panel()
    return frames


def _flow_frame(frames):
    for f in frames:
        if isinstance(f, list) and f and "今日(万)" in f[0]:
            return pd.DataFrame(f)
    pytest.fail("资金流向变化 dataframe 未被渲染")


def test_fund_flow_table_is_arrow_compatible_with_stale_asof():
    """混合「当日 + 滞后一日」时，资金流表必须能无损转 Arrow（回归 #ArrowInvalid）。"""
    from src.analysis.pre_post_market import PostMarketReport

    report = PostMarketReport(
        report_time="16:00", report_date="2026-10-14",
        fund_flow_changes=[
            {"code": "512100", "today_flow": 11823.4, "today_date": "2026-10-14",
             "yesterday_flow": -3200.0, "yesterday_date": "2026-10-13",
             "flow_change": 15023.4},
            # 这一只滞后一日 —— 正是当初触发 ArrowInvalid 的那类数据
            {"code": "159949", "today_flow": 11823.0, "today_date": "2026-10-13",
             "yesterday_flow": 880.0, "yesterday_date": "2026-10-10",
             "flow_change": 10943.0},
            {"code": "588000", "today_flow": -456.7, "today_date": "2026-10-14",
             "yesterday_flow": 120.0, "yesterday_date": "2026-10-13",
             "flow_change": -576.7},
        ])

    df = _flow_frame(_render_and_capture(report))

    # ① 核心回归：不得抛 ArrowInvalid
    pa.Table.from_pandas(df, preserve_index=False)

    # ② 数值列必须是数值（不被字符串污染）
    for col in ("今日(万)", "昨日(万)", "变化(万)"):
        assert pd.api.types.is_numeric_dtype(df[col]), f"{col} 应为数值列，实为 {df[col].dtype}"

    # ③ 口径信息一个都不能丢
    rows = {r["代码"]: r for _, r in df.iterrows()}
    assert rows["512100"]["今日口径"] == "当日"
    assert rows["159949"]["今日口径"] == "as-of 2026-10-13", "滞后一日必须标注 as-of"
    assert rows["588000"]["今日口径"] == "当日"
    assert rows["159949"]["昨日口径"] == "as-of 2026-10-10"
    # 数值语义保留（可排序）而非被 round 成字符串
    assert int(rows["159949"]["今日(万)"]) == 11823


def test_fund_flow_missing_asof_shows_no_data_not_zero():
    """as-of 缺失 == 取不到数据，必须显式标「无数据」，不得与「恰好 +0」混淆（#140）。"""
    from src.analysis.pre_post_market import PostMarketReport

    report = PostMarketReport(
        report_time="16:00", report_date="2026-10-14",
        fund_flow_changes=[
            {"code": "510300", "today_flow": 0.0, "today_date": None,
             "yesterday_flow": 0.0, "yesterday_date": None, "flow_change": 0.0},
        ])

    df = _flow_frame(_render_and_capture(report))
    pa.Table.from_pandas(df, preserve_index=False)

    row = df.iloc[0]
    assert row["今日口径"] == "无数据", "as-of 缺失必须显式标『无数据』"
    assert row["昨日口径"] == "无数据"
