"""Tests for recent_trading_days (盲区#12 根治: 窗口改"最近N真实交易日"计数).

纯单元测试: 通过注入 collect_core._TRADE_CAL_CACHE 控制交易日历,
避免触发 akshare 网络请求。is_trading_date 在日历非空时按"周末+日历"判定,
日历为空时退化为"非周末即交易日"。
"""
import pytest
from datetime import date, timedelta

from src.data_sources import collect_core


def _all_weekdays_in_range(start: date, end: date, holidays=frozenset()) -> set:
    """构造"真实交易日历"集合: 区间内的工作日, 剔除指定节假日。"""
    days = set()
    d = start
    while d <= end:
        if d.weekday() < 5 and d not in holidays:
            days.add(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)
    return days


@pytest.fixture
def calendar_guard():
    """保存并恢复模块级 _TRADE_CAL_CACHE, 防止测试间相互污染。"""
    saved = collect_core._TRADE_CAL_CACHE
    yield
    collect_core._TRADE_CAL_CACHE = saved


# 2026 国庆假期: 10-01~10-07 为法定节假日(均为工作日, 但非交易日)。
_NATIONAL_DAY_HOLIDAYS = {
    date(2026, 10, 1), date(2026, 10, 2), date(2026, 10, 5),
    date(2026, 10, 6), date(2026, 10, 7),
}


def test_recent_trading_days_skips_holidays(calendar_guard):
    """正常日历: 10-09 往前 5 个真实交易日须跳过国庆 10-01~10-07。"""
    collect_core._TRADE_CAL_CACHE = _all_weekdays_in_range(
        date(2026, 9, 21), date(2026, 10, 12), _NATIONAL_DAY_HOLIDAYS)
    got = collect_core.recent_trading_days("2026-10-09", 5)
    assert got == ["2026-09-28", "2026-09-29", "2026-09-30",
                   "2026-10-08", "2026-10-09"]


def test_recent_trading_days_n1(calendar_guard):
    """边界: n=1 仅返回 end_date(当其为交易日)。"""
    collect_core._TRADE_CAL_CACHE = _all_weekdays_in_range(
        date(2026, 9, 21), date(2026, 10, 12), _NATIONAL_DAY_HOLIDAYS)
    assert collect_core.recent_trading_days("2026-10-09", 1) == ["2026-10-09"]


def test_recent_trading_days_degenerate_empty_calendar(calendar_guard):
    """日历加载失败(空集合): 退化为非周末, 不抛异常。"""
    collect_core._TRADE_CAL_CACHE = set()
    got = collect_core.recent_trading_days("2026-10-09", 5)
    # 退化下 10-01~10-07 视为交易日(非周末), 最近5日含假日。
    assert got == ["2026-10-05", "2026-10-06", "2026-10-07",
                   "2026-10-08", "2026-10-09"]


def test_recent_trading_days_bad_input(calendar_guard):
    """异常入参: 返回空列表而非抛异常。"""
    collect_core._TRADE_CAL_CACHE = _all_weekdays_in_range(
        date(2026, 9, 21), date(2026, 10, 12), _NATIONAL_DAY_HOLIDAYS)
    assert collect_core.recent_trading_days("not-a-date", 3) == []
    assert collect_core.recent_trading_days("", 3) == []
