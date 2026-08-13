"""P2-A 本地交易日历单元测试"""
import sys
from datetime import date
from pathlib import Path

PROJECT_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_DIR))

from src.utils.trading_calendar import (
    is_trading_day,
    next_trading_day,
    prev_trading_day,
    last_trading_day_on_or_before,
    get_trading_days,
)


class TestIsTradingDay:
    def test_weekends(self):
        assert not is_trading_day("2026-08-08")   # 周六
        assert not is_trading_day("2026-08-09")   # 周日
        assert is_trading_day("2026-08-10")       # 周一

    def test_2026_spring_festival(self):
        # 2026 春节 02-15~02-23 休市
        assert not is_trading_day("2026-02-15")
        assert not is_trading_day("2026-02-18")
        assert not is_trading_day("2026-02-23")
        assert is_trading_day("2026-02-24")       # 周二开市

    def test_2026_national_day(self):
        # 2026 国庆 10-01~10-07 休市
        assert not is_trading_day("2026-10-01")
        assert not is_trading_day("2026-10-05")
        assert not is_trading_day("2026-10-07")
        assert is_trading_day("2026-10-08")

    def test_2026_others(self):
        assert not is_trading_day("2026-01-01")   # 元旦
        assert is_trading_day("2026-01-05")
        assert not is_trading_day("2026-04-05")   # 清明区间内
        assert is_trading_day("2026-04-07")
        assert not is_trading_day("2026-06-19")   # 端午
        assert is_trading_day("2026-06-22")
        assert not is_trading_day("2026-09-26")   # 中秋
        assert is_trading_day("2026-09-28")

    def test_2025(self):
        assert not is_trading_day("2025-01-01")   # 元旦
        assert is_trading_day("2025-01-02")
        assert not is_trading_day("2025-02-01")   # 春节区间内(01-28~02-04)
        assert is_trading_day("2025-02-05")
        assert not is_trading_day("2025-10-03")   # 国庆+中秋(10-01~10-08)
        assert is_trading_day("2025-10-09")

    def test_2024(self):
        assert not is_trading_day("2024-02-10")   # 春节(02-09~02-17)
        assert is_trading_day("2024-02-19")
        assert not is_trading_day("2024-06-10")   # 端午单日
        assert is_trading_day("2024-06-11")
        assert not is_trading_day("2024-10-03")   # 国庆
        assert is_trading_day("2024-10-08")


class TestNextPrevTradingDay:
    def test_friday_to_monday(self):
        # 2026-08-07 周五 -> 2026-08-10 周一
        assert next_trading_day("2026-08-07") == date(2026, 8, 10)

    def test_crosses_national_holiday(self):
        # 2026-09-30 周三 -> 国庆 10-01~10-07 -> 10-08 周四
        assert next_trading_day("2026-09-30") == date(2026, 10, 8)

    def test_prev_before_weekend(self):
        # 2026-08-10 周一 -> 2026-08-07 周五
        assert prev_trading_day("2026-08-10") == date(2026, 8, 7)
        # 2026-08-09 周日 -> 2026-08-07 周五
        assert prev_trading_day("2026-08-09") == date(2026, 8, 7)


class TestLastOnOrBefore:
    def test_inclusive_self(self):
        assert last_trading_day_on_or_before("2026-08-10") == date(2026, 8, 10)

    def test_weekend_back_to_friday(self):
        assert last_trading_day_on_or_before("2026-08-09") == date(2026, 8, 7)
        assert last_trading_day_on_or_before("2026-08-08") == date(2026, 8, 7)

    def test_holiday_back(self):
        # 2026-10-05 国庆休市 -> 回退到 2026-09-30 周三
        assert last_trading_day_on_or_before("2026-10-05") == date(2026, 9, 30)


class TestGetTradingDays:
    def test_january_2026(self):
        days = get_trading_days("2026-01-01", "2026-01-31")
        assert date(2026, 1, 1) not in days          # 元旦休市
        assert date(2026, 1, 3) not in days          # 元旦休市
        assert date(2026, 1, 4) not in days          # 周日
        assert date(2026, 1, 5) in days              # 开市
        assert all(d.weekday() < 5 for d in days)    # 无周末

    def test_single_non_trading_day_is_empty(self):
        # 2026-02-20 是周六 -> 该日无交易日，返回空列表（周末被正确排除）
        days = get_trading_days("2026-02-20", "2026-02-20")
        assert days == []

    def test_start_after_end_is_swapped(self):
        # 起止颠倒时内部交换，结果仍为该区间交易日（升序）
        a = get_trading_days("2026-01-04", "2026-01-08")
        b = get_trading_days("2026-01-08", "2026-01-04")
        assert a == b
        assert all(x < y for x, y in zip(a, a[1:]))
