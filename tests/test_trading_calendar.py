"""P2-A 本地交易日历单元测试"""
import logging
import sys
from datetime import date
from pathlib import Path

PROJECT_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_DIR))

from src.utils.trading_calendar import (
    CALENDAR_COVERED_YEARS,
    degraded_years,
    get_trading_days,
    has_official_calendar,
    is_trading_day,
    last_trading_day_on_or_before,
    next_trading_day,
    prev_trading_day,
    uncovered_years,
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


class TestCoverageAndDegradation:
    """task #66：覆盖外年份的退化必须可检测、可告警，而不是静默给出看似正常的答案。

    背景：_HOLIDAY_RANGES 只有 2024/2025/2026，而库内数据回溯到 1990
    （index_quotes）/ 2009（index_pe_history）/ 2012（portfolio_snapshots），
    任何"回看历史"的调用方都会拿到「仅周末」口径的退化答案。
    """

    def test_covered_years_are_exactly_the_builtin_table(self):
        assert CALENDAR_COVERED_YEARS == (2024, 2025, 2026)
        for y in CALENDAR_COVERED_YEARS:
            assert has_official_calendar(y) is True

    def test_uncovered_years_detects_both_ends(self):
        assert has_official_calendar(2023) is False     # 历史侧：数据能回溯到 2012
        assert has_official_calendar(2027) is False     # 未来侧：每年初需补表
        assert uncovered_years("2023-01-01", "2023-12-31") == [2023]
        assert uncovered_years("2023-12-01", "2024-02-01") == [2023]
        assert uncovered_years("2024-01-01", "2026-12-31") == []
        assert uncovered_years("2026-12-01", "2027-03-01") == [2027]

    def test_uncovered_year_answers_are_wrong_but_not_silent(self, caplog):
        """2023 春节 01-23~01-27 是周一~周五，退化口径会把它们判成交易日。"""
        with caplog.at_level(logging.WARNING, logger="src.utils.trading_calendar"):
            assert is_trading_day("2023-01-23") is True, "退化口径：节假日被当成交易日"
            assert is_trading_day("2023-10-02") is True
        # 同样日期在覆盖年份里被正确判为非交易日（对照组）
        assert is_trading_day("2024-02-12") is False    # 2024 春节
        assert is_trading_day("2026-10-05") is False    # 2026 国庆
        # 退化不是静默的：日志 + 可编程查询两条路都要有
        assert "无 2023 年官方休市表" in caplog.text
        assert 2023 in degraded_years()

    def test_get_trading_days_warns_with_reliability_count(self, caplog):
        """批量入口必须报出「返回多少天、其中多少天不可信」。"""
        with caplog.at_level(logging.WARNING, logger="src.utils.trading_calendar"):
            days = get_trading_days("2023-01-01", "2023-01-31")
        msgs = [r.getMessage() for r in caplog.records]
        assert any("2023" in m and "退化口径" in m for m in msgs), msgs
        # 2023-01-01(周日) 之外的工作日全被算进来，含元旦/春节整周
        assert date(2023, 1, 2) in days      # 元旦补休（真实休市）
        assert date(2023, 1, 23) in days     # 春节（真实休市）
        assert all(d.weekday() < 5 for d in days)

    def test_get_trading_days_is_quiet_for_covered_range(self, caplog):
        with caplog.at_level(logging.WARNING, logger="src.utils.trading_calendar"):
            get_trading_days("2024-01-01", "2026-12-31")
        assert not any("无官方休市表" in r.getMessage() for r in caplog.records)

    def test_crossing_range_warns_even_if_every_year_is_covered(self, caplog):
        """2023-12-30~2024-01-02 跨了缺表年，必须告警（哪怕只有 2 个工作日）。"""
        with caplog.at_level(logging.WARNING, logger="src.utils.trading_calendar"):
            days = get_trading_days("2023-12-29", "2024-01-03")
        assert any("2023" in r.getMessage() and "退化口径" in r.getMessage()
                   for r in caplog.records)
        # 2024-01-01 是元旦（有表 → 正确排除）；2023-12-29 周五正常开市
        assert date(2024, 1, 1) not in days
        assert date(2023, 12, 29) in days

    def test_todays_year_must_be_covered(self):
        """年度维护闸门：当前年份不在覆盖表里就红，逼每年初补表，而不是静默退化。

        这就是「显式告警」在 CI 上的落点：2027-01-01 起若不补 _HOLIDAY_RANGES，
        本用例会失败并给出要补哪一年。（本次 2026 通过。）
        """
        y = date.today().year
        assert has_official_calendar(y), (
            f"_HOLIDAY_RANGES 缺 {y} 年（当前覆盖 {CALENDAR_COVERED_YEARS}）："
            f"交易日判定会静默退化为仅周末规则（节假日按工作日算成交易日），"
            f"请先补官方休市区间再放开本用例")
