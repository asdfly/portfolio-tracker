"""A股本地交易日历（P2-A）

设计目标
- 完全离线：内置 2024 / 2025 / 2026 官方休市区间（来源：上交所 / 深交所 / 北交所公告）
- 交易日定义：周一至周五 且 不在休市区间内
- 年度表缺失时退化为"仅周末"规则，并打一次 warning（表需每年初更新）

说明
- 休市区间以闭区间 (start, end) 表达，含两端；周末本身已自动排除，不在区间内重复列。
- 元旦若跨年（如 2023-12-30~2024-01-01），只需记 2024-01-01（其余两天为周末自动休）。
- 2027 及以后年份未内置，会退化为仅周末规则——这是已知限制，警告提示维护者补表。
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 年度休市区间（闭区间，ISO 日期字符串）。数据来源：三大交易所官方公告。
# ---------------------------------------------------------------------------
_HOLIDAY_RANGES: Dict[int, List[Tuple[str, str]]] = {
    2024: [
        ("2024-01-01", "2024-01-01"),   # 元旦
        ("2024-02-09", "2024-02-17"),   # 春节（除夕休市）
        ("2024-04-04", "2024-04-06"),   # 清明节
        ("2024-05-01", "2024-05-05"),   # 劳动节
        ("2024-06-10", "2024-06-10"),   # 端午节
        ("2024-09-15", "2024-09-17"),   # 中秋节
        ("2024-10-01", "2024-10-07"),   # 国庆节
    ],
    2025: [
        ("2025-01-01", "2025-01-01"),   # 元旦
        ("2025-01-28", "2025-02-04"),   # 春节
        ("2025-04-04", "2025-04-06"),   # 清明节
        ("2025-05-01", "2025-05-05"),   # 劳动节
        ("2025-05-31", "2025-06-02"),   # 端午节
        ("2025-10-01", "2025-10-08"),   # 国庆节 + 中秋节
    ],
    2026: [
        ("2026-01-01", "2026-01-03"),   # 元旦
        ("2026-02-15", "2026-02-23"),   # 春节
        ("2026-04-04", "2026-04-06"),   # 清明节
        ("2026-05-01", "2026-05-05"),   # 劳动节
        ("2026-06-19", "2026-06-21"),   # 端午节
        ("2026-09-25", "2026-09-27"),   # 中秋节
        ("2026-10-01", "2026-10-07"),   # 国庆节
    ],
}

_MAX_LOOKAHEAD = 30   # next_trading_day 最多向前看的天数
_MAX_LOOKBACK = 30    # prev / last_on_or_before 最多向后看的天数

# 年度表缺失警告去重（每缺一年只告警一次）
_missing_year_warned: set = set()


def _to_date(d) -> date:
    """接受 date / datetime / 'YYYY-MM-DD' 字符串，统一成 date。"""
    if isinstance(d, datetime):
        return d.date()
    if isinstance(d, date):
        return d
    return datetime.strptime(str(d)[:10], "%Y-%m-%d").date()


def _closed_dates_for_year(year: int) -> Optional[set]:
    """返回该年休市日期集合；年份无内置表时返回 None（调用方退化为仅周末）。"""
    ranges = _HOLIDAY_RANGES.get(year)
    if ranges is None:
        return None
    out: set = set()
    for s, e in ranges:
        cur = _to_date(s)
        end = _to_date(e)
        while cur <= end:
            out.add(cur)
            cur += timedelta(days=1)
    return out


def is_trading_day(d) -> bool:
    """判断某天是否为 A股交易日（周一~周五 且 非休市）。"""
    d = _to_date(d)
    if d.weekday() >= 5:           # 周六=5, 周日=6
        return False
    closed = _closed_dates_for_year(d.year)
    if closed is None:
        if d.year not in _missing_year_warned:
            _missing_year_warned.add(d.year)
            logger.warning(
                f"交易日历无 {d.year} 年官方休市表，退化为仅周末规则；"
                f"请补充 _HOLIDAY_RANGES 以保证节假日准确。"
            )
        return True                # 仅周末规则：工作日即视为交易日
    return d not in closed


def next_trading_day(d, max_lookahead: int = _MAX_LOOKAHEAD) -> date:
    """返回 d 之后（不含 d）最近的交易日。"""
    d = _to_date(d)
    cur = d + timedelta(days=1)
    for _ in range(max_lookahead):
        if is_trading_day(cur):
            return cur
        cur += timedelta(days=1)
    logger.warning(f"next_trading_day: 在 {max_lookahead} 天内未找到交易日，返回 {cur}")
    return cur


def prev_trading_day(d, max_lookback: int = _MAX_LOOKBACK) -> date:
    """返回 d 之前（不含 d）最近的交易日。"""
    d = _to_date(d)
    cur = d - timedelta(days=1)
    for _ in range(max_lookback):
        if is_trading_day(cur):
            return cur
        cur -= timedelta(days=1)
    logger.warning(f"prev_trading_day: 在 {max_lookback} 天内未找到交易日，返回 {cur}")
    return cur


def last_trading_day_on_or_before(d) -> date:
    """返回不晚于 d 的最近交易日（d 本身若为交易日则直接返回）。"""
    d = _to_date(d)
    cur = d
    for _ in range(max_lookback := _MAX_LOOKBACK):
        if is_trading_day(cur):
            return cur
        cur -= timedelta(days=1)
    logger.warning(f"last_trading_day_on_or_before: 在 {max_lookback} 天内未找到交易日，返回 {cur}")
    return cur


def get_trading_days(start, end) -> List[date]:
    """返回 [start, end] 闭区间内所有交易日（升序）。"""
    s, e = _to_date(start), _to_date(end)
    if s > e:
        s, e = e, s
    out: List[date] = []
    cur = s
    # 安全上限：区间跨度 + 余量，避免极端情况下死循环
    cap = (e - s).days + 1 + _MAX_LOOKAHEAD
    for _ in range(max(cap, 1)):
        if cur > e:
            break
        if is_trading_day(cur):
            out.append(cur)
        cur += timedelta(days=1)
    return out


if __name__ == "__main__":
    # 只读自检
    checks = [
        ("2026-08-08", False),   # 周六
        ("2026-08-09", False),   # 周日
        ("2026-08-10", True),    # 周一
        ("2026-02-18", False),   # 春节休市
        ("2026-02-24", True),    # 春节后开市
        ("2026-10-05", False),   # 国庆休市
        ("2026-10-08", True),    # 国庆后开市
        ("2025-01-01", False),   # 元旦
        ("2024-02-10", False),   # 春节
    ]
    for ds, exp in checks:
        got = is_trading_day(ds)
        print(f"{ds}: is_trading_day={got} (expect {exp}) {'OK' if got == exp else 'FAIL'}")
    print("next_trading_day(2026-09-30) =", next_trading_day("2026-09-30"), "(expect 2026-10-08)")
    print("last_on_or_before(2026-08-09) =", last_trading_day_on_or_before("2026-08-09"), "(expect 2026-08-07)")
