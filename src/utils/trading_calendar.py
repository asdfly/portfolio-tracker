"""A股本地交易日历（P2-A）

设计目标
- 完全离线：内置 2024 / 2025 / 2026 官方休市区间（来源：上交所 / 深交所 / 北交所公告）
- 交易日定义：周一至周五 且 不在休市区间内
- 年度表缺失时退化为"仅周末"规则，并打一次 warning（表需每年初更新）

覆盖范围与退化（2026-09-16 排查，task #66）
- **只有 2024/2025/2026 有官方休市表**；其余年份（含 2023 及更早、2027 及以后）一律
  退化为"仅周末"规则：节假日只要落在周一~周五就会被判成交易日。
- 这不是"理论上"的问题：库内 index_pe_history 覆盖 2009-10-30~2026-09-15（2018~2023
  每年 1600~3660 行）、index_quotes 覆盖 1990 至今、portfolio_snapshots 覆盖 2012 至今，
  所以任何"回看历史"的调用方都会踩在没有表的年份上。
  实测：`get_trading_days(2023-01-01, 2023-12-31)` 在退化口径下返回 260 天，
  用 index_quotes 的真实行情日期做基准是 242 天 —— 多出的 18 天全是节假日
  （2023-01-02、01-23~01-27 春节、05-01~05-03、06-22~06-23、09-29~10-06 国庆）。
  这类偏差会让"两个日期之间有多少个交易日"这种对外数字整体偏大（例：11 天春节间隔
  会被算成约 7 个交易日），且答案看起来完全正常。
- 因此退化不再只是"打一条日志"：`has_official_calendar` / `uncovered_years` /
  `degraded_years` 三个公开函数让调用方可编程地判断自己是否拿到了退化答案，
  两个批量入口（`get_trading_days`）会额外给出「本次结果里有多少天落在缺表年份内」。

说明
- 休市区间以闭区间 (start, end) 表达，含两端；周末本身已自动排除，不在区间内重复列。
- 元旦若跨年（如 2023-12-30~2024-01-01），只需记 2024-01-01（其余两天为周末自动休）。
- 补历史年份 vs 数据驱动的取舍见 task #66 报告：index_quotes 的"真实日期"本身就含伪行
  （实测 2026-06-19 收盘与前一日逐字相同，而该日是端午休市），直接用它反推会把污染
  固化进日历，故本模块仍以官方公告为准。
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

# 有官方休市表的年份（唯一权威来源就是 _HOLIDAY_RANGES，不另行维护）
CALENDAR_COVERED_YEARS: Tuple[int, ...] = tuple(sorted(_HOLIDAY_RANGES))

# 年度表缺失警告去重（每缺一年只告警一次）
_missing_year_warned: set = set()

# 本次进程内**实际发生过退化判定**的年份；日志可能被过滤/淹没，故同时留可编程痕迹
_degraded_years: set = set()


def has_official_calendar(year: int) -> bool:
    """该年份是否有官方休市表。False = 该年任何判定都是「仅周末」口径，不可信。"""
    try:
        return int(year) in _HOLIDAY_RANGES
    except (TypeError, ValueError):
        return False


def uncovered_years(start, end) -> List[int]:
    """[start, end] 区间内没有官方休市表的年份（升序）。非空 ⇒ 结果不可信。"""
    s, e = _to_date(start), _to_date(end)
    if s > e:
        s, e = e, s
    return [y for y in range(s.year, e.year + 1) if not has_official_calendar(y)]


def degraded_years() -> List[int]:
    """本次进程内实际退化的年份（升序）。管线可据此把「数字不可信」显式暴露出去。"""
    return sorted(_degraded_years)


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
    """判断某天是否为 A股交易日（周一~周五 且 非休市）。

    年份无官方休市表时退化为「仅周末」口径：节假日若落在工作日会被判成交易日。
    退化会（1）打一次 warning、（2）记入 `degraded_years()`，两者都只做一次/年。
    """
    d = _to_date(d)
    if d.weekday() >= 5:           # 周六=5, 周日=6
        return False
    closed = _closed_dates_for_year(d.year)
    if closed is None:
        _degraded_years.add(d.year)
        if d.year not in _missing_year_warned:
            _missing_year_warned.add(d.year)
            logger.warning(
                f"交易日历无 {d.year} 年官方休市表，退化为仅周末规则——该年所有"
                f"节假日（若为工作日）都会被判成交易日；已覆盖年份仅 "
                f"{list(CALENDAR_COVERED_YEARS)}。请补充 _HOLIDAY_RANGES，"
                f"或勿把该年的交易日计数用于对外数字。"
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
    """返回 [start, end] 闭区间内所有交易日（升序）。

    区间跨越无官方休市表的年份时，会额外打一条汇总 warning：这类结果用于
    「间隔交易日数」等对外数字会整体偏大（实测 2023 全年 260 vs 真实 242）。
    """
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

    missing = uncovered_years(s, e)
    if missing:
        covered = set(CALENDAR_COVERED_YEARS)
        unreliable = sum(1 for d in out if d.year not in covered)
        logger.warning(
            "get_trading_days(%s~%s) 跨 %s 年，这些年份无官方休市表：返回的 %d 天中"
            "有 %d 天是退化口径（节假日工作日被算成交易日），用于「间隔交易日数」"
            "会偏大；已覆盖年份仅 %s。",
            s, e, missing, len(out), unreliable, list(CALENDAR_COVERED_YEARS),
        )
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
