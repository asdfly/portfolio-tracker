"""日频表写库守卫：非交易日拒绝写入（P1#11 采集侧下半场，2026-10-27）

## 为什么单独成模块，而不是在每一处写库点各写一遍

P1#11 上半场（2026-10-27 第一轮）只覆盖了 `portfolio_snapshots / portfolio_summary /
index_quotes / etf_technical` 四张主表，用的是 `PortfolioAnalyzer._compute_persist_enabled()`
—— 那是**运行级**判定（"这次运行该不该写库"）。盘面类三张表不由该分析器写，而是散落在
四个互不相干的写入点，且每个点的日期来源都不同：

| 表 | 写入点 | 日期来源 |
|---|---|---|
| `fund_flows` | `fund_flow.save_fund_flows()` | DataFrame 每行的 `date` 列 |
| `market_breadth` | `market_breadth.collect_market_breadth()` | 调用方传入的 `date_str` |
| `sector_daily_change` | `gen_combo_report._upsert_sectors()` | 报告 `DATA_DATE` |
| `sector_daily_change` | `backfill_sector_change.py` | NeoData K 线每行的日期 |

⚠️ 关键区别：这些点的日期**不是"今天"**。回填脚本合法地写 2026-07 的历史日期；
主分析可能因为跨零点而以昨天的日期落库。所以守卫必须判「**被写入的那个日期**」是不是
交易日，而不是判「运行当天」是不是交易日 —— 后者会把合法的历史回填全部误杀。

## 实测的污染形态（2026-10-01~10-07 国庆）

三张表都在国庆 6 个非交易日写了行，且**内容是 09-30 的逐字复制**：

- `fund_flows`：10-01~10-07 每天 90 行，与 09-30 的 90 行 100% 相同（含 net_inflow 数值）
- `sector_daily_change`：10-01 / 10-07 的 13 行 MIN/MAX 与 09-30 完全一致（−2.99 / 4.18）
- `market_breadth`：同样有 10-01~10-07 的行

这不是"多写了几行空行"，而是**把前一个交易日的资金流向伪装成新交易日的数据**。
下游任何「较上一交易日」的比较、连续领涨天数、主力资金趋势都会把这段复制当成真实变化。

## 三态语义（不静默）

本模块刻意**不返回 bool**，而是返回 `(allow, status, reason)`：

- `allow`：判定为交易日 ⇒ 正常写入
- `deny`：判定为非交易日 ⇒ 调用方**必须跳过**并计数
- `allow_degraded`：该年份无官方休市表，**判不了** ⇒ 放行（与改动前同一行为）但留 WARNING
- `allow_forced`：`PT_FORCE_PERSIST=1` 强制放行

`allow_degraded` 单独成一态，是因为「放行」有两种完全不同的理由：一种是有把握它是
交易日，另一种是**没有能力判断**。二者都返回 True，但混在一起就等于把"判不了"伪装成
"判过了" —— 这也是本项目反复踩过的静默失真型缺陷。
"""
from __future__ import annotations

import logging
import os
from datetime import date, datetime
from typing import Any, Callable, Dict, Iterable, List, Tuple

logger = logging.getLogger(__name__)

# 逃生阀：与 portfolio._compute_persist_enabled 同名同值，排查/回填时只设一个变量即可
FORCE_ENV = "PT_FORCE_PERSIST"

# 判定状态
ALLOW = "allow"                    # 确认是交易日
DENY = "deny"                      # 确认非交易日，必须跳过
ALLOW_DEGRADED = "allow_degraded"  # 无官方休市表，判不了，按旧行为放行
ALLOW_FORCED = "allow_forced"      # PT_FORCE_PERSIST=1
ALLOW_UNPARSED = "allow_unparsed"  # 日期无法解析，放行但留痕（见下）

_RESULT_LABEL = {
    ALLOW: "交易日",
    DENY: "非交易日",
    ALLOW_DEGRADED: "日历缺表-无法判定",
    ALLOW_FORCED: "强制放行",
    ALLOW_UNPARSED: "日期不可解析",
}


def _to_date(d: Any):
    """接受 date / datetime / 'YYYY-MM-DD' / 'YYYYMMDD' 字符串。

    🔴 必须支持 `YYYYMMDD`：`market_breadth.date` 实测全是这种无横线格式
    （41/41 行），而其它表是 `YYYY-MM-DD`。这本身是既存的不一致（见
    `docs/history` 与台账），但**不能因此让守卫在该表上失效** —— 那会出现
    「主表拦住了、market_breadth 还在写」的分裂，比不修更糟。
    """
    if isinstance(d, datetime):
        return d.date()
    if isinstance(d, date):
        return d
    s = str(d).strip()
    if len(s) >= 10 and s[4] == "-":          # 2026-10-27（可能带时间尾巴）
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    if len(s) >= 8 and s[:8].isdigit():       # 20261027
        return datetime.strptime(s[:8], "%Y%m%d").date()
    raise ValueError("unparsed date: %r" % (d,))


def check_write_date(d: Any, table: str = "") -> Tuple[bool, str, str]:
    """判定日期 `d` 是否可作为交易日写入日频表。

    Returns:
        (allow, status, reason)
        - allow: True 表示允许写入
        - status: 见 ALLOW / DENY / ALLOW_DEGRADED / ALLOW_FORCED / ALLOW_UNPARSED
        - reason: 人类可读说明，**含表名**，便于从日志反查是哪张表在写

    ⚠️ 调用方在 `status == DENY` 时**必须跳过写入**，且**必须计数上报**
    （跳过不报等于静默：读者会以为当天没数据是采集失败，而不是休市）。
    """
    tag = "[休市守卫%s]" % (":" + table if table else "")

    if os.environ.get(FORCE_ENV) == "1":
        reason = "%s %s 被 %s=1 强制放行（休市日会写入冻结行，仅限排查/回填）" % (
            tag, d, FORCE_ENV)
        logger.warning(reason)
        return True, ALLOW_FORCED, reason

    try:
        dd = _to_date(d)
    except (ValueError, TypeError) as e:
        # 不因为日期格式怪异就拦下所有写入 —— 那会把一个格式问题放大成数据断流。
        # 但必须留痕，否则下次排查又要从零猜。
        reason = "%s 日期不可解析(%r: %s) ⇒ 放行，但本次未做交易日校验" % (tag, d, e)
        logger.warning(reason)
        return True, ALLOW_UNPARSED, reason

    try:
        from src.utils.trading_calendar import has_official_calendar, is_trading_day
    except ImportError:
        reason = "%s 交易日历不可用 ⇒ 退回仅周末规则判定" % tag
        logger.warning(reason)
        # 与 P1#11 上半场同一退路口径：至少挡住周末，节假日拦不住（显式而非静默）
        if dd.weekday() >= 5:
            return False, DENY, "%s %s 为周末（日历不可用，按周末规则拦截）" % (tag, dd)
        return True, ALLOW_DEGRADED, reason

    if not has_official_calendar(dd.year):
        # 判不了 ≠ 判过了。放行保持与改动前一致，但状态单独标记。
        reason = ("%s %s 所在年份 %d 无官方休市表 ⇒ 无法判定，按旧行为放行"
                  "（节假日若落在工作日会照写）" % (tag, dd, dd.year))
        logger.warning(reason)
        return True, ALLOW_DEGRADED, reason

    if is_trading_day(dd):
        return True, ALLOW, "%s %s 为交易日" % (tag, dd)

    return False, DENY, "%s %s 为休市日（周末或法定节假日）⇒ 拒绝写入" % (tag, dd)


def filter_rows_by_trading_date(
    rows: Iterable[Any],
    date_getter: Callable[[Any], Any],
    table: str = "",
) -> Tuple[List[Any], Dict[str, int], str]:
    """从一批待写行里剔除落在非交易日的行。

    专为「一个 payload 里混了多个日期」的场景设计（`fund_flows` 的 df、回填脚本的
    K 线行）——这类场景没法在函数入口一次性早退，只能逐行判。

    Args:
        rows: 待写行（任意类型，只要 date_getter 能取到日期）
        date_getter: row -> 日期
        table: 表名，仅用于日志

    Returns:
        (kept_rows, dropped_counter, summary_text)
        - dropped_counter: {日期字符串: 被剔除的行数}
        - summary_text: 一行摘要，调用方直接打进日志即可；无剔除时为空串
    """
    kept: List[Any] = []
    dropped: Dict[str, int] = {}
    for row in rows:
        try:
            d = date_getter(row)
        except Exception:
            # 取不到日期不是"非交易日"，是调用方的 bug；放行让它照旧暴露
            kept.append(row)
            continue
        allow, _status, _reason = check_write_date(d, table=table)
        if allow:
            kept.append(row)
        else:
            key = str(d)
            dropped[key] = dropped.get(key, 0) + 1

    if not dropped:
        return kept, dropped, ""

    parts = ", ".join("%s×%d" % (k, v) for k, v in sorted(dropped.items()))
    summary = ("[休市守卫:%s] 剔除非交易日行 %d 条（%s），保留 %d 条"
               % (table, sum(dropped.values()), parts, len(kept)))
    logger.warning(summary)
    return kept, dropped, summary
