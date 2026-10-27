"""
投资组合分析器 - 整合所有分析功能（含风险分析）
"""
import logging
from datetime import date, datetime, timedelta
from typing import Dict, List, Any, Optional, Set
import sys
from pathlib import Path

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import (
    DATA_SOURCES, INDEX_CODES, TECH_INDICATORS,
    RISK_CONFIG, is_otc_fund,
)
from src.data_sources import DataSourceManager
from src.analysis.technical import TechnicalAnalyzer
from src.analysis.risk import RiskAnalyzer
from src.analysis.portfolio_risk import PortfolioRiskAnalyzer
from src.analysis.snapshot_gate import (
    check_snapshot_baseline, expected_universe, record_error_alert,
)
from src.utils.database import DatabaseManager
from src.utils.position_reader import PositionReader
from data_loader import get_db_connection
import sqlite3

logger = logging.getLogger(__name__)

# ============================================================================
# 份额折算闸门（conversion guard）
# ----------------------------------------------------------------------------
# 依据：A 股 ETF 单日涨跌幅上限为 ±10%（科创板/创业板类 ±20%），因此
# 「当日价 / 前日价」偏离 1 超过 ±25% 只可能是基金份额折算（拆分/合并）或基准重置，
# 不可能是真实收益。已实测 7 次折算（512010 ÷3.907、512100 ×2.76、516160 ×3.158、
# 159300 ×3.567、510500 ×3.566、159220 ÷2.0015、512810 ÷1.9988）全部远超该阈值。
# ============================================================================
CONVERSION_SUSPECT_RATIO = 0.25

# 复权价比探测命中的行，距目标日的最大自然日数。超过即视为陈旧不可用。
# 为什么必须有这条：探测用 `date <= 目标日 ORDER BY date DESC LIMIT 1`，陈旧的标的
# （场外基金只挂月末行、已清仓标的停更、采集失败留旧值）会让 prev/curr 两次探测
# 命中同一行，qfq_ratio 恒为 1.0 —— 等于把一个真实的折算跳变悄悄替换成
# 「伪造的 0% 收益」，比直接剔除更隐蔽。7 日取自然日，与交易日口径无关；
# 折算日前后都有正常交易日，7 日足够宽，不会误伤停牌造成的快照断点。
CONVERSION_QFQ_MAX_STALENESS_DAYS = 7

# 快照侧 quantity 比值的「未调整」容差。当复权价缺失（pre-2018 折算 / 表缺失 /
# 行情陈旧）时，用 `qty_ratio = quantity(date)/quantity(prev)` 区分真假折算：
#   qty_ratio ≈ 1.0（落在 [1-容差, 1+容差]）→ 价格跳变没有数量调整配平 ⇒ 折算假收益，剔除；
#   qty_ratio 远离 1.0（≈ 折算倍数）→ 数量已调增吸收折价（B族），用当日 quantity 计入即得真实 ~0。
# 0.15 的容差把「未调整」与「已按 2~4 倍调整」彻底分开（无真实交易会让 quantity 落在中间）。
CONVERSION_QTY_FLAT_TOL = 0.15


# ============================================================================
# 持仓合并（position merge，#115）
# ----------------------------------------------------------------------------
# 通达信导出的持仓文件只含**场内 22 只**；12 只场外基金结构性不在导出里
# （它们只存在于 portfolio_snapshots，由 scripts/fetch_otc_fund_nav.py 维护）。
# 原实现把「文件」当成**全组合的权威源**（整体替换 positions），于是
#   positions ≡ 22 只 ⇒ total_value ≡ 场内-only。
# 实证（logs/ 逐日）：09-01/02/04/07/08/09/10/11/14 与 09-16/17 的管线自报
#   总市值全部是 933k~952k 量级，而库内当日快照真实合计约 1.51M —— 恒少 38%。
# 修正方向：改为**合并**（并集）而不是替换；文件侧 code 一律以文件为准
#   （真实调仓不得被吞），文件里没有的 code 用库内该 code 最近一行前向填充。
# 合并候选域（不是"库里有就并"）：最近快照日的 code 集合 ∪ 目标日基线标的域。
#   为什么必须过滤：027293 是"周五份额"，近 30 个快照日里只在周五出现；
#   若逐 code 取最近一行，周三合并会把它从上周五填进来，把 total_value 抬高
#   29,364.32（34 行 ⇒ 1,527,928.85 vs 不过滤 35 行 ⇒ 1,557,293.17）。
#   域外标的**不并入但必须告警留痕**（`outside_universe`）。
#
# 顺手处置（§6.4 死路径）：已**删除** `_save_to_database`。它内联保存
#   snapshot + summary 却全仓零调用点，是一个能绕过 #116(b) 基线闸门的
#   summary 写入口——保留零调用后门等价于给闸门开门；"向后兼容"没有对象。
#   保存汇总的唯一合法路径是 run_daily_analysis 步骤6.5 闸门之后那一次调用。
# ============================================================================

# 前向填充允许的最大陈旧自然日数。超过则该 code 视为已退出的历史残留，
# 不并入当日组合（防"复活"早已清仓的标的，如 159732 停在 2026-07-30）。
FORWARD_FILL_MAX_STALENESS_DAYS = 10


class PortfolioAnalyzer:
    """投资组合分析器"""

    def __init__(self):
        self.ds_manager = DataSourceManager(DATA_SOURCES)
        self.tech_analyzer = TechnicalAnalyzer(TECH_INDICATORS)
        self.risk_analyzer = RiskAnalyzer(
            risk_free_rate=RISK_CONFIG['risk_free_rate'],
            trading_days_per_year=RISK_CONFIG['trading_days_per_year']
        )
        self.portfolio_risk_analyzer = PortfolioRiskAnalyzer(
            risk_free_rate=RISK_CONFIG['risk_free_rate']
        )
        self.db = DatabaseManager()
        self.position_reader = PositionReader()
        self.today = self._determine_trading_date()

    def _determine_trading_date(self) -> str:
        """确定当前交易日日期
        
        在开盘前（9:30前）运行时，API返回的是前一交易日的收盘数据，
        因此应使用前一交易日作为日期，而非date.today()。
        
        Returns:
            交易日字符串，格式 YYYY-MM-DD
        """
        now = datetime.now()
        current_time = now.hour * 100 + now.minute  # e.g. 930 = 9:30
        
        if current_time < 930:
            # 开盘前：使用前一交易日
            days_back = 3 if now.weekday() == 0 else 1  # 周一→上周五, 其他→昨天
            trading_date = now - timedelta(days=days_back)
            logger.info(
                f"开盘前运行({now.strftime('%H:%M')}), "
                f"使用前一交易日: {trading_date.strftime('%Y-%m-%d')}"
            )
            return trading_date.strftime('%Y-%m-%d')
        else:
            return now.strftime('%Y-%m-%d')

    def _detect_position_file_updated(self) -> bool:
        """检测持仓文件是否比上次写入DB的日期更新
        
        通过比较持仓文件日期与数据库中最新持仓记录日期来判断。
        如果文件日期 > 数据库最新日期，说明有新的持仓文件需要导入。
        同日文件更新检测：当file_date == db_date时，进一步比较文件修改时间
        与DB记录的created_at，或比较持仓数量是否有变化。
        """
        from config.settings import _find_latest_position_file, _extract_position_file_date

        file_path = _find_latest_position_file()
        file_date = _extract_position_file_date(file_path)

        if not file_date:
            logger.warning(f"无法从持仓文件名提取日期: {file_path}")
            return False

        # 查询数据库中最新的持仓记录日期
        latest_db = self.db.get_latest_portfolio()
        if not latest_db:
            # 数据库中没有任何记录，需要导入
            logger.info("数据库无持仓记录，需要导入持仓文件")
            return True

        db_date = latest_db[0].get('date', '')
        if file_date > db_date:
            logger.info(f"持仓文件日期 {file_date} > 数据库最新日期 {db_date}，需要更新持仓")
            return True

        # 同日文件更新检测
        if file_date == db_date:
            # 方法1: 比较文件修改时间与DB快照时间
            try:
                conn = get_db_connection(self.db.db_path)
                cur = conn.cursor()
                # 查询DB中当天snapshot的记录数和各code的数量
                cur.execute(
                    "SELECT code, quantity FROM portfolio_snapshots WHERE date=?",
                    (db_date,)
                )
                db_rows = {r[0]: r[1] for r in cur.fetchall()}
                conn.close()

                # 读取文件中的持仓数量
                positions = self.position_reader.read_positions()
                file_rows = {p['code']: p['quantity'] for p in positions}

                # 比较持仓列表是否一致
                if set(file_rows.keys()) != set(db_rows.keys()):
                    logger.info(f"同日持仓品种变化: 文件{len(file_rows)}只 vs DB{len(db_rows)}只，需要更新")
                    return True

                # 比较各品种数量
                changed_codes = []
                for code in file_rows:
                    if file_rows[code] != db_rows.get(code):
                        changed_codes.append(f"{code}({db_rows.get(code)}→{file_rows[code]})")

                if changed_codes:
                    logger.info(f"同日持仓数量变化: {', '.join(changed_codes)}，需要更新")
                    return True
                else:
                    logger.info(f"持仓文件日期 {file_date} == 数据库最新日期 {db_date}，持仓数量一致，无需更新")
                    return False
            except (sqlite3.OperationalError, sqlite3.IntegrityError, KeyError, ValueError) as e:
                logger.warning(f"同日持仓比对失败，跳过更新检测: {e}")
                return False

        logger.info(f"持仓文件日期 {file_date} < 数据库最新日期 {db_date}，持仓无更新，保持不变")
        return False

    def _merge_position_universe(self) -> Dict[str, Any]:
        """合并候选标的域 = 「最近一个快照日的 code 集合」 ∪ 「目标日基线标的域」。

        为什么需要这一层（不是"库里有就并进来"）：
          库里存在**只在特定星期出现的行**。实测近 30 个快照日行数分布 {34: 24, 35: 6}，
          35 行日全部是周五且多出的恒为 027293 —— 即 027293 是"周五份额"，
          非周五的权威快照**不含它**。若不做候选域过滤而单纯"逐 code 取最近一行"，
          周三合并会把 027293 从上周五前向填充进来，把 total_value 抬高 29,364.32
          （+1.9%）：库副本反证里 34 行 ⇒ 1,527,928.85，而不过滤时是 35 行 ⇒ 1,557,293.17。
          这正是"合并"最容易引入的新失真，必须在源头掐掉。

        两个子集都要，缺一不可：
          * 最近快照日 —— 覆盖**基线交集之外的真实持仓**（例如 3 天前才建的仓，
            30 日交集里没有它，但它在最近一天的行里），避免"新持仓被静默丢掉"；
          * 目标日基线域 —— 覆盖**最近快照日自身残缺**的情形：09-16 当天只写了 22 行，
            最近快照日的 code 集合里根本没有那 12 只场外，只能靠基线域补出来。

        基线不可判定（新库/历史不足 5 天）时 expected 为空集，退化为"最近快照日的 code
        集合"，并显式标注 baseline_available=False。
        """
        with get_db_connection(self.db.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute(
                "SELECT MAX(date) AS d FROM portfolio_snapshots WHERE date <= ?",
                (self.today,))
            row = cur.fetchone()
            latest_day = row["d"] if row else None
            recent = set()
            if latest_day:
                cur.execute(
                    "SELECT DISTINCT code FROM portfolio_snapshots WHERE date = ?",
                    (latest_day,))
                recent = {r["code"] for r in cur.fetchall()}
            uni = expected_universe(conn, self.today)

        expected = set(uni["expected_codes"]) if uni else set()
        return {
            "latest_day": latest_day,
            "recent_codes": recent,
            "expected_codes": expected,
            "baseline_available": uni is not None,
            "candidates": recent | expected,
        }

    def _load_snapshot_rows_per_code(self) -> Dict[str, Dict[str, Any]]:
        """取库内每个 code 在 ``<= self.today`` 的最近一行快照（含该行自己的 date）。

        为什么不按"最近一个快照日"整批取，而要逐 code 取各自最近一行：
          - 整批取的前提是"源日恰好 34 只齐全"，源日自身残缺时整批一起少
            （09-17 08:59 的无人值守回填就是这么把 09-16 打回 933,195.10 的）；
          - 场外基金 T+1 披露、且历史上常有单只断更，各只的"最近可得分位"本来就不同日；
          - 逐 code 取才能给出"这个 code 的价格取自哪一天"的逐条日志（#115 硬要求）。

        取 ``date <= self.today`` 而非 ``< self.today``：当日已有的行也要作为合并源，
        否则"当天先跑了一次残缺、下午再跑"时无法自愈。

        返回**全集**（不做候选域过滤），候选域过滤与"为什么被排除"的告警放在
        ``_merge_positions`` 里做 —— 过滤掉谁必须留痕，不能静默。
        """
        with get_db_connection(self.db.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute(
                """
                SELECT s.* FROM portfolio_snapshots s
                JOIN (
                    SELECT code, MAX(date) AS max_date
                    FROM portfolio_snapshots
                    WHERE date <= ?
                    GROUP BY code
                ) t ON s.code = t.code AND s.date = t.max_date
                """,
                (self.today,),
            )
            return {r["code"]: dict(r) for r in cur.fetchall()}

    def _merge_positions(self, file_positions: List[Dict[str, Any]]):
        """把「持仓文件（场内台账）」与「库内快照（含场外）」合并为并集（#115）。

        规则（顺序即优先级）：
          1. 文件里出现的 code **一律以文件为准**（数量/成本/名称都用文件的）——
             真实调仓（加仓、减仓、换标的）绝不能被库内旧行吞掉；
          2. 候选域 = 最近快照日 code 集合 ∪ 目标日基线标的域（见
             ``_merge_position_universe``）；域外的近期行（典型：只有周五出现的
             027293）**不并入**，并显式告警；
          3. 域内、且文件里没有的 code ⇒ 前向填充，**价格仍是它最近可得的那一日**
             （T+1 披露所致，15:30 时当日净值根本不存在）；
          4. 库内行距目标日超过 ``FORWARD_FILL_MAX_STALENESS_DAYS`` ⇒ 视为已清仓的
             历史残留，不并入（防"复活"早已退出的标的）。

        绝不伪造场外当日净值：填充行的 current_price/market_value 原样取自它自己的
        那一行，只把该行的现值带进当日组合，不改写成"当日值"。

        Returns:
            ``(merged_positions, info)``；info 供落日志与 results 留档。
        """
        file_by_code = {p["code"]: p for p in file_positions}
        db_rows = self._load_snapshot_rows_per_code()
        uni = self._merge_position_universe()
        candidates = uni["candidates"]
        today_dt = date.fromisoformat(str(self.today)[:10])

        carried: List[Dict[str, Any]] = []
        stale: List[Dict[str, Any]] = []
        outside: List[Dict[str, Any]] = []
        for code in sorted(db_rows):
            if code in file_by_code:
                continue
            row = db_rows[code]
            row_date = str(row.get("date") or "")[:10]
            try:
                age = (today_dt - date.fromisoformat(row_date)).days
            except ValueError:
                stale.append({"code": code, "date": row_date, "age_days": None,
                              "reason": f"日期无法解析: {row_date!r}"})
                continue
            if age > FORWARD_FILL_MAX_STALENESS_DAYS:
                stale.append({"code": code, "date": row_date, "age_days": age,
                              "reason": f"陈旧 {age} 天 > {FORWARD_FILL_MAX_STALENESS_DAYS} 天"})
                continue
            if code not in candidates:
                # 域外：库里有、但既不在最近快照日、也不在目标日基线域
                outside.append({"code": code, "date": row_date, "age_days": age,
                                "name": row.get("name"),
                                "market_value": row.get("market_value")})
                continue
            carried.append({"code": code, "date": row_date, "age_days": age,
                            "name": row.get("name")})

        merged = list(file_positions) + [dict(db_rows[c["code"]]) for c in carried]

        # 显式日志：合并了哪些 code、价格各自取自哪一天（不许静默 fill-forward）
        by_date: Dict[str, List[str]] = {}
        for c in carried:
            by_date.setdefault(c["date"], []).append(c["code"])
        logger.info(
            "[持仓合并] 持仓文件 %d 只（场内台账） + 库内前向填充 %d 只 ⇒ 合并后 %d 只"
            "（候选域 %d 只：最近快照日 %s 的 %d 只 ∪ 基线域 %d 只，基线可用=%s）",
            len(file_positions), len(carried), len(merged), len(candidates),
            uni["latest_day"], len(uni["recent_codes"]), len(uni["expected_codes"]),
            uni["baseline_available"]
        )
        for d, codes in sorted(by_date.items()):
            logger.info(
                "[持仓合并] 共 %d 只取自 %s（未在导出中，价格沿用该日净值，不伪造当日净值）: %s",
                len(codes), d, ", ".join(codes)
            )
        for c in outside:
            logger.warning(
                "[持仓合并] 不并入 %s(%s)：库内最近一行 %s（%s 天前，市值 %.2f）"
                "既不在最近快照日也不在目标日基线域 —— 典型为只在特定星期出现的份额"
                "（027293 仅周五），并入会把 total_value 抬高；如属真实持仓请更新持仓文件",
                c["code"], c.get("name") or "", c["date"], c["age_days"],
                float(c.get("market_value") or 0)
            )
        for c in stale:
            logger.warning("[持仓合并] 跳过 %s(%s)：%s",
                           c["code"], c.get("name") or "", c["reason"])

        # ====================================================================
        # 口径 B（2026-09-17 裁定）：价格「当日可得」的 code 集合
        # --------------------------------------------------------------------
        # `daily_return` 的分子/分母只允许包含**当日价新鲜**的标的：
        #   * 文件里的 code —— 场内台账，TDX 当日导出，价格即当日；
        #   * carried 且 `age_days == 0` —— 库内源行就是目标日本身。
        # 其余 carried（`age_days >= 1`）是**前向填充**：价格取自更早的一天，
        # 它跨的不是一个交易日（场外 T+1 披露，15:30 时当日净值根本不存在），
        # 纳入会把一段多日收益压成「一个交易日」的收益 —— 必须排除。
        # 判据复用 `_merge_positions` 的 `age` 语义，不另立第二套（两套判据必然分叉）。
        # ====================================================================
        fresh_codes = {p["code"] for p in file_positions}
        fresh_codes |= {c["code"] for c in carried if c.get("age_days") == 0}
        stale_price_codes = sorted(c["code"] for c in carried if c.get("age_days") != 0)
        for c in carried:
            if c.get("age_days") != 0:
                logger.info(
                    "[口径B] %s(%s) 价格取自 %s（%s 天前）⇒ 排除在 daily_return 的分子/分母之外",
                    c["code"], c.get("name") or "", c["date"], c["age_days"]
                )

        info = {
            "file_n": len(file_positions),
            "db_n": len(db_rows),
            "carried_n": len(carried),
            "merged_n": len(merged),
            "carried": carried,
            "stale_skipped": stale,
            "outside_universe": outside,
            "fresh_codes": sorted(fresh_codes),
            "stale_price_codes": stale_price_codes,
            "universe": {
                "latest_day": uni["latest_day"],
                "recent_n": len(uni["recent_codes"]),
                "expected_n": len(uni["expected_codes"]),
                "baseline_available": uni["baseline_available"],
            },
            "source_dates": {d: sorted(codes) for d, codes in by_date.items()},
        }
        return merged, info

    def _compute_persist_enabled(self) -> bool:
        """本次运行是否应把结果写入日频表（P1#11，2026-10-27）。

        ## 为什么休市日「可以跑，但不能写」

        休市日（周末 / 法定节假日）运行时，行情源返回的是**上一交易日的冻结价**。
        这些价格本身没毛病，问题在落库的 `date`：一旦写成 `self.today`（休市日），
        `MAX(date)` 就跳到了休市日 —— 而 `MAX(date)` 在整个下游被当作「最近交易日」
        的同义词使用（报告 DATA_DATE、NAV 结算、补采队列基准、风险指标窗口）。

        实测代价（2026-10-01~10-07 国庆）：连续 7 天写入 `total_value=1564503.62`
        的冻结行、`daily_return=0.0`，报告 `DATA_DATE` 逐日跳到休市日，并在正文里
        拿冻结的 0.0 与真实沪深300涨幅相减去讲「跑输」。台账为此建了 5 条独立待办
        （#67/#68/#75/#29/#60），全部指向同一个根因：**采集器不认节假日**。

        ## 为什么不是「休市日直接不跑」

        休市日仍需产出报告（读者要看「最近交易日收盘情况」的复盘），因此**分析照跑**，
        只把写库这一步关掉。报告侧早已按 `RUN_DATE ∈ CN_HOLIDAYS` 做了文案降级
        （`IS_HOLIDAY_RUN`），数据源取到的是最近交易日真实收盘 —— 不写库反而更准。

        ## 退化与逃生
        - 年份无官方休市表时 `trading_calendar` 退化为「仅周末」，此时判为交易日
          ⇒ 行为与本改动前完全一致（不为历史年份引入新的静默差异）。
        - `PT_FORCE_PERSIST=1` 强制写库（排查/回填需要时用），并打 WARNING 留痕。
        """
        import os as _os
        if _os.environ.get('PT_FORCE_PERSIST') == '1':
            logger.warning("[P1#11] PT_FORCE_PERSIST=1 ⇒ 忽略交易日判定，强制写库"
                           "（休市日会写入冻结行，仅限排查/回填使用）")
            return True
        try:
            from src.utils.trading_calendar import is_trading_day as _cal_is_td
        except ImportError:
            # 不静默放行：日历不可用时退回「周末规则」——与本改动前同一口径，
            # 即节假日仍会写行。显式 WARNING 而非静默。
            logger.warning("[P1#11] 交易日历不可用 ⇒ 退回仅周末规则，节假日仍会写库")
            return datetime.strptime(str(self.today)[:10], "%Y-%m-%d").weekday() < 5
        d = datetime.strptime(str(self.today)[:10], "%Y-%m-%d").date()
        if _cal_is_td(d):
            return True
        logger.warning(
            "[P1#11] %s 为休市日（周末或法定节假日）⇒ 分析照跑、**不写日频表**："
            "portfolio_snapshots / portfolio_summary / index_quotes / etf_technical "
            "一律跳过，避免 MAX(date) 被冻结行顶到休市日", self.today)
        return False

    def run_daily_analysis(self) -> Dict[str, Any]:
        """执行每日分析
        
        逻辑：
        - 如果持仓文件有更新 → 读取新持仓文件，更新持仓快照 + 行情 + 技术指标 + 风险指标
        - 如果持仓文件无更新 → 从数据库读取最近一次持仓，仅更新行情（价格、市值、盈亏等）
        - 指数行情、技术指标每天都会重新采集/计算
        """
        logger.info("=" * 60)
        logger.info("开始执行投资组合每日分析")
        logger.info("=" * 60)

        results = {
            'date': self.today,
            'positions': [],
            'position_merge': {},
            'indices': {},
            'technical': {},
            'risk': {},
            'summary': {}
        }

        # P1#11：休市日分析照跑、写库关闭。所有 save_* 调用统一由该布尔门控，
        # 避免「漏改一处就连带污染一张表」。
        persist = self._compute_persist_enabled()
        results['persist'] = persist

        try:
            # 检测持仓文件是否有更新
            position_updated = self._detect_position_file_updated()

            # 口径 B：只有「持仓文件更新」分支会调用 _merge_positions，另一分支没有
            # 合并信息 ⇒ 显式置 None，由 _calculate_summary 按退化顺序判定新鲜度并留痕，
            # 不要在这里"顺手"编一份假的 fresh 集合。
            merge_info = None

            if position_updated:
                # ========== 持仓有更新：读取新文件 ==========
                logger.info("步骤1: 检测到新持仓文件，读取持仓数据...")
                file_positions = self.position_reader.read_positions()
                logger.info(f"读取到 {len(file_positions)} 条持仓记录（持仓文件 = 场内台账）")

                # ---- 步骤1.5: 与库内快照合并（#115）----
                # 持仓文件只含场内 22 只；12 只场外基金结构性不在导出里，只能从
                # 库内快照前向填充。原实现整体替换 ⇒ total_value 恒为场内-only。
                positions, merge_info = self._merge_positions(file_positions)
                results['positions'] = positions
                results['position_merge'] = merge_info
                logger.info("步骤1.5: 合并后持仓 %d 只（文件 %d + 库内填充 %d）",
                            len(positions), merge_info['file_n'], merge_info['carried_n'])

                # 更新实时行情
                logger.info("步骤2: 获取实时行情...")
                self._update_realtime_quotes(positions)

                # 保存持仓快照（新持仓数据写入数据库）
                if persist:
                    logger.info("步骤2.5: 保存新持仓快照到数据库...")
                    self.db.save_portfolio_snapshot(self.today, positions)
                else:
                    logger.info("步骤2.5: 休市日 ⇒ 跳过持仓快照写入（P1#11）")

            else:
                # ========== 持仓无更新：从数据库加载最近持仓 ==========
                logger.info("步骤1: 持仓文件无更新，从数据库加载最近持仓数据...")
                db_positions = self.db.get_latest_portfolio()

                if not db_positions:
                    raise RuntimeError("数据库中无持仓记录，且持仓文件无更新，无法继续分析")

                # 将数据库字段映射为分析器需要的格式
                positions = []
                for row in db_positions:
                    pos = dict(row)
                    # 确保关键字段存在
                    pos.setdefault('code', row.get('code'))
                    pos.setdefault('name', row.get('name'))
                    pos.setdefault('quantity', row.get('quantity'))
                    pos.setdefault('cost_price', row.get('cost_price'))
                    pos.setdefault('current_price', row.get('current_price'))
                    pos.setdefault('market_value', row.get('market_value'))
                    pos.setdefault('pnl', row.get('pnl'))
                    pos.setdefault('pnl_rate', row.get('pnl_rate'))
                    pos.setdefault('ytd_return', row.get('ytd_return'))
                    pos.setdefault('beta', row.get('beta'))
                    positions.append(pos)

                results['positions'] = positions
                logger.info(f"从数据库加载 {len(positions)} 条持仓记录")

                # 更新实时行情（用最新价格重新计算市值和盈亏）
                logger.info("步骤2: 获取实时行情并更新持仓价格...")
                self._update_realtime_quotes(positions)

                # 用更新后的价格重新保存今日持仓快照（保持持仓结构不变，仅更新价格）
                if persist:
                    logger.info("步骤2.5: 更新持仓价格数据到数据库...")
                    self.db.save_portfolio_snapshot(self.today, positions)
                else:
                    logger.info("步骤2.5: 休市日 ⇒ 跳过持仓快照写入（P1#11）")

            # ========== 以下步骤每天都会执行 ==========

            # 获取指数行情
            logger.info("步骤3: 获取指数行情...")
            index_quotes = self._fetch_index_quotes()
            results['indices'] = index_quotes

            # 保存指数行情
            # 🔴 占位行 bug 根治（2026-10-08）：缓存兜底（_cached）行情是陈旧值，若以
            # self.today 落库会成为「最新日期行」并伪装成今日收盘价，污染 MAX(date) 与下游
            # gen_combo_report / enhanced_report。故仅持久化真实实时行情行；缓存兜底行只在
            # 内存中供当日分析使用（change_pct=None 如实反映「无今日真实价」），不写库。
            fresh_index_quotes = {
                code: q for code, q in index_quotes.items()
                if not q.get("_cached")
            }
            if persist:
                self.db.save_index_quotes(self.today, fresh_index_quotes)
            else:
                logger.info("步骤3: 休市日 ⇒ 跳过指数行情写入（P1#11），"
                            "报告取库内最近交易日收盘")

            # 计算技术指标
            logger.info("步骤4: 计算技术指标...")
            tech_results = self._calculate_technical_indicators(positions)
            results['technical'] = tech_results

            # 保存技术指标
            if persist:
                for code, indicators in tech_results.items():
                    self.db.save_technical_indicators(self.today, code, indicators)
            else:
                logger.info("步骤4: 休市日 ⇒ 跳过技术指标写入（P1#11）")

            # 风险分析
            logger.info("步骤5: 风险分析...")
            risk_results = self.portfolio_risk_analyzer.analyze_portfolio_risk(
                positions, index_quotes
            )
            results['risk'] = risk_results

            # 计算汇总数据
            logger.info("步骤6: 计算汇总数据...")
            summary = self._calculate_summary(positions, index_quotes, risk_results,
                                              merge_info=merge_info)
            results['summary'] = summary

            # ========== 步骤6.5: 快照基线闸门（#116 契约2）==========
            # 判据：当日 portfolio_snapshots 覆盖基线标的域 **且** 本次 positions
            # 覆盖当日全部快照行。不通过 ⇒ **拒绝生成** portfolio_summary。
            # 为什么必须在写库之前、而不是事后校验：
            #   2026-09-16 场外整篮子被跳过（场外源当日无净值），当日快照只有 22 行，
            #   total_value 只剩场内 ETF 的 933,195.10（真实 1,527,928.85，少 38.1%），
            #   残缺值被写进库并推给用户；2026-09-17 08:59 的无人值守回填运行又把
            #   刚修好的 09-16 summary 打回同一个残缺值 —— 一次拒绝比十次事后补救便宜。
            #
            # P1#11：休市日**不写** snapshot/summary，闸门的前提（「当日快照存在」）
            # 本就不成立；此时跑闸门必然判 not ok 并写一条 error 级告警 —— 那是纯噪音，
            # 且会把「休市」误报成「快照塌缩」。故在闸门之前先短路。
            if not persist:
                # summary 仍放进 results 供报告侧使用（报告要展示「最近交易日」的
                # 分析结果，只是不落库）。
                logger.info("步骤6.5/7: 休市日 ⇒ 跳过快照闸门与 portfolio_summary 写入（P1#11）")
                logger.info("分析完成(休市日, 仅分析不落库)!")
                return results

            gate = check_snapshot_baseline(self.db.db_path, self.today, positions,
                                           computed_total_value=summary.get('total_value'))
            results['snapshot_gate'] = gate
            if not gate["ok"]:
                logger.error("[快照闸门] 拒绝生成 portfolio_summary(%s): %s",
                             self.today, gate["reason"])
                logger.error("[快照闸门] error 级告警落库=%s（alerts 表）",
                             gate.get("alert_written"))
                logger.info("分析完成(汇总被基线闸门拒绝, 未写入 portfolio_summary)!")
                return results

            # 保存组合汇总
            logger.info("步骤7: 保存汇总数据到数据库...")
            risk_summary = summary.get('risk_summary', {})
            # P1#14a（2026-10-27）：回撤分窗口三列此前**从未落库**（实测 3506/3506 行
            # 100% NULL）。根因不是算不出来——risk.py 的 drawdown_metrics 一直在返回
            # max_drawdown_60d/_1y/_all，_calculate_summary 也一直把它们收进
            # risk_summary；是**这里组装写库字典时只挑了 3 个键**，另外 3 个被静默丢弃。
            # 即「算出来了、也传进来了、最后一步没写」——纯丢键，不是数据缺口。
            summary_with_risk = {
                **summary,
                'sharpe_ratio': risk_summary.get('sharpe_ratio'),
                'max_drawdown': risk_summary.get('max_drawdown'),
                'max_drawdown_60d': risk_summary.get('max_drawdown_60d'),
                'max_drawdown_1y': risk_summary.get('max_drawdown_1y'),
                'max_drawdown_all': risk_summary.get('max_drawdown_all'),
                'volatility': risk_summary.get('annual_volatility'),
                # P1#12：结算标记随值一起落库（`**summary` 已带上，此处显式化以防
                # 有人把 `**summary` 拆掉时静默丢键——这两列的缺失是静默失败而非报错）
                'daily_return_settled': summary.get('daily_return_settled'),
            }
            self.db.save_portfolio_summary(self.today, summary_with_risk)

            logger.info("分析完成!")
            return results

        except (sqlite3.OperationalError, sqlite3.IntegrityError) as e:
            logger.error(f"分析过程出错: {e}", exc_info=True)
            raise

    def _update_realtime_quotes(self, positions: List[Dict[str, Any]]):
        """更新实时行情到持仓数据"""
        codes = [p['code'] for p in positions]
        quotes = self.ds_manager.get_batch_quotes(codes)

        # 口径 B（2026-09-17）：记录**本次真正取到实时行情**的 code 集合。
        # 判「当日价是否新鲜」不能看 `realtime_price` 是否存在 —— 下面 else 分支对
        # 「无行情」的标的（典型：场外基金）也会把 realtime_price 赋成旧值，
        # 两类标的长得一模一样。真正可分辨的只有「有没有拿到 quote」。
        self._quoted_codes: Set[str] = set(quotes.keys())

        for pos in positions:
            code = pos['code']
            if code in quotes:
                quote = quotes[code]
                price = quote.get('price', 0)

                # 价格合理性校验：价格必须为正，且与当前价格偏离不超过±30%
                current_price = pos.get('current_price', 0)
                if price > 0 and current_price > 0 and abs(price - current_price) / current_price > 0.3:
                    logger.warning(
                        f"行情价格异常: {code}({pos.get('name')}) "
                        f"实时={price}, 当前={current_price}, 偏离={abs(price-current_price)/current_price*100:.1f}%, 跳过更新"
                    )
                    continue

                if price > 0:
                    pos['realtime_price'] = price
                else:
                    pos['realtime_price'] = current_price

                pos['realtime_change_pct'] = quote.get('change_pct', 0)
                pos['volume'] = quote.get('volume', 0)
                pos['amount'] = quote.get('amount', 0)
                pos['high'] = quote.get('high', 0)
                pos['low'] = quote.get('low', 0)
                pos['open'] = quote.get('open', 0)
                pos['pre_close'] = quote.get('pre_close', 0)

                # 重新计算市值和盈亏
                pos['realtime_market_value'] = pos['realtime_price'] * pos['quantity']
                pos['realtime_pnl'] = (pos['realtime_price'] - pos['cost_price']) * pos['quantity']
            else:
                # 无实时行情时，使用已有价格计算盈亏（覆盖场外基金等无行情品种）
                cur_p = pos.get('current_price', 0) or 0
                cost_p = pos.get('cost_price', 0) or 0
                qty = pos.get('quantity', 0) or 0
                pos['realtime_price'] = cur_p
                pos['realtime_market_value'] = cur_p * qty
                pos['realtime_pnl'] = (cur_p - cost_p) * qty
                pos['realtime_change_pct'] = 0
                pos['pre_close'] = 0

    def _fetch_index_quotes(self) -> Dict[str, Dict[str, Any]]:
        """获取指数行情

        单只指数取数失败必须被隔离——仅告警跳过该只，不得上抛拖垮整轮主分析。
        注意 ``get_quote`` 在故障转移（多源）全失败后抛的是 ``DataSourceError``
        （继承 ``Exception``，**非** ``OSError``），故需显式捕获；底层网络异常仍可能
        以 ``OSError`` 形态漏出，一并捕获。

        实时取数失败后**回退 DB 缓存兜底**（2026-09-24 新增，针对中证2000）：
          中证2000(932000) 既有取数链被 host 级 RST 阻断（东财 push2his 不可达），
          腾讯 fqkline 仅返 1 天、tushare 无权限、NeoData 无 .CSI 数据。该指数已由
          官方中证指数公司源（akshare stock_zh_index_hist_csindex）通过
          scripts/backfill/refresh_sh932000.py 做全量回填并每日刷新（见
          scripts/backfill/backfill_index_quotes_westock.py 的「数据源解耦说明」）。
          故实时失败时回退到库内最新一行，保证 index_quotes 始终有该基准，避免下游模块读空。
          回退值带 ``_cached=True`` 标记，change_pct 仅在缓存即当日时采信，否则置 None。
          ⚠️ 带 ``_cached`` 的行情**不会落库**（见 save_index_quotes 与 analyze 的占位行根治），
          以免污染 MAX(date) 并伪装成今日收盘价。
        """
        from src.data_sources.base import DataSourceError
        quotes = {}
        for code in INDEX_CODES.keys():
            try:
                quote = self.ds_manager.get_quote(code)
                quotes[code] = quote
            except (DataSourceError, OSError) as e:
                logger.warning(f"获取指数 {code} 实时行情失败，尝试回退 DB 缓存: {e}")
                cached = self._fallback_index_quote_from_db(code)
                if cached is not None:
                    quotes[code] = cached
                    logger.warning(f"指数 {code} 使用 DB 缓存兜底（非实时，日期={cached.get('_cached_date')}）")
                else:
                    logger.warning(f"指数 {code} 既无实时也无 DB 缓存，跳过该基准")
        return quotes

    def _fallback_index_quote_from_db(self, code: str) -> Dict[str, Any]:
        """实时取数失败时的 DB 缓存兜底：取 index_quotes 中该 code 的最新一行。

        返回与 ``get_quote`` 同构的 dict（price/change_pct/volume/amount/name），
        附加 ``_cached`` 与 ``_cached_date`` 标记供日志/下游识别。取数异常时返回 None。
        """
        import sqlite3
        try:
            conn = sqlite3.connect(str(self.db.db_path))
            row = conn.execute(
                "SELECT date, name, close, change_pct, volume, amount "
                "FROM index_quotes WHERE code=? ORDER BY date DESC LIMIT 1",
                (code,),
            ).fetchone()
            conn.close()
            if not row:
                return None
            d, name, close, change_pct, volume, amount = row
            is_today = (d == self.today)
            return {
                "name": name,
                "price": close,
                "change_pct": (change_pct if is_today else None),
                "volume": volume,
                "amount": amount,
                "_cached": True,
                "_cached_date": d,
            }
        except Exception as e:  # 兜底本身不能拖垮主分析
            logger.warning(f"读取指数 {code} DB 缓存失败: {e}")
            return None

    @staticmethod
    def _pick_number(d: Dict[str, Any], *keys, default=0):
        """按顺序取第一个**存在**的数值，0 是合法值不得跳过。

        为什么不能写 `d.get(a) or d.get(b) or 0`：
        `or` 链会把合法的 0 当成假值继续往后找。若 realtime_pnl 真实为 0（当日盈亏持平），
        会错误地回落到旧的 pnl（一个非零的历史值），把"持平"显示成盈利/亏损。
        同时这里用 `v == v` 排除 NaN（NaN 不等于自身），避免 nan 污染求和。
        """
        for k in keys:
            v = d.get(k)
            if v is not None and v == v:  # v == v 为 False 仅当 v 是 NaN
                return v
        return default

    def _conversion_qfq_ratio(self, cur, code: str, prev_dt: str):
        """折算闸门辅助：取 code 在 prev_dt / self.today 的前复权(qfq)价比。

        各取「date <= 目标日」的最近一行（容忍停牌/非交易日），但命中行必须距目标日
        不超过 CONVERSION_QFQ_MAX_STALENESS_DAYS 自然日，否则视为不可用。

        任一探测「查不到」或「陈旧」都返回 None —— 调用方据此把该标的从日收益里
        剔除，绝不让假价比、也不让伪造的 0% 收益进入求和。

        Args:
            cur: 调用方已打开的游标（复用 _calculate_summary 里的 conn，不新开连接）
            code: 标的代码
            prev_dt: 前一交易日（YYYY-MM-DD）

        Returns:
            qfq_ratio = close_today / close_prev，或 None
        """
        closes = {}
        probe_dates = {}
        try:
            for label, target in (('prev', prev_dt), ('curr', self.today)):
                cur.execute(
                    "SELECT date, close FROM etf_price_history "
                    "WHERE code = ? AND date <= ? AND close IS NOT NULL AND close > 0 "
                    "ORDER BY date DESC LIMIT 1",
                    (code, target)
                )
                row = cur.fetchone()
                if not row or row[1] is None:
                    logger.warning(
                        "[折算闸门] %s 无可用复权价（无复权价数据，目标日 %s），已剔除",
                        code, target
                    )
                    return None
                try:
                    row_dt = date.fromisoformat(str(row[0])[:10])
                    target_dt = date.fromisoformat(str(target)[:10])
                except ValueError:
                    logger.warning(
                        "[折算闸门] %s 无可用复权价（日期无法解析 %s），已剔除",
                        code, row[0]
                    )
                    return None
                stale_days = (target_dt - row_dt).days
                if stale_days > CONVERSION_QFQ_MAX_STALENESS_DAYS:
                    logger.warning(
                        "[折算闸门] %s 无可用复权价（复权价数据陈旧：最近 %s，距目标 %d 天 "
                        "> %d 天），已剔除",
                        code, row[0], stale_days, CONVERSION_QFQ_MAX_STALENESS_DAYS
                    )
                    return None
                closes[label] = float(row[1])
                probe_dates[label] = row_dt
        except sqlite3.OperationalError:
            # 表不存在（老库/测试夹具）等同「查不到」
            logger.warning(
                "[折算闸门] %s 无可用复权价（etf_price_history 不可用），已剔除", code
            )
            return None
        if closes['prev'] <= 0 or closes['curr'] <= 0:
            return None
        # 两次探测命中同一行 —— 价比恒为 1.0，是无意义的伪造值（今日行情尚未落库
        # 时很常见）。宁可剔除，也不要让折算跳变被替换成「0% 收益」。
        if probe_dates['prev'] == probe_dates['curr']:
            logger.warning(
                "[折算闸门] %s 无可用复权价（两次探测命中同一行 %s，价比无意义），已剔除",
                code, probe_dates['prev']
            )
            return None
        return closes['curr'] / closes['prev']

    def _calculate_technical_indicators(self, positions: List[Dict[str, Any]]) -> Dict[str, Any]:
        """计算技术指标"""
        from src.data_sources.base import DataSourceError

        results = {}

        for pos in positions:
            code = pos['code']
            # 场内ETF/LOF代码规则: 51/56/58开头为上海，15开头为深圳
            # 场外基金（519/001/002/003/004/005/007/008/100/166等）无K线，直接跳过
            is_on_market = (code.startswith('51') or code.startswith('56')
                           or code.startswith('58') or code.startswith('15'))
            if not is_on_market:
                logger.debug(f"跳过非场内标的 {code}({pos.get('name', '')})的技术指标计算")
                continue
            if code.startswith('51') or code.startswith('56') or code.startswith('58'):
                ds_code = f'sh{code}'
            else:
                ds_code = f'sz{code}'

            try:
                kline = self.ds_manager.get_kline(ds_code, period='day', count=40)

                if len(kline) >= 30:
                    indicators = self.tech_analyzer.calculate_all(kline)
                    results[code] = indicators
                else:
                    logger.warning(f"{code} K线数据不足")

            except DataSourceError as e:
                logger.warning(f"获取 {code}({pos.get('name', '')}) K线失败，跳过技术指标计算: {e}")
            except (ValueError, KeyError, TypeError, IndexError) as e:
                logger.warning(f"计算 {code} 技术指标失败: {e}")

        return results

    def _calculate_summary(self, positions: List[Dict[str, Any]],
                          index_quotes: Dict[str, Dict[str, Any]],
                          risk_results: Dict[str, Any],
                          merge_info: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """计算汇总数据

        Args:
            merge_info: `_merge_positions` 的返回值；口径 B（2026-09-17）用它判定
                「价格是否当日可得」。为 ``None`` 时按下列顺序退化（并留痕），
                **不静默**：① 本次真正取到实时行情的 code 集合；② 持仓行自身的
                `date` 是否等于目标日。
        """
        # 使用实时价格计算
        total_value = sum(p.get('realtime_market_value', p['market_value']) for p in positions)
        total_cost = sum(p['cost_price'] * p['quantity'] for p in positions)
        total_pnl = sum(self._pick_number(p, 'realtime_pnl', 'pnl') for p in positions)

        # 计算日涨跌（校正版：用共同持仓相同数量×当日价格 vs 前日市值，避免新增/加仓导致跳变）
        daily_pnl = 0
        daily_return = 0
        prev_value = 0
        guard_fired = False  # 折算闸门是否介入（用于抑制下方 total_value fallback）
        # 口径 B（2026-09-17）：`daily_return == 0` **不是**「未计算」——真实平价日
        # 算出来就是合法值 0.0。用 `== 0` 当哨兵会让平价日静默换回「全持仓口径」的
        # 兜底分支，序列变成「多数日 A 口径、个别日 B 口径」的混合口径。
        # 故改用独立布尔哨兵，`0.0` 一律保留。
        daily_return_computed = False
        coverage = {
            "computed": False,
            "freshness_source": None,
            "included_n": None,
            "total_n": len(positions),
            "comparable_n": None,
            "prev_value_included": None,
            "prev_value_comparable": None,
            "value_share": None,
            "excluded_stale": [],
        }

        def _resolve_fresh_codes():
            """返回 (fresh_codes, source)。source 供报告显式标注，禁止静默。

            退化顺序（每一档都必须留痕）：
              ① `merge_info` —— 权威：文件侧 code + carried 中 `age_days == 0`；
              ② 本次真正取到实时行情的 code 集合（`_update_realtime_quotes` 记录）；
              ③ 持仓行自带的 `date`：**缺失**视为「无 as-of 证据 ⇒ 当日」，早于目标日
                 才判非当日。

            为什么 ③ 对「缺失」取「当日」而不是「非当日」：
              两档都不可用时（`merge_info=None` 且没跑过 `_update_realtime_quotes`，例如
              直接单测 `_calculate_summary`），若把**所有**标的判成非当日，共同篮子会变空
              ⇒ `daily_return` 直接翻到「全持仓 `total_value/prev_value`」口径。那是比
              「不收窄」**更大**的一次静默口径变更，方向还相反。
              缺 as-of 证据时保守地沿用原有（不收窄）语义 + WARNING 留痕，才是「显式标记」
              而不是「显式拒绝一切」。
            """
            if merge_info is not None:
                return set(merge_info.get("fresh_codes") or []), "merge"
            quoted = getattr(self, "_quoted_codes", None)
            if quoted is not None:
                return {p["code"] for p in positions if p["code"] in quoted}, "realtime_quote"
            today_s = str(self.today)[:10]
            fresh, no_asof = set(), []
            for p in positions:
                d = str(p.get("date") or "")[:10]
                if not d:
                    no_asof.append(p["code"])       # 无 as-of 证据 ⇒ 保守视为当日
                elif d == today_s:
                    fresh.add(p["code"])
            if no_asof:
                logger.warning(
                    "[口径B] %d 只标的没有可用的 as-of 证据（既无合并信息、也无实时行情集合）"
                    "⇒ 保守视为「当日」，不做收窄：%s",
                    len(no_asof), ", ".join(sorted(no_asof)[:20])
                )
            fresh |= set(no_asof)
            return fresh, "row_date"

        try:
            conn = get_db_connection(self.db.db_path)
            cur = conn.cursor()
            # 获取前一交易日
            cur.execute(
                "SELECT date FROM portfolio_summary WHERE date < ? ORDER BY date DESC LIMIT 1",
                (self.today,)
            )
            prev_row = cur.fetchone()
            if prev_row and prev_row[0]:
                prev_dt = prev_row[0]
                cur.execute(
                    "SELECT total_value FROM portfolio_summary WHERE date = ?",
                    (prev_dt,)
                )
                prev_summary_row = cur.fetchone()
                prev_value = prev_summary_row[0] if prev_summary_row and prev_summary_row[0] else 0
                # 获取前日各标的持仓
                cur.execute(
                    "SELECT code, quantity, market_value FROM portfolio_snapshots WHERE date = ?",
                    (prev_dt,)
                )
                prev_snapshots = {r[0]: (r[1], r[2]) for r in cur.fetchall()}
                # 当日持仓代码集合
                curr_codes = {p['code']: p for p in positions}
                # 口径 B（2026-09-17 裁定）：先取「当日可比」= 当日持仓 ∩ 前日快照，
                # 再按「当日价是否新鲜」收窄。**不可比与不新鲜都不进分子也不进分母。**
                # 为什么要收窄：场外基金 T+1 披露，15:30 时当日净值根本不存在，
                # 它的「当日价」实际取自更早一天；把它算进来等于把一段多日收益
                # 压成「一个交易日」的收益（09-16 那根 +15~18% 就是这么来的）。
                comparable_codes = set(curr_codes.keys()) & set(prev_snapshots.keys())
                fresh_codes, freshness_source = _resolve_fresh_codes()
                excluded_stale = sorted(comparable_codes - fresh_codes)
                common_codes = comparable_codes & fresh_codes
                prev_value_comparable = sum(prev_snapshots[c][1] or 0 for c in comparable_codes)
                prev_value_included = sum(prev_snapshots[c][1] or 0 for c in common_codes)
                coverage.update({
                    "freshness_source": freshness_source,
                    "included_n": len(common_codes),
                    "total_n": len(curr_codes),
                    "comparable_n": len(comparable_codes),
                    "prev_value_included": round(prev_value_included, 2),
                    "prev_value_comparable": round(prev_value_comparable, 2),
                    "value_share": (round(prev_value_included / prev_value_comparable, 4)
                                    if prev_value_comparable else None),
                    "excluded_stale": excluded_stale,
                })
                if excluded_stale:
                    logger.warning(
                        "[口径B] daily_return 排除 %d 只价格非当日的标的"
                        "（纳入 %d/%d 只，占前日可比市值 %s）：%s",
                        len(excluded_stale), len(common_codes), len(comparable_codes),
                        ("%.2f%%" % (coverage["value_share"] * 100)
                         if coverage["value_share"] is not None else "未知"),
                        ", ".join(excluded_stale)
                    )
                # 用共同持仓的（前日数量×当日价格）vs（前日市值）计算纯价格收益
                price_adj_mv = 0
                prev_common_mv = 0
                for code in common_codes:
                    prev_qty, prev_mv = prev_snapshots[code]
                    curr_pos = curr_codes[code]
                    # FIX: 优先使用 realtime_price（_update_realtime_quotes 更新后的最新价格）
                    curr_price = curr_pos.get('realtime_price', curr_pos.get('current_price', 0))
                    # 份额折算闸门：quantity 在分子分母约掉后，每个标的最多只能贡献
                    # curr_price/prev_price 的收益。价比偏离 1 超阈值即为份额折算，
                    # 必须换成复权价比或剔除，否则折算日的假收益（±250%）会被 NAV 永久累乘吸收。
                    prev_price = (prev_mv / prev_qty) if prev_qty else 0
                    if curr_price > 0 and prev_price > 0:
                        raw_ratio = curr_price / prev_price
                        if abs(raw_ratio - 1) > CONVERSION_SUSPECT_RATIO:
                            if is_otc_fund(code):
                                # 场外基金（按金额申赎）绝不发生份额折算。大价比一律视为
                                # 真实净值变动（净值口径切换等），不触发复权替换/剔除，保留原口径。
                                logger.info(
                                    "[折算闸门] %s 为场外基金，原始价比 %.3f 不按折算处理，保留原口径",
                                    code, raw_ratio
                                )
                            else:
                                guard_fired = True
                                qfq_ratio = self._conversion_qfq_ratio(cur, code, prev_dt)
                                if qfq_ratio is None:
                                    # 复权价缺失（pre-2018 折算 / 表缺失 / 行情陈旧）：
                                    # 改用快照侧 quantity 比值判别，避免「要么假收益、要么丢标的」。
                                    curr_qty = curr_pos.get('quantity') or 0
                                    qty_ratio = (curr_qty / prev_qty) if prev_qty else 0
                                    if abs(qty_ratio - 1) < CONVERSION_QTY_FLAT_TOL:
                                        # quantity 未调整 ⇒ 大价比是未配平的折算假收益 → 剔除
                                        logger.warning(
                                            "[折算闸门] %s 原始价比 %.3f 超阈值且复权价缺失："
                                            "快照 quantity 未调整(qty_ratio=%.3f)，判定为折算假收益，"
                                            "已从日收益剔除（%s→%s）",
                                            code, raw_ratio, qty_ratio, prev_dt, self.today
                                        )
                                        continue
                                    # quantity 已调增吸收折价（B族）⇒ 用当日 quantity 计入，
                                    # curr_price*curr_qty≈prev_mv，即得真实 ~0 收益，无需复权价。
                                    logger.warning(
                                        "[折算闸门] %s 原始价比 %.3f 超阈值且复权价缺失："
                                        "快照 quantity 已调整(qty_ratio=%.3f)，按当日 quantity 计入"
                                        "（%s→%s）",
                                        code, raw_ratio, qty_ratio, prev_dt, self.today
                                    )
                                    price_adj_mv += curr_price * curr_qty
                                    prev_common_mv += prev_mv
                                    continue
                                logger.warning(
                                    "[折算闸门] %s 原始价比 %.3f 超阈值，改用复权价比 %.3f（%s→%s）",
                                    code, raw_ratio, qfq_ratio, prev_dt, self.today
                                )
                                price_adj_mv += prev_price * qfq_ratio * prev_qty
                                prev_common_mv += prev_mv
                                continue
                    price_adj_mv += curr_price * prev_qty
                    prev_common_mv += prev_mv
                if prev_common_mv > 0:
                    daily_pnl = price_adj_mv - prev_common_mv
                    daily_return = daily_pnl / prev_common_mv * 100
                    daily_return_computed = True
                    coverage["computed"] = True
            conn.close()
        except (sqlite3.OperationalError, KeyError):
            pass

        # fallback: 用 total_value 简单对比（仅当无前日快照数据时）
        # 闸门介入时抑制：daily_return 可能恰好为 0，但 total_value 仍含被折算污染的
        # 当日价格，走这条分支会把刚拦下的假收益原样算回来。
        # 口径 B（2026-09-17）：判定哨兵由 `daily_return == 0` 改为独立布尔
        # `daily_return_computed` —— 原写法把**真实平价日**（合法值 0.0）误判成
        # 「未计算」，静默把分子分母换回全持仓口径，使 daily_return 序列变成
        # 「多数日『当日新鲜』口径 + 个别日『全持仓』口径」的混合口径。
        if not daily_return_computed and prev_value > 0 and not guard_fired:
            daily_pnl = total_value - prev_value
            daily_return = daily_pnl / prev_value * 100
            # 这是**口径切换**，必须显式留痕（日志 + alerts），不许静默。
            logger.warning(
                "[口径B] 无可用「当日新鲜」共同篮子（freshness_source=%s, 纳入=%s）："
                "daily_return 回退为全持仓口径 total_value/prev_value（%.2f），"
                "该值口径与其它交易日不同，报告须显式标注",
                coverage.get("freshness_source"), coverage.get("included_n"), daily_return
            )
            coverage["caliber"] = "total_value_fallback"
            record_error_alert(
                self.db.db_path,
                "daily_return_caliber_fallback",
                "daily_return 回退为全持仓口径（total_value/prev_value）："
                f"date={self.today}, freshness_source={coverage.get('freshness_source')}, "
                f"included={coverage.get('included_n')}, comparable={coverage.get('comparable_n')}, "
                f"prev_value={prev_value}, guard_fired={guard_fired}",
            )
        else:
            coverage.setdefault("caliber", "fresh_price_basket")

        # ===== P1#12（2026-10-27）：日收益「是否已结算」显式标记 =====
        # 症状（台账 2026-10-16）：真实交易日 18:02 报告时点 daily_return 仍 =0.0，
        # 而当日快照已写、vs_hs300 已算 —— 这个 0.0 与「今天真的不涨不跌」在库内
        # **完全同形**，下游没有任何判据能区分「已结算的平价」与「价格没更新」。
        #
        # 为什么写标记列而不是把 daily_return 写成 NULL：
        #   `daily_return` 是 nav_engine 的主口径输入（`cumprod(1+r)`）。写 NULL 后
        #   pandas 读成 NaN 会被 cumprod 传播成整条 NAV 失真，代价远大于收益。
        #   单独一列 `daily_return_settled` 既做到「显式标记」，又不动既有数值语义。
        #
        # 判据（全部基于当轮实测值，不含写死常量）：
        #   ① `daily_return_computed == False` —— 没有任何标的的**当日新价**进入分子；
        #   ② 且未从「全持仓回退口径」拿到非 0 变化 —— 回退口径能给出非 0 值说明
        #      total_value 确实动了（如场外 NAV 补更新），值有信息量，只是口径不同
        #      （caliber='total_value_fallback' 已单独标注）；
        #   ③ 于是剩下的 0.0 只可能来自「旧价对旧价」——判为未结算。
        # 注意：**有**新鲜价时哪怕算出 0.0 也必须保留为「已结算」。真实平价日是合法值，
        # 把它一并判成未结算会让平价日批量消失，那比现在的问题更糟。
        _inc = coverage.get("included_n")
        daily_return_settled = True
        daily_return_unsettled_reason = None
        if not daily_return_computed:
            if coverage.get("caliber") == "total_value_fallback" and daily_return:
                pass  # 回退口径有非 0 变化：值可用，口径差异由 caliber 标记
            else:
                daily_return_settled = False
                daily_return_unsettled_reason = (
                    f"无共同篮子当日新价（included_n={_inc}, "
                    f"prev_value={prev_value}, guard_fired={guard_fired}, "
                    f"caliber={coverage.get('caliber')}）"
                )
                logger.warning(
                    "[P1#12] daily_return 未结算 ⇒ 标记 daily_return_settled=0：%s"
                    "（该行 daily_return=%.2f 不得读作「真实平价」）",
                    daily_return_unsettled_reason, daily_return
                )
                record_error_alert(
                    self.db.db_path,
                    "daily_return_unsettled",
                    "daily_return 未结算（无当日新价）："
                    f"date={self.today}, {daily_return_unsettled_reason}",
                )
        coverage["settled"] = daily_return_settled
        if daily_return_unsettled_reason:
            coverage["unsettled_reason"] = daily_return_unsettled_reason

        # 对比沪深300
        hs300_quote = index_quotes.get('sh000300', {})
        # 防 None：`dict.get('change_pct', 0)` 的默认值只在**键缺失**时生效，
        # 键存在但值为 None（缓存兜底行情）时会让下面这行抛 TypeError。
        hs300_change = hs300_quote.get('change_pct')
        if hs300_change is None:
            hs300_change = 0
        vs_hs300 = daily_return - hs300_change

        # 盈亏统计
        profit_count = len([p for p in positions
                            if self._pick_number(p, 'realtime_pnl', 'pnl') > 0])
        loss_count = len([p for p in positions
                          if self._pick_number(p, 'realtime_pnl', 'pnl') < 0])

        # 最大贡献/拖累
        sorted_by_pnl = sorted(positions,
                              key=lambda x: self._pick_number(x, 'realtime_pnl', 'pnl'),
                              reverse=True)

        # 风险指标摘要
        risk_summary = {}
        portfolio_metrics = risk_results.get('portfolio_metrics', {})

        if 'risk_adjusted_metrics' in portfolio_metrics:
            ram = portfolio_metrics['risk_adjusted_metrics']
            risk_summary['sharpe_ratio'] = ram.get('sharpe_ratio')
            risk_summary['sharpe_grade'] = ram.get('sharpe_grade')

        if 'drawdown_metrics' in portfolio_metrics:
            dm = portfolio_metrics['drawdown_metrics']
            risk_summary['max_drawdown'] = dm.get('max_drawdown')
            # P1-3: 回撤分窗口（headline 已用 ALL 档）
            risk_summary['max_drawdown_60d'] = dm.get('max_drawdown_60d')
            risk_summary['max_drawdown_1y'] = dm.get('max_drawdown_1y')
            risk_summary['max_drawdown_all'] = dm.get('max_drawdown_all')
            risk_summary['current_drawdown'] = dm.get('current_drawdown')

        if 'volatility_metrics' in portfolio_metrics:
            vm = portfolio_metrics['volatility_metrics']
            risk_summary['annual_volatility'] = vm.get('annual_volatility')

        concentration = risk_results.get('concentration_risk', {})
        risk_summary['max_weight'] = concentration.get('max_weight')
        risk_summary['hhi'] = concentration.get('hhi')

        # P0-5: 真实持有期收益（含分红）——优先 portfolio_nav 累计TWR（已含分红再投资），
        # 回退到原 open-position 口径（仅当 NAV 账本尚未建立）
        nav_return_pct = None
        try:
            _nc = get_db_connection(self.db.db_path)
            _cur = _nc.cursor()
            _cur.execute("SELECT twr_cumulative FROM portfolio_nav ORDER BY date DESC LIMIT 1")
            _row = _cur.fetchone()
            _nc.close()
            if _row and _row[0] is not None:
                nav_return_pct = float(_row[0]) * 100
        except Exception:
            nav_return_pct = None

        return {
            'date': self.today,
            'total_value': round(total_value, 2),
            'total_cost': round(total_cost, 2),
            'total_pnl': round(total_pnl, 2),
            'total_return_pct': round(nav_return_pct, 2) if nav_return_pct is not None
                                 else (round(total_pnl / total_cost * 100, 2) if total_cost and total_cost > 0 else 0),
            'daily_pnl': round(daily_pnl, 2),
            'daily_return': round(daily_return, 2),
            # P1#12（2026-10-27）：0/1 显式标记「该行 daily_return 是否由当日新价结算」。
            # 0 ⇒ 值仍是 0.0，但**不得**读作真实平价（价格未更新 / 无前日基准）。
            'daily_return_settled': 1 if daily_return_settled else 0,
            'daily_return_unsettled_reason': daily_return_unsettled_reason,
            # 口径 B（2026-09-17）：daily_return 的**覆盖度**必须随值一起走，
            # 否则「22 只口径的收益率」会被读者拿去乘「34 只的总市值」。
            # 报告侧读不到本键时须显式印「覆盖度未记录」，不许静默省略。
            'daily_return_coverage': coverage,
            'vs_hs300': round(vs_hs300, 2),
            'profit_count': profit_count,
            'loss_count': loss_count,
            'position_count': len(positions),
            'top_contributor': sorted_by_pnl[0]['name'] if sorted_by_pnl else '',
            'top_drag': sorted_by_pnl[-1]['name'] if sorted_by_pnl else '',
            'hs300_change': hs300_change,
            'risk_summary': risk_summary,
            'risk_warnings': risk_results.get('risk_warnings', [])
        }

