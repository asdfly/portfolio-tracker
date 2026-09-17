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
    RISK_CONFIG,
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
                logger.info("步骤2.5: 保存新持仓快照到数据库...")
                self.db.save_portfolio_snapshot(self.today, positions)

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
                logger.info("步骤2.5: 更新持仓价格数据到数据库...")
                self.db.save_portfolio_snapshot(self.today, positions)

            # ========== 以下步骤每天都会执行 ==========

            # 获取指数行情
            logger.info("步骤3: 获取指数行情...")
            index_quotes = self._fetch_index_quotes()
            results['indices'] = index_quotes

            # 保存指数行情
            self.db.save_index_quotes(self.today, index_quotes)

            # 计算技术指标
            logger.info("步骤4: 计算技术指标...")
            tech_results = self._calculate_technical_indicators(positions)
            results['technical'] = tech_results

            # 保存技术指标
            for code, indicators in tech_results.items():
                self.db.save_technical_indicators(self.today, code, indicators)

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
            summary_with_risk = {
                **summary,
                'sharpe_ratio': risk_summary.get('sharpe_ratio'),
                'max_drawdown': risk_summary.get('max_drawdown'),
                'volatility': risk_summary.get('annual_volatility')
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
        """获取指数行情"""
        quotes = {}
        for code in INDEX_CODES.keys():
            try:
                quote = self.ds_manager.get_quote(code)
                quotes[code] = quote
            except OSError as e:
                logger.warning(f"获取指数 {code} 失败: {e}")
        return quotes

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
                            guard_fired = True
                            qfq_ratio = self._conversion_qfq_ratio(cur, code, prev_dt)
                            if qfq_ratio is None:
                                # 查不到可用复权价（典型：场外基金 / 行情陈旧）：
                                # 两边同时剔除，绝不让假价比进入求和
                                logger.warning(
                                    "[折算闸门] %s 原始价比 %.3f 超阈值且无可用复权价"
                                    "（缺失或陈旧），已从日收益计算中剔除（%s→%s）",
                                    code, raw_ratio, prev_dt, self.today
                                )
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

        # 对比沪深300
        hs300_quote = index_quotes.get('sh000300', {})
        hs300_change = hs300_quote.get('change_pct', 0)
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

