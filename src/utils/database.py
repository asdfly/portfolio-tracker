"""
数据库模块 - SQLite数据持久化
"""
import sqlite3
from typing import Dict, List, Any, Optional
from pathlib import Path
import logging

from config.settings import DATABASE_PATH

logger = logging.getLogger(__name__)

class DatabaseManager:
    """数据库管理器"""

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or str(DATABASE_PATH)
        self._init_db()

    def _init_db(self):
        """初始化数据库表结构（统一从 db_schema 执行全部 DDL）"""
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)

        from src.utils.db_schema import init_all_tables
        with get_db_connection(self.db_path) as conn:
            init_all_tables(conn)
            logger.info("数据库初始化完成")

    def save_portfolio_snapshot(self, date_str: str, holdings: List[Dict[str, Any]]):
        """保存持仓快照 + 同事务写「写入完成标记」（P1#13 遗留补齐，2026-10-27）

        ## 为什么快照也需要完成标记

        P1#13 第一轮只给 `index_quotes` 加了 `ingest_markers`，快照侧仍靠
        `run_sentinel._snapshot_probe()` **数行数**间接判断（P0#2）。数行数的固有盲区：
        「写入 12 行后进程被杀」与「本来就只该有 12 行」在库里完全同形 —— 2026-10-26
        那次 35→12 的塌缩正是靠比对近 5 日中位数才识别出来的，属于事后推断，
        而不是写入方留下的直接证据。

        加标记后，写入方**主动声明**本次意图写入多少行（`rows_expected`）、实际落库
        多少行（`rows_written`）。于是三种形态可直接区分：

        | 库内形态 | 含义 |
        |---|---|
        | 有行 + `status=complete` | 正常写完 |
        | 有行 + 无标记 | **orphan**：写入方中途被杀（有行无标） |
        | 无行 + `status=empty` | 写入方跑过但产出 0 行（如持仓文件读空） |

        ## 为什么同事务

        标记与业务行必须要么一起可见、要么都不见。若先 commit 行再单独写标记，
        两次 commit 之间被杀就又回到「有行无标」——而这恰恰是我们要区分出来的异常态，
        真异常与竞态会混在一起，标记就失去判别力。

        ## 为什么不像 `index_quotes` 那样改 `executemany`

        该函数被 `run_daily_analysis` 调用**两次**（步骤 2.5 首次落快照、拿到实时价后
        再更新一次价格）。逐行 `INSERT OR REPLACE` 与 `executemany` 对 (date, code)
        主键语义等价，但改成 executemany 会顺带改动「两次调用之间的可见性」这一
        无关变量，而快照层是全项目风险最高的地方（`total_value` 静默失真的源头）。
        本函数只新增标记写入，**不动既有写入节奏**。
        """
        with get_db_connection(self.db_path) as conn:
            cursor = conn.cursor()

            written = 0
            missing_code = []
            for holding in holdings:
                # 优先使用实时行情数据（来自新浪API），确保与summary一致
                current_price = holding.get('realtime_price', holding.get('current_price'))
                market_value = holding.get('realtime_market_value', holding.get('market_value'))
                pnl = holding.get('realtime_pnl', holding.get('pnl'))

                if not holding.get('code'):
                    missing_code.append(str(holding.get('name') or '<无名>'))
                    continue

                cursor.execute("""
                    INSERT OR REPLACE INTO portfolio_snapshots 
                    (date, code, name, quantity, cost_price, current_price, 
                     market_value, pnl, pnl_rate, ytd_return, beta)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    date_str,
                    holding.get('code'),
                    holding.get('name'),
                    holding.get('quantity'),
                    holding.get('cost_price'),
                    current_price,
                    market_value,
                    pnl,
                    holding.get('pnl_rate'),
                    holding.get('ytd_return'),
                    holding.get('beta')
                ))
                written += 1

            # 完成标记：与业务行同一事务提交
            if not written:
                status = 'empty'
            elif missing_code:
                status = 'partial'
            else:
                status = 'complete'
            detail = ''
            if missing_code:
                detail = '缺 code 跳过: ' + ','.join(missing_code[:10])
            cursor.execute("""
                INSERT OR REPLACE INTO ingest_markers
                (table_name, date, rows_written, rows_expected, skipped, status, detail)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                'portfolio_snapshots', date_str, written, len(holdings),
                len(missing_code), status, detail
            ))

            conn.commit()
            logger.info("[portfolio_snapshots] %s 写入 %d/%d 行（跳过缺code %d，status=%s）",
                        date_str, written, len(holdings), len(missing_code), status)
            logger.info(f"保存持仓快照: {date_str}, {len(holdings)}条记录")

    def save_portfolio_summary(self, date_str: str, summary: Dict[str, Any]):
        """保存组合汇总"""
        with get_db_connection(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO portfolio_summary 
                (date, total_value, total_cost, total_pnl, daily_pnl, daily_return,
                 vs_hs300, profit_count, loss_count, sharpe_ratio, max_drawdown,
                 max_drawdown_60d, max_drawdown_1y, max_drawdown_all, volatility,
                 daily_return_settled)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                date_str,
                summary.get('total_value'),
                summary.get('total_cost'),
                summary.get('total_pnl'),
                summary.get('daily_pnl'),
                summary.get('daily_return'),
                summary.get('vs_hs300'),
                summary.get('profit_count'),
                summary.get('loss_count'),
                summary.get('sharpe_ratio'),
                summary.get('max_drawdown'),
                summary.get('max_drawdown_60d'),
                summary.get('max_drawdown_1y'),
                summary.get('max_drawdown_all'),
                summary.get('volatility'),
                # P1#12（2026-10-27）：0/1 标记 daily_return 是否由当日新价结算。
                # 缺失（None）与 1 等价处理：历史行没有该列值，不能被当成「未结算」，
                # 否则所有历史行会被下游批量标成不可信。只有显式写 0 才是「未结算」。
                summary.get('daily_return_settled'),
            ))
            conn.commit()

    def save_index_quotes(self, date_str: str, quotes: Dict[str, Dict[str, Any]]):
        """保存指数行情（整批写入 + 同事务写「写入完成标记」，P1#13）

        P1#13（2026-10-27）改造前的三个缺陷：
        1. 逐条 `cursor.execute()` —— 语法上确实在一个事务里（commit 在循环外），
           但**没有任何持久痕迹说明这次是「全部写完」还是「写到一半被杀」**。
           2026-10-22 实测当日 index_quotes 只有 1 行（sh932000，补采脚本单独写的），
           而当天主分析是被 SIGINT 硬杀的；两者在库里无从区分。
        2. `_cached` 被跳过是**正确**的（宁缺不假），但跳过后当日行数天然少于
           INDEX_CODES 全集，下游 `SELECT MAX(date)` 拿到该日时无法判断「12 行是正常
           缩减」还是「只采到 12 个」。
        3. 空 quotes 直接静默 commit，等于「什么都没写但也没报错」。

        改造后：先在内存里筛出全部待写行 → `executemany` 一次落库 → **同一事务内**
        写一行 `ingest_markers`（rows_written / rows_expected / skipped / status）。
        有行必有标，下游可用 `status='complete'` 判定该日 index_quotes 可信。
        """
        with get_db_connection(self.db_path) as conn:
            cursor = conn.cursor()

            from config.settings import INDEX_CODES
            rows = []
            skipped_codes = []
            for code, quote in quotes.items():
                # 🔴 占位行 bug 根治（2026-10-08）：缓存兜底（_cached）行情是陈旧值，
                # 以 date_str 落库会污染 MAX(date) 并伪装成「今日收盘」。写路径直接拒绝，
                # 确保任何调用方都不会把占位行写入 index_quotes。
                if quote.get("_cached"):
                    logger.debug(f"跳过缓存兜底指数 {code} 的占位写入（非今日实时行情）")
                    skipped_codes.append(code)
                    continue
                std_name = INDEX_CODES.get(code, quote.get('name', ''))
                rows.append((
                    date_str,
                    code,
                    std_name,
                    quote.get('price'),
                    quote.get('change_pct'),
                    quote.get('volume'),
                    quote.get('amount')
                ))

            # 整批写入：单条 executemany，异常即整批回滚，不留半截日
            if rows:
                cursor.executemany("""
                    INSERT OR REPLACE INTO index_quotes
                    (date, code, name, close, change_pct, volume, amount)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, rows)

            # 完成标记与业务表同事务提交 —— 二者要么一起可见，要么都不见
            status = 'complete' if not skipped_codes else 'partial'
            if not rows:
                status = 'empty'
            detail = ''
            if skipped_codes:
                detail = '缓存兜底跳过: ' + ','.join(sorted(skipped_codes))
            cursor.execute("""
                INSERT OR REPLACE INTO ingest_markers
                (table_name, date, rows_written, rows_expected, skipped, status, detail)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                'index_quotes', date_str, len(rows), len(quotes),
                len(skipped_codes), status, detail
            ))

            conn.commit()
            logger.info("[index_quotes] %s 写入 %d/%d 行（跳过缓存 %d，status=%s）",
                        date_str, len(rows), len(quotes), len(skipped_codes), status)


    def save_technical_indicators(self, date_str: str, code: str, 
                                   indicators: Dict[str, Any]):
        """保存技术指标"""
        with get_db_connection(self.db_path) as conn:
            cursor = conn.cursor()

            ma = indicators.get('ma', {})
            macd = indicators.get('macd', {})
            rsi = indicators.get('rsi', {})
            kdj = indicators.get('kdj', {})
            bollinger = indicators.get('bollinger', {})
            atr = indicators.get('atr', {})
            trend = indicators.get('trend', {})

            cursor.execute("""
                INSERT OR REPLACE INTO etf_technical 
                (date, code, ma_signal, macd_signal, rsi_value, rsi_status,
                 kdj_signal, bollinger_position, atr_pct, trend)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                date_str, code,
                ma.get('signal'),
                macd.get('signal'),
                rsi.get('RSI'),
                rsi.get('status'),
                kdj.get('signal'),
                bollinger.get('position'),
                atr.get('ATR_pct'),
                trend.get('trend')
            ))
            conn.commit()

    def rebuild_latest_technical(self, codes):
        """统一口径刷新「当日」技术指标（问题八落地）。

        对每个 code 取 portfolio_snapshots.current_price 全历史，用统一算法
        compute_technical_unified 计算最新一根（K 线末日）并写入 etf_technical。

        关键修正：行日期 = 该行所用价格窗口末日的日期（K 线末日），**绝不**用 self.today
        —— 避免「行写 D、指标算到 D-1」的间歇性错位（行情源未及时更新时）。
        算法与 backfill_full_history.rebuild_etf_technical 完全一致。
        """
        from src.analysis.technical import compute_technical_unified
        with get_db_connection(self.db_path) as conn:
            cur = conn.cursor()
            written = 0
            for code in codes:
                cur.execute(
                    "SELECT date, current_price FROM portfolio_snapshots "
                    "WHERE code = ? AND current_price > 0 ORDER BY date",
                    (code,),
                )
                rows = cur.fetchall()
                if len(rows) < 20:
                    continue
                dates = [r[0] for r in rows]
                prices = [float(r[1]) for r in rows]
                i = len(prices) - 1
                ind = compute_technical_unified(prices, dates, i)
                row_date = str(dates[i])  # K 线末日，非 self.today
                assert row_date == str(dates[i]), "行日期必须等于价格窗口末日"
                cur.execute(
                    "INSERT OR REPLACE INTO etf_technical "
                    "(date, code, ma_signal, macd_signal, rsi_value, rsi_status, "
                    "kdj_signal, bollinger_position, atr_pct, trend) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (row_date, code,
                     ind["ma"]["signal"], ind["macd"]["signal"],
                     ind["rsi"]["RSI"], ind["rsi"]["status"],
                     ind["kdj"]["signal"], ind["bollinger"]["position"],
                     ind["atr"]["ATR_pct"], ind["trend"]["trend"]),
                )
                written += 1
            conn.commit()
            return written

    def get_latest_portfolio(self, date_str: Optional[str] = None) -> List[Dict[str, Any]]:
        """获取最新持仓数据"""
        with get_db_connection(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            if date_str:
                cursor.execute("""
                    SELECT * FROM portfolio_snapshots WHERE date = ?
                """, (date_str,))
            else:
                cursor.execute("""
                    SELECT * FROM portfolio_snapshots 
                    WHERE date = (SELECT MAX(date) FROM portfolio_snapshots)
                """)

            rows = cursor.fetchall()
            return [dict(row) for row in rows]

    def get_portfolio_history(self, days: int = 30) -> List[Dict[str, Any]]:
        """获取组合历史数据"""
        with get_db_connection(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute("""
                SELECT * FROM portfolio_summary
                ORDER BY date DESC
                LIMIT ?
            """, (days,))

            rows = cursor.fetchall()
            # 按时间正序返回（DESC查询取最近N条后需反转，确保收益率计算方向正确）
            return [dict(row) for row in reversed(rows)]

    def get_price_history(self, code: str, days: int = 60) -> List[Dict[str, Any]]:
        """获取价格历史"""
        with get_db_connection(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute("""
                SELECT date, current_price, market_value, pnl
                FROM portfolio_snapshots
                WHERE code = ?
                ORDER BY date DESC
                LIMIT ?
            """, (code, days))

            rows = cursor.fetchall()
            return [dict(row) for row in rows]


_indexes_created = False

def get_db_connection(db_path=None):
    """获取数据库连接，首次调用时创建索引。

    委托给 data_loader.get_db_connection，并在首次调用时确保索引就绪。
    为保持向后兼容，接受可选的 db_path 参数。
    """
    global _indexes_created
    from data_loader import get_db_connection as _dl_get_conn
    conn = _dl_get_conn(db_path)
    if not _indexes_created:
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_snap_date ON portfolio_snapshots(date)",
            "CREATE INDEX IF NOT EXISTS idx_snap_code_date ON portfolio_snapshots(code, date)",
            "CREATE INDEX IF NOT EXISTS idx_summary_date ON portfolio_summary(date)",
            "CREATE INDEX IF NOT EXISTS idx_idx_quote_code_date ON index_quotes(code, date)",
            "CREATE INDEX IF NOT EXISTS idx_tech_date ON etf_technical(date)",
            "CREATE INDEX IF NOT EXISTS idx_tech_code_date ON etf_technical(code, date)",
            "CREATE INDEX IF NOT EXISTS idx_fund_flows_cat_date ON fund_flows(category, date)",
            "CREATE INDEX IF NOT EXISTS idx_fund_flows_code_date ON fund_flows(code, date)",
        ]
        for sql in indexes:
            try:
                conn.execute(sql)
            except sqlite3.OperationalError:  # 索引创建失败可忽略
                pass
        conn.commit()
        _indexes_created = True
    return conn

