
"""
历史数据回填模块 - 补充K线历史数据使风险指标可用
"""
import sqlite3
import logging
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional
from pathlib import Path

logger = logging.getLogger(__name__)


class HistoricalDataBackfiller:
    """历史数据回填器"""

    def __init__(self, db_path: str, ds_manager):
        self.db_path = db_path
        self.ds = ds_manager

    def backfill_etf_klines(self, codes: List[str], days: int = 60):
        """回填ETF历史K线数据到portfolio_snapshots"""
        logger.info(f"开始回填 {len(codes)} 个ETF的 {days} 天历史数据")

        success_count = 0
        fail_count = 0

        for code in codes:
            try:
                klines = self.ds.get_kline(code, "day", days)
                if not klines:
                    logger.warning(f"{code}: 无K线数据")
                    fail_count += 1
                    continue

                # 按日期正序排列
                klines_sorted = sorted(klines, key=lambda x: x['date'])

                # 获取已有的日期
                existing_dates = self._get_existing_dates(code)
                new_count = 0

                for kline in klines_sorted:
                    kdate = kline['date']
                    if kdate in existing_dates:
                        continue
                    if kdate >= date.today().strftime('%Y-%m-%d'):
                        continue

                    self._save_snapshot_from_kline(kdate, code, kline)
                    new_count += 1

                logger.info(f"{code}: 回填 {new_count} 天数据 (共获取 {len(klines)} 条)")
                success_count += 1

            except Exception as e:
                logger.error(f"{code} 回填失败: {e}")
                fail_count += 1

        logger.info(f"回填完成: 成功 {success_count}, 失败 {fail_count}")

        # 回填指数数据
        self._backfill_index_history(days)

        return success_count, fail_count

    def backfill_portfolio_history(self, days: int = 60):
        """基于已有快照数据生成组合汇总历史"""
        logger.info(f"开始生成 {days} 天组合汇总历史")

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # 获取所有有数据的日期
        cursor.execute("""
            SELECT DISTINCT date FROM portfolio_snapshots
            ORDER BY date DESC
        """)
        dates = [row[0] for row in cursor.fetchall()]

        if not dates:
            logger.warning("无快照数据，跳过汇总生成")
            return 0

        generated = 0
        for dt in dates:
            # 检查是否已有汇总
            cursor.execute("SELECT 1 FROM portfolio_summary WHERE date = ?", (dt,))
            if cursor.fetchone():
                continue

            # 计算当日汇总
            cursor.execute("""
                SELECT
                    SUM(market_value) as total_value,
                    SUM(cost_price * quantity) as total_cost,
                    SUM(pnl) as total_pnl,
                    SUM(CASE WHEN pnl >= 0 THEN 1 ELSE 0 END) as profit_count,
                    SUM(CASE WHEN pnl < 0 THEN 1 ELSE 0 END) as loss_count
                FROM portfolio_snapshots
                WHERE date = ?
            """, (dt,))

            row = cursor.fetchone()
            if not row or not row[0]:
                continue

            total_value = row[0]
            total_cost = row[1] or 0
            total_pnl = row[2] or 0
            profit_count = row[3] or 0
            loss_count = row[4] or 0

            # 计算日收益率（与前一交易日对比）
            daily_return = 0.0
            prev_value = 0
            cursor.execute("""
                SELECT SUM(market_value) as prev_value
                FROM portfolio_snapshots
                WHERE date = (SELECT MAX(date) FROM portfolio_snapshots WHERE date < ?)
            """, (dt,))
            prev_row = cursor.fetchone()
            if prev_row and prev_row[0]:
                prev_value = prev_row[0]
                if prev_value > 0:
                    daily_return = (total_value - prev_value) / prev_value * 100

            daily_pnl = total_value - prev_value if prev_value > 0 else 0

            cursor.execute("""
                INSERT OR REPLACE INTO portfolio_summary
                (date, total_value, total_cost, total_pnl, daily_pnl, daily_return,
                 profit_count, loss_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (dt, total_value, total_cost, total_pnl, daily_pnl, daily_return,
                  profit_count, loss_count))

            generated += 1

        conn.commit()
        conn.close()

        logger.info(f"生成 {generated} 天组合汇总")
        return generated

    def _save_snapshot_from_kline(self, date_str: str, code: str, kline: dict):
        """从K线数据保存快照"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        close_price = kline.get('close', 0)
        # 使用当前持仓的数量（简化处理）
        cursor.execute("""
            SELECT name, quantity, cost_price FROM portfolio_snapshots
            WHERE code = ? ORDER BY date DESC LIMIT 1
        """, (code,))
        row = cursor.fetchone()

        if not row:
            conn.close()
            return

        name, quantity, cost_price = row
        market_value = close_price * quantity if quantity else 0
        pnl = market_value - (cost_price * quantity) if (cost_price and quantity) else 0
        pnl_rate = (pnl / (cost_price * quantity) * 100) if (cost_price and quantity and cost_price > 0) else 0

        cursor.execute("""
            INSERT OR REPLACE INTO portfolio_snapshots
            (date, code, name, quantity, cost_price, current_price,
             market_value, pnl, pnl_rate)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (date_str, code, name, quantity, cost_price, close_price,
              market_value, pnl, pnl_rate))

        conn.commit()
        conn.close()

    def _get_existing_dates(self, code: str) -> set:
        """获取已有日期"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT date FROM portfolio_snapshots WHERE code = ?", (code,))
        dates = {row[0] for row in cursor.fetchall()}
        conn.close()
        return dates

    def _backfill_index_history(self, days: int = 60):
        """回填指数历史数据"""
        from config.settings import INDEX_CODES

        logger.info(f"回填指数历史数据...")

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        backfilled = 0
        for code, name in INDEX_CODES.items():
            try:
                # 新浪指数K线
                klines = self.ds.get_kline(code, "day", days)
                if not klines:
                    continue

                existing = set()
                cursor.execute("SELECT DISTINCT date FROM index_quotes WHERE code = ?", (code,))
                existing = {row[0] for row in cursor.fetchall()}

                for kline in sorted(klines, key=lambda x: x['date']):
                    kdate = kline['date']
                    if kdate in existing:
                        continue

                    cursor.execute("""
                        INSERT OR REPLACE INTO index_quotes
                        (date, code, name, close, volume)
                        VALUES (?, ?, ?, ?, ?)
                    """, (kdate, code, name, kline.get('close', 0), kline.get('volume', 0)))
                    backfilled += 1

            except Exception as e:
                logger.debug(f"指数 {code} 回填失败: {e}")

        conn.commit()
        conn.close()
        logger.info(f"回填指数数据: {backfilled} 条")
