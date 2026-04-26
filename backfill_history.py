#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
历史数据回填脚本 - 补充过去60个交易日数据使风险指标可用

使用方法:
    python backfill_history.py              # 回填60天
    python backfill_history.py --days 90    # 回填90天
    python backfill_history.py --dry-run    # 试运行，只显示不写入
"""

import sys
import os
import argparse
import logging
import time
from datetime import datetime, timedelta

# 确保项目根目录在路径中
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_ROOT)

from config.settings import DATA_SOURCES, INDEX_CODES, DATABASE_PATH
from src.data_sources import DataSourceManager
from src.utils.backfill import HistoricalDataBackfiller

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(
            os.path.join(PROJECT_ROOT, "logs", "backfill.log"),
            encoding="utf-8"
        ) if os.path.exists(os.path.join(PROJECT_ROOT, "logs")) else logging.StreamHandler()
    ]
)
logger = logging.getLogger("backfill_history")


def get_etf_codes_from_db(db_path):
    """从数据库获取最新的持仓ETF代码列表"""
    import sqlite3
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT code FROM portfolio_snapshots
        ORDER BY code
    """)
    codes = [row[0] for row in cursor.fetchall()]
    conn.close()
    logger.info(f"从数据库获取到 {len(codes)} 个ETF代码")
    return codes


def get_trading_days_range(days):
    """计算需要回填的交易日范围"""
    # 大致估算：60天日历日约40个交易日，90天约60个交易日
    # 为了确保足够，多取一些
    calendar_days = int(days * 1.5)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=calendar_days)
    return start_date, end_date


def verify_backfill(db_path, expected_days):
    """验证回填结果"""
    import sqlite3
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    print("\n" + "=" * 60)
    print("回填验证报告")
    print("=" * 60)

    # portfolio_snapshots统计
    cursor.execute("SELECT COUNT(DISTINCT date) FROM portfolio_snapshots")
    snapshot_dates = cursor.fetchone()[0]
    cursor.execute("SELECT MIN(date), MAX(date) FROM portfolio_snapshots")
    min_date, max_date = cursor.fetchone()
    print(f"\nportfolio_snapshots:")
    print(f"  交易日数: {snapshot_dates}")
    print(f"  日期范围: {min_date} ~ {max_date}")

    # portfolio_summary统计
    cursor.execute("SELECT COUNT(*) FROM portfolio_summary")
    summary_count = cursor.fetchone()[0]
    cursor.execute("SELECT date, daily_return, total_value FROM portfolio_summary ORDER BY date")
    summaries = cursor.fetchall()
    print(f"\nportfolio_summary:")
    print(f"  记录数: {summary_count}")
    print(f"  日期列表:")
    for s in summaries:
        print(f"    {s[0]}: 总市值={s[2]:,.2f}, 日收益={s[1]:+.2f}%")

    # index_quotes统计
    cursor.execute("SELECT COUNT(DISTINCT date) FROM index_quotes")
    index_dates = cursor.fetchone()[0]
    cursor.execute("SELECT MIN(date), MAX(date) FROM index_quotes")
    min_idx, max_idx = cursor.fetchone()
    print(f"\nindex_quotes:")
    print(f"  交易日数: {index_dates}")
    print(f"  日期范围: {min_idx} ~ {max_idx}")

    # etf_technical统计
    cursor.execute("SELECT COUNT(DISTINCT date) FROM etf_technical")
    tech_dates = cursor.fetchone()[0]
    print(f"\netf_technical:")
    print(f"  交易日数: {tech_dates}")

    # 风险指标可用性检查
    has_daily_returns = summary_count >= 2
    can_calc_sharpe = summary_count >= 20
    can_calc_drawdown = summary_count >= 5

    print(f"\n风险指标可用性:")
    print(f"  日收益率计算: {'可用' if has_daily_returns else '不足 (需>=2天)'}")
    print(f"  夏普比率:     {'可用' if can_calc_sharpe else '不足 (需>=20天, 当前' + str(summary_count) + '天)'}")
    print(f"  最大回撤:     {'可用' if can_calc_drawdown else '不足 (需>=5天, 当前' + str(summary_count) + '天)'}")

    conn.close()

    return summary_count >= 20


def run_backfill(days=60, dry_run=False):
    """执行回填"""
    start_time = time.time()

    print("=" * 60)
    print(f"历史数据回填 - 目标: {days}个交易日")
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    if dry_run:
        print("*** 试运行模式 - 不会写入数据库 ***")
    print("=" * 60)

    # 初始化数据源
    logger.info("初始化数据源...")
    ds = DataSourceManager(DATA_SOURCES)
    status = ds.get_source_status()
    for name, ok in status.items():
        logger.info(f"  {name}: {'可用' if ok else '不可用'}")

    # 获取持仓代码
    etf_codes = get_etf_codes_from_db(DATABASE_PATH)
    if not etf_codes:
        logger.error("数据库中无持仓数据，请先运行 run_analysis.py")
        return False

    if dry_run:
        print(f"\n[试运行] 将回填 {len(etf_codes)} 个ETF的 {days} 天历史数据")
        print(f"[试运行] ETF列表: {', '.join(etf_codes[:5])}... 等{len(etf_codes)}只")
        print(f"[试运行] 指数列表: {len(INDEX_CODES)} 个")
        return True

    # 创建回填器
    backfiller = HistoricalDataBackfiller(str(DATABASE_PATH), ds)

    # 阶段一：回填ETF K线数据 -> portfolio_snapshots
    print(f"\n--- 阶段一: ETF K线数据回填 ---")
    success, fail = backfiller.backfill_etf_klines(etf_codes, days)
    print(f"ETF回填完成: 成功 {success}, 失败 {fail}")

    # 阶段二：基于快照数据生成组合汇总 -> portfolio_summary
    print(f"\n--- 阶段二: 组合汇总数据生成 ---")
    generated = backfiller.backfill_portfolio_history(days)
    print(f"组合汇总生成: {generated} 天")

    # 验证结果
    ok = verify_backfill(DATABASE_PATH, days)

    elapsed = time.time() - start_time
    print(f"\n总耗时: {elapsed:.1f}秒")

    if ok:
        print("\n回填成功! 已积累足够历史数据，风险指标可正常计算。")
        print("下次运行 run_analysis.py 时，夏普比率/最大回撤/波动率将自动计算。")
    else:
        remaining = 20 - generated
        print(f"\n回填完成，但历史数据仍不足（需再积累约 {remaining} 个交易日）。")

    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="投资组合历史数据回填")
    parser.add_argument("--days", type=int, default=60, help="回填交易日数 (默认60)")
    parser.add_argument("--dry-run", action="store_true", help="试运行，不写入数据库")
    args = parser.parse_args()

    run_backfill(days=args.days, dry_run=args.dry_run)
