"""
Phase C: 自定义指标模板初始化 + 批量回测回填

用法:
    python backfill_indicators.py           # 全量执行（9指标 x 23ETF x 3周期 = 621条）
    python backfill_indicators.py --dry-run  # 仅展示回测预览
"""

import sqlite3
import sys
import time
import json
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config.settings import DATABASE_PATH
from src.analysis.indicator_backtest import (
    INDICATOR_TEMPLATES,
    backtest_technical_composite,
    save_backtest_result,
)

LOOKBACK_PERIODS = [
    (250, "250D (1Y)"),
    (500, "500D (2Y)"),
]

# 每次只回填2个ETF再commit，避免内存问题
BATCH_SIZE = 5


def init_indicator_templates(conn):
    """将INDICATOR_TEMPLATES写入custom_indicators表"""
    cur = conn.cursor()
    inserted = 0
    for tmpl in INDICATOR_TEMPLATES:
        formula_str = json.dumps(tmpl["formula"], ensure_ascii=False)
        cur.execute("""
            INSERT OR IGNORE INTO custom_indicators (name, formula, description, is_template)
            VALUES (?, ?, ?, 1)
        """, (tmpl["name"], formula_str, tmpl["description"]))
        if cur.rowcount > 0:
            inserted += 1
    conn.commit()
    print(f"[模板初始化] 新增 {inserted} 个，跳过已存在的")
    return inserted


def get_all_etf_codes(conn):
    """从portfolio_snapshots获取所有ETF代码"""
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT code, name FROM portfolio_snapshots ORDER BY code")
    return [(r[0], r[1]) for r in cur.fetchall()]


def get_indicator_id(conn, name):
    cur = conn.cursor()
    cur.execute("SELECT id FROM custom_indicators WHERE name = ?", (name,))
    row = cur.fetchone()
    return row[0] if row else None


def clear_existing_results(conn, indicator_id, period_label):
    """清除已有回测结果（幂等重跑）"""
    cur = conn.cursor()
    cur.execute("""
        DELETE FROM indicator_backtest_results
        WHERE indicator_id = ? AND test_period = ?
    """, (indicator_id, period_label))
    deleted = cur.rowcount
    conn.commit()
    return deleted


def run_batch_backtest(conn, dry_run=False):
    """对每个指标 x 每个ETF x 每个回溯周期执行回测"""
    etfs = get_all_etf_codes(conn)
    print(f"\n[回测目标] {len(etfs)} 个ETF x {len(INDICATOR_TEMPLATES)} 个指标 x {len(LOOKBACK_PERIODS)} 个周期")
    print(f"  ETFs: {[code for code, _ in etfs]}")

    total_tasks = len(INDICATOR_TEMPLATES) * len(etfs) * len(LOOKBACK_PERIODS)
    done = 0
    success = 0
    skip = 0
    error = 0

    for tmpl in INDICATOR_TEMPLATES:
        ind_id = get_indicator_id(conn, tmpl["name"])
        if not ind_id:
            print(f"  [ERROR] 指标 '{tmpl['name']}' 未找到ID，跳过")
            error += len(etfs) * len(LOOKBACK_PERIODS)
            done += len(etfs) * len(LOOKBACK_PERIODS)
            continue

        for lookback, period_label in LOOKBACK_PERIODS:
            if not dry_run:
                deleted = clear_existing_results(conn, ind_id, period_label)

            for code, name in etfs:
                done += 1
                try:
                    result = backtest_technical_composite(
                        conn, code, tmpl["formula"], lookback=lookback
                    )
                    if "error" in result and result.get("total_signals", 0) == 0:
                        skip += 1
                        if done % 50 == 0:
                            print(f"  进度: {done}/{total_tasks} | 成功={success} 跳过={skip} 错误={error}")
                        continue

                    if dry_run:
                        print(f"  [DRY] {tmpl['name']} | {code}({name}) | {period_label} | "
                              f"signals={result.get('total_signals', 0)} win_rate={result.get('win_rate', 0)}%")
                    else:
                        save_backtest_result(conn, ind_id, result, period_label)
                        success += 1

                    if done % 50 == 0:
                        print(f"  进度: {done}/{total_tasks} | 成功={success} 跳过={skip} 错误={error}")
                except Exception as e:
                    error += 1
                    if done % 50 == 0:
                        print(f"  进度: {done}/{total_tasks} | 成功={success} 跳过={skip} 错误={error}")
                    continue

            # 每完成一个周期就commit一次
            if not dry_run:
                conn.commit()

    print(f"\n[回测完成] 总计={total_tasks} 成功={success} 跳过={skip} 错误={error}")
    return success, skip, error


def main():
    dry_run = "--dry-run" in sys.argv

    print("=" * 60)
    print(f"Phase C: 自定义指标模板初始化 + 批量回测")
    print(f"  时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  模式: {'DRY-RUN (仅预览)' if dry_run else '正式执行'}")
    print("=" * 60)

    conn = sqlite3.connect(str(DATABASE_PATH))

    # Step 1: 初始化指标模板
    print("\n--- Step 1: 初始化指标模板 ---")
    init_indicator_templates(conn)

    # 确认模板数量
    cur = conn.cursor()
    cur.execute("SELECT id, name FROM custom_indicators WHERE is_template = 1 ORDER BY id")
    templates = cur.fetchall()
    print(f"  当前模板: {len(templates)} 个")
    for tid, tname in templates:
        cur.execute("SELECT COUNT(*) FROM indicator_backtest_results WHERE indicator_id = ?", (tid,))
        cnt = cur.fetchone()[0]
        print(f"    [{tid}] {tname}: {cnt} 条回测结果")

    # Step 2: 批量回测
    print("\n--- Step 2: 批量回测 ---")
    t0 = time.time()
    success, skip, error = run_batch_backtest(conn, dry_run=dry_run)
    elapsed = time.time() - t0

    # Step 3: 验证结果
    print(f"\n--- Step 3: 验证结果 ---")
    cur.execute("SELECT COUNT(*) FROM custom_indicators WHERE is_template = 1")
    ind_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM indicator_backtest_results")
    bt_count = cur.fetchone()[0]
    print(f"  指标模板: {ind_count} 个")
    print(f"  回测结果: {bt_count} 条")
    print(f"  耗时: {elapsed:.1f}s")

    # 展示Top5胜率
    print(f"\n  Top 10 胜率 (250D):")
    cur.execute("""
        SELECT ci.name, ibr.test_period, ibr.win_rate, ibr.total_signals, ibr.avg_pnl
        FROM indicator_backtest_results ibr
        JOIN custom_indicators ci ON ci.id = ibr.indicator_id
        WHERE ibr.test_period = '250D (1Y)' AND ibr.total_signals > 0
        ORDER BY ibr.win_rate DESC
        LIMIT 10
    """)
    for row in cur.fetchall():
        print(f"    {row[0]:15s} | {row[1]:12s} | 胜率={row[2]:.1f}% | 信号={row[3]} | 均收益={row[4]:+.2f}%")

    conn.close()
    print(f"\n{'=' * 60}")
    print("Phase C 完成")


if __name__ == "__main__":
    main()
