# -*- coding: utf-8 -*-
"""
import_statement_flows.py — 招商证券对账单流水导入（带 flow_category 分类标记）

用法:
    # 试运行(仅解析+汇总, 不写库)
    python scripts/import_statement_flows.py --dry-run

    # 正式导入 9月 + 10月 对账单
    python scripts/import_statement_flows.py

    # 仅导入指定月份
    python scripts/import_statement_flows.py --months 202609 202610

设计:
    - 解析复用 import_aug_2026.parse_pdf_trades (已修复场外基金 4 数值列误杀)
    - 每条流水按 action 打 flow_category ∈ {证券, 基金, 资金, 分红, 其他}
    - 导入幂等: 先 DELETE 该月区间已有行, 再 INSERT, 不触碰其他月份(如 8 月存量)
    - 存量行(action 已存在但 flow_category 为 NULL)一并回填标记
    - 加 --dry-run 可先预览解析质量再决定写入
"""
import os
import sys
import argparse
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("import_flows")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.db_schema import ensure_trade_records_flow_category
from scripts.import_aug_2026 import parse_pdf_trades, _classify_action
from src.utils.database import DatabaseManager, get_db_connection

STATEMENT_DIR = r"C:/Users/HUAWEI/WPSDrive/2779396_1/WPS云盘/李丹同志工作文档/GQ/招商证券/对账单"

# (文件名, 起始日, 结束日) — 仅导入这些区间, 其余月份不受影响
DEFAULT_STATEMENTS = [
    ("20260901-20260930.pdf", "2026-09-01", "2026-09-30"),
    ("20261001-20261031.pdf", "2026-10-01", "2026-10-31"),
]

INSERT_SQL = """INSERT INTO trade_records
   (date, market, code, name, action, quantity, price, amount,
    commission, stamp_tax, change_amount, flow_category)
   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)"""


def parse_month(pdf_path, start, end):
    """解析单月对账单, 返回 (纳入流水, 异常行, 区间外被丢弃行)。"""
    trades, issues = parse_pdf_trades(pdf_path)
    in_range, out_range = [], []
    for t in trades:
        if start <= t["date"] <= end:
            in_range.append(t)
        else:
            out_range.append(t)
    return in_range, issues, out_range


def main():
    ap = argparse.ArgumentParser(description="招商证券对账单流水导入(带分类标记)")
    ap.add_argument("--months", nargs="*", help="仅导入指定月份前缀, 如 202609 202610")
    ap.add_argument("--dry-run", action="store_true", help="仅解析与汇总, 不写库")
    args = ap.parse_args()

    stmts = DEFAULT_STATEMENTS
    if args.months:
        stmts = [s for s in DEFAULT_STATEMENTS if any(s[0].startswith(m) for m in args.months)]
        if not stmts:
            logger.error("未匹配到任何月份: %s", args.months)
            return 1

    # 1) 解析
    all_in, all_issues = [], []
    print("\n[1] 解析对账单")
    for fn, start, end in stmts:
        path = os.path.join(STATEMENT_DIR, fn)
        if not os.path.exists(path):
            logger.warning("文件不存在, 跳过: %s", path)
            continue
        in_range, issues, out_range = parse_month(path, start, end)
        all_in.extend(in_range)
        all_issues.extend(issues)
        print(f"  {fn}: 区间纳入 {len(in_range)} 条, 异常 {len(issues)} 条, 区间外 {len(out_range)} 条")
        from collections import Counter
        print(f"      分类: {dict(Counter(t['flow_category'] for t in in_range))}")
        print(f"      业务: {dict(Counter(t['action'] for t in in_range))}")

    if all_issues:
        print(f"\n  ⚠️ 解析异常 {len(all_issues)} 行 (样例前5):")
        for it in all_issues[:5]:
            print("    ", it)

    if not all_in:
        print("  无流水可导入。")
        return 0

    from collections import Counter
    print(f"\n  合计纳入 {len(all_in)} 条; 分类分布: {dict(Counter(t['flow_category'] for t in all_in))}")

    if args.dry_run:
        print("\n[DRY-RUN] 未写入数据库。确认无误后去掉 --dry-run 执行。")
        return 0

    # 2) 写库
    print("\n[2] 写入数据库")
    db = DatabaseManager()
    conn = get_db_connection(db.db_path)
    cur = conn.cursor()

    # 2a 迁移: 确保 flow_category 列存在
    ensure_trade_records_flow_category(conn)
    print("    ✅ flow_category 列已就绪")

    # 2b 存量回填: 把 action 已存在但 flow_category 为 NULL 的行补标
    backfill = cur.execute(
        "UPDATE trade_records SET flow_category = ? WHERE flow_category IS NULL AND action IS NOT NULL",
        ("__pending__",),
    )
    # 逐 action 回填 (用 Python 端 _classify_action, 与导入一致)
    pending = cur.execute(
        "SELECT id, action FROM trade_records WHERE flow_category = '__pending__'"
    ).fetchall()
    for _id, action in pending:
        cur.execute(
            "UPDATE trade_records SET flow_category = ? WHERE id = ?",
            (_classify_action(action), _id),
        )
    if pending:
        print(f"    存量回填 flow_category: {len(pending)} 行")

    # 2c 按月幂等导入
    before = cur.execute("SELECT COUNT(*) FROM trade_records").fetchone()[0]
    for fn, start, end in stmts:
        path = os.path.join(STATEMENT_DIR, fn)
        if not os.path.exists(path):
            continue
        in_range, _, _ = parse_month(path, start, end)
        # 先清该月区间, 再插入 (幂等)
        cur.execute(
            "DELETE FROM trade_records WHERE date BETWEEN ? AND ?", (start, end)
        )
        n = 0
        for t in in_range:
            cur.execute(INSERT_SQL, (
                t["date"], t["market"], t["code"], t["name"], t["action"],
                t["quantity"], t["price"], t["amount"], t["commission"],
                t["stamp_tax"], t["change_amount"], t["flow_category"],
            ))
            n += 1
        print(f"    {fn}: 写入 {n} 条")
    conn.commit()
    after = cur.execute("SELECT COUNT(*) FROM trade_records").fetchone()[0]
    print(f"    trade_records 总行数: {before} -> {after}")

    # 3) 校验
    print("\n[3] 导入后分类校验")
    rows = cur.execute(
        "SELECT flow_category, COUNT(*) FROM trade_records GROUP BY flow_category ORDER BY 2 DESC"
    ).fetchall()
    for cat, cnt in rows:
        print(f"    {cat}: {cnt}")

    conn.close()
    print("\n✅ 导入完成。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
