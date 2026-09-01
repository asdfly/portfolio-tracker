"""
导入 2026年8月 交易数据 + 最新持仓快照

数据源：
  1) 招商证券对账单 PDF  (C:/Users/HUAWEI/Downloads/20260801-20260831.pdf)
     - 流水明细 (pages 3-8) -> trade_records
     - 证券余额 (pages 1-2) 仅用于交叉校验
  2) 通达信持仓导出      (c:/zd_zsone/T0002/export/持仓股20260831.xls)
     - 经 PositionReader 解析 -> portfolio_snapshots (date=2026-08-31)

解析策略（PDF 流水）：
  采用 pdfplumber.extract_words() 的坐标法，规避文本换行导致字段错位。
  业务标志之后的数值块顺序固定为：
    发生数量, 成交均价/净值, 成交金额, 佣金, 印花税, 其他费, 变动金额, [资金余额...]
  场内证券有 8+ 个数字(含资金余额列)，场外产品(基金定投/申赎)仅 7 个数字
  (无资金余额列) —— 阈值取 >=7 兼容两类，下标映射一致。

用法：
  python scripts/import_aug_2026.py            # 默认真实写入(apply)
  python scripts/import_aug_2026.py --dry-run  # 只校验不写入
"""
import sys
import os
import re
import argparse
import logging
from pathlib import Path
from collections import Counter, defaultdict

# 项目路径
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.utils.position_reader import PositionReader
from src.utils.database import DatabaseManager
from data_loader import get_db_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("import_aug_2026")

PDF_PATH = r"C:/Users/HUAWEI/Downloads/20260801-20260831.pdf"
XLS_PATH = r"c:/zd_zsone/T0002/export/持仓股20260831.xls"
SNAP_DATE = "2026-08-31"

# 业务标志词表
ACTION_VOCAB = {
    "银行转存", "产品定时定额投资确认", "产品赎回确认", "产品申购确认",
    "产品红利发放", "证券买入", "证券卖出", "股息入账",
    "质押回购拆出", "质押回购拆入",
}
MARKET_VOCAB = {"资金", "场外开基", "上海", "深圳", "港股通"}


def _is_num(tok: str) -> bool:
    return bool(re.match(r"^-?\d+(\.\d+)?$", tok or ""))


def parse_pdf_trades(pdf_path: str):
    """解析 PDF 流水明细 -> list[dict] (字段对齐 trade_records)。"""
    import pdfplumber
    import warnings
    warnings.filterwarnings("ignore")

    SKIP_NAME = {"人民币", "CNY"} | MARKET_VOCAB
    # 变动金额缺失时按方向推算现金变动
    BUY_ACTIONS = {"证券买入", "产品定时定额投资确认", "产品申购确认"}
    SELL_ACTIONS = {"证券卖出", "产品赎回确认", "股息入账", "产品红利发放", "银行转存"}

    trades = []
    issues = []
    with pdfplumber.open(pdf_path) as pdf:
        for pi, pg in enumerate(pdf.pages):
            words = pg.extract_words()
            rows = defaultdict(list)
            for w in words:
                rows[round(w["top"], 1)].append((round(w["x0"], 1), w["text"]))
            for top in sorted(rows.keys()):
                toks = sorted(rows[top], key=lambda t: t[0])
                texts = [t[1] for t in toks]
                # 找日期
                date_tok = None
                for x, t in toks:
                    if re.match(r"^2026\d{4}$", t):
                        date_tok = t
                        break
                if not date_tok:
                    continue  # 非流水行

                date = f"{date_tok[:4]}-{date_tok[4:6]}-{date_tok[6:8]}"

                # 结构判定：是否有证券代码(6位)或市场词
                code = ""
                code_x = None
                for x, t in toks:
                    if re.match(r"^\d{6}$", t):
                        code = t
                        code_x = x
                        break
                has_struct = bool(code) or any(t in MARKET_VOCAB for x, t in toks)

                # 业务标志
                action = None
                action_x = None
                for x, t in toks:
                    if t in ACTION_VOCAB or re.match(r"^(证券买|证券卖|产品|银行转存|股息|质押)", t or ""):
                        action = t
                        action_x = x
                        break
                if not action:
                    if not has_struct:
                        continue  # 页眉/页脚噪声，静默跳过
                    issues.append((pi + 1, top, "无业务标志", texts))
                    continue

                # 市场
                market = next((t for x, t in toks if t in MARKET_VOCAB), "")

                # 名称：code 与 action 之间、非数字、非币种/市场词
                # (兼容 "1000ETF" 这类无中文字符的证券简称)
                name_parts = []
                for x, t in toks:
                    if code_x is not None:
                        if not (code_x < x < action_x):
                            continue
                    else:
                        if not (x < action_x):
                            continue
                    if _is_num(t) or t in SKIP_NAME:
                        continue
                    name_parts.append(t)
                name = "".join(name_parts)

                # 数值块：action 之后所有 token（变动金额可能以 '-' 占位/缺失）
                post = [t for x, t in toks if x > action_x]
                nums = [t for t in post if _is_num(t) or t == "-"]
                # 至少需有 发生数量/价格/金额/佣金/印花税 5 个，否则视为异常
                if len(nums) < 5:
                    issues.append((pi + 1, top, f"数值字段仅{len(nums)}个(<5)", texts))
                    continue

                def _num(t, default=0.0):
                    return float(t) if _is_num(t) else default

                qty = _num(nums[0])
                price = _num(nums[1])
                amount = _num(nums[2])
                commission = _num(nums[3])
                stamp = _num(nums[4])
                other = _num(nums[5]) if len(nums) > 5 else 0.0
                commission = commission + other  # 佣金 + 其他费(合并入佣金列)

                # 变动金额：优先取第7个数值；'-'/缺失时按买卖方向推算
                if len(nums) > 6 and _is_num(nums[6]):
                    change_amount = float(nums[6])
                elif action in BUY_ACTIONS:
                    change_amount = -(amount + commission + stamp)
                elif action in SELL_ACTIONS:
                    change_amount = amount - commission - stamp
                else:
                    change_amount = 0.0

                trades.append({
                    "date": date,
                    "market": market,
                    "code": code,
                    "name": name,
                    "action": action,
                    "quantity": qty,
                    "price": price,
                    "amount": amount,
                    "commission": round(commission, 2),
                    "stamp_tax": stamp,
                    "change_amount": change_amount,
                })
    return trades, issues


def parse_pdf_balances(pdf_path: str):
    """解析 PDF 证券余额 (pages 1-2) 场内部分 -> {code: (qty, mv)}，仅校验用。

    证券余额含三块：沪A / 深A (均为场内 ETF，市场=上海|深圳) 与 场外产品持仓
    (市场列为 中登深圳/华夏基金 等，非 上海|深圳)。仅匹配 上海|深圳 行，
    既对齐通达信"场内ETF"口径，又自然排除场外产品。
    行格式：账号(A+数字或纯数字) 代码(6位) 名称 人民币 市场 当前数量 可用数量 成本价 收盘价 市值
    """
    import pdfplumber
    import warnings
    warnings.filterwarnings("ignore")
    bal = {}
    # 参考成本价可能为负数(如 -0.2918，成本已被分红完全覆盖)，故各数值列允许 -?
    pat = re.compile(
        r"^([A-Z]?\d+)\s+(\d{6})\s+(\S+)\s+人民币\s+(上海|深圳)\s+"
        r"(-?[\d.]+)\s+(-?[\d.]+)\s+(-?[\d.]+)\s+(-?[\d.]+)\s+(-?[\d.]+)\s*$"
    )
    with pdfplumber.open(pdf_path) as pdf:
        for pg in pdf.pages[:2]:
            txt = pg.extract_text() or ""
            for line in txt.splitlines():
                m = pat.match(line.strip())
                if m:
                    bal[m.group(2)] = (float(m.group(5)), float(m.group(9)))
    return bal


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="只校验不写入")
    ap.add_argument("--pdf", default=PDF_PATH)
    ap.add_argument("--xls", default=XLS_PATH)
    args = ap.parse_args()

    print("=" * 70)
    print("导入 2026年8月 交易数据 + 持仓快照")
    print("=" * 70)

    # 1) 持仓（通达信 xls）
    print("\n[1] 解析通达信持仓文件:", args.xls)
    reader = PositionReader(file_path=args.xls)
    positions = reader.read_positions()
    xls_bal = {p["code"]: p["quantity"] for p in positions}
    print(f"    持仓 {len(positions)} 条; 总市值 {sum(p['market_value'] for p in positions):,.2f}")

    # 2) PDF 证券余额（交叉校验）
    pdf_bal = parse_pdf_balances(args.pdf)
    print(f"[2] PDF 证券余额(场内) {len(pdf_bal)} 条（交叉校验基准）")

    # 交叉校验
    only_xls = set(xls_bal) - set(pdf_bal)
    only_pdf = set(pdf_bal) - set(xls_bal)
    qty_diff = {c: (xls_bal[c], pdf_bal[c][0]) for c in set(xls_bal) & set(pdf_bal)
                if abs(xls_bal[c] - pdf_bal[c][0]) > 1e-6}
    print(f"    仅通达信有: {sorted(only_xls)}")
    print(f"    仅PDF有(场内): {sorted(only_pdf)}")
    print(f"    数量不一致: {qty_diff}")
    if not only_xls and not only_pdf and not qty_diff:
        print("    ✅ 两源场内持仓完全一致")
    else:
        print("    ⚠️ 两源持仓存在差异，请人工复核（详见上）")

    # 3) PDF 流水 -> trade_records
    print("\n[3] 解析 PDF 流水明细")
    trades, issues = parse_pdf_trades(args.pdf)
    aug = [t for t in trades if "2026-08-01" <= t["date"] <= "2026-08-31"]
    print(f"    共解析流水 {len(trades)} 条，其中8月 {len(aug)} 条")
    print("    业务标志分布:", dict(Counter(t["action"] for t in aug)))
    if issues:
        print(f"    ⚠️ 解析异常 {len(issues)} 行（样例前5）:")
        for it in issues[:5]:
            print("      ", it)
    # 样本
    print("    样本(前3):")
    for t in aug[:3]:
        print("      ", t)

    if args.dry_run:
        print("\n[DRY-RUN] 未写入数据库。确认无误后去掉 --dry-run 执行。")
        return

    # 4) 写入
    print("\n[4] 写入数据库")
    db = DatabaseManager()
    conn = get_db_connection(db.db_path)
    cur = conn.cursor()
    # 4a 交易：先清8月已有（幂等），再插入
    cur.execute("DELETE FROM trade_records WHERE date >= ?", ("2026-08-01",))
    before = cur.execute("SELECT COUNT(*) FROM trade_records").fetchone()[0]
    n_ins = 0
    for t in aug:
        cur.execute(
            """INSERT INTO trade_records
               (date, market, code, name, action, quantity, price, amount, commission, stamp_tax, change_amount)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (t["date"], t["market"], t["code"], t["name"], t["action"],
             t["quantity"], t["price"], t["amount"], t["commission"], t["stamp_tax"], t["change_amount"]),
        )
        n_ins += 1
    conn.commit()
    after = cur.execute("SELECT COUNT(*) FROM trade_records").fetchone()[0]
    print(f"    trade_records: {before} -> {after} (新增8月 {n_ins} 条)")

    # 4b 持仓快照
    db.save_portfolio_snapshot(SNAP_DATE, positions)
    snap_n = cur.execute(
        "SELECT COUNT(*) FROM portfolio_snapshots WHERE date=?", (SNAP_DATE,)
    ).fetchone()[0]
    print(f"    portfolio_snapshots[{SNAP_DATE}]: {snap_n} 条")
    conn.close()
    print("\n✅ 导入完成。")


if __name__ == "__main__":
    main()
