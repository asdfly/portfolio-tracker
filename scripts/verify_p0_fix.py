"""P0-1 / P0-3 修复验证脚本（只读连接生产库，不做任何写操作）。

运行：
  venv313/Scripts/python.exe scripts/verify_p0_fix.py
"""
import logging
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

DB = ROOT / "data" / "database" / "portfolio.db"
AS_OF = "2026-09-15"

from config.settings import (ETF_LOT_SIZE, is_otc_fund,        # noqa: E402
                             OTC_FUND_CODES as OTC)
from src.analysis.rebalance_engine import (                    # noqa: E402
    RebalanceEngine, calc_trade_shares, compute_rebalance_suggestion,
)
from src.models import RebalanceTrade                          # noqa: E402
from src.utils.trading_calendar import last_trading_day_on_or_before  # noqa: E402


def old_holdings(con, as_of):
    """修复前逻辑：取「全局最新快照日期」那一批行。"""
    import pandas as pd
    d = last_trading_day_on_or_before(as_of)
    df = pd.read_sql_query(
        "SELECT code, name, market_value, current_price FROM portfolio_snapshots WHERE date=?",
        con, params=[str(d)])
    if df.empty:  # 旧逻辑的兜底：退到 <= d 最近一个有快照的日期
        row = pd.read_sql_query(
            "SELECT DISTINCT date FROM portfolio_snapshots WHERE date<=? "
            "ORDER BY date DESC LIMIT 1", con, params=[str(d)])
        if not row.empty:
            df = pd.read_sql_query(
                "SELECT code, name, market_value, current_price FROM portfolio_snapshots WHERE date=?",
                con, params=[str(row.iloc[0, 0])])
    df = df.dropna(subset=["market_value"])
    return df


def main():
    logging.basicConfig(level=logging.WARNING, format="[WARN] %(message)s")
    con = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)

    print("=" * 72)
    print("【P0-1】持仓快照口径对比")
    print("=" * 72)
    df_old = old_holdings(con, AS_OF)
    eng = RebalanceEngine(con)
    mv_new, total_new, names, prices, snap_dates, stale = eng.get_current_holdings(
        AS_OF, with_meta=True)
    total_old = float(df_old["market_value"].sum())
    print(f"  修复前：{len(df_old)} 只，总市值 {total_old:,.2f}")
    print(f"  修复后：{len(mv_new)} 只，总市值 {total_new:,.2f}")
    pct = (total_new / total_old - 1) * 100 if total_old else float("nan")
    print(f"  恢复标的 {len(mv_new) - len(df_old)} 只，"
          f"市值 +{total_new - total_old:,.2f} 元（+{pct:.1f}%）")

    from config.settings import DELISTED_CODES
    print(f"  已清仓排除（config.DELISTED_CODES）：{sorted(DELISTED_CODES)}")
    print(f"  场内 {len(set(mv_new) - set(OTC))} 只 + 场外 {len(set(mv_new) & set(OTC))} 只"
          f" = {len(mv_new)} 只")
    lost = sorted(set(mv_new) - set(df_old["code"]))
    print(f"  修复前被漏掉的标的（{len(lost)} 只）：")
    for c in lost:
        tag = "场外" if is_otc_fund(c) else "场内"
        print(f"    {c} [{tag}] {names.get(c, '')[:24]:<26} 市值 {mv_new[c]:>12,.2f} "
              f"快照 {snap_dates.get(c)}")

    print(f"\n  陈旧快照告警（>{7} 天）：{len(stale)} 只")
    for s in stale[:20]:
        print(f"    [{s.get('level','陈旧')}] {s['code']} {s['name'][:20]:<22} "
              f"已停更 {s['days']} 天（{s['snapshot_date']}）")

    print("\n" + "=" * 72)
    print("【P0-3】手/份整手取整验证")
    print("=" * 72)
    cases = [
        # (标的, 价格, 交易额, 说明)
        ("512010", 0.372, 69607.0, "场内 ETF：69607/0.372 = 187115 份"),
        ("159949", 1.542, 34283.0, "场内 ETF：34283/1.542 = 22233 份"),
        ("512810", 0.667, 1973.0, "场内 ETF：1973/0.667 = 2958 份"),
        ("001323", 5.9287, 25216.0, "场外基金：不取整手"),
        ("880013", 1.0, 38528.0, "场外基金：不取整手"),
    ]
    ok = True
    for code, price, value, note in cases:
        sh = calc_trade_shares(value, price, code)
        t = RebalanceTrade(code=code, shares=sh, lot_traded=not is_otc_fund(code))
        is_lot = not is_otc_fund(code)
        expect_lots = sh // ETF_LOT_SIZE
        if is_lot:
            good = (sh % ETF_LOT_SIZE == 0) and (t.lots == expect_lots)
        else:
            good = (t.lots == 0) and sh == int(value / price)
        ok = ok and good
        print(f"  {code} 价{price:>7} 额{value:>8,.0f} -> shares={sh:>7,} lots={t.lots:>6,} "
              f"[{'场内' if is_lot else '场外'}] {'OK' if good else 'FAIL'}  ({note})")
    print(f"  整手取整断言：{'全部通过' if ok else '存在失败'}")

    print("\n  端到端（strategy=equal_weight）真实方案前 8 笔：")
    plan = compute_rebalance_suggestion(con, as_of_date=AS_OF, strategy="equal_weight")
    print(f"    总市值 {plan.total_value:,.2f}｜参与标的 {len(plan.current_weights)} 只｜"
          f"交易 {len(plan.trades)} 笔｜换手 {plan.turnover:.4f}")
    bad = [t for t in plan.trades
           if t.lot_traded and (t.shares % ETF_LOT_SIZE != 0 or t.lots != t.shares // ETF_LOT_SIZE)]
    print(f"    场内未整手 / 手数不同步的交易：{len(bad)} 笔（应为 0）")
    zero = [t for t in plan.trades if t.shares == 0]
    print(f"    下单量为 0 的交易：{len(zero)} 笔（应为 0，不足 1 手的腿已被丢弃）")
    print(f"    被丢弃的腿：{len(plan.dropped_legs)} 笔"
          + ("" if not plan.dropped_legs else "：" + "、".join(
              f"{d['code']}({d['trade_value']:,.0f}元<{d['min_value']:,.0f}元)"
              for d in plan.dropped_legs)))
    print(f"    波动率预警："
          + ("；".join(w for w in RebalanceEngine(con).compute_risk_metrics(AS_OF).get("warnings", [])
                       if "波动率" in w) or "无"))
    for t in plan.trades[:8]:
        unit = f"{t.lots:,}手" if t.lot_traded else f"{t.shares:,}份"
        print(f"    {t.direction} {t.code} {t.name[:18]:<20} {t.trade_value:>10,.0f}元 "
              f"-> {unit:>10} (= {t.shares:,} 份)")

    con.close()
    print("\n验证完成（全程只读，未写入生产库）")


if __name__ == "__main__":
    main()
