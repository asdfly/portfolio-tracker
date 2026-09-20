"""份额折算闸门 · 历史折算日与 C族 判别性测试（临时库，确定性，不碰生产库）

覆盖 Issue-Twelve 修复 (c) 的「公式侧伪收益修复」：
  - 7 只历史 ETF 折算日（A族/B族，含 510500 2015 这一 pre-2018、无 qfq 的行）：
    修复后每个折算日的 |daily_return| 不得出现 ±50%/±250% 的伪收益（应落在 ~0 附近）。
  - C族（场外基金，按金额申赎）：净值口径切换会带来大价比，但绝不是份额折算，
    闸门不得误剔 —— 真实净值变动须原样保留。
  - B族/pre-2018 无 qfq 时：用快照侧 quantity 比值判别，quantity 已调增则按当日
    quantity 计入（真实 ~0），而不是丢标的。

所有夹具均按 portfolio_snapshots / portfolio_summary / etf_price_history 的真实字段构造，
curr_price 一律取真实未复权市价（与线上一致）。
"""
import logging
import sqlite3
import sys
from pathlib import Path

import pytest

PROJECT_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_DIR))

from src.analysis.portfolio import PortfolioAnalyzer              # noqa: E402
from src.utils.database import DatabaseManager                    # noqa: E402

SCHEMA = """
CREATE TABLE portfolio_snapshots (
    date TEXT, code TEXT, quantity REAL, current_price REAL, market_value REAL
);
CREATE TABLE portfolio_summary (date TEXT, total_value REAL);
CREATE TABLE etf_price_history (date TEXT, code TEXT, close REAL);
"""

# (code, 前日快照, 折算日快照, 是否 qfq 覆盖, qfq 复权比值)
# 真实数据里每只 ETF 折算日的 market_value 都是连续的（quantity 已调增吸收折价），
# 故这里 prev_mv == split_mv；curr_qty != prev_qty 是 B族/pre-2018 合并的典型签名。
SPLIT_DAYS = [
    # code, prev_date, prev_qty, prev_price, split_date, split_qty, split_price, qfq_rows
    ("510500", "2015-04-10", 2300.0, 2.243, "2015-04-15", 659.87, 7.818, None),       # pre-2018 合并，无 qfq
    ("512010", "2021-06-24", 260000.0, 3.206, "2021-06-28", 997081.0, 0.836, (1.00, 1.00)),  # 拆分
    ("512100", "2022-09-01", 5300.0, 0.982, "2022-09-05", 1918.39, 2.713, (1.00, 1.00)),     # 合并
    ("159300", "2024-06-24", 2000.0, 0.973, "2024-06-25", 562.10, 3.462, (1.00, 1.00)),       # 合并
    ("516160", "2024-09-13", 8000.0, 0.497, "2024-09-18", 2489.67, 1.597, (1.00, 1.00)),      # 拆分
    ("512810", "2025-06-20", 25000.0, 1.197, "2025-06-23", 50000.0, 0.608, (1.00, 1.00)),      # 拆分(B族)
    ("159220", "2025-11-07", 10000.0, 1.240, "2025-11-10", 20000.0, 0.627, (1.00, 1.00)),      # 拆分(B族)
]


def _make_db(tmp_path, *, code, prev_date, prev_qty, prev_price,
             split_date, split_qty, split_price, qfq_rows):
    prev_mv = round(prev_price * prev_qty, 2)
    split_mv = round(split_price * split_qty, 2)
    db_path = tmp_path / "portfolio.db"
    conn = sqlite3.connect(str(db_path))
    conn.executescript(SCHEMA)
    conn.execute(
        "INSERT INTO portfolio_snapshots VALUES (?,?,?,?,?)",
        (prev_date, code, prev_qty, prev_price, prev_mv),
    )
    # 折算日快照也写进库（prev_dt 探测用），其 quantity/mv 为真实签名
    conn.execute(
        "INSERT INTO portfolio_snapshots VALUES (?,?,?,?,?)",
        (split_date, code, split_qty, split_price, split_mv),
    )
    conn.execute("INSERT INTO portfolio_summary VALUES (?,?)", (prev_date, prev_mv))
    if qfq_rows is not None:
        c_prev, c_curr = qfq_rows
        conn.execute("INSERT INTO etf_price_history VALUES (?,?,?)", (prev_date, code, c_prev))
        conn.execute("INSERT INTO etf_price_history VALUES (?,?,?)", (split_date, code, c_curr))
    conn.commit()
    conn.close()
    return db_path


def _analyzer(db_path, today):
    pa = PortfolioAnalyzer.__new__(PortfolioAnalyzer)
    pa.db = DatabaseManager(str(db_path))
    pa.today = today
    return pa


def _positions(code, split_qty, split_price):
    return [{
        "code": code, "name": code, "quantity": split_qty, "cost_price": split_price,
        "current_price": split_price, "realtime_price": split_price,
        "market_value": split_price * split_qty,
        "realtime_market_value": split_price * split_qty, "pnl": 0.0,
    }]


def _daily_return(db_path, code, split_qty, split_price, today):
    pa = _analyzer(db_path, today)
    return pa._calculate_summary(
        _positions(code, split_qty, split_price),
        {"sh000300": {"change_pct": 0.0}}, {},
    )["daily_return"]


class TestEtfSplitDaysNoPseudo:
    @pytest.mark.parametrize("case", SPLIT_DAYS, ids=lambda c: c[0])
    def test_no_pseudo_spike(self, tmp_path, case):
        (code, prev_date, prev_qty, prev_price, split_date, split_qty,
         split_price, qfq_rows) = case
        db_path = _make_db(
            tmp_path, code=code, prev_date=prev_date, prev_qty=prev_qty, prev_price=prev_price,
            split_date=split_date, split_qty=split_qty, split_price=split_price, qfq_rows=qfq_rows,
        )
        dr = _daily_return(db_path, code, split_qty, split_price, split_date)
        # 折算日前一天的真实日收益本应≈0%（市值连续）；修复后绝不出现 ±50%/±250% 伪收益。
        assert abs(dr) < 0.15, f"{code} {split_date}: daily_return={dr}"


class TestCfamilyAndDiscriminator:
    def test_otc_large_move_kept_not_dropped(self, tmp_path, caplog):
        """C族（场外基金）净值口径切换导致大价比 → 闸门不得误剔，真实变动原样保留。"""
        code = "519770"  # 场外基金（config.settings.OTC_FUND_CODES）
        db_path = _make_db(
            tmp_path, code=code, prev_date="2026-08-28", prev_qty=1000.0, prev_price=1.000,
            split_date="2026-08-29", split_qty=1000.0, split_price=1.300,  # +30% 真实净值跳变
            qfq_rows=None,
        )
        with caplog.at_level(logging.INFO):
            dr = _daily_return(db_path, code, 1000.0, 1.300, "2026-08-29")
        # 旧闸门的写法（OTC 无 qfq 会被误剔）会得到 0；修复后保留真实 +30%。
        assert dr == pytest.approx(30.0, abs=0.2), dr
        # 不得出现「剔除 / 折算假收益」类日志；应记录「场外基金，不按折算处理」
        assert not any("剔除" in r.message for r in caplog.records)
        assert any("场外基金" in r.message for r in caplog.records)

    def test_preexisting_split_no_qfq_uses_curr_qty(self, tmp_path, caplog):
        """510500 2015 型：pre-2018 合并、qfq 缺失、quantity 已调增 → 按当日 quantity 计入≈0。"""
        case = SPLIT_DAYS[0]  # 510500
        (code, prev_date, prev_qty, prev_price, split_date, split_qty,
         split_price, _qfq) = case
        db_path = _make_db(
            tmp_path, code=code, prev_date=prev_date, prev_qty=prev_qty, prev_price=prev_price,
            split_date=split_date, split_qty=split_qty, split_price=split_price, qfq_rows=None,
        )
        with caplog.at_level(logging.WARNING):
            dr = _daily_return(db_path, code, split_qty, split_price, split_date)
        assert abs(dr) < 0.15, dr
        # 命中「quantity 已调整」分支，而非「无可用复权价」或「折算假收益剔除」
        assert any("quantity 已调整" in r.message for r in caplog.records)

    def test_pre2018_split_no_qfq_flat_qty_dropped(self, tmp_path, caplog):
        """防御情形：pre-2018 折算且快照 quantity 未调整（A族假收益）→ 剔除，不进求和。"""
        code = "510500"
        # 与 SPLIT_DAYS[0] 同日期，但把 split_qty 设为 == prev_qty（未调整）→ A族假收益
        db_path = _make_db(
            tmp_path, code=code, prev_date="2015-04-10", prev_qty=2300.0, prev_price=2.243,
            split_date="2015-04-15", split_qty=2300.0, split_price=7.818, qfq_rows=None,
        )
        with caplog.at_level(logging.WARNING):
            dr = _daily_return(db_path, code, 2300.0, 7.818, "2015-04-15")
        assert dr == 0, dr  # 全量被剔除 → 0，且 guard_fired 抑制 fallback
        assert any("折算假收益" in r.message and "剔除" in r.message for r in caplog.records)
