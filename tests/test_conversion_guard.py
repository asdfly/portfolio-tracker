"""份额折算闸门回归测试（临时库，确定性，不碰生产库）

背景：`portfolio_snapshots.current_price` 存的是真实未复权市价，而 ETF 会发生
基金份额折算（拆分/合并）。`_calculate_summary` 里 quantity 在分子分母约掉，
每个标的直接贡献 `curr_price / prev_price`——折算日该值为 ±250% 的假收益，
会被 portfolio_nav 的 TWR 永久累乘吸收。

覆盖路径：
  A. 价比超阈值 + etf_price_history 有连续鲜活的 qfq 序列 → 改用复权价比；
  B. 价比超阈值 + 无 qfq 数据 → 剔除该标的，且不被 total_value fallback 算回来；
  C. 正常波动（±10% 内）→ 闸门不介入，原行为不变；
  D. 价比超阈值 + qfq 行情陈旧（>7 自然日）→ 剔除该标的，不得被当成 1.0 混入
     （旧行为会把折算跳变稀释成「伪造的低收益」，不留痕迹）。
"""
import logging
import sqlite3
import sys
from pathlib import Path

import pytest

PROJECT_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_DIR))

from src.analysis.portfolio import (                              # noqa: E402
    CONVERSION_QFQ_MAX_STALENESS_DAYS, PortfolioAnalyzer,
)
from src.utils.database import DatabaseManager                    # noqa: E402

PREV_DT = "2026-09-14"
TODAY = "2026-09-15"
CODE = "512010"        # 实测 2021-06-25 发生 ÷3.907 拆分的标的
FRESH_CODE = "510300"

SCHEMA = """
CREATE TABLE portfolio_snapshots (
    date TEXT, code TEXT, quantity REAL, current_price REAL, market_value REAL
);
CREATE TABLE portfolio_summary (date TEXT, total_value REAL);
CREATE TABLE etf_price_history (date TEXT, code TEXT, close REAL);
"""


def _snap(code, price, qty=1000.0, day=PREV_DT):
    return (day, code, qty, price, price * qty)


def _make_db(tmp_path, *, snapshots, prev_total_value, qfq_rows=()):
    """1:2 拆分夹具：quantity 恒定，仅价格腰斩。

    prev_total_value > 0 是必要的：否则 total_value fallback 本就不会触发，用例 B 失去意义。
    """
    db_path = tmp_path / "portfolio.db"
    conn = sqlite3.connect(str(db_path))
    conn.executescript(SCHEMA)
    conn.executemany("INSERT INTO portfolio_snapshots VALUES (?,?,?,?,?)", snapshots)
    conn.execute("INSERT INTO portfolio_summary VALUES (?,?)", (PREV_DT, prev_total_value))
    conn.executemany("INSERT INTO etf_price_history VALUES (?,?,?)", qfq_rows)
    conn.commit()
    conn.close()
    return db_path


def _analyzer(db_path):
    """绕开 __init__ 里的重型分析器，只装配 _calculate_summary 依赖的属性。"""
    pa = PortfolioAnalyzer.__new__(PortfolioAnalyzer)
    pa.db = DatabaseManager(str(db_path))
    pa.today = TODAY
    return pa


def _positions(*items):
    """items: (code, name, curr_price)，quantity 恒为 1000。"""
    return [{
        "code": code, "name": name, "quantity": 1000.0, "cost_price": 1.5,
        "current_price": curr_price, "realtime_price": curr_price,
        "market_value": curr_price * 1000.0, "realtime_market_value": curr_price * 1000.0,
        "pnl": 0.0,
    } for code, name, curr_price in items]


def _summary(pa, positions):
    return pa._calculate_summary(positions, {"sh000300": {"change_pct": 0.35}}, {})


class TestConversionGuard:
    def test_split_uses_qfq_ratio(self, tmp_path, caplog):
        """A：折算日改用复权价比，日收益落回真实值（约 +2%），而非 −50%。"""
        db_path = _make_db(
            tmp_path,
            snapshots=[_snap(CODE, 2.0)],
            prev_total_value=2000.0,
            # qfq 序列连续（折算前已回溯复权），真实当日收益 +2.0%
            qfq_rows=[(PREV_DT, CODE, 1.00), (TODAY, CODE, 1.02)],
        )
        with caplog.at_level(logging.WARNING):
            summary = _summary(_analyzer(db_path), _positions((CODE, "医药ETF", 1.0)))

        # 未加闸门时 raw_ratio = 1.0/2.0 = 0.5 -> daily_return = −50%
        assert abs(summary["daily_return"]) < 30, summary["daily_return"]
        assert summary["daily_return"] == pytest.approx(2.0, abs=0.1)
        assert any("[折算闸门]" in r.message and "改用复权价比" in r.message
                   for r in caplog.records), [r.message for r in caplog.records]

    def test_split_without_qfq_drops_code_and_suppresses_fallback(self, tmp_path, caplog):
        """B：无复权价 -> 剔除该标的；daily_return 必须为 0，不被 fallback 算回来。"""
        db_path = _make_db(tmp_path, snapshots=[_snap(CODE, 2.0)], prev_total_value=2000.0)
        with caplog.at_level(logging.WARNING):
            summary = _summary(_analyzer(db_path), _positions((CODE, "医药ETF", 1.0)))

        # 若 fallback 未被抑制：(1000 − 2000) / 2000 = −50%
        assert summary["daily_return"] == 0, summary["daily_return"]
        assert summary["daily_pnl"] == 0
        assert any("[折算闸门]" in r.message and "剔除" in r.message
                   for r in caplog.records), [r.message for r in caplog.records]

    def test_normal_move_not_intercepted(self, tmp_path, caplog):
        """C：±10% 内的正常波动不得被闸门拦截，原价比行为不变。"""
        db_path = _make_db(
            tmp_path,
            snapshots=[_snap(CODE, 1.0)],
            prev_total_value=1000.0,
            qfq_rows=[(PREV_DT, CODE, 1.00), (TODAY, CODE, 1.05)],
        )
        with caplog.at_level(logging.WARNING):
            summary = _summary(_analyzer(db_path), _positions((CODE, "医药ETF", 1.05)))

        assert summary["daily_return"] == pytest.approx(5.0, abs=0.01)
        assert not any("[折算闸门]" in r.message for r in caplog.records)

    def test_stale_qfq_drops_only_that_code(self, tmp_path, caplog):
        """D：陈旧行情（>7 自然日）的折算标的必须被剔除，不得当成 1.0 稀释日收益。

        旧行为（缺陷）：512010 的 prev/curr 两次探测都命中 08-15 那一行，qfq_ratio
        恒为 1.0 -> daily_return = (2000×1.0 + 1000×1.05) / 3000 = 1.667%，
        折算跳变被悄悄替换成「伪造的 1.667% 收益」。
        新行为：512010 直接剔除 -> daily_return 只剩 510300 的 +5%。
        """
        db_path = _make_db(
            tmp_path,
            snapshots=[_snap(CODE, 2.0), _snap(FRESH_CODE, 1.0)],
            prev_total_value=3000.0,
            qfq_rows=[
                ("2026-08-15", CODE, 0.97),        # 距 PREV_DT 30 天 —— 陈旧
                (PREV_DT, FRESH_CODE, 1.00),       # 新鲜
                (TODAY, FRESH_CODE, 1.05),
            ],
        )
        with caplog.at_level(logging.WARNING):
            summary = _summary(_analyzer(db_path), _positions(
                (CODE, "医药ETF", 1.0), (FRESH_CODE, "300ETF", 1.05)))

        # 旧行为下这里会是 1.667（被 512010 的伪造 1.0 稀释）
        assert summary["daily_return"] == pytest.approx(5.0, abs=0.01), summary["daily_return"]
        assert any("[折算闸门]" in r.message and "陈旧" in r.message and "剔除" in r.message
                   for r in caplog.records), [r.message for r in caplog.records]


class TestQfqRatioStaleness:
    """_conversion_qfq_ratio 的陈旧/同源探测保护。"""

    @staticmethod
    def _ratio(db_path, code=CODE, prev_dt=PREV_DT):
        conn = sqlite3.connect(str(db_path))
        try:
            return _analyzer(db_path)._conversion_qfq_ratio(conn.cursor(), code, prev_dt)
        finally:
            conn.close()

    def test_stale_row_returns_none(self, tmp_path):
        """唯一一行距目标日 30 天（超 7 天）-> None，绝不返回 1.0。"""
        db_path = _make_db(
            tmp_path, snapshots=[_snap(CODE, 2.0)], prev_total_value=2000.0,
            qfq_rows=[("2026-08-15", CODE, 0.97)],
        )
        assert self._ratio(db_path) is None

    def test_same_row_hit_returns_none(self, tmp_path):
        """同一行同时满足 <= prev_dt 和 <= today（即使新鲜）-> 价比无意义 -> None。"""
        db_path = _make_db(
            tmp_path, snapshots=[_snap(CODE, 2.0)], prev_total_value=2000.0,
            qfq_rows=[("2026-09-13", CODE, 0.97)],   # 距 PREV_DT 1 天、距 TODAY 2 天
        )
        assert self._ratio(db_path) is None

    def test_fresh_distinct_rows_return_ratio(self, tmp_path):
        """两行都新鲜且不同日 -> 正常返回复权价比。"""
        db_path = _make_db(
            tmp_path, snapshots=[_snap(CODE, 2.0)], prev_total_value=2000.0,
            qfq_rows=[(PREV_DT, CODE, 1.00), (TODAY, CODE, 1.05)],
        )
        assert self._ratio(db_path) == pytest.approx(1.05, abs=1e-9)
        assert CONVERSION_QFQ_MAX_STALENESS_DAYS == 7
