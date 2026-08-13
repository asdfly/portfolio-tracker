"""P2-B 再平衡引擎单元测试（内存库，确定性）"""
import sys
import sqlite3
from pathlib import Path

PROJECT_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_DIR))

from src.analysis.rebalance_engine import (
    RebalanceEngine,
    compute_rebalance_suggestion,
)
from src.utils.trading_calendar import next_trading_day, is_trading_day


def _make_db():
    """构造内存库，写入 3 个标的、as_of=2026-08-07 的持仓快照。"""
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """CREATE TABLE portfolio_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            code TEXT NOT NULL,
            name TEXT,
            quantity REAL,
            cost_price REAL,
            current_price REAL,
            market_value REAL,
            pnl REAL,
            pnl_rate REAL,
            ytd_return REAL,
            beta REAL,
            UNIQUE(date, code)
        )"""
    )
    rows = [
        # 510300 权重 ~70%（明显超配）, 588000 ~19%, 512010 ~11%
        ("2026-08-07", "510300", "300ETF", 1000, 3.8, 4.0, 4000.0, 0.0, 0.0, 0.0, 1.0),
        ("2026-08-07", "588000", "科创50ETF", 1000, 1.0, 1.1, 1100.0, 0.0, 0.0, 0.0, 1.0),
        ("2026-08-07", "512010", "医药ETF", 1000, 0.5, 0.6, 600.0, 0.0, 0.0, 0.0, 1.0),
    ]
    conn.executemany(
        "INSERT INTO portfolio_snapshots "
        "(date,code,name,quantity,cost_price,current_price,market_value,pnl,pnl_rate,ytd_return,beta) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        rows,
    )
    conn.commit()
    return conn


AS_OF = "2026-08-07"


def _make_layered_db():
    """构造内存库：宽基 10% / 医药 90%（类别内市值占比均衡）。

    用于锁定 P0 修复：大类严重偏离时，即便「类别内市值占比」已与分层目标一致
    （单标偏离被稀释），也必须因「类别偏离」触发再平衡。
    """
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """CREATE TABLE portfolio_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            code TEXT NOT NULL,
            name TEXT,
            quantity REAL,
            cost_price REAL,
            current_price REAL,
            market_value REAL,
            pnl REAL,
            pnl_rate REAL,
            ytd_return REAL,
            beta REAL,
            UNIQUE(date, code)
        )"""
    )
    # 宽基两只各 1000（合计 2000 / 20000 = 10%），医药两只各 9000（合计 18000 = 90%）
    rows = [
        ("2026-08-07", "510300", "300ETF", 1000, 1.0, 1.0, 1000.0, 0.0, 0.0, 0.0, 1.0),
        ("2026-08-07", "588000", "科创50ETF", 1000, 1.0, 1.0, 1000.0, 0.0, 0.0, 0.0, 1.0),
        ("2026-08-07", "512010", "医药ETF", 9000, 1.0, 1.0, 9000.0, 0.0, 0.0, 0.0, 1.0),
        ("2026-08-07", "159992", "医药ETF", 9000, 1.0, 1.0, 9000.0, 0.0, 0.0, 0.0, 1.0),
    ]
    conn.executemany(
        "INSERT INTO portfolio_snapshots "
        "(date,code,name,quantity,cost_price,current_price,market_value,pnl,pnl_rate,ytd_return,beta) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        rows,
    )
    conn.commit()
    return conn


class TestCurrentWeights:
    def test_weights_sum_to_one(self):
        conn = _make_db()
        eng = RebalanceEngine(conn)
        weights, total, names, prices = eng.get_current_weights(AS_OF)
        assert abs(sum(weights.values()) - 1.0) < 1e-9
        assert total == 5700.0
        assert weights["510300"] > weights["512010"]
        assert prices["510300"] == 4.0

    def test_falls_back_to_latest_snapshot(self):
        conn = _make_db()
        eng = RebalanceEngine(conn)
        # 周六查询 -> 回退到最近交易日 2026-08-07
        weights, _, _, _ = eng.get_current_weights("2026-08-08")
        assert "510300" in weights


class TestProposeEqualWeight:
    def test_triggers_when_concentrated(self):
        conn = _make_db()
        plan = compute_rebalance_suggestion(conn, as_of_date=AS_OF, strategy="equal_weight")
        assert plan.action_needed is True
        assert plan.strategy == "equal_weight"
        # 超配的 510300 应卖出，低配的应买入
        by_code = {t.code: t for t in plan.trades}
        assert by_code["510300"].direction == "卖出"
        assert by_code["588000"].direction == "买入"
        assert by_code["512010"].direction == "买入"
        assert len(plan.trades) == 3

    def test_no_action_when_already_equal(self):
        conn = _make_db()
        eng = RebalanceEngine(conn)
        weights, _, _, _ = eng.get_current_weights(AS_OF)
        # 目标=当前 -> 偏离=0 -> 不调仓
        plan = eng.propose(AS_OF, target_weights=weights, threshold=0.05)
        assert plan.action_needed is False

    def test_execution_date_is_trading_day(self):
        conn = _make_db()
        plan = compute_rebalance_suggestion(conn, as_of_date=AS_OF, strategy="equal_weight")
        assert plan.execution_date == str(next_trading_day(AS_OF))
        assert is_trading_day(plan.execution_date)


class TestCostModel:
    def test_cost_matches_backtest_formula(self):
        conn = _make_db()
        eng = RebalanceEngine(conn, commission_rate=0.0003, slippage_rate=0.0005)
        plan = eng.propose_equal_weight(AS_OF)
        # 成本公式与 backtest 一致: 2 * 单边cost_rate * 换手率 * 总市值
        # estimated_cost 已按 2 位小数四舍五入，对比时也四舍五入
        expected_cost = 2 * eng.cost_rate * plan.turnover * plan.total_value
        assert abs(plan.estimated_cost - round(expected_cost, 2)) < 1e-6
        # 数值 sanity（集中组合再平衡到等权，成本应为小额正数）
        assert 0 < plan.estimated_cost < 50


class TestPeriodic:
    def test_due_when_long_gap(self):
        conn = _make_db()
        plan = compute_rebalance_suggestion(
            conn, as_of_date=AS_OF, strategy="periodic",
            period_days=20, last_rebalance_date="2026-01-05",
        )
        assert plan.action_needed is True
        assert plan.strategy == "periodic"

    def test_not_due_when_recent(self):
        conn = _make_db()
        plan = compute_rebalance_suggestion(
            conn, as_of_date=AS_OF, strategy="periodic",
            period_days=20, last_rebalance_date="2026-08-06",
        )
        assert plan.action_needed is False
        assert "未到" in plan.reason

    def test_no_history_treated_as_due(self):
        conn = _make_db()
        plan = compute_rebalance_suggestion(
            conn, as_of_date=AS_OF, strategy="periodic",
            period_days=20, last_rebalance_date=None,
        )
        assert plan.action_needed is True


class TestCustomTarget:
    def test_custom_requires_target(self):
        import pytest
        conn = _make_db()
        with pytest.raises(ValueError):
            compute_rebalance_suggestion(conn, as_of_date=AS_OF, strategy="custom")

    def test_custom_with_target(self):
        conn = _make_db()
        target = {"510300": 0.5, "588000": 0.3, "512010": 0.2}
        plan = compute_rebalance_suggestion(
            conn, as_of_date=AS_OF, strategy="custom", target_weights=target,
        )
        assert plan.action_needed is True
        # 510300 当前0.70 -> 目标0.5 仍超配, 卖出
        by_code = {t.code: t for t in plan.trades}
        assert by_code["510300"].direction == "卖出"


class TestLayered:
    """分层再平衡（不要完全平均）的回归锁。

    需求：再平衡建议不应把所有标的拉向 1/n 等权，而应按类别基准分层。
    """

    def test_classify_sector_uses_config(self):
        from src.analysis.rebalance_engine import RebalanceEngine
        conn = _make_db()
        eng = RebalanceEngine(conn)
        # 510300 -> 宽基, 588000 -> 宽基, 512010 -> 医药（来自 ETF_CATEGORIES）
        assert eng.classify_sector("510300") == "宽基"
        assert eng.classify_sector("588000") == "宽基"
        assert eng.classify_sector("512010") == "医药"

    def test_layered_targets_are_differentiated(self):
        """分层目标权重必须分化（极差>0），不能是等权。"""
        conn = _make_db()
        plan = compute_rebalance_suggestion(conn, as_of_date=AS_OF, strategy="layered")
        tw = plan.target_weights
        assert abs(sum(tw.values()) - 1.0) < 1e-9
        vals = list(tw.values())
        spread = max(vals) - min(vals)
        # 等权时极差=0；分层必须 > 0，且本用例下宽基明显重仓
        assert spread > 0.05, f"分层目标未分化, 极差={spread}"
        # 宽基类（510300）目标应高于医药类（512010）
        assert tw["510300"] > tw["512010"]

    def test_layered_not_equal_to_equal_weight(self):
        """同一组合，分层目标与等权目标应不同。"""
        conn = _make_db()
        ly = compute_rebalance_suggestion(conn, as_of_date=AS_OF, strategy="layered")
        eq = compute_rebalance_suggestion(conn, as_of_date=AS_OF, strategy="equal_weight")
        # 等权三标的都 ≈ 0.333
        assert max(eq.target_weights.values()) - min(eq.target_weights.values()) < 1e-6
        # 分层则明显不均
        assert max(ly.target_weights.values()) - min(ly.target_weights.values()) > 0.05


class TestLayeredTrigger:
    """P0 回归锁：类别严重偏离时必须触发再平衡（不因单标偏离被稀释而不触发）。

    场景：宽基 10% / 医药 90%，且类别内市值占比已与分层目标一致（单标偏离≈0）。
    旧版因 shrinkage=0.5 把目标拉向当前 + 只看单标偏离，导致大类偏离 24% 被判无需调仓；
    修复后由「类别偏离 > SECTOR_DEVIATION_THRESHOLD」触发。
    """

    def test_category_deviation_triggers_rebalance(self):
        conn = _make_layered_db()
        plan = compute_rebalance_suggestion(conn, as_of_date=AS_OF, strategy="layered")
        assert plan.action_needed is True, "类别严重偏离必须触发再平衡"
        assert "类别偏离" in plan.reason, f"reason 应说明类别偏离触发: {plan.reason}"
        by_code = {t.code: t for t in plan.trades}
        # 宽基被低配 -> 应买入；医药被超配 -> 应卖出
        assert by_code["510300"].direction == "买入"
        assert by_code["588000"].direction == "买入"
        assert by_code["512010"].direction == "卖出"
        assert by_code["159992"].direction == "卖出"

    def test_no_trigger_when_single_sector_on_target(self):
        """单一类别、且已在该类别战略目标内时，不应触发。"""
        conn = sqlite3.connect(":memory:")
        conn.execute(
            """CREATE TABLE portfolio_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT NOT NULL, code TEXT NOT NULL,
                name TEXT, quantity REAL, cost_price REAL, current_price REAL, market_value REAL,
                pnl REAL, pnl_rate REAL, ytd_return REAL, beta REAL, UNIQUE(date, code))"""
        )
        conn.execute(
            "INSERT INTO portfolio_snapshots "
            "(date,code,name,quantity,cost_price,current_price,market_value,pnl,pnl_rate,ytd_return,beta) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            ("2026-08-07", "510300", "300ETF", 1000, 1.0, 1.0, 1000.0, 0.0, 0.0, 0.0, 1.0),
        )
        conn.commit()
        plan = compute_rebalance_suggestion(conn, as_of_date=AS_OF, strategy="layered")
        assert plan.action_needed is False
        conn.close()
