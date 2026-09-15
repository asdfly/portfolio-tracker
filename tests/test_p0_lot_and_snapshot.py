"""P0-1 / P0-3 回归测试（内存库，确定性，不碰生产库）

P0-1：get_current_holdings 必须按 code 各取 `date <= 目标日期` 的最新一条，
      不能取「全局最新快照日期」那一批行（否则停更标的整批消失）。
P0-3：场内 ETF 的 shares 必须向下取整到 100 的整数倍（1 手 = 100 份），
      并同步给出 lots；场外基金按金额申购，不取整手。
"""
import sqlite3
import sys
from pathlib import Path

PROJECT_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_DIR))

from config.settings import ETF_LOT_SIZE, is_otc_fund                # noqa: E402
from src.analysis.rebalance_engine import (                          # noqa: E402
    RebalanceEngine, calc_trade_shares,
)
from src.models import RebalanceTrade                                # noqa: E402

AS_OF = "2026-08-07"          # 周五
STALE_DATE = "2026-06-26"     # 距 AS_OF 42 天

SCHEMA = """
CREATE TABLE portfolio_snapshots (
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
)
"""


def _snap(date, code, name, price, mv, beta=1.0):
    return (date, code, name, 1000, price, price, mv, 0.0, 0.0, 0.0, beta)


def _make_db():
    """510300 当天有快照；519770（场外）只在 42 天前的旧快照里出现。"""
    conn = sqlite3.connect(":memory:")
    conn.execute(SCHEMA)
    conn.executemany(
        "INSERT INTO portfolio_snapshots "
        "(date,code,name,quantity,cost_price,current_price,market_value,"
        "pnl,pnl_rate,ytd_return,beta) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        [
            _snap(AS_OF, "510300", "300ETF", 4.0, 40000.0),
            _snap(AS_OF, "512010", "医药ETF", 1.0, 10000.0),
            _snap(STALE_DATE, "519770", "交银优择回报混合A", 5.0, 50000.0, beta=0.8),
            _snap("2026-06-01", "519770", "交银优择回报混合A", 4.0, 40000.0),
        ],
    )
    conn.commit()
    return conn


class TestPerCodeLatestSnapshot:
    def test_stale_code_not_dropped(self):
        """旧逻辑只取全局最新日期，会把 519770 整只漏掉。"""
        eng = RebalanceEngine(_make_db())
        mv, total, names, prices = eng.get_current_holdings(AS_OF)
        assert set(mv) == {"510300", "512010", "519770"}, mv
        assert total == 40000.0 + 10000.0 + 50000.0
        # 519770 取的是它自己最新的那条（2026-06-26 / 50000），不是更早的 40000
        assert mv["519770"] == 50000.0

    def test_with_meta_returns_snapshot_dates_and_stale(self):
        eng = RebalanceEngine(_make_db())
        mv, total, names, prices, snap_dates, stale = eng.get_current_holdings(
            AS_OF, with_meta=True)
        assert snap_dates["510300"] == AS_OF
        assert snap_dates["519770"] == STALE_DATE
        assert [s["code"] for s in stale] == ["519770"]
        assert stale[0]["days"] == 42
        assert stale[0]["snapshot_date"] == STALE_DATE
        # 停更 >30 天升级为「疑似失效」，但**不静默丢弃**（仍在 mv 里）
        assert stale[0]["level"] == "疑似失效"
        assert "519770" in mv

    def test_delisted_code_excluded_from_holdings(self):
        """已清仓标的（159732）即使有残留快照，也不能进入持仓集合。"""
        conn = _make_db()
        conn.execute(
            "INSERT INTO portfolio_snapshots "
            "(date,code,name,quantity,cost_price,current_price,market_value,"
            "pnl,pnl_rate,ytd_return,beta) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            _snap("2026-07-30", "159732", "消费电子ETF华夏", 1.242, 6210.0),
        )
        conn.commit()
        mv, total, _, _, snap_dates, stale = RebalanceEngine(conn).get_current_holdings(
            AS_OF, with_meta=True)
        assert "159732" not in mv
        assert set(mv) == {"510300", "512010", "519770"}
        assert total == 100000.0          # 未把 6,210 元算进来
        assert "159732" not in snap_dates
        assert "159732" not in [s["code"] for s in stale]

    def test_fresh_only_has_no_stale(self):
        conn = sqlite3.connect(":memory:")
        conn.execute(SCHEMA)
        conn.executemany(
            "INSERT INTO portfolio_snapshots "
            "(date,code,name,quantity,cost_price,current_price,market_value,"
            "pnl,pnl_rate,ytd_return,beta) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            [_snap(AS_OF, "510300", "300ETF", 4.0, 4000.0)],
        )
        conn.commit()
        eng = RebalanceEngine(conn)
        _, _, _, _, _, stale = eng.get_current_holdings(AS_OF, with_meta=True)
        assert stale == []

    def test_backward_compatible_4_tuple(self):
        eng = RebalanceEngine(_make_db())
        out = eng.get_current_holdings(AS_OF)
        assert isinstance(out, tuple) and len(out) == 4
        weights, total, names, prices = eng.get_current_weights(AS_OF)
        assert abs(sum(weights.values()) - 1.0) < 1e-9

    def test_risk_metrics_beta_uses_same_snapshot(self):
        eng = RebalanceEngine(_make_db())
        m = eng.compute_risk_metrics(AS_OF)
        # 519770 的 beta=0.8 必须被计入（旧逻辑取当天快照，会退化成默认 1.0）
        assert m["portfolio_beta"] < 1.0


class TestLotRounding:
    def test_exchange_etf_rounds_down_to_lot(self):
        # 69607 / 0.372 = 187115 份 -> 187100 份 = 1871 手
        sh = calc_trade_shares(69607.0, 0.372, "512010")
        assert sh == 187100
        assert sh % ETF_LOT_SIZE == 0
        assert RebalanceTrade(code="512010", shares=sh).lots == 1871

    def test_small_trade_rounds_to_zero(self):
        assert calc_trade_shares(50.0, 1.0, "512010") == 0

    def test_otc_fund_not_rounded(self):
        assert is_otc_fund("519770")
        # 25216 / 5.9287 = 4253 份，场外按金额申购，保持 4253 不取整到 4200
        sh = calc_trade_shares(25216.0, 5.9287, "519770")
        assert sh == 4253
        t = RebalanceTrade(code="519770", shares=sh, lot_traded=False)
        assert t.lots == 0          # 场外无「手」概念

    def test_lots_always_derived_from_shares(self):
        t = RebalanceTrade(code="512010", shares=12345)
        assert t.lots == 123
        t.shares = 200
        t.__post_init__()
        assert t.lots == 2

    def test_stale_under_30_days_is_normal_level(self):
        conn = sqlite3.connect(":memory:")
        conn.execute(SCHEMA)
        conn.executemany(
            "INSERT INTO portfolio_snapshots "
            "(date,code,name,quantity,cost_price,current_price,market_value,"
            "pnl,pnl_rate,ytd_return,beta) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            [
                _snap(AS_OF, "510300", "300ETF", 4.0, 4000.0),
                _snap("2026-07-28", "512010", "医药ETF", 1.0, 1000.0),  # 停更 10 天
            ],
        )
        conn.commit()
        _, _, _, _, _, stale = RebalanceEngine(conn).get_current_holdings(
            AS_OF, with_meta=True)
        assert [(s["code"], s["days"], s["level"]) for s in stale] == [
            ("512010", 10, "陈旧")]

    def test_sub_lot_leg_dropped_and_reported(self):
        """金额不足 1 手（100×price）的腿必须丢弃，不能留下「有金额无份额」的建议。"""
        conn = sqlite3.connect(":memory:")
        conn.execute(SCHEMA)
        conn.executemany(
            "INSERT INTO portfolio_snapshots "
            "(date,code,name,quantity,cost_price,current_price,market_value,"
            "pnl,pnl_rate,ytd_return,beta) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            [
                _snap(AS_OF, "159650", "国开债ETF博时", 108.051, 54025.5),
                _snap(AS_OF, "512010", "医药ETF", 0.372, 111600.0),
            ],
        )
        conn.commit()
        eng = RebalanceEngine(conn)
        weights, _, _, _ = eng.get_current_weights(AS_OF)
        target = dict(weights)
        target["159650"] -= 0.0005          # ~83 元，不足 1 手（需 10,805 元）
        target["512010"] += 0.0005          # ~83 元 / 0.372 = 223 份 -> 200 份
        plan = eng.propose(AS_OF, target, threshold=0.0, force=True)
        assert [d["code"] for d in plan.dropped_legs] == ["159650"]
        assert all(t.code != "159650" for t in plan.trades)
        assert all(t.shares > 0 for t in plan.trades)
        # 被丢弃的腿不计入换手，避免换手率虚高
        assert plan.turnover < 0.0005

    def test_propose_trades_are_whole_lots(self):
        eng = RebalanceEngine(_make_db())
        plan = eng.propose_equal_weight(AS_OF)
        assert plan.action_needed
        for t in plan.trades:
            if t.code in ("510300", "512010"):
                assert t.shares % ETF_LOT_SIZE == 0
                assert t.lots == t.shares // ETF_LOT_SIZE
            else:  # 场外 519770
                assert t.lot_traded is False
                assert t.lots == 0


def _db_one_code(code, snap_date):
    """只含一只标的、快照停在 snap_date 的内存库。"""
    conn = sqlite3.connect(":memory:")
    conn.execute(SCHEMA)
    conn.execute(
        "INSERT INTO portfolio_snapshots "
        "(date,code,name,quantity,cost_price,current_price,market_value,"
        "pnl,pnl_rate,ytd_return,beta) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        _snap(snap_date, code, code, 1.0, 1000.0),
    )
    conn.commit()
    return conn


class TestStaleThresholdByDisclosureCadence:
    """陈旧阈值必须按披露节奏逐只取，不能一刀切用 7 天。"""

    @staticmethod
    def _stale_codes(code, snap_date):
        conn = _db_one_code(code, snap_date)
        # 目标日固定为 AS_OF，保证 days 可预期
        _, _, _, _, _, stale = RebalanceEngine(conn).get_current_holdings(
            AS_OF, with_meta=True)
        return stale

    def test_exchange_etf_uses_7_days(self):
        # 场内日更：停更 10 天应告警
        stale = self._stale_codes("510300", "2026-07-28")
        assert [(s["code"], s["days"]) for s in stale] == [("510300", 10)]

    def test_otc_monthly_disclosure_not_false_positive(self):
        # 场外只有月末导入：停更 20 天属正常节奏，7 天阈值会误报，35 天阈值不应报
        assert self._stale_codes("519770", "2026-07-18") == []

    def test_otc_missed_month_end_still_flagged(self):
        # 但整月漏跑（46 天）必须仍被抓到
        stale = self._stale_codes("519770", STALE_DATE)
        assert [s["code"] for s in stale] == ["519770"]
        assert stale[0]["days"] == 42
        assert stale[0]["level"] == "疑似失效"

    def test_weekly_nav_override(self):
        # 027293 净值每周五更新：停更 10 天正常；超过 14 天覆盖值才告警
        assert self._stale_codes("027293", "2026-07-28") == []      # 10 天
        stale = self._stale_codes("027293", "2026-07-18")           # 20 天
        assert [s["code"] for s in stale] == ["027293"]
        assert stale[0]["threshold"] == 14


def _db_with_features(vol_20d):
    """带 etf_features 的内存库（不建 etf_predictions 表，验证不再依赖 risk_lgb）。"""
    conn = sqlite3.connect(":memory:")
    conn.execute(SCHEMA)
    conn.executemany(
        "INSERT INTO portfolio_snapshots "
        "(date,code,name,quantity,cost_price,current_price,market_value,"
        "pnl,pnl_rate,ytd_return,beta) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        [_snap(AS_OF, "512010", "医药ETF", 1.0, 10000.0)],
    )
    conn.execute("CREATE TABLE etf_features (date TEXT, code TEXT, vol_20d REAL)")
    conn.execute("INSERT INTO etf_features VALUES (?,?,?)", (AS_OF, "512010", vol_20d))
    conn.commit()
    return conn


class TestVolWarningUsesRealizedVol:
    def test_reads_vol_20d_annualized(self):
        # 0.025 日波动 × √252 × 100 ≈ 39.7% > 30% -> 应告警
        m = RebalanceEngine(_db_with_features(0.025)).compute_risk_metrics(AS_OF)
        assert any("波动率预警" in w for w in m["warnings"])

    def test_no_warning_when_low_vol(self):
        # 0.010 日波动 ≈ 15.9% < 30% -> 不告警
        m = RebalanceEngine(_db_with_features(0.010)).compute_risk_metrics(AS_OF)
        assert not any("波动率预警" in w for w in m["warnings"])

    def test_does_not_require_etf_predictions(self):
        # 库里根本没有 etf_predictions 表，也不能抛异常（risk_lgb 已下线）
        m = RebalanceEngine(_db_with_features(0.025)).compute_risk_metrics(AS_OF)
        assert "warnings" in m
