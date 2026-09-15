"""P1-1 tests: advice_outcome attribution table + settlement (no look-ahead).

在隔离库上验证: 归因表 DDL、T+5/10/20 结算、未来函数红线(open->settled / skipped)。
不触碰生产库(遵循 conftest 的 DATABASE_PATH 隔离与 sqlite3 兜底改道)。
"""
import sqlite3
from datetime import date, timedelta


def _make_conn():
    conn = sqlite3.connect(":memory:")
    conn.execute("""
        CREATE TABLE advice_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            advice_type TEXT, priority TEXT, title TEXT, description TEXT,
            confidence REAL, related_codes TEXT, source TEXT, status TEXT DEFAULT 'pending',
            action_items TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE portfolio_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL, code TEXT NOT NULL, name TEXT,
            quantity REAL, cost_price REAL, current_price REAL,
            market_value REAL, pnl REAL, pnl_rate REAL, ytd_return REAL, beta REAL,
            UNIQUE(date, code)
        )
    """)
    conn.execute("""
        CREATE TABLE portfolio_summary (
            date TEXT PRIMARY KEY, total_value REAL, total_cost REAL, total_pnl REAL,
            daily_pnl REAL, daily_return REAL
        )
    """)
    return conn


def _seed_prices(conn, codes, start, days, drift=0.001):
    """插入连续日历日的快照价(价格随天数轻微漂移, 制造非零收益)。"""
    d = date.fromisoformat(start)
    for i in range(days):
        day = (d + timedelta(days=i)).isoformat()
        for j, code in enumerate(codes):
            price = round(1.0 + drift * i + 0.0001 * j, 6)
            conn.execute(
                "INSERT OR REPLACE INTO portfolio_snapshots "
                "(date, code, name, quantity, cost_price, current_price, market_value, pnl, pnl_rate, ytd_return, beta) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (day, code, code, 1, 1.0, price, price, 0, 0, 0, 1.0),
            )
        nav = round(100000.0 * (1.0 + drift * i), 2)
        conn.execute(
            "INSERT OR REPLACE INTO portfolio_summary (date, total_value, total_cost, total_pnl, daily_pnl, daily_return) "
            "VALUES (?,?,?,?,?,?)",
            (day, nav, 100000.0, 0, 0, 0),
        )
    conn.commit()


def test_advice_outcome_table_created():
    from src.utils.db_schema import ensure_advice_outcome_table
    conn = _make_conn()
    ensure_advice_outcome_table(conn)
    cols = [c[1] for c in conn.execute("PRAGMA table_info(advice_outcome)").fetchall()]
    for need in ("advice_id", "as_of_date", "settle_status", "fwd_return_5",
                 "fwd_return_10", "fwd_return_20", "bench_return_5",
                 "bench_return_10", "bench_return_20", "settle_date",
                 "calc_method", "notes"):
        assert need in cols
    idx = [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='advice_outcome'").fetchall()]
    assert "idx_ao_advice" in idx and "idx_ao_asof" in idx


def test_settlement_old_advice_settled_new_advice_skipped():
    from src.utils.db_schema import ensure_advice_outcome_table, ensure_advice_history_action_items_column
    from run_analysis import run_stage_advice_settle

    conn = _make_conn()
    # 价格区间 2026-04-20 ~ 2026-09-15(连续日历日)
    _seed_prices(conn, ["510300", "510500", "159915"], "2026-04-20", 149)

    # 老建议: 2026-04-25(远超 20 交易日) -> 全部 horizon 已过 -> settled
    conn.execute(
        "INSERT INTO advice_history (created_at, advice_type, priority, title, description, confidence, related_codes, source, status, action_items) "
        "VALUES ('2026-04-25 09:30:00','rebalance','high','再平衡','desc',0.8,'510300,510500','smart_report','pending','[\"降仓\"]')"
    )
    # 新建议: 2026-09-10(距运行日 2026-09-12 仅 2 交易日) -> T+5 尚未到来 -> skipped
    conn.execute(
        "INSERT INTO advice_history (created_at, advice_type, priority, title, description, confidence, related_codes, source, status, action_items) "
        "VALUES ('2026-09-10 09:30:00','risk','medium','风控','desc',0.6,'159915','smart_report','pending','[\"关注\"]')"
    )
    conn.commit()

    as_of_date = "2026-09-12"
    run_stage_advice_settle(conn, as_of_date)

    rows = conn.execute(
        "SELECT advice_id, settle_status, fwd_return_5, fwd_return_10, fwd_return_20, "
        "bench_return_5, bench_return_20 FROM advice_outcome ORDER BY advice_id"
    ).fetchall()
    assert len(rows) == 2

    old = rows[0]
    assert old[1] == 'settled'
    # 老建议三 horizon 收益均非 None(实际有漂移, 应非零)
    assert old[2] is not None and old[3] is not None and old[4] is not None
    assert old[5] is not None and old[6] is not None  # bench

    new = rows[1]
    assert new[1] == 'skipped'
    # 新建议因未来函数红线, forward return 全为 NULL(绝不回看未来)
    assert new[2] is None and new[3] is None and new[4] is None


def test_legs_forward_return_helper():
    """_calc_legs_forward_return: 区间内返回非 None; 无数据(早于快照)返回 None。"""
    from run_analysis import _calc_legs_forward_return
    conn = _make_conn()
    _seed_prices(conn, ["510300"], "2026-09-01", 20)  # 到 09-20
    # 存在的 exit 09-10 -> 有值(就近取 <=exit_date 的快照, 符合"不回看"定义)
    assert _calc_legs_forward_return(conn, ["510300"], "2026-09-05", "2026-09-10") is not None
    # entry/exit 均早于快照起始(09-01) -> 无可用价 -> None(不臆造)
    assert _calc_legs_forward_return(conn, ["510300"], "2026-08-10", "2026-08-20") is None


def test_action_items_persisted_on_insert(db_connection):
    """smart_report 生成建议时, action_items 应落入 advice_history(幂等列存在)。

    注意: generate_full_report 内部走自身 get_db_connection()(=DATABASE_PATH 隔离副本),
    与 db_connection fixture 指向同一临时副本, 故可直接查询该连接。
    """
    from src.report.smart_report import SmartReportGenerator
    from src.utils.db_schema import ensure_advice_history_action_items_column
    ensure_advice_history_action_items_column(db_connection)
    gen = SmartReportGenerator(db_connection)
    gen.generate_full_report({
        'summary': {'total_value': 100000, 'total_pnl': 5000},
        'risk': {'portfolio_metrics': {
            'risk_adjusted_metrics': {'sharpe_ratio': 0.3},
            'drawdown_metrics': {'max_drawdown': 0.05},
            'volatility_metrics': {'annual_volatility': 0.15},
        }},
        'technical': {},
    })
    rows = db_connection.execute(
        "SELECT action_items FROM advice_history "
        "WHERE source='smart_report' AND action_items IS NOT NULL AND action_items != '[]'"
    ).fetchall()
    assert len(rows) > 0
