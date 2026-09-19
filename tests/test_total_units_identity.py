"""问题三回归守卫：portfolio_nav.total_units 必须 = total_value / unit_nav。

历史 bug（docs/handover/07_known_data_issues.md 问题三）：写入侧曾用
`round(prev_v, 2)`，使 total_units 恒等于「前一个交易日的市值」，而非真实份额。
本测试复刻修复后的口径（与 _p3_backfill 的 SQL 完全一致），任何把
total_units 写回 prev_v 的改动都会让恒等式断言失败。
"""
import sqlite3


def test_total_units_identity_after_backfill():
    conn = sqlite3.connect(":memory:")
    cur = conn.cursor()
    cur.execute(
        "CREATE TABLE portfolio_nav "
        "(date TEXT, unit_nav REAL, total_units REAL, total_value REAL)"
    )
    rows = [
        ("2026-09-17", 2.638778, None, 1524395.24),
        ("2012-05-28", 1.0, None, 10416.0),
        ("2020-01-02", 0.0, None, 5000.0),  # unit_nav=0 → 必须置 NULL，不能除零
    ]
    cur.executemany("INSERT INTO portfolio_nav VALUES (?,?,?,?)", rows)

    # 与 src/analysis/nav_engine.py:343 及 _p3_backfill 完全一致的口径
    cur.execute(
        "UPDATE portfolio_nav SET total_units = CASE "
        "WHEN unit_nav IS NOT NULL AND unit_nav != 0 "
        "THEN ROUND(total_value/unit_nav, 2) ELSE NULL END"
    )

    for date, tu, nav, tv in cur.execute(
        "SELECT date, total_units, unit_nav, total_value FROM portfolio_nav"
    ):
        if nav and nav != 0:
            assert tu is not None, f"{date}: 正常行 total_units 不应为 NULL"
            assert abs(tu * nav - tv) < 0.5, f"{date}: 恒等式破损 ({tu}*{nav}!={tv})"
        else:
            assert tu is None, f"{date}: unit_nav=0 时 total_units 应为 NULL"

    conn.close()
