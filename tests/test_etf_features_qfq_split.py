"""回归：etf_features 的 return/momentum/volatility 类特征须从前复权价算，
避免拆分日 current_price 合法跳变造成的伪 ±200% 收益（问题十二 A族）。

见 docs/handover/07_known_data_issues.md 问题十二。
- 有 qfq 覆盖：ret_1d 取真实日常收益（小值），非拆分跳变。
- 无 qfq 覆盖：该 (code,date) 置 NULL，绝不伪造。
"""
import os
import gc
import sqlite3
import tempfile
import pandas as pd

from src.utils.db_schema import init_all_tables
from src.analysis.predictor.features import build_feature_matrix


def _cleanup_db(path):
    """Python3.12+ sqlite3.connect 对同参做 LRU 缓存，close() 不立即释放句柄；
    gc.collect() 后重试删除。"""
    for _ in range(5):
        try:
            os.remove(path)
            return
        except PermissionError:
            gc.collect()
    try:
        os.remove(path)
    except OSError:
        pass


def _seed(conn):
    init_all_tables(conn)
    cur = conn.cursor()
    # TEST01: 拆分日 current_price 2.0->1.0，但有连续前复权价 1.00->1.01
    cur.executemany(
        "INSERT INTO portfolio_snapshots(date,code,name,quantity,cost_price,current_price,market_value) "
        "VALUES(?,?,?,?,?,?,?)",
        [("2024-01-02", "TEST01", "TEST01", 100, 2.0, 2.0, 200.0),
         ("2024-01-03", "TEST01", "TEST01", 100, 2.0, 1.0, 100.0)],
    )
    cur.executemany(
        "INSERT INTO etf_price_history(date,code,open,high,low,close,volume,amount,adj_close,source) "
        "VALUES(?,?,?,?,?,?,?,?,?,?)",
        [("2024-01-02", "TEST01", 1.0, 1.0, 1.0, 1.00, 0, 0.0, 1.00, "unit"),
         ("2024-01-03", "TEST01", 1.01, 1.01, 1.01, 1.01, 0, 0.0, 1.01, "unit")],
    )
    # TEST02: 同样拆分跳变，但无 etf_price_history -> 保留原始 ret（不覆盖、不置空）
    cur.executemany(
        "INSERT INTO portfolio_snapshots(date,code,name,quantity,cost_price,current_price,market_value) "
        "VALUES(?,?,?,?,?,?,?)",
        [("2024-01-02", "TEST02", "TEST02", 100, 2.0, 2.0, 200.0),
         ("2024-01-03", "TEST02", "TEST02", 100, 2.0, 1.0, 100.0)],
    )
    # TEST03: 有 etf_price_history 但拆分日当天缺行 -> qfq 对齐为 NaN -> ret_1d 置空
    cur.executemany(
        "INSERT INTO portfolio_snapshots(date,code,name,quantity,cost_price,current_price,market_value) "
        "VALUES(?,?,?,?,?,?,?)",
        [("2024-01-02", "TEST03", "TEST03", 100, 2.0, 2.0, 200.0),
         ("2024-01-03", "TEST03", "TEST03", 100, 2.0, 1.0, 100.0)],
    )
    cur.execute(
        "INSERT INTO etf_price_history(date,code,open,high,low,close,volume,amount,adj_close,source) "
        "VALUES(?,?,?,?,?,?,?,?,?,?)",
        ("2024-01-02", "TEST03", 1.0, 1.0, 1.0, 1.00, 0, 0.0, 1.00, "unit"),
    )
    conn.commit()


def test_qfq_split_ret_uses_adjusted_price():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    try:
        conn = sqlite3.connect(path)
        _seed(conn)
        conn.close()

        conn = sqlite3.connect(path)
        feat = build_feature_matrix(conn, ["TEST01", "TEST02", "TEST03"])
        conn.close()

        r01 = feat[(feat.code == "TEST01") & (feat.date == "2024-01-03")].iloc[0]
        # 真实前复权日收益 ≈ +1%，绝非 -50% 拆分跳变
        assert abs(r01["ret_1d"] - 0.01) < 0.05, f"TEST01 ret_1d 应为真实收益, got {r01['ret_1d']}"

        r02 = feat[(feat.code == "TEST02") & (feat.date == "2024-01-03")].iloc[0]
        # 无 qfq 覆盖 -> 保留原始 current_price 跳变（-50%）
        assert r02["ret_1d"] < -0.3, f"TEST02 无 qfq 应保留原始 ret, got {r02['ret_1d']}"

        r03 = feat[(feat.code == "TEST03") & (feat.date == "2024-01-03")].iloc[0]
        # 有 qfq 表但当日缺行 -> 置空（NaN）
        assert pd.isna(r03["ret_1d"]), f"TEST03 当日无 qfq 应置 NULL, got {r03['ret_1d']}"
    finally:
        _cleanup_db(path)
