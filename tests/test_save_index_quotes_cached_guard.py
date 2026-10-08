"""测试 save_index_quotes 对缓存兜底（_cached）行情的占位写入防护。

根因（2026-10-08）：主管线 ``_fetch_index_quotes`` 在实时取数失败时回退 DB 缓存，
返回带 ``_cached=True`` 的 dict。若该 dict 被 ``save_index_quotes(self.today, …)`` 落库，
会写入一条「今日日期 + 陈旧收盘价 + change_pct=None」的占位行，污染 MAX(date)
并伪装成今日收盘价（下游 gen_combo_report / enhanced_report 据此选「报告日」）。
本测试验证 write path 拒绝写入 ``_cached`` 行情，同时正常写入真实实时行情。
"""
import os
import sqlite3

from src.utils.database import DatabaseManager


def _rows(db_path):
    conn = sqlite3.connect(db_path)
    try:
        return conn.execute(
            "SELECT code, date, close, change_pct FROM index_quotes ORDER BY code"
        ).fetchall()
    finally:
        conn.close()


def _safe_remove(db_path):
    # Windows + WAL 会在主库旁生成 -wal/-shm 侧车文件并短时持锁；
    # 先 checkpoint 折叠回主库，再尽力删除（忽略残留锁错误）。
    try:
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.close()
    except sqlite3.Error:
        pass
    for p in (db_path, db_path + "-wal", db_path + "-shm", db_path + "-journal"):
        try:
            os.unlink(p)
        except OSError:
            pass


def test_save_index_quotes_skips_cached_but_keeps_real():
    import tempfile
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    db_path = tmp.name
    try:
        dm = DatabaseManager(db_path=db_path)

        real = {"name": "沪深300", "price": 4000.0, "change_pct": 1.23,
                "volume": 1e9, "amount": 2e9}
        cached = {"name": "中证2000", "price": 3107.26, "change_pct": None,
                  "volume": 2.6e10, "amount": 3372.38, "_cached": True,
                  "_cached_date": "2026-09-30"}

        dm.save_index_quotes("2026-10-08", {"sh000300": real, "sh932000": cached})

        rows = _rows(db_path)
        codes = {r[0] for r in rows}
        assert "sh000300" in codes, "真实实时行情应被写入"
        assert "sh932000" not in codes, "缓存兜底行情不应落库（防占位行 bug）"

        row300 = next(r for r in rows if r[0] == "sh000300")
        assert row300[1] == "2026-10-08"
        assert row300[2] == 4000.0
        assert row300[3] == 1.23
    finally:
        _safe_remove(db_path)


def test_save_index_quotes_writes_when_no_cached_flag():
    """不带 _cached 标记的 dict（如缓存日期恰为今日的真实写入）仍正常落库。"""
    import tempfile
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    db_path = tmp.name
    try:
        dm = DatabaseManager(db_path=db_path)
        quote = {"name": "中证2000", "price": 3107.26, "change_pct": -0.69,
                 "volume": 2.6e10, "amount": 3372.38}
        dm.save_index_quotes("2026-09-30", {"sh932000": quote})
        rows = _rows(db_path)
        assert "sh932000" in {r[0] for r in rows}
    finally:
        _safe_remove(db_path)
