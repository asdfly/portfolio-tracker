"""问题十一 —— 复制行旁路表消费逻辑单元测试。

不依赖生产库：用内存 SQLite 构造最小场景，证明
``src.analysis.replica_void`` 的各消费封装在 void 行上行为正确：

- ``void_codes_on`` / ``is_replica_void``：正确识别当日 void 标的；
- ``void_excluded_common_codes``：共同持仓口径下剔除非观测端（daily_return 据此排除 void）；
- ``void_aware_total_value`` / ``effective_market_value``：void 行按最近非 void 观测向后结转，
  本库数据下 void 行已持有上次已知值，故结转非破坏性（数值不变）；
- ``snapshot_rows_with_void_flag``：为每只持仓附加 ``is_void`` 标志，供报告侧标脏。

该逻辑是 06-30 daily_return 由 +3.78% 修正为 +0.75% 的根因（见 docs/handover/07_known_data_issues.md 问题十一）。
"""
import sqlite3

import pytest

from src.analysis.replica_void import (
    effective_market_value,
    is_replica_void,
    snapshot_rows_with_void_flag,
    void_aware_total_value,
    void_codes_on,
    void_excluded_common_codes,
)


@pytest.fixture
def db():
    """最小场景：A 正常，B 在 06-30 成为 void（装的是 06-12 的陈旧值 200）。"""
    c = sqlite3.connect(':memory:')
    c.row_factory = sqlite3.Row
    c.execute("CREATE TABLE portfolio_snapshots (date TEXT, code TEXT, market_value REAL, name TEXT)")
    c.execute("CREATE TABLE portfolio_snapshots_replica_void (date TEXT, code TEXT)")
    rows = [
        ('2026-06-12', 'A', 100.0, 'Aname'),
        ('2026-06-12', 'B', 200.0, 'Bname'),
        ('2026-06-30', 'A', 110.0, 'Aname'),   # 当日有效观测
        ('2026-06-30', 'B', 200.0, 'Bname'),   # void：陈旧复制行（= 06-12 值）
    ]
    c.executemany("INSERT INTO portfolio_snapshots VALUES (?,?,?,?)", rows)
    c.execute("INSERT INTO portfolio_snapshots_replica_void VALUES ('2026-06-30','B')")
    c.commit()
    return c


def test_void_codes_on(db):
    assert void_codes_on(db, '2026-06-30') == {'B'}
    assert void_codes_on(db, '2026-06-12') == set()


def test_is_replica_void(db):
    assert is_replica_void(db, 'B', '2026-06-30') is True
    assert is_replica_void(db, 'A', '2026-06-30') is False


def test_void_excluded_common_codes():
    # 共同持仓 A,B,C；B 在 curr 端 void、C 在 prev 端 void → 仅 A 参与收益比较
    common = {'A', 'B', 'C'}
    assert void_excluded_common_codes(common, {'C'}, {'B'}) == {'A'}


def test_void_aware_total_value_non_destructive(db):
    # B 为 void 但自身已持有上次已知值（200），结转后仍为 200 → 总市值 = 110 + 200
    assert void_aware_total_value(db, '2026-06-30') == 310.0


def test_effective_market_value_carry_forward(db):
    # B 在 06-30 为 void，最近非 void 观测是 06-12 的 200
    assert effective_market_value(db, 'B', '2026-06-30') == 200.0
    # 非 void 标的直接返回自身市值
    assert effective_market_value(db, 'A', '2026-06-30') == 110.0


def test_snapshot_rows_with_void_flag(db):
    rows = snapshot_rows_with_void_flag(db, '2026-06-30')
    by_code = {r['code']: r for r in rows}
    assert by_code['B']['is_void'] is True
    assert by_code['A']['is_void'] is False
    assert by_code['B']['eff_market_value'] == 200.0
    assert by_code['A']['eff_market_value'] == 110.0
