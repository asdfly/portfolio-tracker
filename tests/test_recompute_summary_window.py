# -*- coding: utf-8 -*-
"""`scripts/recompute_summary_window.py` 缺口日修复的回归测试。

背景（2026-09-16，任务 #59 第 5 项）
------------------------------------
`portfolio_summary` 全历史有 2 个"只有快照、没有汇总行"的日期
（`2026-09-03`、`2026-09-15`）。`src/analysis/portfolio.py:482-488` 的 `prev_dt`
取自 `portfolio_summary` 自己，所以缺行会让**下一个交易日**的 `daily_return`
变成多日值却贴着单日标签。实测 09-04 存的就是 09-02→09-04 的两日链
（`(1+0.166128%)(1-0.582187%)-1 = -0.417026284%`，与现库差 1.16e-07 pp）。

而修复它所用的 `scripts/recompute_summary_window.py` 原先自身有两个缺陷：

1. `:70-72` 日期清单取自 `portfolio_summary` 自己 ⇒ **缺口日永远进不了清单**
   （实测窗口 2026-09-03~2026-09-16 只覆盖 7 天，漏掉 09-03/09-15）。
2. `:197-203` 打印块 `o = old[dt]` 假定每个日期都有旧行 ⇒ 窗口一旦含新增日期，
   就以 `TypeError: unsupported format string passed to NoneType.__format__`
   在**写库之前** `rc=1` 退出。

本文件覆盖修复后的三件事：① 日期源包含缺口日；② 反方向（只有汇总行）显式报出、
不静默丢；③ 新增日期渲染不崩且可见。全部使用合成库，**不触碰生产库**。
"""
import importlib.util
import re
import sqlite3
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT_PATH = REPO_ROOT / "scripts" / "recompute_summary_window.py"

spec = importlib.util.spec_from_file_location("recompute_summary_window_under_test", SCRIPT_PATH)
rsw = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rsw)


# ---------------------------------------------------------------------------
# 合成库
# ---------------------------------------------------------------------------
_SUMMARY_DDL = """
CREATE TABLE portfolio_summary (
    date TEXT PRIMARY KEY,
    total_value REAL, total_cost REAL, total_pnl REAL,
    daily_pnl REAL, daily_return REAL, vs_hs300 REAL,
    profit_count INTEGER, loss_count INTEGER,
    sharpe_ratio REAL, max_drawdown REAL, volatility REAL,
    snapshot_type TEXT
);
"""
_SNAPSHOT_DDL = """
CREATE TABLE portfolio_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT, code TEXT, name TEXT,
    quantity REAL, cost_price REAL, current_price REAL, market_value REAL,
    pnl REAL, pnl_rate REAL, ytd_return REAL, beta REAL
);
"""
_QUOTES_DDL = """
CREATE TABLE index_quotes (
    date TEXT, code TEXT, name TEXT, close REAL, change_pct REAL,
    volume REAL, amount REAL
);
"""


def _build_db(path, snapshots, summary_dates, quotes=()):
    """建一个最小合成库。

    :param snapshots: [(date, code, quantity, current_price)] —— market_value 由
                      quantity*current_price 推出，保证与脚本口径自洽。
    :param summary_dates: 需要预置汇总行的日期（其余列为占位值）。
    :param quotes: [(date, change_pct)]，写进 index_quotes 的 sh000300。
    """
    conn = sqlite3.connect(str(path))
    conn.executescript(_SUMMARY_DDL + _SNAPSHOT_DDL + _QUOTES_DDL)
    for d, code, qty, price in snapshots:
        conn.execute(
            "INSERT INTO portfolio_snapshots "
            "(date, code, name, quantity, cost_price, current_price, market_value, pnl) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (d, code, code, qty, price, price, qty * price, 0.0),
        )
    for d in summary_dates:
        conn.execute(
            "INSERT INTO portfolio_summary (date, total_value, daily_return, snapshot_type) "
            "VALUES (?,?,?,?)", (d, 1.0, 0.0, "daily"))
    for d, chg in quotes:
        conn.execute(
            "INSERT INTO index_quotes (date, code, name, close, change_pct) "
            "VALUES (?,?,?,?,?)", (d, "sh000300", "沪深300", 3500.0, chg))
    conn.commit()
    conn.close()
    return str(path)


# 三天的价格：09-03 相对 09-02 +1%，09-04 相对 09-03 再 +1%
# ⇒ 真单日 = +1.00%；两日链 = (1.01)(1.01)-1 = +2.01%（差异足以区分）
_SNAPS_GAP = [
    ("2026-09-02", "AAA", 100.0, 10.0),
    ("2026-09-02", "BBB", 100.0, 20.0),
    ("2026-09-03", "AAA", 100.0, 10.1),
    ("2026-09-03", "BBB", 100.0, 20.2),
    ("2026-09-04", "AAA", 100.0, 10.201),
    ("2026-09-04", "BBB", 100.0, 20.402),
]
_QUOTES = [("2026-09-03", -0.10), ("2026-09-04", 0.05)]


@pytest.fixture
def gap_db(tmp_path):
    """09-03 是缺口日（有快照、无汇总行）；09-02 与 09-04 有汇总行。"""
    return _build_db(tmp_path / "gap.db", _SNAPS_GAP,
                     summary_dates=["2026-09-02", "2026-09-04"], quotes=_QUOTES)


@pytest.fixture
def gap_db_no_snapshot(tmp_path):
    """对照组：把 09-03 的快照也拿掉 ⇒ 复现"缺行 ⇒ 下一日变多日"的原始缺陷。"""
    snaps = [s for s in _SNAPS_GAP if s[0] != "2026-09-03"]
    return _build_db(tmp_path / "gap_no_snap.db", snaps,
                     summary_dates=["2026-09-02", "2026-09-04"], quotes=_QUOTES)


def _conn(path):
    return sqlite3.connect(path)


# ---------------------------------------------------------------------------
# ① 日期源
# ---------------------------------------------------------------------------
class TestResolveDates:
    def test_includes_snapshot_only_date(self, gap_db):
        conn = _conn(gap_db)
        dates, snaps_only, sums_only = rsw.resolve_dates(conn.cursor(), "2026-09-03", "2026-09-04")
        conn.close()
        assert "2026-09-03" in dates, "缺口日必须出现在重算清单里（修复前它进不来）"
        assert snaps_only == ["2026-09-03"]
        assert sums_only == []
        assert dates == ["2026-09-03", "2026-09-04"]

    def test_reports_summary_only_date_instead_of_dropping(self, tmp_path):
        """反方向（有汇总行、无快照）必须显式报出，不能静默丢。"""
        path = _build_db(tmp_path / "sum_only.db",
                         [("2026-09-02", "AAA", 100.0, 10.0)],
                         summary_dates=["2026-09-02", "2026-09-04"])
        conn = _conn(path)
        dates, snaps_only, sums_only = rsw.resolve_dates(conn.cursor(), "2026-09-02", "2026-09-04")
        conn.close()
        assert sums_only == ["2026-09-04"]
        assert snaps_only == []
        # 并集语义：它仍在 dates 里（由 compute 因缺快照而跳过），不是凭空消失
        assert "2026-09-04" in dates

    def test_excludes_dates_outside_window(self, gap_db):
        conn = _conn(gap_db)
        dates, _, _ = rsw.resolve_dates(conn.cursor(), "2026-09-03", "2026-09-03")
        conn.close()
        assert dates == ["2026-09-03"]

    def test_snapshot_only_date_is_warned_and_skipped(self, tmp_path, capsys):
        """只有汇总行、没有快照的日期：算不出 12 列 ⇒ 跳过，但必须打印警告。"""
        path = _build_db(tmp_path / "sum_only2.db",
                         [("2026-09-02", "AAA", 100.0, 10.0)],
                         summary_dates=["2026-09-02", "2026-09-04"])
        conn = _conn(path)
        res = rsw.compute(conn, "2026-09-02", "2026-09-04")
        conn.close()
        out = capsys.readouterr().out
        assert "只有汇总行、没有快照" in out
        assert "2026-09-04" in out
        assert "2026-09-04" not in res, "无快照的日期不能被写进结果"


# ---------------------------------------------------------------------------
# ② 缺口日被补上，且"多日值"确实被修正为单日值
# ---------------------------------------------------------------------------
class TestComputeHealsGapDay:
    def test_gap_day_is_computed(self, gap_db, capsys):
        conn = _conn(gap_db)
        res = rsw.compute(conn, "2026-09-03", "2026-09-04")
        conn.close()
        assert set(res) == {"2026-09-03", "2026-09-04"}
        assert "将补算" in capsys.readouterr().out

    def test_day_after_gap_is_single_day_not_two_day(self, gap_db):
        """核心断言：缺口补上后，09-04 应是 +1.00% 单日，而不是 +2.01% 两日链。"""
        conn = _conn(gap_db)
        res = rsw.compute(conn, "2026-09-03", "2026-09-04")
        conn.close()
        assert abs(res["2026-09-03"]["daily_return"] - 1.0) < 1e-9
        assert abs(res["2026-09-04"]["daily_return"] - 1.0) < 1e-9, (
            "09-04 变成了多日值：缺口没有被真正利用起来")
        assert abs(res["2026-09-04"]["daily_pnl"] - 30.3) < 1e-9

    def test_control_without_gap_snapshot_reproduces_two_day_value(self, gap_db_no_snapshot):
        """对照组：把 09-03 快照拿掉，09-04 立刻退化成 +2.01% —— 缺陷机理的可执行证据。"""
        conn = _conn(gap_db_no_snapshot)
        res = rsw.compute(conn, "2026-09-03", "2026-09-04")
        conn.close()
        assert set(res) == {"2026-09-04"}
        assert abs(res["2026-09-04"]["daily_return"] - 2.01) < 1e-9, (
            "对照组的预期是两日链 +2.01%")
        assert abs(res["2026-09-04"]["daily_return"] - 1.0) > 0.9, (
            "对照组不该等于单日值，否则本测试失去区分力")

    def test_vs_hs300_uses_same_day_change_pct(self, gap_db):
        conn = _conn(gap_db)
        res = rsw.compute(conn, "2026-09-03", "2026-09-04")
        conn.close()
        assert abs(res["2026-09-03"]["vs_hs300"] - (1.0 - (-0.10))) < 1e-9
        assert abs(res["2026-09-04"]["vs_hs300"] - (1.0 - 0.05)) < 1e-9


# ---------------------------------------------------------------------------
# ③ 对照表渲染：新增日期不崩、且可见
# ---------------------------------------------------------------------------
_FAKE_COMPUTED = {
    "2026-09-03": dict(total_value=1528186.86, daily_return=0.16612753822948226,
                       sharpe_ratio=1.0854, max_drawdown=6.720171323433938,
                       volatility=22.5584, profit_count=19, loss_count=15),
    "2026-09-04": dict(total_value=1561001.04, daily_return=-0.5821866501325468,
                       sharpe_ratio=1.514, max_drawdown=6.720171323433867,
                       volatility=22.1339, profit_count=20, loss_count=15),
}


class TestFormatDiffTable:
    def test_new_date_does_not_crash(self):
        """修复前的 TypeError: unsupported format string passed to NoneType.__format__"""
        lines, new_dates = rsw.format_diff_table(_FAKE_COMPUTED, {})
        assert new_dates == ["2026-09-03", "2026-09-04"]
        assert len(lines) == 3  # 表头 + 2 行

    def test_new_date_is_visible_in_output(self):
        lines, new_dates = rsw.format_diff_table(_FAKE_COMPUTED, {})
        row = [l for l in lines if l.startswith("2026-09-03")][0]
        assert "<无行>" in row, "新增日期的旧列应显式渲染为 <无行>"
        assert "NEW" in row, "新增日期必须在行尾有标记，不能被静默吞掉"

    def test_existing_row_has_no_new_marker(self):
        old = {"2026-09-04": (None, 1561001.04, -0.4170264005939382, None,
                              20, 15, 1.0854, 6.72, 22.5584)}
        computed = {"2026-09-04": _FAKE_COMPUTED["2026-09-04"]}
        lines, new_dates = rsw.format_diff_table(computed, old)
        assert new_dates == []
        row = lines[1]
        assert "NEW" not in row
        assert "<无行>" not in row
        assert "1,561,001" in row
        assert "-0.417" in row and "-0.582" in row

    def test_null_column_renders_as_none_not_missing_row(self):
        """行存在但该列为 NULL ⇒ 'None'；整行不存在 ⇒ '<无行>'。两者不能混淆。"""
        old = {"2026-09-04": (None, 1561001.04, -0.4170264005939382, None,
                              20, 15, None, None, None)}
        computed = {"2026-09-04": _FAKE_COMPUTED["2026-09-04"]}
        lines, _ = rsw.format_diff_table(computed, old)
        assert "<无行>" not in lines[1]
        assert "None" in lines[1]

    def test_all_cells_have_fixed_width(self):
        """列宽固定：每行长度一致，避免表格错位。"""
        lines, _ = rsw.format_diff_table(_FAKE_COMPUTED, {})
        assert len({len(l) for l in lines}) == 1


# ---------------------------------------------------------------------------
# ④ 备份落点：必须进 data/backups/，不得污染 data/database/
# ---------------------------------------------------------------------------
class TestBackupLanding:
    """仓库约定：`data/database/` 只允许有 `portfolio.db`（防 worker 连错库）。

    原实现把备份写成 `f"{db_path}.bak_recompute_{stamp}"`，备份直接躺在库旁边 ——
    一次 `--backup` 就破坏该约定。这几条用例把约定钉死。
    """

    @staticmethod
    def _prod_like(tmp_path):
        """造一个和真实布局同形的最小库：<tmp>/database/portfolio.db。"""
        dbdir = tmp_path / "database"
        dbdir.mkdir()
        return dbdir, _build_db(dbdir / "portfolio.db",
                                [("2026-09-02", "AAA", 100.0, 10.0)],
                                summary_dates=["2026-09-02"])

    def test_backup_lands_in_backup_dir_not_next_to_db(self, tmp_path, monkeypatch, capsys):
        dbdir, src = self._prod_like(tmp_path)
        backup_dir = tmp_path / "backups"
        monkeypatch.setattr(rsw, "BACKUP_DIR", backup_dir)

        dst = rsw.backup(src)
        out = capsys.readouterr().out

        assert Path(dst).exists()
        assert Path(dst).parent == backup_dir, "备份必须落在 BACKUP_DIR 下"
        assert Path(dst).parent != dbdir, "备份不得留在数据库同目录"
        assert Path(dst).stat().st_size == Path(src).stat().st_size
        assert str(backup_dir) in out, "备份路径必须打印出来，便于回填审计要素"
        assert sorted(p.name for p in dbdir.iterdir()) == ["portfolio.db"], (
            "data/database/ 只能有 portfolio.db —— 本用例就是这条约定的守卫")

    def test_backup_uses_conventional_name(self, tmp_path, monkeypatch):
        _, src = self._prod_like(tmp_path)
        monkeypatch.setattr(rsw, "BACKUP_DIR", tmp_path / "bk")
        name = Path(rsw.backup(src)).name
        assert name.startswith("portfolio.db.bak_recompute_"), (
            "应沿用 data/backups/ 既有命名惯例 portfolio.db.bak_recompute_<ts>，实际=%s" % name)
        assert len(name) == len("portfolio.db.bak_recompute_") + len("20260916_115500")

    def test_backup_creates_backup_dir_if_missing(self, tmp_path, monkeypatch):
        _, src = self._prod_like(tmp_path)
        backup_dir = tmp_path / "not_yet" / "backups"
        monkeypatch.setattr(rsw, "BACKUP_DIR", backup_dir)
        assert not backup_dir.exists()
        rsw.backup(src)
        assert backup_dir.is_dir(), "目录不存在时应自动创建"

    def test_backup_leaves_no_sibling_file(self, tmp_path, monkeypatch):
        """回归：原实现会在库旁边留下 *.bak_recompute_<ts>。"""
        dbdir, src = self._prod_like(tmp_path)
        before = sorted(p.name for p in dbdir.iterdir())
        monkeypatch.setattr(rsw, "BACKUP_DIR", tmp_path / "backups")
        rsw.backup(src)
        after = sorted(p.name for p in dbdir.iterdir())
        assert before == after, "备份不得在数据库目录留下任何新文件"

    def test_backup_dir_defaults_to_data_backups(self):
        from config.settings import BACKUP_DIR as SETTINGS_BACKUP_DIR
        assert Path(SETTINGS_BACKUP_DIR).name == "backups"
        assert Path(SETTINGS_BACKUP_DIR).parent.name == "data"
        assert Path(rsw.BACKUP_DIR) == Path(SETTINGS_BACKUP_DIR)


# ---------------------------------------------------------------------------
# ⑤ main() 端到端（dry-run / apply / 不吞异常）
# ---------------------------------------------------------------------------
@pytest.fixture
def patched_script_db(monkeypatch):
    """把脚本的 DATABASE_PATH 指到合成库，并设置 argv。extra 用于追加 --apply 等。"""
    def _apply(path, extra=()):
        monkeypatch.setattr(rsw, "DATABASE_PATH", Path(path))
        monkeypatch.setattr(sys, "argv", [
            "recompute_summary_window.py",
            "--start-date", "2026-09-03", "--end-date", "2026-09-04",
            *extra,
        ])
    return _apply


class TestMainEndToEnd:
    def test_dry_run_with_gap_does_not_crash_and_does_not_write(self, gap_db, patched_script_db, capsys):
        patched_script_db(gap_db)
        q = "SELECT date FROM portfolio_summary ORDER BY date"
        before = sqlite3.connect(gap_db).execute(q).fetchall()
        rc = rsw.main()
        out = capsys.readouterr().out
        after = sqlite3.connect(gap_db).execute(q).fetchall()
        assert rc == 0, "窗口含新增日期时不得以 rc!=0 退出（修复前的 TypeError 路径）"
        # 窗口内 09-02 的快照不在窗口里；预置汇总行只有 09-04 ⇒ 新增的只有 09-03
        assert "共 2 天（其中新增 1 天: ['2026-09-03']）" in out
        assert "NEW" in out
        assert before == after, "dry-run 不得写库"

    def test_apply_with_gap_writes_row_without_crash(self, gap_db, patched_script_db, capsys):
        patched_script_db(gap_db, extra=["--apply"])
        rc = rsw.main()
        out = capsys.readouterr().out
        assert rc == 0
        conn = sqlite3.connect(gap_db)
        rows = dict(conn.execute(
            "SELECT date, daily_return FROM portfolio_summary WHERE date IN "
            "('2026-09-02','2026-09-03','2026-09-04')").fetchall())
        types = dict(conn.execute(
            "SELECT date, snapshot_type FROM portfolio_summary WHERE date IN "
            "('2026-09-03','2026-09-04')").fetchall())
        conn.close()
        assert "2026-09-03" in rows, "缺口日必须被真正写库"
        assert "APPLY" in out
        # 缺口日被补上后，09-04 变为单日值
        assert abs(rows["2026-09-04"] - 1.0) < 1e-9
        # 新增行的 snapshot_type 由 COALESCE 兜底为 'daily'
        assert types["2026-09-03"] == "daily"

    def test_second_apply_reports_no_new_dates(self, gap_db, patched_script_db, capsys):
        """幂等：跑第二遍时缺口已补，不应再出现新增日期。"""
        patched_script_db(gap_db, extra=["--apply"])
        rsw.main()
        capsys.readouterr()
        patched_script_db(gap_db)          # 第二次去掉 --apply，只观察表
        rc = rsw.main()
        out = capsys.readouterr().out
        assert rc == 0
        assert "共 2 天" in out
        assert "其中新增" not in out

    def test_unexpected_error_is_not_swallowed(self, gap_db, patched_script_db, monkeypatch):
        """护栏：禁用"整个打印块套 try/except"式静默 —— 真异常必须往上抛。"""
        patched_script_db(gap_db)

        def _boom(*a, **k):
            raise RuntimeError("boom")

        monkeypatch.setattr(rsw, "format_diff_table", _boom)
        with pytest.raises(RuntimeError, match="boom"):
            rsw.main()

    def test_apply_propagates_write_error(self, gap_db, patched_script_db, monkeypatch):
        """同上：写库失败必须抛，不能返回 0 假装成功。"""
        patched_script_db(gap_db, extra=["--apply"])

        def _boom(*a, **k):
            raise sqlite3.OperationalError("disk I/O error")

        monkeypatch.setattr(rsw, "apply_summary", _boom)
        with pytest.raises(sqlite3.OperationalError):
            rsw.main()


# ---------------------------------------------------------------------------
# ⑥ 前驱日期并集（防止把缺口复制到窗口第一天）
# ---------------------------------------------------------------------------
def test_previous_day_before_window_is_also_union_based(tmp_path):
    """窗口起点【之前】若是缺口日（只有快照），prev_dt 也要认它。"""
    snaps = [
        ("2026-09-01", "AAA", 100.0, 10.0),
        # 09-02 只有快照、没有汇总行（窗口外的缺口）
        ("2026-09-02", "AAA", 100.0, 10.1),
        ("2026-09-03", "AAA", 100.0, 10.201),
    ]
    path = _build_db(tmp_path / "prev_gap.db", snaps,
                     summary_dates=["2026-09-01", "2026-09-03"])
    conn = _conn(path)
    res = rsw.compute(conn, "2026-09-03", "2026-09-03")
    conn.close()
    # prev_dt 若只看 summary 会取到 09-01（跨过 09-02）⇒ +2.01%；
    # 取并集后 prev_dt = 09-02 ⇒ +1.00%
    assert abs(res["2026-09-03"]["daily_return"] - 1.0) < 1e-9


# ---------------------------------------------------------------------------
# ⑥ 备份落点：必须落 data/backups/，不得污染 data/database/
# ---------------------------------------------------------------------------
class TestBackupLandingSpot:
    """仓库约定 `data/database/` 只允许有 portfolio.db（防 worker 连错库）。

    原 `backup()` 写 `f"{db_path}.bak_recompute_{stamp}"` ⇒ 备份直接躺在 data/database/ 里，
    一次 `--backup` 就破坏该约定、并留下一个"看起来能连"的库。
    """

    def test_backup_goes_to_backup_dir_not_next_to_db(self, gap_db, tmp_path, monkeypatch, capsys):
        bdir = tmp_path / "backups"
        monkeypatch.setattr(rsw, "BACKUP_DIR", bdir)
        dst = rsw.backup(gap_db)
        assert Path(dst).parent == bdir
        assert Path(dst).is_file()
        assert Path(dst).stat().st_size == Path(gap_db).stat().st_size
        # 源库所在目录（模拟 data/database/）不得出现任何备份产物
        assert list(Path(gap_db).parent.glob("*bak_recompute*")) == []
        assert "[备份]" in capsys.readouterr().out

    def test_backup_dir_created_when_missing(self, gap_db, tmp_path, monkeypatch):
        bdir = tmp_path / "nested" / "backups"
        monkeypatch.setattr(rsw, "BACKUP_DIR", bdir)
        assert not bdir.exists()
        dst = rsw.backup(gap_db)
        assert bdir.is_dir()
        assert Path(dst).is_file()

    def test_backup_filename_follows_existing_convention(self, gap_db, tmp_path, monkeypatch):
        monkeypatch.setattr(rsw, "BACKUP_DIR", tmp_path)
        dst = rsw.backup(gap_db)
        # 与 data/backups/portfolio.db.bak_recompute_20260915_110813 同款命名
        assert re.search(r"bak_recompute_\d{8}_\d{6}$", Path(dst).name)
        assert Path(dst).name.startswith("gap.db.bak_recompute_")

    def test_main_backup_flag_does_not_pollute_db_dir(self, gap_db, patched_script_db,
                                                       tmp_path, monkeypatch):
        bdir = tmp_path / "bk"
        monkeypatch.setattr(rsw, "BACKUP_DIR", bdir)
        patched_script_db(gap_db, extra=["--backup"])
        assert rsw.main() == 0
        assert list(Path(gap_db).parent.glob("*bak_recompute*")) == [], (
            "data/database/ 里出现了备份产物 —— 破坏『该目录只允许 portfolio.db』的约定")
        assert len(list(bdir.glob("*bak_recompute*"))) == 1
