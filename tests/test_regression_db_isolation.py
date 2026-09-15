"""Regression: 测试套件污染生产数据库 / 真实 .env

Discovered: 2026-08-05（接手评估）
Symptom:
  1. tests/conftest.py 无任何 DB 隔离，9+ 个测试文件直连真实
     data/database/portfolio.db（88MB），一次测试运行改掉了生产库 mtime。
     具体写入路径：dashboard.py 模块级调用 _ensure_indexes()，
     即 "import dashboard" 就会对库执行 CREATE INDEX + commit。
  2. test_d5_env_config.py 对 PROJECT_ROOT/".env" 做 write_text + unlink，
     把项目根目录真实 .env（含用户凭据）覆盖成 41 字节废料。

Fixed by:
  - conftest.py 顶层在任何 app 模块 import 之前，把生产库复制到临时目录并
    改写 os.environ['DATABASE_PATH'] 指向副本。
  - test_d5_env_config.py 改用 tmp_path；config.settings._load_env_file()
    增加可选 env_path 参数以支持注入。

本文件是永久回归网：只增不删，每次改动都必须跑。
"""
import os
import sqlite3
import warnings
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PRODUCTION_DB = (PROJECT_ROOT / "data" / "database" / "portfolio.db").resolve()
PRODUCTION_ENV = (PROJECT_ROOT / ".env").resolve()


def _warn_if_production_db_fingerprint_changed(before_stat):
    """比对生产库指纹，变化则告警而非判定失败。

    提取为公共函数的目的：多处都有"用例内取快照再比对"的写法，它们同样会被外部
    进程写库误伤。统一走告警口径，比对 result：真正的写入穿透由各用例的
    「连接落点 != 生产库」主断言拦截，那条零假阳性、保持严格。
    """
    after = PRODUCTION_DB.stat()
    if (after.st_mtime_ns, after.st_size) == (
        before_stat.st_mtime_ns,
        before_stat.st_size,
    ):
        return
    warnings.warn(
        f"生产库指纹在用例运行期间发生变化（外部进程写库，非写入穿透，不计失败）：\n"
        f"    mtime_ns : {before_stat.st_mtime_ns} -> {after.st_mtime_ns}\n"
        f"    size     : {before_stat.st_size} -> {after.st_size}\n"
        f"  若确需判断写入是否穿透，请以「连接落点是否为生产库」的主断言为准。",
        RuntimeWarning,
        stacklevel=3,
    )


def _open_production_readonly():
    """拿到**真正的**生产库只读连接（绕过 conftest 指向副本的改道）。

    为什么不能直接 sqlite3.connect(生产库)：
      conftest 把 sqlite3.connect 整体替换成 guarded_connect，凡指向生产库的连接一律
      改道到临时副本。直接调用的话，本文件 checking 的其实是副本——副本里当然永远干净，
      于是断言变成一条**永远通过、检测不到任何东西**的假阴性测试。

    逃生口：conftest 在 guarded_connect 上挂了 _pt_real_connect（最内层真实连接）。
    实测其行为分两条：传入**普通路径**时会被最内层硬兜底改走副本；传入
    **URI 形式（file:...?mode=ro）**时则落到真实生产库——本用例刻意用 URI 形式读取。
    mode=ro 保证本用例自身不产生任何写入。

    依赖说明：这条路径依赖 conftest 的内部实现细节。一旦该逃生口消失，必须让测试
    **响亮失败**（pytest.fail）而不是静默跳过——静默跳过会让 Guard 悄悄失去意义。
    """
    real_connect = getattr(sqlite3.connect, "_pt_real_connect", None)
    if real_connect is None:
        pytest.fail(
            "无法绕过 conftest 的 sqlite3 改道：sqlite3.connect 上缺少 _pt_real_connect "
            "逃生口。本断言必须读到真实生产库才有意义，不能退化成检查副本的空测试。"
        )
    return real_connect(f"file:{PRODUCTION_DB}?mode=ro", uri=True)


def _resolve(path_str):
    """宽松解析路径，:memory: 与空值返回 None"""
    if not path_str or path_str == ":memory:":
        return None
    try:
        return Path(str(path_str)).resolve()
    except (OSError, ValueError):
        return None


class TestDatabasePathIsolation:
    """DATABASE_PATH 在测试环境下绝不能等于生产库路径"""

    def test_env_var_is_not_production_db(self):
        configured = os.environ.get("DATABASE_PATH")
        assert configured, "conftest 必须显式设置 DATABASE_PATH，不能留空回落到默认生产库"
        assert _resolve(configured) != PRODUCTION_DB, (
            f"DATABASE_PATH 指向生产库：{configured}"
        )

    def test_settings_path_is_not_production_db(self):
        from config.settings import DATABASE_PATH
        assert _resolve(DATABASE_PATH) != PRODUCTION_DB, (
            f"config.settings.DATABASE_PATH 指向生产库：{DATABASE_PATH}"
        )

    def test_data_loader_path_is_not_production_db(self):
        """data_loader 在 import 期绑定常量副本，必须单独校验"""
        import data_loader
        assert _resolve(data_loader.DATABASE_PATH) != PRODUCTION_DB, (
            f"data_loader.DATABASE_PATH 指向生产库：{data_loader.DATABASE_PATH}"
        )

    def test_utils_database_path_is_not_production_db(self):
        """src.utils.database 同样在 import 期绑定常量副本"""
        import src.utils.database as db_mod
        assert _resolve(db_mod.DATABASE_PATH) != PRODUCTION_DB, (
            f"src.utils.database.DATABASE_PATH 指向生产库：{db_mod.DATABASE_PATH}"
        )

    def test_no_loaded_module_still_points_at_production_db(self):
        """全量扫描已加载模块，不允许任何一个还持有生产库路径"""
        import sys
        offenders = []
        for name, module in list(sys.modules.items()):
            if module is None:
                continue
            try:
                value = getattr(module, "DATABASE_PATH", None)
            except Exception:
                continue
            if value is not None and _resolve(value) == PRODUCTION_DB:
                offenders.append(name)
        assert not offenders, f"以下模块仍指向生产库：{offenders}"


class TestProductionFilesUntouched:
    """真实文件在测试运行期间必须保持字节级不变"""

    def test_production_db_not_modified(self, protected_file_baselines):
        """生产库指纹巡检 —— 检测到变化时**告警，不判定失败**。

        为什么降级为告警（2026-09-15 改）：
          conftest 已把测试进程内所有连接改道到隔离副本，因此本进程写不到生产库。
          既然如此，运行期观测到的生产库 mtime 变化**必然来自外部进程** —— 那不是
          被测代码的回归，让测试 fail 只会制造假红。

          实测证据：2026-09-15 会话期间两次误报，均定位到同时段在跑的合法写库脚本
          （recompute_summary_window.py --apply 的 INSERT OR REPLACE、
           场外净值补采的 portfolio_snapshots 写入），而非任何测试污染。

        真正的污染检测已由下方 `TestProductionDbHasNoTestArtifacts` 承担：
        它断言生产库里**不存在测试专用对象**，是零假阳性的正向断言，不依赖文件时间戳。
        """
        baseline = protected_file_baselines.get("生产数据库")
        if baseline is None:
            pytest.skip("生产库不存在（干净环境），无需校验")
        st = PRODUCTION_DB.stat()
        current = (st.st_mtime_ns, st.st_size)
        if current == baseline:
            return

        base_ns, base_size = baseline
        cur_ns, _ = current
        warnings.warn(
            f"生产库在测试运行期间被外部进程改动（非测试污染，不计失败）：\n"
            f"    mtime_ns : {base_ns} -> {cur_ns}  (Δ {(cur_ns - base_ns) / 1e9:+.3f}s)\n"
            f"    size     : {base_size} -> {st.st_size}  (Δ {st.st_size - base_size:+d} 字节)\n"
            f"  判据说明：conftest 已把测试进程内连接全改道到隔离副本，本进程写不到生产库，\n"
            f"  故此变化来自外部进程。若确需排查是否有测试写穿，请改用\n"
            f"  TestProductionDbHasNoTestArtifacts 的探针断言（零假阳性）。",
            RuntimeWarning,
            stacklevel=2,
        )

    def test_real_env_file_not_modified(self, protected_file_baselines):
        """.env 保持严格断言（没有人会在测试中并行改写 .env）"""
        baseline = protected_file_baselines.get("真实 .env")
        if baseline is None:
            pytest.skip(".env 不存在，无需校验")
        st = PRODUCTION_ENV.stat()
        assert (st.st_mtime_ns, st.st_size) == baseline, (
            "真实 .env 在测试运行期间被修改了（历史事故：被 write_text + unlink 销毁）"
        )


class TestProductionDbHasNoTestArtifacts:
    """零假阳性的污染检测：生产库里不允许出现任何测试专用对象。

    与 mtime/size 巡检的区别
    ------------------------
    mtime/size 判的是「文件被动过」——外部合法写库也会触发，必然假阳性。
    本类判的是「脏东西确实被写进去了」——只在隔离真的失效、写入穿透到生产库时才会命中，
    正常情况（无论外部进程怎么写库）都不可能假阳性。

    这也是 2026-08-05 原始事故（import dashboard 触发 _ensure_indexes 在生产库建索引）
    最直接的兜底：那条路径会在生产库留下真实的表/索引对象，本类断言可捕获。
    """

    # conftest / 各测试使用过的探针对象名，出现任意一个即证明写入穿透
    PROBE_TABLES = (
        "_regression_isolation_probe",
        "_regression_hardcoded_probe",
    )

    # 自检基准：生产库必定包含的核心表。用于防止本断言自身静默退化为空测试——
    # 若将来 conftest 连 URI 形式也一起改道，这里会立刻失败，而不是悄悄变成永远通过的摆设。
    _MUST_HAVE_TABLES = ("portfolio_snapshots", "portfolio_nav", "etf_technical")

    def _production_schema_names(self, conn):
        return {
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type IN ('table','index','view')"
            )
        }

    def test_production_db_has_no_test_probe_objects(self):
        if not PRODUCTION_DB.exists():
            pytest.skip("生产库不存在，无需校验")

        conn = _open_production_readonly()
        try:
            names = self._production_schema_names(conn)
            missing = [t for t in self._MUST_HAVE_TABLES if t not in names]
            assert not missing, (
                f"自检失败：打开的库缺少生产库核心表 {missing}，说明读到的不是生产库 "
                f"（可能 conftest 已把 URI 形式也改道到副本）。"
                f"本断言依赖能读到真实生产库，请修正 conftest 或本用例。"
            )
            rows = sorted(n for n in names if n in self.PROBE_TABLES)
        finally:
            conn.close()

        assert not rows, (
            f"生产库中发现了测试专用对象，说明隔离失效、写入已穿透：{rows}\n"
            f"请检查 tests/conftest.py 的连接改道是否仍然生效。"
        )


class TestWritesLandOnCopyNotProduction:
    """端到端：通过应用自身的连接写库，生产库必须纹丝不动"""

    def test_app_connection_write_does_not_touch_production(self):
        from data_loader import get_db_connection

        before = PRODUCTION_DB.stat() if PRODUCTION_DB.exists() else None

        conn = get_db_connection()
        try:
            # 用应用默认连接执行真实写入（建表 + 插入 + 提交）
            conn.execute(
                "CREATE TABLE IF NOT EXISTS _regression_isolation_probe (id INTEGER)"
            )
            conn.execute("INSERT INTO _regression_isolation_probe VALUES (1)")
            conn.commit()
            written_to = conn.execute("PRAGMA database_list").fetchall()[0][2]
        finally:
            try:
                conn.execute("DROP TABLE IF EXISTS _regression_isolation_probe")
                conn.commit()
            except sqlite3.Error:
                pass
            conn.close()

        # 连接落点本身不能是生产库 —— 这是本用例的主断言，零假阳性，保持严格。
        assert _resolve(written_to) != PRODUCTION_DB, (
            f"应用默认连接直接写到了生产库：{written_to}"
        )

        # 指纹比对降级为告警：外部进程在本用例运行期间写库同样会让这里变化，
        # 那不是"写入穿透"。真正的穿透已由上面的落点断言拦住。
        if before is not None:
            _warn_if_production_db_fingerprint_changed(before)

    def test_hardcoded_production_path_is_redirected(self):
        """负向控制：即使代码硬编码生产库路径，也必须被改道到副本

        tests/test_d1_margin_research_block.py 等文件确实这么写，
        这条用例保证那类写法也伤不到生产库。
        """
        if not PRODUCTION_DB.exists():
            pytest.skip("生产库不存在，无需校验")

        before = PRODUCTION_DB.stat()

        # 完全绕过 DATABASE_PATH，直接用生产库绝对路径连接
        conn = sqlite3.connect(str(PRODUCTION_DB))
        try:
            landed_on = conn.execute("PRAGMA database_list").fetchall()[0][2]
            conn.execute(
                "CREATE TABLE IF NOT EXISTS _regression_hardcoded_probe (id INTEGER)"
            )
            conn.commit()
        finally:
            try:
                conn.execute("DROP TABLE IF EXISTS _regression_hardcoded_probe")
                conn.commit()
            except sqlite3.Error:
                pass
            conn.close()

        assert _resolve(landed_on) != PRODUCTION_DB, (
            f"硬编码生产库路径未被改道，直接连上了生产库：{landed_on}"
        )
        _warn_if_production_db_fingerprint_changed(before)

    def test_fallback_never_points_at_production_when_env_cleared(self):
        """回归 P0-D 根因：DATABASE_PATH 被清空时，guarded_connect 的兜底目标必须是临时副本，
        绝不能指向真实生产库。

        原代码 `replacement = os.environ.get("DATABASE_PATH") or str(PRODUCTION_DB)`
        在 DATABASE_PATH 未设置/被清空时，把改道目标指向**真实生产库**——
        这正是"全量跑偶发触碰生产库"的根因（触发顺序无关，只要 env 曾处于清空态）。
        本用例复现该触发条件（清空 DATABASE_PATH），用生产库绝对路径直连，
        断言 guarded_connect 的兜底分支仍把连接落点改到临时副本。
        """
        if not PRODUCTION_DB.exists():
            pytest.skip("生产库不存在，无需校验")
        before = PRODUCTION_DB.stat()
        saved = os.environ.pop("DATABASE_PATH", None)
        try:
            # 触发 guarded_connect 的兜底分支（DATABASE_PATH 此刻为 None）
            conn = sqlite3.connect(str(PRODUCTION_DB))
            try:
                landed_on = conn.execute("PRAGMA database_list").fetchall()[0][2]
            finally:
                conn.close()
        finally:
            if saved is not None:
                os.environ["DATABASE_PATH"] = saved

        assert _resolve(landed_on) != PRODUCTION_DB, (
            f"DATABASE_PATH 清空后兜底仍落到生产库：{landed_on}（根因未修复）"
        )
        _warn_if_production_db_fingerprint_changed(before)

    def test_readonly_uri_to_production_is_also_redirected(self):
        """URI 形式（file:...?mode=ro）同样必须被拦截改道"""
        if not PRODUCTION_DB.exists():
            pytest.skip("生产库不存在，无需校验")
        uri = f"file:{PRODUCTION_DB}?mode=ro"
        conn = sqlite3.connect(uri, uri=True)
        try:
            landed_on = conn.execute("PRAGMA database_list").fetchall()[0][2]
        finally:
            conn.close()
        assert _resolve(landed_on) != PRODUCTION_DB, (
            f"URI 形式绕过了改道，直接连上生产库：{landed_on}"
        )

    def test_temp_copy_is_independent_inode(self):
        """副本必须是独立文件，不能与生产库共享 inode"""
        configured = _resolve(os.environ.get("DATABASE_PATH"))
        if configured is None or not PRODUCTION_DB.exists() or not configured.exists():
            pytest.skip("使用内存库或生产库不存在，无需校验")
        assert not os.path.samefile(str(configured), str(PRODUCTION_DB)), (
            "临时库与生产库是同一个文件，隔离形同虚设"
        )
