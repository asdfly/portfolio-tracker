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
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PRODUCTION_DB = (PROJECT_ROOT / "data" / "database" / "portfolio.db").resolve()
PRODUCTION_ENV = (PROJECT_ROOT / ".env").resolve()


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
        baseline = protected_file_baselines.get("生产数据库")
        if baseline is None:
            pytest.skip("生产库不存在（干净环境），无需校验")
        st = PRODUCTION_DB.stat()
        assert (st.st_mtime_ns, st.st_size) == baseline, (
            "生产库在测试运行期间被修改了"
        )

    def test_real_env_file_not_modified(self, protected_file_baselines):
        baseline = protected_file_baselines.get("真实 .env")
        if baseline is None:
            pytest.skip(".env 不存在，无需校验")
        st = PRODUCTION_ENV.stat()
        assert (st.st_mtime_ns, st.st_size) == baseline, (
            "真实 .env 在测试运行期间被修改了（历史事故：被 write_text + unlink 销毁）"
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

        # 连接落点本身不能是生产库
        assert _resolve(written_to) != PRODUCTION_DB, (
            f"应用默认连接直接写到了生产库：{written_to}"
        )

        # 生产库指纹不变
        if before is not None:
            after = PRODUCTION_DB.stat()
            assert (after.st_mtime_ns, after.st_size) == (
                before.st_mtime_ns,
                before.st_size,
            ), "写入穿透到了生产库（副本可能是硬链接/软链接而非真实复制）"

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
        after = PRODUCTION_DB.stat()
        assert (after.st_mtime_ns, after.st_size) == (
            before.st_mtime_ns,
            before.st_size,
        ), "硬编码路径的写入落到了生产库上"

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
