"""测试公共 fixture

包含 P0 级数据库隔离：保证整个测试套件永远不会读写生产库。
"""
import atexit
import os
import shutil
import sys
import tempfile
import pytest
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


# ============================================================================
# P0 数据库隔离
# ----------------------------------------------------------------------------
# 背景：多个模块用 `from config.settings import DATABASE_PATH` 在 import 期就把
# 路径绑定成模块级常量（data_loader.py、src/utils/database.py、tabs/*.py 等），
# 事后 monkeypatch config.settings 无法回溯修正这些已绑定的副本。
# 更要命的是 dashboard.py 在模块级调用 `_ensure_indexes()`，即
# "import dashboard" 本身就会对库执行 CREATE INDEX + commit（真实写入）。
#
# 因此隔离必须在 **任何 app 模块被 import 之前** 完成，且必须通过 os.environ
# 生效——环境变量是唯一能在 `importlib.reload(config.settings)` 之后依然存活的
# 重定向手段（test_d5_env_config.py 中确实存在 reload 用法）。
#
# 策略：把生产库【复制】一份到临时目录，让 DATABASE_PATH 指向副本。
# 这样测试仍能读到真实数据（行为与改造前一致，套件保持绿），
# 而任何写入都落在副本上，生产库零污染。
# 必须是真实复制（独立 inode），不能用硬链接/软链接，否则写入会穿透。
# ============================================================================

PRODUCTION_DB = (project_root / "data" / "database" / "portfolio.db").resolve()

_TEMP_DB_DIR = None

# 会话期间"尝试直连生产库"的记录（硬兜底拦截后填入）；会话结束时汇总，0 即代表全程未触碰生产库。
_PROD_CONNECT_ATTEMPTS = []


def _same_file(path_str, target: Path) -> bool:
    """判断 path_str 是否指向 target（宽松解析，容忍不存在的路径）"""
    if not path_str or path_str == ":memory:":
        return False
    try:
        return Path(path_str).resolve() == target
    except (OSError, ValueError):
        return False


def _cleanup_temp_db():
    """会话结束时删除临时库副本"""
    global _TEMP_DB_DIR
    if _TEMP_DB_DIR and os.path.isdir(_TEMP_DB_DIR):
        shutil.rmtree(_TEMP_DB_DIR, ignore_errors=True)
    _TEMP_DB_DIR = None


def _install_db_isolation() -> str:
    """把 DATABASE_PATH 重定向到生产库的临时副本，返回生效路径。"""
    global _TEMP_DB_DIR

    configured = os.environ.get("DATABASE_PATH")

    # CI 用 DATABASE_PATH=:memory:，或调用方已显式指定了非生产库路径 —— 尊重之。
    # 只有当未配置、或被显式指向生产库时，才强制接管。
    if configured and not _same_file(configured, PRODUCTION_DB):
        return configured

    tmp_dir = tempfile.mkdtemp(prefix="pt_test_db_")
    # 文件名保留 portfolio.db：部分测试断言 'portfolio.db' in str(DATABASE_PATH)
    tmp_db = Path(tmp_dir) / "portfolio.db"

    if PRODUCTION_DB.exists():
        shutil.copy2(PRODUCTION_DB, tmp_db)
        # 若存在 WAL/SHM 边车文件，一并复制，保证副本数据完整
        for suffix in ("-wal", "-shm"):
            side = PRODUCTION_DB.with_name(PRODUCTION_DB.name + suffix)
            if side.exists():
                shutil.copy2(side, tmp_db.with_name(tmp_db.name + suffix))

    _TEMP_DB_DIR = tmp_dir
    os.environ["DATABASE_PATH"] = str(tmp_db)
    atexit.register(_cleanup_temp_db)  # 兜底：即使 pytest 异常退出也清理
    return str(tmp_db)


def _repatch_loaded_modules(new_path: str):
    """兜底：修正在隔离生效前就已经绑定了生产库路径的模块常量。

    正常情况下 conftest 顶层代码先于所有 app 模块执行，这里不会命中；
    保留此函数是为了防御第三方插件提前 import config.settings 的情况。
    """
    new_value = Path(new_path)
    for module in list(sys.modules.values()):
        if module is None:
            continue
        try:
            current = getattr(module, "DATABASE_PATH", None)
        except Exception:
            continue
        if current is not None and _same_file(str(current), PRODUCTION_DB):
            try:
                setattr(module, "DATABASE_PATH", new_value)
            except Exception:
                pass


# 记录"硬编码了生产库路径"的调用点，会话结束时汇总提醒
REDIRECTED_CALLERS = set()


def _install_sqlite_redirector():
    """给 sqlite3.connect 装兜底拦截器：指向生产库的连接一律改道到临时副本。

    为什么需要这一层（env var 重定向还不够）：
      1) 测试侧硬编码绝对路径：test_d1_margin_research_block.py:9、
         test_fix_data_quality.py:8、test_market_event_signals.py:8、
         test_p2_reports_utils.py:63。
      2) 【更危险】应用代码硬编码相对路径：src/report/smart_report.py:61
         写死 'data/database/portfolio.db' 并执行 CREATE TABLE + INSERT，
         完全绕过 DATABASE_PATH。test_d3_closed_loop_feedback.py 会走到这条路径。
      这两类都碰不到 DATABASE_PATH，只能在 sqlite3.connect 这一层兜住。

    为什么是"改道"而不是"抛错"：
      抛错会把上述当前能通过的用例变红；改道则让它们照常读到同样的数据
      （副本内容与生产库一致），同时把写入挡在副本上。
      —— 既满足"永不碰生产库"，又保证套件仍绿。

    这是比"事后比对指纹"更强的保证：无论谁、用什么路径拼法（绝对/相对/URI），
    都碰不到生产库。
    """
    import sqlite3
    import traceback

    if getattr(sqlite3.connect, "_pt_guarded", False):
        return

    _orig_sqlite_connect = sqlite3.connect

    # --- 永久硬兜底 (qa-3 整改 P0-D):
    # 即使 guarded_connect 因任何原因放行了指向生产库的连接（路径绕过、DATABASE_PATH 被清空、
    # importlib.reload(config.settings) 把模块级 DATABASE_PATH 重新算回生产默认路径等），
    # 这一层也绝不真正触碰生产库——任何"解析后等于生产库"的 real connect 一律改道到临时副本。
    # 这是**不可绕过**的保证：它作用在真实的 sqlite3.connect 入口，无论调用方用什么路径拼法、
    # 什么执行顺序、什么环境变量状态，都碰不到生产库文件。
    _prod_connect_attempts = []

    def _real_connect(database, *a, **k):
        try:
            _r = Path(str(database)).resolve()
        except Exception:
            _r = None
        if _r == PRODUCTION_DB:
            # 记录（仅内存，不写文件）以便会话结束时汇总；生产库本身已被改道到副本，零污染。
            _prod_connect_attempts.append({
                "arg": str(database),
                "stack": "".join(traceback.format_stack()),
            })
            database = TEST_DB_PATH
        return _orig_sqlite_connect(database, *a, **k)

    # 暴露给 pytest_sessionfinish 汇总报告
    global _PROD_CONNECT_ATTEMPTS
    _PROD_CONNECT_ATTEMPTS = _prod_connect_attempts

    def guarded_connect(database, *args, **kwargs):
        candidate = str(database)
        # URI 形式（file:xxx?mode=ro）与普通路径都要覆盖
        if candidate.startswith("file:"):
            candidate = candidate[5:].split("?", 1)[0]

        if _same_file(candidate, PRODUCTION_DB):
            # 关键修复 (qa-3, P0-D 根因):
            # 原代码 `or str(PRODUCTION_DB)` 在 DATABASE_PATH 未设置/被清空时会把改道目标指向
            # **真实生产库**——这正是"全量跑偶发触碰生产库"的根因（无论触发顺序如何）。
            # TEST_DB_PATH 是 conftest 顶层已确定的临时副本绝对路径，改道只落在副本上，绝不碰生产库。
            replacement = os.environ.get("DATABASE_PATH") or str(TEST_DB_PATH)
            # 记录调用点（跳过本文件所在栈帧），便于事后治理硬编码
            for frame in reversed(traceback.extract_stack()[:-1]):
                if "conftest.py" not in frame.filename:
                    REDIRECTED_CALLERS.add(f"{frame.filename}:{frame.lineno}")
                    break
            # 排查用：PT_DEBUG_DB_REDIRECT=1 时打印完整调用栈
            if os.environ.get("PT_DEBUG_DB_REDIRECT"):
                print(f"\n[DB-REDIRECT] {database} -> {replacement}")
                traceback.print_stack()
            return _real_connect(replacement, *args, **kwargs)

        return _real_connect(database, *args, **kwargs)

    guarded_connect._pt_guarded = True
    guarded_connect._pt_real_connect = _real_connect
    sqlite3.connect = guarded_connect


# 顶层立即执行——必须先于任何 app 模块的 import
TEST_DB_PATH = _install_db_isolation()
_repatch_loaded_modules(TEST_DB_PATH)
_install_sqlite_redirector()

def _fingerprint(path: Path):
    """返回 (mtime_ns, size)，文件不存在则返回 None"""
    if not path.exists():
        return None
    st = path.stat()
    return (st.st_mtime_ns, st.st_size)


# 受保护的真实文件：生产库 + 项目根目录 .env（含用户凭据）
PRODUCTION_ENV = (project_root / ".env").resolve()

# 指纹快照，供会话结束时校验（见 pytest_sessionfinish）
_PROTECTED_FILES = {
    "生产数据库": PRODUCTION_DB,
    "真实 .env": PRODUCTION_ENV,
}
_BASELINE_FINGERPRINTS = {
    name: _fingerprint(path) for name, path in _PROTECTED_FILES.items()
}


@pytest.fixture(scope="session")
def protected_file_baselines():
    """暴露受保护真实文件的基线指纹，供回归测试断言。"""
    return dict(_BASELINE_FINGERPRINTS)


@pytest.fixture(scope="session", autouse=True)
def isolated_database():
    """会话级自动 fixture：保证并暴露隔离后的数据库路径。"""
    assert not _same_file(os.environ.get("DATABASE_PATH"), PRODUCTION_DB), (
        "数据库隔离失效：DATABASE_PATH 指向了生产库"
    )
    yield os.environ.get("DATABASE_PATH")
    _cleanup_temp_db()


@pytest.fixture(autouse=True)
def _guard_db_isolation():
    """函数级自动 fixture：每个用例执行后复核隔离未被破坏，并复位 DATABASE_PATH。

    原 leaks 的概率性来源之一：某个用例（如 `test_d5_env_config.py` 的
    `importlib.reload(config.settings)`）在运行期间把 DATABASE_PATH 清空或改向，
    其副作用残留到后续用例，使 guarded_connect 的兜底 `or ...` 落到生产库。
    这里在用例结束后强制把 DATABASE_PATH 复位到临时副本，使任何单测副作用都不会
    泄露到后续用例——隔离对执行顺序不再敏感。
    """
    current_before = os.environ.get("DATABASE_PATH")
    yield
    current = os.environ.get("DATABASE_PATH")
    if not current or _same_file(current, PRODUCTION_DB):
        # 不允许"未设置"或"指向生产库"的状态残留到下一个用例
        os.environ["DATABASE_PATH"] = str(TEST_DB_PATH)
        # 同步刷新 config.settings 的模块级常量，避免 reload/缓存导致的生产回退
        try:
            import config.settings as _cs
            _cs.DATABASE_PATH = Path(TEST_DB_PATH)
        except Exception:
            pass
    assert not _same_file(os.environ.get("DATABASE_PATH"), PRODUCTION_DB), (
        f"测试污染风险：用例执行后 DATABASE_PATH 指向生产库 ({current_before} -> {current})"
    )


def pytest_sessionfinish(session, exitstatus):
    """会话结束时校验受保护的真实文件未被改动——污染即报错，绝不静默通过。"""
    violations = []
    for name, path in _PROTECTED_FILES.items():
        before = _BASELINE_FINGERPRINTS.get(name)
        after = _fingerprint(path)
        if before != after:
            violations.append(f"{name} ({path}): before={before} after={after}")

    reporter = session.config.pluginmanager.get_plugin("terminalreporter")

    if violations:
        session.exitstatus = 1
        if reporter is not None:
            reporter.write_line("", red=True)
            reporter.write_line("[P0] 测试污染了真实文件：", red=True)
            for v in violations:
                reporter.write_line(f"  - {v}", red=True)

    # 不算失败，但要暴露出来：这些调用点硬编码了生产库路径，应逐步改用 DATABASE_PATH
    if REDIRECTED_CALLERS and reporter is not None:
        reporter.write_line("")
        reporter.write_line(
            "[提示] 以下位置硬编码了生产库路径，已被自动改道到隔离副本：",
            yellow=True,
        )
        for caller in sorted(REDIRECTED_CALLERS):
            reporter.write_line(f"  - {caller}", yellow=True)

    # 硬兜底拦截统计：若有任何连接曾"解析后等于生产库"而被改道，说明存在绕过风险点，
    # 虽已被零污染拦截，仍应暴露以便治理。（0 即代表全程未触碰生产库）
    _attempts = _PROD_CONNECT_ATTEMPTS
    if _attempts and reporter is not None:
        reporter.write_line("")
        reporter.write_line(
            f"[DB-隔离] 本会话有 {len(_attempts)} 次连接曾指向生产库，已全部改道到临时副本（零污染）。",
            yellow=True,
        )
        # 统计去重的调用栈栈顶（最后一次调用的来源），方便定位硬编码/缓存点
        _seen = []
        for _a in _attempts:
            _top = _a["stack"].strip().splitlines()[-3:] if _a["stack"] else []
            _sig = "\n".join(_top)
            if _sig not in _seen:
                _seen.append(_sig)
        for _s in _seen[:10]:
            reporter.write_line("  " + _s.replace("\n", "\n  "), yellow=True)


@pytest.fixture
def db_connection():
    """提供数据库连接（不手动 close）"""
    from src.utils.database import get_db_connection
    conn = get_db_connection()
    yield conn


@pytest.fixture
def sample_positions():
    """标准持仓 DataFrame（9列）"""
    import pandas as pd
    return pd.DataFrame({
        "code": ["510300", "510500", "159915"],
        "name": ["沪深300ETF", "中证500ETF", "创业板ETF"],
        "quantity": [1000, 500, 800],
        "cost_price": [4.5, 6.2, 2.1],
        "current_price": [4.8, 6.0, 2.3],
        "market_value": [4800.0, 3000.0, 1840.0],
        "pnl": [300.0, -100.0, 160.0],
        "pnl_rate": [6.67, -3.23, 9.52],
        "beta": [0.95, 1.05, 1.15],
    })


@pytest.fixture
def sample_summary():
    """标准汇总 DataFrame（4列，30行）"""
    import pandas as pd, numpy as np
    np.random.seed(42)
    dates = pd.date_range("2024-01-01", periods=30, freq="D").strftime("%Y-%m-%d")
    return pd.DataFrame({
        "date": dates,
        "total_value": np.random.normal(100000, 5000, 30).round(2),
        "total_pnl": np.random.normal(1000, 500, 30).round(2),
        "daily_return": np.random.normal(0.001, 0.02, 30).round(6),
    })


@pytest.fixture
def sample_index_quotes():
    """标准指数行情 DataFrame（7列，10行）"""
    import pandas as pd, numpy as np
    np.random.seed(42)
    dates = pd.date_range("2024-01-20", periods=10, freq="D")
    rows = []
    for d in dates:
        rows.append({"date": d.strftime("%Y-%m-%d"), "code": "sh000300", "name": "沪深300",
                      "close": round(3500 + np.random.randn() * 30, 2),
                      "change_pct": round(np.random.randn() * 1.5, 2),
                      "volume": int(np.random.rand() * 1e8),
                      "amount": int(np.random.rand() * 1e10)})
    return pd.DataFrame(rows)
