"""回归网：生产库指纹守卫的「默认硬失败 + 显式 opt-in 放行」统一口径

背景（2026-09-16 口径统一）
--------------------------
`tests/conftest.py::pytest_sessionfinish` 的指纹巡检原先是**无条件硬失败**，
而 `tests/test_regression_db_isolation.py` 里同一件事已于 2026-09-15 降级为
RuntimeWarning：因为存在**合法外部写入方**（`scripts/recompute_summary_window.py --apply`
的 INSERT OR REPLACE、场外基金净值补采写 portfolio_snapshots、
`data/backups/` 备份 + 参数化 UPDATE 等已授权维护写库窗口）。测试进程内所有连接已被
conftest 改道到隔离副本，此时观测到的 mtime/size 变化**必然来自外部进程**，
判 fail 就是假红。两个守卫互相打脸，本文件锁定统一后的口径：

  * 默认（未设 `WB_ALLOW_PROD_WRITE`）—— 与改造前**完全一致**：硬失败 + exitstatus=1；
  * `WB_ALLOW_PROD_WRITE=1`/`true` —— 显式授权维护写库，降级为 RuntimeWarning，不置 exitstatus；
  * 真实 `.env` **不受**逃生口覆盖，任何情况下改动都判失败（2026-08-05 事故防线）。

⚠️ 本文件全部用例都使用**合成指纹 + 依赖注入**，绝不 touch / 改写生产库或其 mtime。
"""
import contextlib
import sys
import types
import warnings
from pathlib import Path

import pytest

# conftest 由 pytest 加载为 tests.conftest（tests/ 含 __init__.py）。优先复用
# 已加载的模块对象，保证 monkeypatch 打到 pytest 真正在用的那份。
ct = sys.modules.get("tests.conftest") or sys.modules.get("conftest")
if ct is None:  # pragma: no cover - 正常情况下 conftest 必先于本文件加载
    import importlib

    ct = importlib.import_module("tests.conftest")

PROD_KEY = "生产数据库"
ENV_KEY = "真实 .env"

# 合成基线指纹：固定的 (mtime_ns, size)，与真实磁盘无关
_FP0 = (1_700_000_000_000_000_000, 1000)
_MUST_SEE_IN_WARN = "已通过 WB_ALLOW_PROD_WRITE 显式授权外部写入，本次不作失败判定"


class _Reporter:
    """记录 write_line 的假 terminalreporter"""

    def __init__(self):
        self.lines = []

    def write_line(self, line="", **kwargs):
        self.lines.append(line)

    @property
    def text(self):
        return "\n".join(self.lines)


class _Session:
    """最小 session 替身：只需 config.pluginmanager.get_plugin 与 exitstatus"""

    def __init__(self, reporter=None):
        self.exitstatus = 0
        self.config = types.SimpleNamespace(
            pluginmanager=types.SimpleNamespace(get_plugin=lambda name: reporter)
        )


@contextlib.contextmanager
def _capture_warnings():
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        yield caught


def _runtime_warnings(caught):
    return [w for w in caught if issubclass(w.category, RuntimeWarning)]


def _install_synthetic_fingerprints(monkeypatch, after_by_key):
    """把 conftest 的指纹取值整体替换成合成值（**不 stat 任何真实文件**）。

    :param after_by_key: {"生产数据库"|"真实 .env": (mtime_ns, size) | None}，
                         未给出的条目按"与基线一致"处理。
    :return: 合成基线 dict（同时已 monkeypatch 到 ct._BASELINE_FINGERPRINTS）
    """
    path_by_key = ct._PROTECTED_FILES
    probe_map = {path_by_key[k]: v for k, v in after_by_key.items()}
    monkeypatch.setattr(ct, "_fingerprint", lambda p: probe_map.get(p, _FP0))
    baseline = {k: _FP0 for k in path_by_key}
    monkeypatch.setattr(ct, "_BASELINE_FINGERPRINTS", baseline)
    return baseline


@pytest.fixture(autouse=True)
def _no_ambient_opt_in(monkeypatch):
    """无论外部 shell 是否设置了 WB_ALLOW_PROD_WRITE，本文件用例的默认态都是"未开启"。

    需要 opt-in 的用例自行 monkeypatch.setenv，避免外部环境渗入导致假绿/假红。
    """
    monkeypatch.delenv(ct.ALLOW_PROD_WRITE_ENV, raising=False)


# ---------------------------------------------------------------------------
# 逃生口解析口径
# ---------------------------------------------------------------------------

class TestOptInParsing:
    """"1"/"true" 视为开启，其余（含 0/false/空/未设置）视为未开启"""

    @pytest.mark.parametrize("raw", ["1", "true", "TRUE", "True", " 1 ", " true "])
    def test_truthy_values_open_the_hatch(self, raw):
        assert ct._external_write_authorized({ct.ALLOW_PROD_WRITE_ENV: raw}) is True

    @pytest.mark.parametrize("raw", ["", "0", "false", "False", "no", "yes", "2", "tru"])
    def test_other_values_keep_strict_mode(self, raw):
        assert ct._external_write_authorized({ct.ALLOW_PROD_WRITE_ENV: raw}) is False

    def test_unset_env_means_strict(self, monkeypatch):
        monkeypatch.delenv(ct.ALLOW_PROD_WRITE_ENV, raising=False)
        assert ct._external_write_authorized() is False

    def test_env_var_name_is_wb_allow_prod_write(self):
        assert ct.ALLOW_PROD_WRITE_ENV == "WB_ALLOW_PROD_WRITE"

    def test_env_file_is_not_covered_by_opt_in(self):
        """真实 .env 刻意排除在逃生口覆盖范围外（安全网不削弱）"""
        assert ENV_KEY not in ct._OPT_IN_COVERED
        assert PROD_KEY in ct._OPT_IN_COVERED


# ---------------------------------------------------------------------------
# 纯函数层：指纹比对与差值文案
# ---------------------------------------------------------------------------

class TestComparatorIsPureAndInjectable:
    """compare_protected_fingerprints 可用合成指纹直接单测，无需触碰磁盘"""

    def test_only_changed_entries_are_returned(self):
        files = {"A": Path("a-not-on-disk"), "B": Path("b-not-on-disk")}
        baselines = {"A": (100, 10), "B": (200, 20)}
        after = {files["A"]: (100, 10), files["B"]: (250, 25)}

        diffs = ct.compare_protected_fingerprints(
            baselines, files=files, fingerprint_fn=lambda p: after[p]
        )

        assert [d["name"] for d in diffs] == ["B"]
        assert diffs[0]["before"] == (200, 20)
        assert diffs[0]["after"] == (250, 25)

    def test_no_change_yields_empty_list(self):
        files = {"A": Path("a-not-on-disk")}
        diffs = ct.compare_protected_fingerprints(
            {"A": (100, 10)}, files=files, fingerprint_fn=lambda p: (100, 10)
        )
        assert diffs == []

    def test_missing_file_is_a_change(self):
        files = {"A": Path("a-not-on-disk")}
        diffs = ct.compare_protected_fingerprints(
            {"A": (100, 10)}, files=files, fingerprint_fn=lambda p: None
        )
        assert len(diffs) == 1
        assert diffs[0]["after"] is None

    def test_delta_text_reports_size_and_mtime(self):
        delta = ct._fmt_delta((1_000_000_000, 100), (1_500_000_000, 50))
        assert "mtime Δ +0.500s" in delta
        assert "size Δ -50 字节" in delta

    def test_delta_text_handles_appearing_or_vanishing_file(self):
        assert "存在性" in ct._fmt_delta(None, (1, 2))
        assert "存在性" in ct._fmt_delta((1, 2), None)


# ---------------------------------------------------------------------------
# 路径 a：无变化 -> 不报错、不警告
# ---------------------------------------------------------------------------

class TestPathANoChange:
    def test_session_stays_green_and_silent(self, monkeypatch):
        _install_synthetic_fingerprints(monkeypatch, {})  # 两个文件都与基线一致
        reporter = _Reporter()
        session = _Session(reporter)

        with _capture_warnings() as caught:
            ct.pytest_sessionfinish(session, 0)

        assert session.exitstatus == 0
        assert _runtime_warnings(caught) == []
        assert "[P0]" not in reporter.text
        assert "[放行]" not in reporter.text

    def test_handler_returns_false_on_no_diff(self):
        assert ct.handle_protected_fingerprint_diffs([], reporter=None, env={}) is False


# ---------------------------------------------------------------------------
# 路径 b：有变化 + 未设置 WB_ALLOW_PROD_WRITE -> 硬失败
# ---------------------------------------------------------------------------

class TestPathBChangedWithoutOptIn:
    def test_session_exitstatus_is_set_to_1(self, monkeypatch):
        _install_synthetic_fingerprints(
            monkeypatch, {PROD_KEY: (_FP0[0] + 350_000_000_000, _FP0[1] + 106_496)}
        )
        reporter = _Reporter()
        session = _Session(reporter)

        with _capture_warnings() as caught:
            ct.pytest_sessionfinish(session, 0)

        assert session.exitstatus == 1, "默认（未授权）路径必须保持与改造前一致的硬失败"
        assert _runtime_warnings(caught) == [], "硬失败路径不应降级为 RuntimeWarning"

    def test_hard_fail_text_contains_before_after_and_escape_hatch(self, monkeypatch):
        _install_synthetic_fingerprints(
            monkeypatch, {PROD_KEY: (_FP0[0] + 350_000_000_000, _FP0[1] + 106_496)}
        )
        reporter = _Reporter()

        ct.pytest_sessionfinish(_Session(reporter), 0)
        text = reporter.text

        assert "[P0] 测试污染了真实文件" in text
        assert PROD_KEY in text
        assert f"before=mtime_ns={_FP0[0]} size={_FP0[1]}" in text
        assert f"after=mtime_ns={_FP0[0] + 350_000_000_000} size={_FP0[1] + 106_496}" in text
        assert "mtime Δ +350.000s" in text
        assert "size Δ +106496 字节" in text
        # 操作者必须能一眼看到"这是不是合法写库"的出路
        assert "WB_ALLOW_PROD_WRITE=1" in text

    def test_handler_return_value_is_true(self, monkeypatch):
        _install_synthetic_fingerprints(
            monkeypatch, {PROD_KEY: (_FP0[0] + 1, _FP0[1] + 1)}
        )
        diffs = ct.compare_protected_fingerprints(ct._BASELINE_FINGERPRINTS)
        assert ct.handle_protected_fingerprint_diffs(diffs, reporter=None, env={}) is True

    @pytest.mark.parametrize("raw", ["0", "false", "", "no"])
    def test_non_truthy_values_do_not_open_the_hatch(self, monkeypatch, raw):
        _install_synthetic_fingerprints(
            monkeypatch, {PROD_KEY: (_FP0[0] + 1, _FP0[1] + 1)}
        )
        reporter = _Reporter()
        session = _Session(reporter)

        ct.pytest_sessionfinish(session, 0)  # 环境变量未设置
        assert session.exitstatus == 1

        session2 = _Session(_Reporter())
        monkeypatch.setenv(ct.ALLOW_PROD_WRITE_ENV, raw)
        with _capture_warnings() as caught:
            ct.pytest_sessionfinish(session2, 0)
        assert session2.exitstatus == 1, f"{raw!r} 不应被当作开启"
        assert _runtime_warnings(caught) == []


# ---------------------------------------------------------------------------
# 路径 c：有变化 + WB_ALLOW_PROD_WRITE=1 -> 仅警告、不失败
# ---------------------------------------------------------------------------

class TestPathCChangedWithOptIn:
    @pytest.mark.parametrize("raw", ["1", "true", "TRUE"])
    def test_downgraded_to_runtime_warning(self, monkeypatch, raw):
        _install_synthetic_fingerprints(
            monkeypatch, {PROD_KEY: (_FP0[0] + 350_000_000_000, _FP0[1] + 106_496)}
        )
        monkeypatch.setenv(ct.ALLOW_PROD_WRITE_ENV, raw)
        reporter = _Reporter()
        session = _Session(reporter)

        with _capture_warnings() as caught:
            ct.pytest_sessionfinish(session, 0)

        assert session.exitstatus == 0, "显式授权后不得再把整轮测试标记为失败"
        runtime = _runtime_warnings(caught)
        assert len(runtime) == 1
        msg = str(runtime[0].message)
        assert _MUST_SEE_IN_WARN in msg
        assert "mtime_ns=" in msg and "size=" in msg
        assert "mtime Δ +350.000s" in msg and "size Δ +106496 字节" in msg
        # 硬失败文案必须消失，换成放行说明
        assert "[P0]" not in reporter.text
        assert "[放行]" in reporter.text
        assert "WB_ALLOW_PROD_WRITE" in reporter.text

    def test_handler_return_value_is_false(self, monkeypatch):
        _install_synthetic_fingerprints(
            monkeypatch, {PROD_KEY: (_FP0[0] + 1, _FP0[1] + 1)}
        )
        diffs = ct.compare_protected_fingerprints(ct._BASELINE_FINGERPRINTS)
        with _capture_warnings() as caught:
            verdict = ct.handle_protected_fingerprint_diffs(
                diffs, reporter=None, env={ct.ALLOW_PROD_WRITE_ENV: "1"}
            )
        assert verdict is False
        assert _MUST_SEE_IN_WARN in str(_runtime_warnings(caught)[0].message)

    def test_opt_in_does_not_mask_env_file_change(self, monkeypatch):
        """逃生口只覆盖生产库：开了 opt-in，真实 .env 被改仍必须硬失败"""
        _install_synthetic_fingerprints(
            monkeypatch,
            {
                PROD_KEY: (_FP0[0] + 1_000_000_000, _FP0[1] + 4096),
                ENV_KEY: (_FP0[0] + 1_000_000_000, _FP0[1] + 41),
            },
        )
        monkeypatch.setenv(ct.ALLOW_PROD_WRITE_ENV, "1")
        reporter = _Reporter()
        session = _Session(reporter)

        with _capture_warnings() as caught:
            ct.pytest_sessionfinish(session, 0)

        assert session.exitstatus == 1
        assert _runtime_warnings(caught) == []
        assert "[P0] 测试污染了真实文件" in reporter.text
        assert ENV_KEY in reporter.text
        # 被授权的那条差异单独列出，且明确标注不计入失败
        assert "opt-in 授权" in reporter.text
