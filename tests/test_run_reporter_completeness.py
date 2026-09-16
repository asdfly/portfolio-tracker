"""P0 回归: RunReporter 的运行完整性 / dq_score 解耦 / 告警不静默。

缺陷实证(2026-09-15):
    管线在阶段一崩溃并 rc=1, `monitor` 里也记了
    `任务执行记录: portfolio_daily_analysis - failed`,
    但同一次运行的 data/reports/run_report_2026-09-15.json 却是
        {"stages": {otc_nav, watchlist}, "dq_score": 100, "alerts": [], "sources": []}
    —— 失败运行比 09-14 的成功运行(dq_score=90.6 / alerts=1)**分更高、告警更少**。

三条根因与本文件的用例一一对应:
    1) dq_score 由"回退公式"推导, 只统计 date_mismatch/spot_stale 两类 issue;
       09-15 当日唯一 issue 是 spot_historical ⇒ 分子为 0 ⇒ 直接 100。
       ⇒ 运行不完整时必须输出 null(TestFailedRun / TestSuccessfulRun)。
    2) stages 只反映"跑到哪", 没有任何字段表达"该跑没跑" ⇒ 报告看不出崩溃。
       ⇒ 新增 run_status(TestFailedRun / TestPartialRun / TestRequiredStageContract)。
    3) alerts 为空 ⇒ dispatch_alerts 第一行静默 return; 加上 events 白名单裁剪,
       运行失败可以被无声吞掉(TestAlertDispatch)。

测试不依赖生产库: RunReporter 不碰数据库, 报告目录一律落在 tmp_path。
"""
import inspect
import json
import re
from pathlib import Path

import pytest

from src.data_sources.collect_core import (
    CRITICAL_STAGES,
    PIPELINE_INCOMPLETE_KIND,
    REQUIRED_STAGES,
    RUN_STATUS_FAILED,
    RUN_STATUS_OK,
    RUN_STATUS_PARTIAL,
    RunReporter,
    dispatch_alerts,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RUN_ANALYSIS_SRC = PROJECT_ROOT / "run_analysis.py"

# 判据写死(不从常量推导): 若有人悄悄放宽 REQUIRED_STAGES, 这里必须红。
# 依据 run_analysis.py:main() 真实调用点 —— basic/risk/monitor 无 try 包裹,
# 挂了整次运行即 rc=1; dq_check 是唯一产出权威 dq_score 的阶段。
EXPECTED_REQUIRED_STAGES = ("basic", "risk", "monitor", "dq_check")
EXPECTED_CRITICAL_STAGES = ("basic",)


def _make_reporter(tmp_path, **kw):
    return RunReporter("2026-09-15", mode="daily",
                       reports_dir=str(tmp_path), **kw)


def _record_full_required(rep):
    """记录全部必需阶段(真实阶段名, 写死) —— 模拟一次跑完的运行。"""
    rep.stage("otc_nav", "ok")
    rep.stage("watchlist", "ok")
    rep.stage("basic", "ok")
    rep.stage("risk", "ok")
    rep.stage("monitor", "ok")
    rep.stage("dq_check", "ok")


def _kinds(report):
    return [a.get("kind") for a in report.get("alerts", [])]


# ---------------------------------------------------------------------------
# 0) 判据本身: 阶段名必须与 run_analysis.py 真实调用点一致
# ---------------------------------------------------------------------------
class TestRequiredStageContract:
    def test_required_and_critical_stages_are_pinned(self):
        assert tuple(REQUIRED_STAGES) == EXPECTED_REQUIRED_STAGES
        assert tuple(CRITICAL_STAGES) == EXPECTED_CRITICAL_STAGES

    def test_required_stage_names_really_used_by_pipeline(self):
        """REQUIRED_STAGES 里的名字必须真的出现在 main() 的 stage() 调用中。

        防止改名/拼写漂移后"必需阶段永远缺失"⇒ 每天误报 pipeline_incomplete。
        """
        src = RUN_ANALYSIS_SRC.read_text(encoding="utf-8")
        used = set(re.findall(r'_reporter\.stage\(\s*"([^"]+)"', src))
        assert used, "未从 run_analysis.py 解析到任何 _reporter.stage(...) 调用"
        missing = [s for s in REQUIRED_STAGES if s not in used]
        assert not missing, f"REQUIRED_STAGES 含管线不存在的阶段名: {missing}"
        # otc_nav / watchlist 是 try 包裹的可降级前置采集, 刻意不算必需
        assert {"otc_nav", "watchlist"} <= used
        assert not ({"otc_nav", "watchlist"} & set(REQUIRED_STAGES))


# ---------------------------------------------------------------------------
# 1) 失败运行: 崩溃在阶段一 ⇒ 不得输出漂亮分数, 且必须告警
# ---------------------------------------------------------------------------
class TestFailedRun:
    def test_missing_basic_is_failed_with_null_dq_score(self, tmp_path):
        """复刻 09-15: 只有 otc_nav/watchlist 被记录, basic 缺失。"""
        rep = _make_reporter(tmp_path)
        rep.stage("otc_nav", "ok")
        rep.stage("watchlist", "ok")

        report, path = rep.finalize_and_write(
            dq_issues=[{
                "issue_type": "spot_historical",      # 09-15 当日真实 issue 类型
                "source": "etf_fundamental",
                "n_affected": 25,
                "action": "rejected",
                "detail": "实时快照类源无法回溯历史日期",
            }],
            reports_dir=str(tmp_path))

        assert report["run_status"] == RUN_STATUS_FAILED
        assert report["dq_score"] is None, (
            "运行不完整时禁止回退公式给出分数(09-15 因此拿到 100)")
        assert report["dq_score_reason"], "dq_score 为 null 必须给出原因"
        assert "basic" in report["dq_score_reason"]
        assert PIPELINE_INCOMPLETE_KIND in _kinds(report)

        alert = next(a for a in report["alerts"]
                     if a.get("kind") == PIPELINE_INCOMPLETE_KIND)
        assert alert["level"] == "critical"
        assert "basic" in json.dumps(alert, ensure_ascii=False)
        assert "basic" in alert["detail"]

        # 落盘内容与返回值一致(读取方按文件读)
        assert path is not None
        on_disk = json.loads(Path(path).read_text(encoding="utf-8"))
        assert on_disk["run_status"] == RUN_STATUS_FAILED
        assert on_disk["dq_score"] is None
        assert on_disk["generated_at"] and on_disk["date"] == "2026-09-15"

    def test_basic_recorded_as_error_is_failed(self, tmp_path):
        rep = _make_reporter(tmp_path)
        rep.stage("otc_nav", "ok")
        rep.stage("basic", "error", note="boom")
        rep.stage("risk", "ok")
        rep.stage("monitor", "ok")
        rep.stage("dq_check", "ok")

        report, _ = rep.finalize_and_write(reports_dir=str(tmp_path))
        assert report["run_status"] == RUN_STATUS_FAILED
        assert report["dq_score"] is None
        assert "basic" in report["dq_score_reason"]

    def test_mark_run_failed_after_all_stages_recorded(self, tmp_path):
        """失败发生在全部必需阶段记录之后(如阶段五发信抛错)。

        此时 stages 看着是齐全的 —— 若只靠阶段推导就会写成 run_status="ok",
        与 09-15 属同类伪装, 必须由 mark_run_failed() 兜住。
        """
        rep = _make_reporter(tmp_path)
        _record_full_required(rep)
        rep.set_dq_score(95.0)
        rep.mark_run_failed("RuntimeError: send_daily_report exploded")

        report, _ = rep.finalize_and_write(reports_dir=str(tmp_path))
        assert report["run_status"] == RUN_STATUS_FAILED
        assert report["dq_score"] is None
        assert "send_daily_report exploded" in report["dq_score_reason"]
        assert PIPELINE_INCOMPLETE_KIND in _kinds(report)

    def test_suppressed_real_score_is_kept_in_reason(self, tmp_path):
        """有真实评分但运行不完整: dq_score 置 null, 数值不丢, 留在 reason 里。"""
        rep = _make_reporter(tmp_path)
        rep.stage("basic", "ok")
        rep.stage("risk", "ok")
        rep.stage("monitor", "ok")
        rep.stage("dq_check", "error", note="checker crashed")
        rep.set_dq_score(85.0)

        report, _ = rep.finalize_and_write(reports_dir=str(tmp_path))
        assert report["run_status"] == RUN_STATUS_PARTIAL
        assert report["dq_score"] is None
        assert "85.0" in report["dq_score_reason"]
        assert "dq_check" in report["dq_score_reason"]


# ---------------------------------------------------------------------------
# 2) 部分完成: 有必需阶段缺失但不涉及 basic
# ---------------------------------------------------------------------------
class TestPartialRun:
    def test_missing_dq_check_is_partial(self, tmp_path):
        rep = _make_reporter(tmp_path)
        rep.stage("otc_nav", "ok")
        rep.stage("basic", "ok")
        rep.stage("risk", "ok")
        rep.stage("monitor", "ok")
        # dq_check 缺失 ⇒ 没有权威评分, 也不允许回退公式

        report, _ = rep.finalize_and_write(reports_dir=str(tmp_path))
        assert report["run_status"] == RUN_STATUS_PARTIAL
        assert report["dq_score"] is None
        assert "dq_check" in report["dq_score_reason"]
        assert PIPELINE_INCOMPLETE_KIND in _kinds(report)

    def test_error_required_stage_is_partial(self, tmp_path):
        rep = _make_reporter(tmp_path)
        rep.stage("basic", "ok")
        rep.stage("risk", "error", note="corr matrix mismatch")
        rep.stage("monitor", "ok")
        rep.stage("dq_check", "ok")

        report, _ = rep.finalize_and_write(reports_dir=str(tmp_path))
        assert report["run_status"] == RUN_STATUS_PARTIAL
        assert report["dq_score"] is None
        assert "risk" in report["dq_score_reason"]

    def test_skipped_optional_stage_does_not_break_ok(self, tmp_path):
        """软截止跳过的非必需阶段(smart/backtest/nav_rebuild)是设计内降级。"""
        rep = _make_reporter(tmp_path)
        _record_full_required(rep)
        rep.stage("smart", "skipped", note="soft deadline protection")
        rep.stage("backtest", "skipped", note="soft deadline protection")
        rep.stage("nav_rebuild", "skipped", note="soft deadline protection")
        rep.set_dq_score(91.0)

        report, _ = rep.finalize_and_write(reports_dir=str(tmp_path))
        assert report["run_status"] == RUN_STATUS_OK
        assert report["dq_score"] == 91.0


# ---------------------------------------------------------------------------
# 3) 成功运行: 正常路径不能被改坏
# ---------------------------------------------------------------------------
class TestSuccessfulRun:
    def test_full_run_uses_checker_score(self, tmp_path):
        rep = _make_reporter(tmp_path)
        _record_full_required(rep)
        rep.set_dq_score(88.5)

        report, path = rep.finalize_and_write(reports_dir=str(tmp_path))
        assert report["run_status"] == RUN_STATUS_OK
        assert report["dq_score"] == 88.5
        assert report["dq_score_reason"]
        assert _kinds(report) == []
        assert json.loads(Path(path).read_text(
            encoding="utf-8"))["dq_score"] == 88.5

    def test_full_run_without_checker_score_still_uses_fallback(self, tmp_path):
        """回归保护: 回退公式在**完整**运行下必须继续可用。

        别把正常路径一起改坏 —— 部分部署/异常顺序下 dq_check 记为 ok 但
        set_dq_score 未被调用时, 仍应给出由 issue 推导的分值。
        """
        rep = _make_reporter(tmp_path)
        _record_full_required(rep)          # 刻意不调 set_dq_score
        report, _ = rep.finalize_and_write(
            dq_issues=[
                {"issue_type": "date_mismatch", "source": "x",
                 "n_affected": 3, "action": "rejected", "detail": ""},
                {"issue_type": "spot_stale", "source": "y",
                 "n_affected": 4, "action": "accepted", "detail": ""},
            ],
            reports_dir=str(tmp_path))

        assert report["run_status"] == RUN_STATUS_OK
        assert report["dq_score"] == 100 - 3 * 5 - 4 * 1     # == 81
        assert "fallback" in report["dq_score_reason"]
        assert "date_mismatch=3" in report["dq_score_reason"]

    def test_fallback_score_is_unchanged_for_mismatch_only(self, tmp_path):
        """公式本身未被改动(分子口径/权重保持 09-15 前语义)。"""
        rep = _make_reporter(tmp_path)
        _record_full_required(rep)
        report, _ = rep.finalize_and_write(
            dq_issues=[{"issue_type": "date_mismatch", "source": "x",
                        "n_affected": 2, "action": "rejected", "detail": ""}],
            reports_dir=str(tmp_path))
        assert report["dq_score"] == 90

    def test_low_score_alert_still_fires(self, tmp_path):
        rep = _make_reporter(tmp_path)
        _record_full_required(rep)
        rep.set_dq_score(70)
        report, _ = rep.finalize_and_write(reports_dir=str(tmp_path))
        assert "dq_low_score" in _kinds(report)
        assert PIPELINE_INCOMPLETE_KIND not in _kinds(report)


# ---------------------------------------------------------------------------
# 4) 告警通道不得静默
# ---------------------------------------------------------------------------
def _write_config(tmp_path, events, enabled=True, log_name="alerts.log"):
    log_path = tmp_path / log_name
    cfg_path = tmp_path / "notification.json"
    cfg_path.write_text(json.dumps({
        "enabled": enabled,
        "channels": {"webhook": {"url": "", "method": "POST"},
                     "log_file": str(log_path)},
        "events": list(events),
    }, ensure_ascii=False), encoding="utf-8")
    return str(cfg_path), log_path


class TestAlertDispatch:
    def test_incomplete_run_pushes_even_if_whitelist_omits_event(self, tmp_path):
        """白名单漏配 pipeline_incomplete 时仍必须推送(运行失败属兜底信息)。"""
        cfg, log_path = _write_config(tmp_path, events=["dq_low_score"])
        rep = _make_reporter(tmp_path)
        rep.stage("otc_nav", "ok")
        rep.stage("watchlist", "ok")

        report, _ = rep.finalize_and_write(reports_dir=str(tmp_path))
        dispatch_alerts(report, cfg)

        assert log_path.exists(), "运行失败却因 events 白名单漏配而静默"
        written = log_path.read_text(encoding="utf-8")
        assert PIPELINE_INCOMPLETE_KIND in written
        assert '"run_status": "failed"' in written

    def test_ok_run_with_no_alerts_stays_silent(self, tmp_path):
        """反向保护: 成功且无告警时不得开始刷屏。"""
        cfg, log_path = _write_config(tmp_path, events=["dq_low_score"])
        rep = _make_reporter(tmp_path)
        _record_full_required(rep)
        rep.set_dq_score(96.0)

        report, _ = rep.finalize_and_write(reports_dir=str(tmp_path))
        dispatch_alerts(report, cfg)
        assert not log_path.exists()

    def test_incomplete_run_logs_error_when_channel_disabled(self, tmp_path, caplog):
        cfg, log_path = _write_config(tmp_path, events=[PIPELINE_INCOMPLETE_KIND],
                                      enabled=False)
        rep = _make_reporter(tmp_path)
        rep.stage("otc_nav", "ok")

        report, _ = rep.finalize_and_write(reports_dir=str(tmp_path))
        with caplog.at_level("ERROR", logger="src.data_sources.collect_core"):
            dispatch_alerts(report, cfg)
        assert not log_path.exists()          # 尊重操作员的总开关
        assert any(r.levelname == "ERROR" for r in caplog.records), \
            "通道被禁用时运行失败必须留下 error 级痕迹"

    def test_incomplete_run_logs_error_when_config_missing(self, tmp_path, caplog):
        rep = _make_reporter(tmp_path)
        rep.stage("otc_nav", "ok")
        report, _ = rep.finalize_and_write(reports_dir=str(tmp_path))

        with caplog.at_level("ERROR", logger="src.data_sources.collect_core"):
            dispatch_alerts(report, str(tmp_path / "nope.json"))
        assert any(r.levelname == "ERROR" for r in caplog.records)

    # --- 接口契约: 旧报告(无 run_status)必须被兼容读取 ---
    def test_legacy_report_without_run_status_does_not_raise(self, tmp_path):
        """2026-09-15 及以前的 run_report 没有 run_status/alerts 缺失场景。

        读取方(dispatch_alerts)不得因缺字段抛异常, 且行为与改造前一致:
        alerts 为空 ⇒ 不推送。
        """
        cfg, log_path = _write_config(tmp_path, events=["dq_low_score"])
        legacy = {"date": "2026-09-14", "mode": "daily", "dq_score": 90.6,
                  "alerts": []}                      # 无 run_status
        assert "run_status" not in legacy
        dispatch_alerts(legacy, cfg)
        assert not log_path.exists()

    def test_legacy_report_with_alerts_still_pushes(self, tmp_path):
        cfg, log_path = _write_config(tmp_path, events=["dq_low_score"])
        legacy = {"date": "2026-09-14", "dq_score": 70,
                  "alerts": [{"level": "warning", "kind": "dq_low_score",
                              "message": "..."}]}
        dispatch_alerts(legacy, cfg)
        assert log_path.exists()
        assert "dq_low_score" in log_path.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# 5) 历史实证: 用 09-15 的真实事实回放, 并校验转录未漂移
# ---------------------------------------------------------------------------
# data/reports/ 未被 git 跟踪, 因此把 09-15 的关键事实转录在此, 保证用例
# 在干净克隆上也真实执行; 若本地有原始产物, 下面的漂移守卫会比对二者。
LEGACY_0915_STAGES = {"otc_nav": "ok", "watchlist": "ok"}      # 无 basic
LEGACY_0915_ISSUES = [{
    "issue_type": "spot_historical", "source": "etf_fundamental",
    "n_affected": 25, "action": "rejected",
    "detail": "实时快照类源无法回溯历史日期；请求 2026-09-14 但数据实为最新"
              "(2026-09-15)，已整体排除，避免重标失真",
}]
LEGACY_0915_REPORT = PROJECT_ROOT / "data" / "reports" / "run_report_2026-09-15.json"


class TestHistoricalReplay:
    def test_replay_2026_09_15_is_now_reported_as_failed(self, tmp_path):
        rep = _make_reporter(tmp_path)
        for name, status in LEGACY_0915_STAGES.items():
            rep.stage(name, status)

        report, _ = rep.finalize_and_write(
            dq_issues=LEGACY_0915_ISSUES, reports_dir=str(tmp_path))

        # 旧代码在完全相同输入下产出 dq_score=100 / alerts=[] / 无 run_status
        assert report["run_status"] == RUN_STATUS_FAILED
        assert report["dq_score"] is None
        assert "basic" in report["dq_score_reason"]
        assert PIPELINE_INCOMPLETE_KIND in _kinds(report)
        assert report["stages"].keys() == LEGACY_0915_STAGES.keys()

    @pytest.mark.skipif(not LEGACY_0915_REPORT.exists(),
                        reason="本地无 09-15 原始 run_report(该目录未被 git 跟踪)")
    def test_transcription_matches_live_artifact(self):
        """转录漂移守卫: 本地还留着 09-15 产物时, 核对事实未被记错。"""
        old = json.loads(LEGACY_0915_REPORT.read_text(encoding="utf-8"))
        assert "run_status" not in old            # 旧格式前提
        assert "dq_score_reason" not in old
        assert old["date"] == "2026-09-15"
        assert old["dq_score"] == 100             # 失败却满分 —— 缺陷本体的证据
        assert old["alerts"] == []
        assert old["sources"] == []
        assert {k: v["status"] for k, v in old["stages"].items()} \
            == LEGACY_0915_STAGES
        assert old["data_quality_issues"][0]["issue_type"] == "spot_historical"
        assert old["data_quality_issues"][0]["n_affected"] == 25


# ---------------------------------------------------------------------------
# 6) 健康探测异常必须留下痕迹(sources 不再恒为 [])
# ---------------------------------------------------------------------------
class TestHealthProbeWiring:
    def test_health_probe_exception_records_failed_source(self, tmp_path):
        """run_analysis.py 的健康探测整块抛异常时, 必须记一条失败源。

        该分支是"数据源全挂却在报告里零痕迹"的根因(09-15 sources == [])。
        main() 没有可注入的测试缝, 故用源码级断言锁住接线。
        """
        src = RUN_ANALYSIS_SRC.read_text(encoding="utf-8")
        assert "数据源健康检测失败" in src
        head, _, tail = src.partition("数据源健康检测失败")
        assert 'record_source("HEALTH_CHECK"' in tail[:400], (
            "健康探测的 except 分支未调用 _reporter.record_source(...)")

    def test_recorded_source_reaches_report(self, tmp_path):
        rep = _make_reporter(tmp_path)
        _record_full_required(rep)
        rep.set_dq_score(90.0)
        rep.record_source("HEALTH_CHECK", fail=1,
                          source_used="exception", detail="probe exploded")

        report, _ = rep.finalize_and_write(reports_dir=str(tmp_path))
        assert report["sources"] != []
        assert report["sources"][0]["name"] == "HEALTH_CHECK"
        assert report["sources"][0]["fail"] == 1


def test_reporter_does_not_require_database(tmp_path):
    """RunReporter 全链路不碰数据库(测试不依赖生产库的材料保证)。"""
    src = inspect.getsource(RunReporter)
    assert "sqlite3" not in src and "get_db_connection" not in src
