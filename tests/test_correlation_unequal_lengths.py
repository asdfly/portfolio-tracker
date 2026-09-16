# -*- coding: utf-8 -*-
"""P0 回归：相关性矩阵「数组长度不等」致每日管线中断（2026-09-15 15:30）。

事故调用栈（logs/scheduled_run.log）：
    run_analysis.py:970 main
      -> run_analysis.py:164 run_stage1_basic
      -> src/analysis/portfolio.py:238 run_daily_analysis
      -> src/analysis/portfolio_risk.py:30 analyze_portfolio_risk
      -> src/analysis/portfolio_risk.py:156 _analyze_correlations
      -> src/analysis/risk.py:288 calculate_correlation_matrix
      -> pd.DataFrame(returns_dict)
    ValueError: All arrays must be of the same length

根因：`get_price_history` 返回的历史行数天然不等（场外基金净值序列短于 ETF，
生产实测分布 33/44/49/60 行 → 收益 32/43/48/59），修复前把裸 numpy 数组按列塞进
DataFrame，pandas 要求等长。

修复（e1610fd）：按 date 升序构造带日期索引的 pd.Series，交给 pandas 按索引对齐，
并用 min_overlap=20 丢弃薄样本。

追加的输入口径守卫（2026-09-16，见 src/analysis/portfolio_risk.py 顶部常量注释）：
  * 相邻快照自然日间隔 > MAX_SINGLE_SESSION_GAP_DAYS(12) → 该位置记 NaN（跨期区间收益
    不得伪装成日收益），但**保留该日期行**；
  * 单日 |log_ret| > SPLIT_SPIKE_LOG_RET(0.30)（份额折算/拆分）→ 该观测记 NaN 并登记到
    unreliable_codes；15%~20% 的真实跳变不得被清洗；
  * overlap_days 改为循环内登记后合并，被跳过的标的不再从报告里消失。

本文件不依赖生产库：以 `__new__` 构造分析器并注入 stub DB，
不调用 DatabaseManager（其 __init__ 会连库执行 DDL），全程不打开任何 SQLite 文件。
"""
import ast
import logging
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src.analysis.portfolio_risk import (MAX_SINGLE_SESSION_GAP_DAYS,
                                         SPLIT_SPIKE_LOG_RET,
                                         PortfolioRiskAnalyzer)
from src.analysis.risk import RiskAnalyzer

MIN_OVERLAP = 20  # 与 _analyze_correlations 内的硬约束一致


# ============================================================================
# 测试替身
# ============================================================================

class _StubDB:
    """复刻 DatabaseManager.get_price_history 的真实行为：date DESC + LIMIT days。"""

    def __init__(self, price_series, summary_rows=None):
        # price_series: {code: [(date_str, price), ...]}，按日期升序给出
        self._prices = {k: list(v) for k, v in price_series.items()}
        self._summary = list(summary_rows or [])
        self.calls = []

    def get_price_history(self, code, days=60):
        self.calls.append((code, days))
        rows = sorted(self._prices.get(code, []), key=lambda x: x[0], reverse=True)
        return [{"date": d, "current_price": p, "market_value": None, "pnl": None}
                for d, p in rows[:days]]

    def get_portfolio_history(self, days=30):
        return list(self._summary[-days:])


class _SpyRiskAnalyzer(RiskAnalyzer):
    """记录 calculate_correlation_matrix 收到的入参，再委托真实实现。"""

    def __init__(self):
        super().__init__()
        self.last_input = None

    def calculate_correlation_matrix(self, returns_dict, min_periods=20):
        self.last_input = dict(returns_dict)
        return super().calculate_correlation_matrix(returns_dict,
                                                    min_periods=min_periods)


class _OfflineAnalyzer(PortfolioRiskAnalyzer):
    """只切断「基准收益」这一条会直连 sqlite 的支路，其余全走真实代码。"""

    def _get_benchmark_returns(self, index_code, days):
        return np.array([])  # 本用例只验证相关性路径


def _make_analyzer(price_series, summary_rows=None):
    """不调用 __init__（会连库执行 DDL），手工装配依赖。"""
    a = _OfflineAnalyzer.__new__(_OfflineAnalyzer)
    a.risk_analyzer = RiskAnalyzer()
    a.db = _StubDB(price_series, summary_rows)
    return a


def _bdates(n, start=0):
    return [d.strftime("%Y-%m-%d")
            for d in pd.bdate_range("2026-01-05", periods=n + start)][start:]


def _prices_from_returns(dates, returns_by_step, p0=1.0):
    """按给定日收益构造价格序列；returns_by_step 与 dates[1:] 一一对应。"""
    prices = [p0]
    for r in returns_by_step:
        prices.append(prices[-1] * (1.0 + r))
    assert len(prices) == len(dates)
    return list(zip(dates, prices))


def _linear_prices(n, start=0, p0=1.0, step=0.001):
    """缓变价格（避免零方差导致相关系数退化为 NaN）。"""
    return [(d, p0 + i * step) for i, d in enumerate(_bdates(n, start))]


def _pos(code, market_value=1000.0):
    return {"code": code, "name": code, "market_value": market_value}


def _summary_rows(n=30):
    return [{"date": d, "total_value": 100000.0 + i, "daily_return": 0.1,
             "total_cost": 90000.0} for i, d in enumerate(_bdates(n))]


# ============================================================================
# 1. 长度不等的序列不再中断
# ============================================================================

class TestUnequalLengthNoLongerRaises:
    def test_pre_fix_shape_would_have_raised(self):
        """锚定根因：不等长裸数组喂 pd.DataFrame 必抛事故原话。

        若哪天 pandas 改了该行为，这条会先红，提醒本文件的其余断言失去意义。
        """
        with pytest.raises(ValueError, match="All arrays must be of the same length"):
            pd.DataFrame({"a": np.zeros(32), "b": np.zeros(43),
                          "c": np.zeros(48), "d": np.zeros(59)})

    def test_production_row_distribution_60_44_33(self):
        """复刻生产行数分布（60/44/33 行 → 59/43/32 个收益）不再中断。"""
        series = {"600001": _linear_prices(60),
                  "600002": _linear_prices(44, p0=2.0, step=0.002),
                  "600003": _linear_prices(33, p0=3.0, step=0.003)}
        positions = [_pos("600001"), _pos("600002"), _pos("600003")]

        out = _make_analyzer(series)._analyze_correlations(positions, 60)

        assert "error" not in out, out
        cm = out["correlation_matrix"]
        cols = list(cm)
        assert len(cols) == 3, "3 只标的都应进入相关矩阵"
        for c in cols:
            assert cm[c][c] == 1.0
        assert out["min_overlap"] == MIN_OVERLAP

    def test_correlations_are_finite_and_in_range(self):
        rng = np.random.default_rng(7)
        series, positions = {}, []
        for code, n, p0 in [("600001", 60, 10.0), ("600002", 40, 20.0),
                            ("600003", 25, 30.0)]:
            series[code] = _prices_from_returns(
                _bdates(n), list(rng.normal(0, 0.01, n - 1)), p0=p0)
            positions.append(_pos(code))

        out = _make_analyzer(series)._analyze_correlations(positions, 60)
        cm = out["correlation_matrix"]

        assert len(cm) == 3 and all(len(row) == 3 for row in cm.values())
        for ci in cm:
            for cj in cm:
                v = cm[ci][cj]
                assert v is not None and np.isfinite(v)
                assert -1.0 <= v <= 1.0

    def test_analyze_portfolio_risk_full_path(self):
        """走 analyze_portfolio_risk 整条路径（事故栈的中段）。"""
        series = {"600001": _linear_prices(60),
                  "600002": _linear_prices(44, p0=2.0, step=0.002),
                  "600003": _linear_prices(33, p0=3.0, step=0.003)}
        positions = [_pos("600001", 5000.0), _pos("600002", 3000.0),
                     _pos("600003", 2000.0)]

        res = _make_analyzer(series, _summary_rows(30)).analyze_portfolio_risk(
            positions, {}, days=60)

        assert set(res) >= {"portfolio_metrics", "concentration_risk",
                            "correlation_analysis", "stress_test", "risk_warnings"}
        ca = res["correlation_analysis"]
        assert "error" not in ca, ca
        assert len(ca["correlation_matrix"]) == 3
        assert res["concentration_risk"]["max_weight"] > 0


# ============================================================================
# 2. 薄样本处理
# ============================================================================

class TestThinSampleHandling:
    def test_len_lt_2_history_skipped_with_overlap_zero(self):
        """只有 1 行历史的标的被跳过，overlap_days 记 0。

        （可用标的 < 2 时函数提前返回 error，此时 overlap_days 仍含被跳过标的。）
        """
        series = {"600001": _linear_prices(60),
                  "999999": [("2026-09-15", 1.0)]}
        out = _make_analyzer(series)._analyze_correlations(
            [_pos("600001"), _pos("999999")], 60)

        assert out["error"] == "数据不足"
        assert out["min_overlap"] == MIN_OVERLAP
        assert out["overlap_days"]["999999_999999"] == 0
        assert out["overlap_days"]["600001_600001"] == 59

    def test_zero_row_history_never_enters_matrix(self):
        """场外基金式「零行」标的：不出现在矩阵中（矩阵仍可产出）。"""
        series = {"600001": _linear_prices(60),
                  "600002": _linear_prices(60, p0=2.0, step=0.002)}
        positions = [_pos("600001"), _pos("600002"), _pos("519770")]

        out = _make_analyzer(series)._analyze_correlations(positions, 60)

        assert "error" not in out, out
        cols = list(out["correlation_matrix"])
        assert len(cols) == 2
        assert not any(c.startswith("519770") for c in cols)
        # 静默隐身缺口已修（2026-09-16）：overlap_days 改为「循环内登记 + 按列刷新」，
        # 被 continue 掉的标的仍留痕。修复前这里是整表重建，本断言原本写作
        # `not in`（用来记录缺口）；缺口修掉后必须反转，并额外锁住原因字段。
        assert out["overlap_days"]["519770_519770"] == 0
        assert out["skipped_codes"] == [
            {"code": "519770_519770", "reason": "rows_lt_2",
             "rows": 0, "valid_returns": 0}]

    def test_20_prices_dropped_21_prices_kept(self):
        """边界：len(values) < min_overlap + 1 (=21) 丢弃；21 恰好保留。"""
        base = _linear_prices(60)
        series = {
            "600001": base,
            "600002": _linear_prices(60, p0=2.0, step=0.002),
            "600020": base[:20],   # 19 个收益观测 < 20 → 丢弃
            "600021": base[:21],   # 20 个收益观测 == min_overlap → 保留
        }
        positions = [_pos(c) for c in ("600001", "600002", "600020", "600021")]

        out = _make_analyzer(series)._analyze_correlations(positions, 60)
        cols = list(out["correlation_matrix"])

        assert "600020_600020" not in cols, "19 个收益观测的标的不许进矩阵"
        assert "600021_600021" in cols, "20 个收益观测的标的必须保留"
        assert len(cols) == 3

    def test_only_one_usable_asset_returns_error(self):
        series = {"600001": _linear_prices(60), "600020": _linear_prices(20)}
        out = _make_analyzer(series)._analyze_correlations(
            [_pos("600001"), _pos("600020")], 60)

        assert out["error"] == "数据不足"
        assert out["overlap_days"]["600020_600020"] == 19
        assert out["overlap_days"]["600001_600001"] == 59
        assert "correlation_matrix" not in out

    def test_overlap_days_semantics_per_drop_reason(self):
        """锁定 `overlap_days` 口径（2026-09-16 团队裁定）：三条丢弃路径的取值必须区分。

        口径统一为「进入矩阵时可用的有效观测数」：
          * `rows_lt_2`（只有 0~1 行，构不成收益）        → **0**
          * `rows_le_min_overlap`（薄样本，行数够算收益）  → **len(values) - 1**
          * `zero_variance`（价格恒定，ρ 无定义）          → **0**
        这里把三条路径放在同一个用例里，避免后人只改其中一条造成口径漂移。
        """
        base = _linear_prices(60)
        series = {
            "600001": base[:21],                        # 20 个收益观测 == min_overlap → 保留
            "600002": base[:20],                        # 19 个 < min_overlap → 薄样本丢
            "600003": [("2026-09-15", 1.0)],            # 1 行 → rows_lt_2
            "600004": [(d, 1.0) for d in _bdates(60)],  # 恒定价 → zero_variance
        }
        positions = [_pos(c) for c in ("600001", "600002", "600003", "600004")]

        out = _make_analyzer(series)._analyze_correlations(positions, 60)

        od = out["overlap_days"]
        assert od["600001_600001"] == 20, "== min_overlap 必须保留"
        assert od["600002_600002"] == 19, "薄样本记 len(values)-1，不是 0"
        assert od["600003_600003"] == 0, "rows_lt_2 记 0"
        assert od["600004_600004"] == 0, "zero_variance 记 0"

        reasons = {s["code"]: s["reason"] for s in out["skipped_codes"]}
        assert reasons["600002_600002"] == "rows_le_min_overlap"
        assert reasons["600003_600003"] == "rows_lt_2"
        assert reasons["600004_600004"] == "zero_variance"


# ============================================================================
# 3. 日期索引对齐正确性（手算可验证）
# ============================================================================

class TestDateIndexAlignment:
    """A、B 两只标的在重叠区间内日收益成比例（B = 2 × A），窗口错开 5 天。

    按日期对齐 → 相关系数 = +1.0；若退化为按位置对齐，交替信号相位差 5 天 → 约 -1.0。
    期望值由价格序列在本测试内手算，不依赖被测实现的任何中间产物。
    """

    def _build(self):
        dates = _bdates(50)                                  # D0..D49
        pattern = {k: (0.01 if k % 2 else -0.01) for k in range(1, 50)}

        a_dates = dates[:40]                                 # D0..D39
        a_prices = _prices_from_returns(
            a_dates, [pattern[k] for k in range(1, 40)], p0=1.0)

        b_dates = dates[5:45]                                # D5..D44
        b_prices = _prices_from_returns(
            b_dates, [2.0 * pattern[k] for k in range(6, 45)], p0=2.0)

        return {"600001": a_prices, "600002": b_prices}, dates

    @staticmethod
    def _returns(prices):
        """手算日收益 Series（并给出其日期索引）。"""
        px = np.array([p for _, p in prices], dtype=float)
        dt = [d for d, _ in prices][1:]
        return pd.Series(np.diff(px) / px[:-1], index=dt)

    def test_only_overlapping_dates_are_used(self):
        series, dates = self._build()
        a = _make_analyzer(series)

        out = a._analyze_correlations([_pos("600001"), _pos("600002")], 60)
        got = out["correlation_matrix"]["600001_600001"]["600002_600002"]

        ra, rb = self._returns(series["600001"]), self._returns(series["600002"])
        common = ra.index.intersection(rb.index)
        # A 的收益在 D1..D39，B 的收益在 D6..D44 → 公共交易日 D6..D39（34 天）
        assert list(common) == dates[6:40]
        assert len(common) == 34

        expected = float(np.corrcoef(ra[common].values, rb[common].values)[0, 1])
        assert expected == pytest.approx(1.0, abs=1e-9)  # 手算：B = 2×A
        assert got == pytest.approx(expected, abs=1e-9), (
            "应基于 34 个公共交易日的收益计算，实际 %r，手算 %r" % (got, expected))

        # 反证：位置对齐（不做日期索引）会得到完全不同的结果
        n = min(len(ra), len(rb))
        positional = float(np.corrcoef(ra.values[:n], rb.values[:n])[0, 1])
        assert positional < 0.9, (
            "样例失效：位置对齐也得到高相关(%r)，无法区分两种实现" % positional)
        assert got != pytest.approx(positional, abs=0.5)

    def test_returns_are_attached_to_the_correct_date(self):
        """数据源是 DESC；若未先升序，收益会挂到错误的日期上。

        构造：唯一一次跳变发生在 D10（1.0 → 1.05），其余恒定。
        正确结果：收益 +0.05 落在 D10，-0.05/1.05 落在 D11，其余为 0。

        跳变幅度刻意取 5%：本用例只校验「收益挂在哪个日期」。幅度若超过
        SPLIT_SPIKE_LOG_RET(0.30)，会被折算守卫正确地判为尖峰并置 NaN
        （该守卫本身另有专门用例 TestSplitSpikeGuard）。
        """
        dates = _bdates(40)
        jump = [(d, 1.05 if i == 10 else 1.0) for i, d in enumerate(dates)]
        series = {"600001": jump, "600002": _linear_prices(40, p0=5.0)}
        spy = _SpyRiskAnalyzer()
        a = _make_analyzer(series)
        a.risk_analyzer = spy

        a._analyze_correlations([_pos("600001"), _pos("600002")], 60)

        s = spy.last_input["600001_600001"]
        assert list(s.index) == sorted(s.index), "传给相关矩阵的索引必须升序"
        assert float(s.loc[dates[10]]) == pytest.approx(0.05, abs=1e-12)
        assert float(s.loc[dates[11]]) == pytest.approx(-0.05 / 1.05, abs=1e-12)
        assert float(s.loc[dates[1]]) == pytest.approx(0.0, abs=1e-12)
        assert float(s.loc[dates[-1]]) == pytest.approx(0.0, abs=1e-12)

    def test_disjoint_assets_yield_nan_not_exception(self):
        series = {"600001": _linear_prices(30),
                  "600002": _linear_prices(30, start=200,
                                           p0=2.0, step=0.002)}
        out = _make_analyzer(series)._analyze_correlations(
            [_pos("600001"), _pos("600002")], 60)

        assert "error" not in out, out
        v = out["correlation_matrix"]["600001_600001"]["600002_600002"]
        assert v is None or (isinstance(v, float) and np.isnan(v))


# ============================================================================
# 4. 边界：全空输入（断言的是源码真实行为，不是臆测行为）
# ============================================================================

class TestEmptyBoundary:
    def test_empty_positions(self):
        out = _make_analyzer({})._analyze_correlations([], 60)
        # 错误分支的稳定契约（2026-09-16 起带可观测字段）。用逐键断言而不是整字典
        # 相等：新增诊断键不应该把「空输入不抛异常」这条用例打红。
        assert out["error"] == "数据不足"
        assert out["overlap_days"] == {}
        assert out["min_overlap"] == MIN_OVERLAP
        assert out["reason"] == "series_lt_2"
        assert out["unreliable_codes"] == {}
        assert out["cross_period_voided"] == {}
        assert out["skipped_codes"] == []
        assert "correlation_matrix" not in out
        assert "dropped_thin" not in out

    def test_all_assets_thin(self):
        """全部标的被跳过：返回 error 字典，各标的 overlap_days=0，无矩阵键。"""
        series = {"999991": [("2026-09-15", 1.0)],
                  "999992": [("2026-09-15", 1.0)]}
        out = _make_analyzer(series)._analyze_correlations(
            [_pos("999991"), _pos("999992")], 60)

        assert out["error"] == "数据不足"
        assert out["overlap_days"] == {"999991_999991": 0, "999992_999992": 0}
        assert out["min_overlap"] == MIN_OVERLAP
        assert "correlation_matrix" not in out
        assert "dropped_thin" not in out  # 该分支不产出 dropped_thin

    def test_all_assets_zero_rows(self):
        out = _make_analyzer({})._analyze_correlations(
            [_pos("519770"), _pos("519771")], 60)
        assert out["error"] == "数据不足"
        assert out["overlap_days"] == {"519770_519770": 0, "519771_519771": 0}


# ============================================================================
# 5. risk.calculate_correlation_matrix 自身的防御
# ============================================================================

class TestCalculateCorrelationMatrixGuards:
    def test_unequal_raw_arrays_raise_explicit_error(self):
        """裸数组长度不等 → 明确中文报错，而不是含糊的 pandas 报错。"""
        with pytest.raises(ValueError, match="长度不等"):
            RiskAnalyzer().calculate_correlation_matrix(
                {"a": np.zeros(32), "b": np.zeros(43)})

    def test_error_message_lists_labels_and_lengths(self):
        with pytest.raises(ValueError) as ei:
            RiskAnalyzer().calculate_correlation_matrix(
                {"a": np.zeros(32), "b": np.zeros(43)})
        msg = str(ei.value)
        assert "a(32)" in msg and "b(43)" in msg

    def test_mixed_series_and_array_raise(self):
        with pytest.raises(ValueError, match="混用"):
            RiskAnalyzer().calculate_correlation_matrix(
                {"a": pd.Series([0.1, 0.2]), "b": np.array([0.1, 0.2])})

    def test_equal_length_raw_arrays_still_work(self):
        """裸数组路径未被破坏（等长时照常出矩阵，注意默认 min_periods=20）。"""
        a = np.linspace(-0.01, 0.01, 30)
        corr = RiskAnalyzer().calculate_correlation_matrix({"a": a, "b": a * 2})
        assert corr.loc["a", "b"] == pytest.approx(1.0, abs=1e-12)

    def test_short_raw_arrays_are_nan_due_to_min_periods(self):
        """只有 3 个观测 < min_periods=20 → NaN（不是 1.0）。"""
        corr = RiskAnalyzer().calculate_correlation_matrix(
            {"a": np.array([0.01, -0.02, 0.03]),
             "b": np.array([0.02, -0.04, 0.06])})
        assert np.isnan(corr.loc["a", "b"])

    def test_aligned_series_accepted(self):
        idx = _bdates(30)
        corr = RiskAnalyzer().calculate_correlation_matrix({
            "a": pd.Series(np.linspace(-0.01, 0.01, 30), index=idx),
            "b": pd.Series(np.linspace(-0.01, 0.01, 30) * 2, index=idx),
        })
        assert list(corr.columns) == ["a", "b"]
        assert corr.loc["a", "b"] == pytest.approx(1.0, abs=1e-12)

    def test_disjoint_series_yield_nan_not_exception(self):
        corr = RiskAnalyzer().calculate_correlation_matrix({
            "a": pd.Series(np.linspace(-0.01, 0.01, 30), index=_bdates(30)),
            "b": pd.Series(np.linspace(-0.01, 0.01, 30),
                           index=_bdates(30, start=200)),
        })
        assert np.isnan(corr.loc["a", "b"])

    def test_overlap_below_min_periods_yields_nan(self):
        """重叠 10 天 < min_periods=20 → 标对为 NaN（不污染平均相关性）。"""
        corr = RiskAnalyzer().calculate_correlation_matrix({
            "a": pd.Series(np.linspace(-0.01, 0.01, 30), index=_bdates(30)),
            "b": pd.Series(np.linspace(-0.01, 0.01, 30),
                           index=_bdates(30, start=20)),
        }, min_periods=20)
        assert np.isnan(corr.loc["a", "b"])

    def test_single_asset_returns_empty_frame(self):
        corr = RiskAnalyzer().calculate_correlation_matrix(
            {"a": pd.Series(np.linspace(-0.01, 0.01, 30), index=_bdates(30))})
        assert corr.empty

    def test_empty_input_returns_empty_frame(self):
        assert RiskAnalyzer().calculate_correlation_matrix({}).empty


# ============================================================================
# 6. 输入口径守卫（2026-09-16）：跨期收益 / 折算尖峰 / 零方差 / 窗口过窄
#
# 背景：`_analyze_correlations` 的输入是 portfolio_snapshots.current_price
# （未复权持仓快照价），不是 etf_price_history.close。12 只场外基金是
# 「月末桩 + 日频」混合序列，且 2026-07 整月只有 07-31 一条，于是
# 06-30 → 07-31 的 31 天区间收益被当成「日收益」喂进相关矩阵。
# 实测影响（真实 34 只持仓副本，见交付报告）：场外×ETF 242 对均值 ρ
# 0.1277 → 0.3090；average_correlation 0.2618 → 0.3480；
# 对照组 ETF×ETF 231 对 Δ=0.0000（0/231），证明是系统性失真而非噪声。
# ============================================================================

class TestCrossPeriodGuard:
    """桩后第一天不得产出收益；且只置 NaN、不丢整行。"""

    STUB_FROM, STUB_TO = 9, 25   # dates[9] -> dates[25]：自然日间隔 24 天（> 12）

    def _build(self):
        dates = _bdates(60)
        px = [1.0]
        for k in range(1, 60):
            # 桩区间（D10~D25）走一段单边行情，让「跨期点」成为明显离群值；
            # 其余日期交替 +1.2%/-1.0%，保证收益序列方差非零（否则 ρ 无定义）。
            r = 0.02 if 10 <= k <= 25 else (0.012 if k % 2 else -0.010)
            px.append(px[-1] * (1.0 + r))
        etf = list(zip(dates, px))
        # 场外序列 = ETF 日期去掉桩区间 → 公共日上的日收益与 ETF 完全一致
        keep = set(dates[:10] + dates[25:])
        otc = [(d, p) for d, p in etf if d in keep]
        return {"600001": etf, "519770": otc}, dates, otc

    def test_stub_is_nan_not_a_fake_daily_return(self):
        series, dates, otc = self._build()
        spy = _SpyRiskAnalyzer()
        a = _make_analyzer(series)
        a.risk_analyzer = spy

        out = a._analyze_correlations([_pos("600001"), _pos("519770")], 60)

        lab = "519770_519770"
        s = spy.last_input[lab]
        stub = dates[self.STUB_TO]
        gap = (pd.Timestamp(stub) - pd.Timestamp(dates[self.STUB_FROM])).days
        assert gap > 12, "样例失效：桩间隔 %d 天未超过容差" % gap

        # 1) 跨期位置置 NaN —— 既不产出伪日收益，也不丢整行。
        #    传进矩阵的序列已按全体交易日并集对齐，故长度 59；其中 15 个 NaN 是
        #    「场外当天没有快照行」的对齐产物，第 16 个才是跨期守卫置的 NaN。
        assert pd.isna(s.loc[stub])
        assert len(s) == 59, "对齐后的索引必须保留跨期位置（NaN 占位，不丢行）"
        assert int(s.isna().sum()) == 16
        # 44 个可能收益里只有跨期那 1 个不可用（丢了整行的话这里会是 42）
        assert out["overlap_days"][lab] == 43
        assert out["cross_period_voided"][lab] == 1

        # 1b) 顺序约束：本样例的跨期收益 log(1.02^16)=0.317 > SPLIT_SPIKE_LOG_RET，
        #     若尖峰判定先跑，519770 会被误记成「折算」。跨期守卫必须先跑。
        assert abs(np.log(1.02 ** 16)) > SPLIT_SPIKE_LOG_RET, "样例失效"
        assert out["unreliable_codes"] == {}, out["unreliable_codes"]

        # 2) 剔除跨期点后，两个同源序列的真实相关性恢复为 +1.0（矩阵仍可用）
        got = out["correlation_matrix"]["600001_600001"][lab]
        assert got == pytest.approx(1.0, abs=1e-9)

        # 3) 反证：把跨期点当日收益喂进去，相关性被严重压低
        px_etf = np.array([p for _, p in series["600001"]], dtype=float)
        px_otc = np.array([p for _, p in otc], dtype=float)
        r_etf = pd.Series(np.diff(px_etf) / px_etf[:-1], index=dates[1:])
        r_otc = pd.Series(np.diff(px_otc) / px_otc[:-1],
                          index=[d for d, _ in otc][1:])
        idx = r_etf.index.intersection(r_otc.index)
        contaminated = float(np.corrcoef(r_etf[idx], r_otc[idx])[0, 1])
        assert contaminated < 0.9, (
            "样例失效：跨期点没把相关性压低（%r），无法区分修复前后" % contaminated)
        assert got - contaminated > 0.1

    def test_daily_segment_after_stub_is_kept(self):
        """守卫只能剔除桩后的那一个观测，不能连桩后面的日频段一起丢。"""
        series, dates, _ = self._build()
        spy = _SpyRiskAnalyzer()
        a = _make_analyzer(series)
        a.risk_analyzer = spy

        a._analyze_correlations([_pos("600001"), _pos("519770")], 60)
        s = spy.last_input["519770_519770"]

        after = [d for d in dates[26:] if d in s.index]
        assert len(after) == 34
        assert np.isfinite(s[after].values).all(), "桩之后的日频段必须全部保留"
        # 桩之前的日频段同样保留
        before = [d for d in dates[1:10] if d in s.index]
        assert len(before) == 9
        assert np.isfinite(s[before].values).all()


class TestSplitSpikeGuard:
    """份额折算（真实案例 159220 2025-11-10、512810 2025-06-23，1:2 折算）：
    快照价当天近似腰斩，|log| ≈ 0.68，远超 A 股涨跌停对应的 |log| ≤ 0.2231。
    """

    def _series(self, ratio=None):
        """ratio=None 时无折算；否则 D20 处价格乘以 ratio（1:2 折算 ≈ 0.5056）。"""
        dates = _bdates(45)
        vals = [1.0 + i * 0.001 for i in range(45)]
        if ratio is not None:
            vals[20] = vals[19] * ratio
            for i in range(21, 45):
                vals[i] = vals[i - 1] * 1.001
        return dates, list(zip(dates, vals))

    def test_split_spike_is_masked_and_recorded(self):
        dates, otc = self._series(0.5056)
        series = {"600001": _linear_prices(45, p0=2.0, step=0.002), "159220": otc}
        spy = _SpyRiskAnalyzer()
        a = _make_analyzer(series)
        a.risk_analyzer = spy

        out = a._analyze_correlations([_pos("600001"), _pos("159220")], 60)

        lab = "159220_159220"
        s = spy.last_input[lab]
        assert pd.isna(s.loc[dates[20]]), "折算日的伪收益必须置 NaN"
        # log(1.019 × 0.5056 / 1.019) = log(0.5056) = -0.6820
        rec = out["unreliable_codes"][lab]
        assert rec["hit_dates"] == [dates[20]]
        assert rec["max_abs_log_ret"] == pytest.approx(abs(np.log(0.5056)), abs=1e-4)
        assert rec["observations_voided"] == 1
        assert rec["reason"] == "split_or_split_like_spike"
        # 只丢折算那一个观测，其余收益仍在（不丢整行）
        assert out["overlap_days"][lab] == 43
        # 折算剔除后矩阵仍可用：ρ 等于「剔除折算观测后」的手算值
        px_a = np.array([p for _, p in series["600001"]], dtype=float)
        px_b = np.array([p for _, p in otc], dtype=float)
        ra = pd.Series(np.diff(px_a) / px_a[:-1], index=dates[1:])
        rb = pd.Series(np.diff(px_b) / px_b[:-1], index=dates[1:])
        keep = [d for d in dates[1:] if d != dates[20]]
        expected = float(np.corrcoef(ra[keep].values, rb[keep].values)[0, 1])
        got = out["correlation_matrix"]["600001_600001"][lab]
        # 返回的矩阵按 4 位小数舍入（corr_matrix.round(4)），故容差取 1e-4
        assert got == pytest.approx(expected, abs=1e-4), (
            "应基于剔除折算观测后的 43 个收益计算：got=%r expected=%r" % (got, expected))

        # 反证：保留 -50% 伪收益会把 ρ 拉到完全不同的值
        contaminated = float(np.corrcoef(ra.values, rb.values)[0, 1])
        assert abs(got - contaminated) > 0.1, (
            "守卫未改变结果：got=%r contaminated=%r" % (got, contaminated))

    def test_real_15_to_18pct_move_is_not_cleaned(self):
        """真实行情的同量级跳变不得被「顺手清洗」。

        真实案例：`159949` 2024-10-08 单日 ±18%（创业板涨跌停 20%），
        |log(1.18)| = 0.1655 < SPLIT_SPIKE_LOG_RET = 0.30 ⇒ 必须原样保留。

        ⚠️ `2026-06-30` 场外基金那根 +15.7%~+18.5% **不是同一回事**：经官方单位净值
        逐条比对（docs/handover/07_known_data_issues.md 问题十一）确认它是
        「陈旧价 → 真值」的水平修正、有效基期是 06-12，**应排除**。
        两者 |log_ret| 都落在 0.14~0.17，**本阈值在原理上无法区分**，
        只能靠「有效基期（last_real_date）」规则分开。
        故本条只验证「阈值不清洗真实跳变」，06-30 那类观测由有效基期规则负责，
        不要误以为本阈值已覆盖它。
        """
        dates, otc = self._series(1.18)
        series = {"600001": _linear_prices(45, p0=2.0, step=0.002), "159949": otc}
        spy = _SpyRiskAnalyzer()
        a = _make_analyzer(series)
        a.risk_analyzer = spy

        out = a._analyze_correlations([_pos("600001"), _pos("159949")], 60)

        s = spy.last_input["159949_159949"]
        assert pd.notna(s.loc[dates[20]])
        assert float(s.loc[dates[20]]) == pytest.approx(0.18, abs=1e-12)
        assert out["unreliable_codes"] == {}, "真实行情不得被折算守卫清洗"
        assert out["overlap_days"]["159949_159949"] == 44

    def test_threshold_is_strictly_greater_than_030_and_symmetric(self):
        """锁定阈值语义：严格大于 0.30 才置 NaN，且上下两侧对称。

        上侧 exp(+0.29) 保留 / exp(+0.31) 置 NaN；下侧 exp(-0.29) 保留 /
        exp(-0.31) 置 NaN（折算多为价格腰斩，下侧更常见，必须同样生效）。
        """
        dates, _ = self._series()
        for ratio, should_mask in ((np.exp(0.29), False),
                                   (np.exp(0.31), True),
                                   (np.exp(-0.29), False),
                                   (np.exp(-0.31), True)):
            _, otc = self._series(ratio)
            series = {"600001": _linear_prices(45, p0=2.0, step=0.002),
                      "600099": otc}
            spy = _SpyRiskAnalyzer()
            a = _make_analyzer(series)
            a.risk_analyzer = spy
            out = a._analyze_correlations([_pos("600001"), _pos("600099")], 60)

            s = spy.last_input["600099_600099"]
            if should_mask:
                assert pd.isna(s.loc[dates[20]])
                assert "600099_600099" in out["unreliable_codes"]
            else:
                assert pd.notna(s.loc[dates[20]])
                assert "600099_600099" not in out["unreliable_codes"]


class TestZeroVarianceGuard:
    def test_constant_nav_skipped_and_recorded(self):
        """货币基金式恒定快照价（生产案例 880013 天添利，单位净值恒 1.0）：
        收益恒 0、方差为 0 → ρ 无定义。必须显式丢弃并登记，而不是以 NaN 混进矩阵。
        """
        series = {"600001": _linear_prices(60),
                  "600002": _linear_prices(60, p0=2.0, step=0.002),
                  "880013": [(d, 1.0) for d in _bdates(60)]}
        positions = [_pos(c) for c in ("600001", "600002", "880013")]

        out = _make_analyzer(series)._analyze_correlations(positions, 60)

        assert "error" not in out, out
        cols = list(out["correlation_matrix"])
        assert len(cols) == 2
        assert not any(c.startswith("880013") for c in cols)
        assert out["skipped_codes"] == [
            {"code": "880013_880013", "reason": "zero_variance",
             "rows": 60, "valid_returns": 0}]
        assert out["overlap_days"]["880013_880013"] == 0


class TestWindowTooNarrowWarning:
    def test_days_le_min_overlap_warns_and_reports_reason(self, caplog):
        """days <= min_overlap 时所有标的必然被丢弃。修复前对此完全沉默，
        调用方拿到「数据不足」无法区分参数问题与数据问题。
        """
        series = {"600001": _linear_prices(60),
                  "600002": _linear_prices(60, p0=2.0, step=0.002)}
        with caplog.at_level(logging.WARNING, logger="src.analysis.portfolio_risk"):
            out = _make_analyzer(series)._analyze_correlations(
                [_pos("600001"), _pos("600002")], MIN_OVERLAP)

        assert out["error"] == "数据不足"
        assert out["reason"] == "window_too_narrow"
        assert "窗口过窄" in caplog.text
        assert out["skipped_codes"] and all(
            s["reason"] == "rows_le_min_overlap" for s in out["skipped_codes"])
        # 窗口够长时不得出现这个 reason
        ok = _make_analyzer(series)._analyze_correlations(
            [_pos("600001"), _pos("600002")], 60)
        assert ok.get("reason") != "window_too_narrow"


class TestGuardsAreNoOpOnCleanData:
    """守卫对「全交易日间隔、无折算」的干净输入必须是恒等变换（零附带损伤）。

    生产侧对照（34 只真实持仓副本）：ETF×ETF 231 对标对 Δ=0.0000。
    """

    def test_clean_daily_input_passes_through_unchanged(self):
        rng = np.random.default_rng(11)
        n = 50
        dates = _bdates(n)
        ra = list(rng.normal(0, 0.01, n - 1))
        ra[10], ra[20] = 0.05, -0.03          # 普通波动，远低于折算阈值
        rb = [0.6 * x + 0.002 for x in ra]    # 与 A 同源、带噪的关系

        series = {"600001": _prices_from_returns(dates, ra, p0=1.0),
                  "600002": _prices_from_returns(dates, rb, p0=2.0)}
        spy = _SpyRiskAnalyzer()
        a = _make_analyzer(series)
        a.risk_analyzer = spy
        out = a._analyze_correlations([_pos("600001"), _pos("600002")], 60)

        assert out["unreliable_codes"] == {}
        assert out["cross_period_voided"] == {}
        assert out["skipped_codes"] == []
        assert out["overlap_days"] == {"600001_600001": n - 1,
                                       "600002_600002": n - 1}
        assert int(spy.last_input["600001_600001"].notna().sum()) == n - 1

        expected = float(np.corrcoef(np.array(ra), np.array(rb))[0, 1])
        got = out["correlation_matrix"]["600001_600001"]["600002_600002"]
        assert got == pytest.approx(expected, abs=1e-9), (
            "干净输入上的相关系数被守卫改变：got=%r expected=%r" % (got, expected))


# ============================================================================
# 7. 收集完整性守卫：防止「双写合并」让用例静默消失
#
# 本文件在 2026-09-16 被两个 writer 并发写入过，产生了重复 `import logging` 与
# **重复的 `class TestCrossPeriodGuard`** —— 后者被 Python 静默覆盖，前一个类的
# 用例既不报错也不执行（pytest 只收到一个类）。这类缺口不会让 CI 变红，只会让
# 有效覆盖凭空下降，必须用静态检查 + 收集结果交叉核对钉死。
# ============================================================================

SOURCE = Path(__file__).read_text(encoding="utf-8")


def _ast_tree():
    return ast.parse(SOURCE)


def _module_test_functions():
    """AST 里所有 `test_*` 函数（**包含**因重复定义而被遮蔽掉的那些）。"""
    names = []
    for node in _ast_tree().body:
        if isinstance(node, ast.ClassDef) and node.name.startswith("Test"):
            names += [m.name for m in node.body
                      if isinstance(m, ast.FunctionDef) and m.name.startswith("test_")]
        elif isinstance(node, ast.FunctionDef) and node.name.startswith("test_"):
            names.append(node.name)
    return names


class TestCollectionCompleteness:
    def test_no_duplicate_class_names(self):
        """重复类名 = 前一个类的用例全部静默不执行。"""
        classes = [n.name for n in _ast_tree().body if isinstance(n, ast.ClassDef)]
        dup = sorted({c for c in classes if classes.count(c) > 1})
        assert not dup, (
            "重复定义的类会被后一个覆盖，前一个类的用例静默不执行：%s" % dup)

    def test_no_duplicate_method_names_within_class(self):
        dup = {}
        for node in _ast_tree().body:
            if not isinstance(node, ast.ClassDef):
                continue
            fns = [m.name for m in node.body if isinstance(m, ast.FunctionDef)]
            d = sorted({f for f in fns if fns.count(f) > 1})
            if d:
                dup[node.name] = d
        assert not dup, "类内重复方法名（后者覆盖前者）：%s" % dup

    def test_no_duplicate_imports(self):
        names = []
        for node in _ast_tree().body:
            if isinstance(node, ast.Import):
                names += [a.asname or a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom):
                names += ["%s.%s" % (node.module, a.asname or a.name)
                          for a in node.names]
        dup = sorted({n for n in names if names.count(n) > 1})
        assert not dup, "重复 import（双写合并残留）：%s" % dup

    def test_every_ast_test_function_is_collected(self, request):
        """AST 里的 `test_*` 数必须等于 pytest 实际收集数（缺口 = 被遮蔽的用例）。

        注意：本断言假定跑的是**整个文件**；用 `-k` 收窄会人为减少收集数而误报。
        """
        want = len(_module_test_functions())
        got = sum(1 for item in request.session.items
                  if item.module.__name__ == __name__
                  and item.originalname.startswith("test_"))
        assert got == want, (
            "AST 里有 %d 个 test_* 函数，实际只收集到 %d 个 —— 有 %d 个被重复类名/"
            "缩进问题静默吞掉了" % (want, got, want - got))
