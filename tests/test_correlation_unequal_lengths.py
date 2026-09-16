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

本文件不依赖生产库：以 `__new__` 构造分析器并注入 stub DB，
不调用 DatabaseManager（其 __init__ 会连库执行 DDL），全程不打开任何 SQLite 文件。
"""
import numpy as np
import pandas as pd
import pytest

from src.analysis.portfolio_risk import PortfolioRiskAnalyzer
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
        # 已记录的缺口：成功返回时 overlap_days 只覆盖进入矩阵的标的，
        # 本循环里被跳过的标的不会出现在这张表里（详见测试文件外的验证报告）。
        assert "519770_519770" not in out["overlap_days"]

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

        构造：唯一一次跳变发生在 D10（1.0 → 2.0），其余恒定。
        正确结果：收益 +1.0 落在 D10，-0.5 落在 D11，其余为 0。
        """
        dates = _bdates(40)
        jump = [(d, 2.0 if i == 10 else 1.0) for i, d in enumerate(dates)]
        series = {"600001": jump, "600002": _linear_prices(40, p0=5.0)}
        spy = _SpyRiskAnalyzer()
        a = _make_analyzer(series)
        a.risk_analyzer = spy

        a._analyze_correlations([_pos("600001"), _pos("600002")], 60)

        s = spy.last_input["600001_600001"]
        assert list(s.index) == sorted(s.index), "传给相关矩阵的索引必须升序"
        assert float(s.loc[dates[10]]) == pytest.approx(1.0, abs=1e-12)
        assert float(s.loc[dates[11]]) == pytest.approx(-0.5, abs=1e-12)
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
        assert out == {"error": "数据不足", "overlap_days": {},
                       "min_overlap": MIN_OVERLAP}

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
