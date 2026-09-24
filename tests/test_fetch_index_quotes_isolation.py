"""回归测试：单只指数取数失败（DataSourceError）不得中断整轮主分析。

根因（2026-09-24）：``_fetch_index_quotes`` 原仅 ``except OSError``，但
``DataSourceManager.get_quote`` 在故障转移全失败后抛 ``DataSourceError``
（继承 ``Exception``，非 ``OSError``），未被捕获 → 上抛中断整轮 → run_status=failed，
basic/risk/monitor/dq_check 全缺。修复后捕获 ``(DataSourceError, OSError)``，单只失败仅跳过。
"""
import types

from src.analysis.portfolio import PortfolioAnalyzer, INDEX_CODES
from src.data_sources.base import DataSourceError


def _analyzer_with_failing(failing_code: str):
    ds = types.SimpleNamespace()

    def get_quote(code):
        if code == failing_code:
            raise DataSourceError(f"所有数据源都无法获取 {code}")
        return {"price": 1.0, "change_pct": 0.0}

    ds.get_quote = get_quote
    return types.SimpleNamespace(ds_manager=ds)


def test_single_index_failure_is_isolated():
    """sh932000 取数失败应被跳过，其余基准指数全部保留，方法不抛异常。"""
    analyzer = _analyzer_with_failing("sh932000")
    result = PortfolioAnalyzer._fetch_index_quotes(analyzer)

    assert "sh932000" not in result
    assert len(result) == len(INDEX_CODES) - 1

    other = next(c for c in INDEX_CODES if c != "sh932000")
    assert result[other]["price"] == 1.0


def test_no_failure_returns_all():
    """无失败时返回全部基准指数。"""
    ds = types.SimpleNamespace()
    ds.get_quote = lambda code: {"price": 2.0, "change_pct": 0.0}
    analyzer = types.SimpleNamespace(ds_manager=ds)

    result = PortfolioAnalyzer._fetch_index_quotes(analyzer)
    assert len(result) == len(INDEX_CODES)
