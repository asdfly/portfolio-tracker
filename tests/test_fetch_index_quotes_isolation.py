"""回归测试：单只指数取数失败（DataSourceError）不得中断整轮主分析。

根因（2026-09-24）：``_fetch_index_quotes`` 原仅 ``except OSError``，但
``DataSourceManager.get_quote`` 在故障转移全失败后抛 ``DataSourceError``
（继承 ``Exception``，非 ``OSError``），未被捕获 → 上抛中断整轮 → run_status=failed，
basic/risk/monitor/dq_check 全缺。修复后捕获 ``(DataSourceError, OSError)``，单只失败仅跳过。
"""
import types

from src.analysis.portfolio import PortfolioAnalyzer, INDEX_CODES
from src.data_sources.base import DataSourceError


def _analyzer_with_failing(failing_code: str, fallback=None):
    """构造 analyzer 桩；``fallback`` 模拟 DB 缓存兜底的返回值（None=也无缓存）。

    ⚠ 必须同时桩掉 ``_fallback_index_quote_from_db``（2026-10-14 修复）：
      该函数是 2026-09-24 新增的 DB 缓存兜底，实时取数失败后就会被调用。
      用 SimpleNamespace 构造的桩缺此属性会抛 AttributeError，把「故障是否被
      隔离」这个真正被测点掩盖成本不相干的报错。
    """
    ds = types.SimpleNamespace()

    def get_quote(code):
        if code == failing_code:
            raise DataSourceError(f"所有数据源都无法获取 {code}")
        return {"price": 1.0, "change_pct": 0.0}

    ds.get_quote = get_quote
    analyzer = types.SimpleNamespace(ds_manager=ds)
    analyzer._fallback_index_quote_from_db = lambda code: fallback
    return analyzer


def test_single_index_failure_is_isolated():
    """sh932000 取数失败应被跳过，其余基准指数全部保留，方法不抛异常。"""
    analyzer = _analyzer_with_failing("sh932000")
    result = PortfolioAnalyzer._fetch_index_quotes(analyzer)

    assert "sh932000" not in result
    assert len(result) == len(INDEX_CODES) - 1

    other = next(c for c in INDEX_CODES if c != "sh932000")
    assert result[other]["price"] == 1.0


def test_index_fallback_keeps_cached_quote_marked():
    """实时失败但 DB 有缓存 ⇒ 该指数以 _cached 形式保留（不多不少正好全集）。

    对应 2026-09-24 中证2000 场景：实时源被 host 级 RST 阻断，靠 DB 缓存续命，
    此时不能把它当作"取数失败"丢掉。
    """
    cached = {"name": "中证2000", "price": 2500.0, "change_pct": None,
              "_cached": True, "_cached_date": "2026-10-13"}
    analyzer = _analyzer_with_failing("sh932000", fallback=cached)
    result = PortfolioAnalyzer._fetch_index_quotes(analyzer)

    assert len(result) == len(INDEX_CODES), "有缓存兜底时指数不得缺失"
    assert result["sh932000"] is cached
    assert result["sh932000"]["_cached"] is True


def test_no_failure_returns_all():
    """无失败时返回全部基准指数。"""
    ds = types.SimpleNamespace()
    ds.get_quote = lambda code: {"price": 2.0, "change_pct": 0.0}
    analyzer = types.SimpleNamespace(ds_manager=ds)

    result = PortfolioAnalyzer._fetch_index_quotes(analyzer)
    assert len(result) == len(INDEX_CODES)
