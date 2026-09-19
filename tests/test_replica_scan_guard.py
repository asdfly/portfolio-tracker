"""问题十一 —— 复制行守卫**生产侧**回归用例（`_scan_replica_rows` / `_replica_key`）。

背景
----
`tests/test_replica_void.py` 只覆盖**消费侧** `src.analysis.replica_void` 的几个封装；
判据本体 `_scan_replica_rows`（`src/analysis/portfolio_risk.py`）在 `tests/` 下此前**零覆盖**，
只在 `audit/_verify_replica_guard.py` 里被 AST 抽取跑过，不进 CI ⇒ 阈值被改没有任何测试会拦。

本文件把判据**逐条锁进 CI**。每个用例的期望值都来自
`src/analysis/portfolio_risk.py` 顶部「复制行（fill-forward 陈旧行）守卫阈值 —— 两级判据」
注释块，**不是凭直觉写的**：

- Tier1（段长）：`L >= COPY_RUN_MIN_LEN(5)` ⇒ 该段全部 L 行（**含段首**）都不是观测。
- Tier2（横截面广度，**仅场外篮子**，`config.settings.is_otc_fund`）：
  `n >= COPY_BREADTH_MIN_N(3)` 且 `c * 2 >= n`（占比 >= 50%）⇒ 这 c 只当日都不是观测。
  注释明确警告：判据**只约束分母 n**，不额外约束 c（n=4、c=2 恰好 50% 即命中）。
- `_replica_key`：`(current_price, round(market_value, 2))`，任一字段缺失返回 `None`。

刻意**硬编码**段长/只数（5、4、3、2、12）而不是从常量派生：本文件的目的就是
「常量被改时用例要红」。若改成从 `COPY_RUN_MIN_LEN` 派生，阈值被改时用例会跟着变绿，
守卫就形同虚设。

不依赖生产库：全部用内存合成数据（conftest 的 DB 隔离守卫与本文件无关）。
场外标的直接复用 `config.settings.OTC_FUND_CODES` 里的真实代码，不 monkeypatch ——
`is_otc_fund` 是纯函数（`str(code) in OTC_FUND_CODES`），用真实代码比打桩更能反映生产行为。
"""
from src.analysis.portfolio_risk import _replica_key, _scan_replica_rows

# 场外篮子（config.settings.OTC_FUND_CODES，共 13 只；此处取 12 只，不含 880013 货币基金）
OTC12 = [
    "001194", "001323", "001407", "001437", "001765", "002152",
    "007994", "008269", "027293", "100032", "166301", "519770",
]

# 场内 ETF（不在场外篮子内），用作 Tier2「仅场外」的负对照
ETF = "512010"

D0 = "2026-08-03"
D1 = "2026-08-04"


def _row(date, price, mv):
    """一行快照；键与 `_replica_key` / `_scan_replica_rows` 读取的字段一致。"""
    return {"date": date, "current_price": price, "market_value": mv}


def _series(entries):
    """`[(code, [(date, price, mv), ...]), ...]` -> `_scan_replica_rows` 要求的 `series`。"""
    return [(code, "%s_名称" % code, [_row(*r) for r in rows]) for code, rows in entries]


def _two_rows(code, flat, i):
    """给一只标的造 D0/D1 两行：`flat=True` 时两行 key 相同（与上一行同值）。"""
    p0 = 1.0 + i
    p1 = p0 if flat else p0 + 0.5      # 只动 price：key 变即可，mv 保持不变
    return (code, [(D0, p0, 1000.0), (D1, p1, 1000.0)])


# ---------------------------------------------------------------- Tier1（段长）

def test_tier1_run_of_five_voids_all_including_head():
    """同 key 连续 5 行 ⇒ 5 行全部作废，**含段首**（段首装的同样是陈旧值）。"""
    dates = ["2026-08-%02d" % d for d in range(3, 8)]      # 03 ~ 07，共 5 行
    hist = [_row(d, 1.234, 10000.0) for d in dates]
    out = _scan_replica_rows([("001194", "001194_名称", hist)])

    assert out.get("001194") == {d: (1, 5) for d in dates}, (
        "Tier1 段长 5 应把这 5 行（含段首 %s）全部标为 tier=1、段长=5" % dates[0])


def test_tier1_run_of_four_is_not_voided():
    """同 key 连续 4 行 ⇒ **不**作废（阈值是 5，不是 4）。"""
    dates = ["2026-08-%02d" % d for d in range(3, 7)]      # 03 ~ 06，共 4 行
    hist = [_row(d, 1.234, 10000.0) for d in dates]
    out = _scan_replica_rows([("001194", "001194_名称", hist)])

    assert out == {}, "段长 4 < 5，不应有任何行被作废"


# ------------------------------------------------- Tier2（横截面广度，仅场外）

def test_tier2_whole_basket_flat_voids_otc_only():
    """场外 12 只全部与各自上一行同 key（12/12 = 100%）⇒ 这 12 只当日全作废；场内不受影响。"""
    entries = [_two_rows(code, flat=True, i=i) for i, code in enumerate(OTC12)]
    # 场内 ETF 同样与上一行同值，但 Tier2 只在场外篮子内统计 ⇒ 不得被作废
    entries.append(_two_rows(ETF, flat=True, i=50))

    out = _scan_replica_rows(_series(entries))

    for code in OTC12:
        assert out.get(code) == {D1: (2, 2)}, (
            "%s 在 %s 应被 Tier2 作废（tier=2、段长=2）" % (code, D1))
    assert ETF not in out, "场内 ETF 不在场外篮子内，不得被 Tier2 作废"


def test_tier2_quarter_basket_flat_is_not_voided():
    """12 只中仅 3 只同 key（3/12 = 25% < 50%）⇒ **不**作废。"""
    entries = [_two_rows(code, flat=(i < 3), i=i) for i, code in enumerate(OTC12)]
    out = _scan_replica_rows(_series(entries))

    assert out == {}, "占比 3/12 = 25% < 50%，不应构成横截面证据"


def test_tier2_n_below_min_is_not_voided():
    """可比场外 n = 2（< 3），即便 2/2 = 100% ⇒ **不**作废（判据只约束分母 n）。"""
    entries = [_two_rows(code, flat=True, i=i) for i, code in enumerate(OTC12[:2])]
    out = _scan_replica_rows(_series(entries))

    assert out == {}, "n=2 < COPY_BREADTH_MIN_N(3)，不构成横截面证据"


def test_tier2_exactly_half_basket_hits():
    """边界 n = 4、c = 2（恰好 50%）⇒ **命中作废**。

    注释特别警告：把下限定在 c 上（而非分母 n 上）会漏掉小篮子事件。
    """
    four = OTC12[:4]
    flat_codes = set(four[:2])
    entries = [_two_rows(code, flat=(code in flat_codes), i=i) for i, code in enumerate(four)]

    out = _scan_replica_rows(_series(entries))

    assert set(out) == flat_codes, "恰好 50% 应命中，且只作废那 2 只同值标的"
    for code in flat_codes:
        assert out[code] == {D1: (2, 2)}
    for code in four[2:4]:
        assert code not in out, "%s 当日是真观测，不得作废" % code


# ------------------------------------------------------- `_replica_key` 取整语义

def test_replica_key_rounds_market_value_to_cents():
    """`market_value` 必须 `round(..., 2)` 再比 —— 这是历史上真踩过的坑。

    写入侧存在亚分位抖动：2026-06-19 的 001407 是 `55498.0 -> 55497.997`
    （quantity 逐位不变）。逐位比较会在此断链，把复制链基期刷到 06-19，
    使 06-30 伪收益的 gap 缩到 11 天而被 12 天闸门放行。
    """
    a = _replica_key(_row("2026-06-18", 6.575, 55498.0))
    b = _replica_key(_row("2026-06-19", 6.575, 55497.997))
    assert a == b, "亚分位抖动必须被 round 到分后判为同一 key"
    assert a == (6.575, 55498.0)

    # 同上取整语义的后果：含抖动行的 5 行链不能被断成两段
    dates = ["2026-06-15", "2026-06-17", "2026-06-18", "2026-06-19", "2026-06-22"]
    mvs = [55498.0, 55498.0, 55498.0, 55497.997, 55498.0]
    hist = [_row(d, 6.575, mv) for d, mv in zip(dates, mvs)]
    out = _scan_replica_rows([("001407", "001407_名称", hist)])
    assert out.get("001407") == {d: (1, 5) for d in dates}, (
        "抖动行不得断链：5 行应整体构成段长 5 的复制段")


def test_missing_price_or_mv_is_none_and_row_not_judged():
    """`current_price` / `market_value` 缺失 ⇒ key 为 None，该行**不参与判等**。

    证据不足不下结论：既不静默作废，也不静默放行。
    """
    assert _replica_key({"current_price": 1.0}) is None
    assert _replica_key({"market_value": 10.0}) is None
    assert _replica_key({}) is None

    # 中间那行 key 为 None ⇒ 自成断点，且它自己与两侧都不参与判等
    hist = [
        _row("2026-08-03", 1.0, 100.0),
        _row("2026-08-04", 1.0, None),
        _row("2026-08-05", 1.0, 100.0),
    ]
    out = _scan_replica_rows([("001194", "001194_名称", hist)])

    assert out == {}, "缺失行不得被作废，也不得把两侧拼成一段"
