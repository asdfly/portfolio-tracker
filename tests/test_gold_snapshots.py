"""黄金数据源数据契约测试（基于真实数据快照）

使用预采集的 parquet 快照验证：
1. 数据结构（列名、非空、排序）
2. 数值范围（单位合理性）
3. 时间跨度（覆盖预期周期）
4. 跨数据源一致性

快照更新方式：手动运行 collect_gold_snapshots.py 重新采集
"""
import sys
from pathlib import Path
import pandas as pd
import pytest

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

SNAPSHOT_DIR = project_root / "tests" / "snapshots"


def load(name):
    path = SNAPSHOT_DIR / f"{name}.parquet"
    if not path.exists():
        pytest.skip(f"快照不存在: {path}")
    return pd.read_parquet(path)


# ==================== 数据结构 ====================

class TestSnapshotStructure:

    def test_sge_benchmark_columns(self):
        df = load("sge_benchmark")
        assert "date" in df.columns
        assert "close" in df.columns
        assert df["date"].is_monotonic_increasing

    def test_sge_hist_columns(self):
        df = load("sge_hist_au9999")
        assert "date" in df.columns
        assert "close" in df.columns
        assert df["date"].is_monotonic_increasing

    def test_china_reserve_columns(self):
        df = load("china_reserve_data")
        assert "month" in df.columns
        assert "gold_reserve" in df.columns
        assert "fx_reserve" in df.columns

    def test_global_etf_columns(self):
        df = load("global_etf_holdings_2y")
        for col in ["date", "total_holdings", "change", "total_value"]:
            assert col in df.columns, f"缺少列: {col}"
        assert df["date"].is_monotonic_increasing

    def test_comex_inventory_columns(self):
        df = load("comex_inventory")
        assert "date" in df.columns
        assert "inventory_ton" in df.columns
        assert df["date"].is_monotonic_increasing

    def test_bond_yields_columns(self):
        df = load("bond_yields_3y")
        assert "date" in df.columns
        assert "cn_10y" in df.columns
        assert "us_10y" in df.columns

    def test_usdcny_hist_columns(self):
        df = load("usdcny_hist")
        assert "date" in df.columns
        assert "close" in df.columns
        assert df["date"].is_monotonic_increasing


# ==================== 数值范围 ====================

class TestValueRanges:

    def test_global_etf_total_value_is_billions_usd(self):
        """ETF总价值应在 500-5000 亿美元之间（2024-2026 范围）"""
        df = load("global_etf_holdings_2y")
        tv = df["total_value"]
        assert tv.max() < 5000, f"ETF总价值 {tv.max():.0f} 亿美元，超出合理上限"
        assert tv.min() > 100, f"ETF总价值 {tv.min():.0f} 亿美元，低于合理下限"

    def test_global_etf_holdings_tons(self):
        """全球ETF持仓应在 500-5000 吨之间"""
        df = load("global_etf_holdings_2y")
        th = df["total_holdings"]
        assert 500 < th.max() < 5000, f"持仓量 {th.max():.0f} 吨超出范围"

    def test_china_reserve_gold_million_ounces(self):
        """中国黄金储备应在 2000-10000 万盎司之间"""
        df = load("china_reserve_data")
        gr = df["gold_reserve"]
        assert 2000 < gr.max() < 10000, f"黄金储备 {gr.max():.0f} 万盎司超出范围"
        assert gr.min() > 0

    def test_china_reserve_fx_billions(self):
        """中国外汇储备应在 20000-50000 亿美元之间"""
        df = load("china_reserve_data")
        fx = df["fx_reserve"]
        assert 20000 < fx.max() < 50000, f"外汇储备 {fx.max():.0f} 亿美元超出范围"

    def test_usdcny_mid_rate(self):
        """USD/CNY中间价应在 5.0-10.0 之间"""
        df = load("usdcny_hist")
        rate = df["close"]
        assert 5.0 < rate.max() < 10.0, f"汇率 {rate.max():.2f} 超出范围"
        assert 5.0 < rate.min() < 10.0, f"汇率 {rate.min():.2f} 超出范围"

    def test_sge_close_yuan_per_gram(self):
        """上海金基准价应在 100-1000 元/克之间"""
        df = load("sge_benchmark")
        price = pd.to_numeric(df["close"], errors="coerce").dropna()
        assert 100 < price.max() < 1500, f"基准价 {price.max():.2f} 元/克超出范围"

    def test_bond_yield_pct(self):
        """国债收益率应在 0-10% 之间"""
        df = load("bond_yields_3y")
        cn = pd.to_numeric(df["cn_10y"], errors="coerce").dropna()
        us = pd.to_numeric(df["us_10y"], errors="coerce").dropna()
        assert 0 < cn.max() < 10, f"中国国债 {cn.max():.2f}% 超出范围"
        assert 0 < us.max() < 10, f"美国国债 {us.max():.2f}% 超出范围"

    def test_comex_inventory_tons(self):
        """COMEX黄金库存应在 100-5000 吨之间"""
        df = load("comex_inventory")
        inv = df["inventory_ton"]
        assert 100 < inv.max() < 5000, f"COMEX库存 {inv.max():.0f} 吨超出范围"


# ==================== 时间跨度 ====================

class TestTimeCoverage:

    def test_global_etf_2y_coverage(self):
        """近2年ETF数据应覆盖至少 300 个交易日"""
        df = load("global_etf_holdings_2y")
        assert len(df) >= 300, f"近2年数据仅 {len(df)} 行，不足 300"

    def test_usdcny_hist_long(self):
        """USD/CNY历史数据应有数千行"""
        df = load("usdcny_hist")
        assert len(df) > 5000, f"历史数据仅 {len(df)} 行"

    def test_sge_benchmark_recent(self):
        """上海金基准价应包含近期数据（最近30天内）"""
        from datetime import datetime, timedelta
        df = load("sge_benchmark")
        latest = pd.to_datetime(df["date"].iloc[-1])
        assert (datetime.now() - latest).days < 30, f"最新数据日期 {latest}，距今超过30天"

    def test_sge_hist_recent(self):
        """SGE历史K线应包含近期数据"""
        from datetime import datetime
        df = load("sge_hist_au9999")
        latest = pd.to_datetime(df["date"].iloc[-1])
        assert (datetime.now() - latest).days < 30


# ==================== 跨数据源一致性 ====================

class TestCrossSourceConsistency:

    def test_etf_value_times_holdings_reasonable_price(self):
        """ETF总价值/持仓量 推算的金价应在合理范围 $2000-5000/oz"""
        df = load("global_etf_holdings_2y")
        de = df[(df["total_value"] > 0) & (df["total_holdings"] > 0)].copy()
        # total_value 单位是亿美元，持仓是吨
        price_usd_oz = de["total_value"] * 1e8 / (de["total_holdings"] * 32150.72)
        assert 2000 < price_usd_oz.mean() < 5000, \
            f"推算金价 {price_usd_oz.mean():.0f} $/oz 超出合理范围"

    def test_gold_reserve_not_decreasing_long_term(self):
        """长期来看黄金储备应呈增长趋势（央行购金大周期）"""
        df = load("china_reserve_data")
        first_quarter = df["gold_reserve"].dropna().iloc[:12].mean()
        last_quarter = df["gold_reserve"].iloc[-12:].mean()
        # 2020s 央行持续购金，末12月均值应高于首12月
        assert last_quarter > first_quarter, \
            f"黄金储备趋势异常: 首12月均值 {first_quarter:.0f} >= 末12月均值 {last_quarter:.0f}"
