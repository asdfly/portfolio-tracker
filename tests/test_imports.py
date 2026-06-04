"""L0 - 模块导入验证测试"""
import os
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def test_import_config():
    """导入 config.settings"""
    import config.settings


def test_import_database():
    """导入 src.utils.database"""
    from src.utils.database import get_db_connection, DatabaseManager


def test_import_chart_utils():
    """导入 src.utils.chart_utils 并验证所有公开函数存在"""
    from src.utils import chart_utils
    expected = [
        "downsample", "_add_min_max_annotations", "_cleanse_daily_returns",
        "_fmt", "_fmt_cell", "_sig", "_rsi_c", "_boll_c", "_atr_c",
    ]
    for name in expected:
        assert hasattr(chart_utils, name), f"chart_utils 缺少函数: {name}"
        assert callable(getattr(chart_utils, name)), f"chart_utils.{name} 不可调用"


def test_import_helpers():
    """导入 tabs._helpers"""
    import tabs._helpers


def test_import_dashboard_main():
    """dashboard_main 已归档到 archive/，改为验证 archive 存在"""
    assert (Path(__file__).parent.parent / "archive" / "dashboard_main.py").exists()


def test_import_tabs_package():
    """导入 tabs 包并验证 11 个 render_tab* 函数"""
    import tabs
    for i in range(1, 12):
        func_name = f"render_tab{i}"
        assert hasattr(tabs, func_name), f"tabs 缺少函数: {func_name}"
        assert callable(getattr(tabs, func_name)), f"tabs.{func_name} 不可调用"


# 逐个导入 Tab 模块
_TAB_MODULES = [
    "tabs.tab1_net_value",
    "tabs.tab2_position",
    "tabs.tab3_risk",
    "tabs.tab4_calendar",
    "tabs.tab5_advanced",
    "tabs.tab6_technical",
    "tabs.tab7_news",
    "tabs.tab8_advice",
    "tabs.tab9_custom",
    "tabs.tab10_fund_flow",
    "tabs.tab11_gold",
]

for i, mod_name in enumerate(_TAB_MODULES, 1):
    def _make_test(mn):
        def _test():
            __import__(mn)
        _test.__name__ = f"test_import_tab{i}"
        _test.__doc__ = f"导入 {mn}"
        globals()[f"test_import_tab{i}"] = _test
    _make_test(mod_name)
