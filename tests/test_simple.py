"""简单模块导入测试（已修复）"""

def test_import_tabs():
    import tabs
    assert hasattr(tabs, "render_tab1")

def test_import_config():
    import config.settings
    assert hasattr(config.settings, "DATABASE_PATH")
