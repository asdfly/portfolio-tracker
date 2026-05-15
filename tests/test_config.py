"""L0 - 配置模块完整性测试"""
import os
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def test_core_constants_exist():
    """验证核心配置常量已定义且类型正确"""
    import config.settings as s
    assert hasattr(s, "DATABASE_PATH"), "DATABASE_PATH 未定义"
    assert hasattr(s, "INDEX_CODES"), "INDEX_CODES 未定义"
    assert hasattr(s, "SECTOR_COLORS"), "SECTOR_COLORS 未定义"
    assert hasattr(s, "ETF_CATEGORIES"), "ETF_CATEGORIES 未定义"
    assert hasattr(s, "PROJECT_ROOT"), "PROJECT_ROOT 未定义"
    assert isinstance(s.INDEX_CODES, dict), "INDEX_CODES 应为 dict"
    assert isinstance(s.SECTOR_COLORS, dict), "SECTOR_COLORS 应为 dict"
    assert isinstance(s.ETF_CATEGORIES, dict), "ETF_CATEGORIES 应为 dict"


def test_database_path_type():
    """验证 DATABASE_PATH 是 Path 对象"""
    import config.settings as s
    assert isinstance(s.DATABASE_PATH, Path)


def test_index_codes_format():
    """验证 INDEX_CODES 键格式为 sh/sz + 6位数字"""
    import config.settings as s
    import re
    pattern = re.compile(r"^(sh|sz)\d{6}$")
    for code, name in s.INDEX_CODES.items():
        assert pattern.match(code), f"指数代码格式错误: {code}"
        assert isinstance(name, str) and len(name) > 0, f"指数名称为空: {code}"


def test_sector_colors_complete():
    """验证 ETF_CATEGORIES 中所有 sector 都在 SECTOR_COLORS 中有颜色映射"""
    import config.settings as s
    sectors_used = {info["sector"] for info in s.ETF_CATEGORIES.values() if "sector" in info}
    missing = sectors_used - set(s.SECTOR_COLORS.keys())
    assert not missing, f"以下行业缺少颜色映射: {missing}"


def test_project_root_valid():
    """验证 PROJECT_ROOT 是有效目录且包含预期子目录"""
    import config.settings as s
    assert s.PROJECT_ROOT.is_dir(), f"PROJECT_ROOT 不是有效目录: {s.PROJECT_ROOT}"
    assert (s.PROJECT_ROOT / "data").is_dir(), "data/ 子目录不存在"
    assert (s.PROJECT_ROOT / "tabs").is_dir(), "tabs/ 子目录不存在"
    assert (s.PROJECT_ROOT / "config").is_dir(), "config/ 子目录不存在"
