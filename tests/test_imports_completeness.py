"""L0.5 - 静态符号完整性检查

防止 tabs/_helpers.py 等公共模块遗漏第三方库 import，
同时确保每个 Tab 模块导入的 helper 函数在其来源中确实存在。
"""

import ast
import os
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# ── 第三方库别名 → 期望来源 ──────────────────────────────────────────
THIRD_PARTY_ALIASES = {
    "pd": "pandas",
    "np": "numpy",
    "st": "streamlit",
    "go": "plotly.graph_objects",
    "px": "plotly.express",
    "plt": "matplotlib.pyplot",
    "ak": "akshare",
}

# ── 需要扫描的项目源码目录 ───────────────────────────────────────────
_SCAN_DIRS = ["tabs", "src", "config", "gold_components"]
# 顶层 .py 文件（dashboard.py 等）
_SCAN_ROOT_FILES = ["dashboard.py", "dashboard_main.py"]


def _project_py_files():
    """返回所有需要扫描的 .py 绝对路径"""
    result = []
    for d in _SCAN_DIRS:
        p = project_root / d
        if p.is_dir():
            for f in p.rglob("*.py"):
                if "__pycache__" not in str(f):
                    result.append(f)
    for name in _SCAN_ROOT_FILES:
        p = project_root / name
        if p.exists():
            result.append(p)
    return result


def _collect_imports(tree):
    """从 AST 收集 import 的本地别名 → 来源模块"""
    imported = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imported[alias.asname or alias.name] = alias.name
        elif isinstance(node, ast.ImportFrom):
            for alias in node.names:
                if alias.name != "*":
                    local = alias.asname or alias.name
                    mod = f"{node.module}.{alias.name}" if node.module else alias.name
                    imported[local] = mod
    return imported


# ── 测试：第三方库 import 完整性 ─────────────────────────────────────

def test_third_party_imports_complete():
    """每个使用第三方库属性（如 go.Figure）的文件都必须 import 该库"""
    all_files = _project_py_files()
    issues = []

    for filepath in all_files:
        source = filepath.read_text(encoding="utf-8")
        try:
            tree = ast.parse(source)
        except SyntaxError:
            continue

        imported = _collect_imports(tree)

        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
                bare = node.value.id
                if bare in THIRD_PARTY_ALIASES and bare not in imported:
                    rel = filepath.relative_to(project_root)
                    issues.append(f"{rel}:{node.lineno} '{bare}.{node.attr}' used but '{bare}' not imported")

    if issues:
        # 只报告 (file, bare_name) 去重后的结果
        unique = sorted(set(i.split(" ")[0] for i in issues))
        msg = "Third-party import missing:\n" + "\n".join(f"  {u}" for u in unique)
        assert False, msg


# ── 测试：helper 函数导入一致性 ─────────────────────────────────────
# tabs/ 包的 __init__.py 导出的 render_tab* 函数必须在其来源模块中存在

_TAB_RENDER_FUNCS = [f"render_tab{i}" for i in range(1, 12)]


def test_tab_render_functions_exist():
    """tabs.__init__ 中注册的 render_tab* 函数在对应子模块中必须存在"""
    import tabs
    for func_name in _TAB_RENDER_FUNCS:
        assert hasattr(tabs, func_name), f"tabs.{func_name} not found in tabs/__init__.py"


def test_tab_render_functions_callable():
    """所有 render_tab* 函数必须是可调用的"""
    import tabs
    for func_name in _TAB_RENDER_FUNCS:
        obj = getattr(tabs, func_name)
        assert callable(obj), f"tabs.{func_name} is not callable"


# ── 测试：_helpers 公共函数在所有 Tab 模块中可访问 ───────────────────

_HELPERS_PUBLIC = [
    "_render_etf_detail_panel",
    "load_etf_detail",
    "load_etf_price_history",
    "_generate_oneclick_report",
    "_load_latest_news",
    "_load_tech_signals",
]


def test_helpers_public_functions_exist():
    """tabs._helpers 中导出的公共函数必须存在"""
    import tabs._helpers as helpers
    for name in _HELPERS_PUBLIC:
        assert hasattr(helpers, name), f"tabs._helpers.{name} not found"
        assert callable(getattr(helpers, name)), f"tabs._helpers.{name} not callable"


def test_helpers_exports_match_consumers():
    """验证 Tab 模块从 _helpers 导入的名称确实存在于 _helpers 中"""
    # Collect all 'from tabs._helpers import X' across tab modules
    tab_dir = project_root / "tabs"
    imported_names = set()
    for f in tab_dir.glob("tab*.py"):
        source = f.read_text(encoding="utf-8")
        try:
            tree = ast.parse(source)
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                if (node.module or "") == "tabs._helpers":
                    for alias in node.names:
                        imported_names.add(alias.name)

    import tabs._helpers as helpers
    missing = [name for name in imported_names if not hasattr(helpers, name)]
    assert not missing, f"Imported from _helpers but not defined: {missing}"
