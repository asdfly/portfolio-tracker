# 投资组合跟踪分析系统 - 全系统测试方案

> 生成时间：2026-05-14
> 项目路径：`C:\Users\HUAWEI\Documents\lingxi-claw\portfolio_tracker`

---

## 一、测试概述

### 1.1 项目现状

| 维度 | 状态 |
|------|------|
| 模块化拆分 | 11个Tab模块 + `_helpers.py` + `chart_utils.py` + `database.py` |
| 数据库表 | `portfolio_snapshots`(4行)、`portfolio_summary`(30行)、`index_quotes`(空)、`etf_technical`(空) |
| 现有测试 | 3个文件、7个测试函数，部分函数名不匹配 |
| 虚拟环境 | `venv\`（streamlit 1.57.0, pandas 3.0.3, numpy 2.4.4, plotly 6.7.0） |
| 统一调用签名 | `(positions, summary, index_quotes, selected_date, selected_benchmark)` |

### 1.2 测试目标

1. **修复现有测试** - 修正函数引用错误（`get_connection`->`get_db_connection`，`calc_ma`->不存在）
2. **模块导入验证** - 确认所有Tab模块、工具模块、配置模块可正常导入
3. **数据完整性测试** - 验证数据库表结构、索引、字段覆盖
4. **chart_utils纯函数测试** - 验证10个工具函数在各种输入下的行为
5. **database.py测试** - 验证`get_db_connection`、`DatabaseManager`核心方法
6. **Tab模块渲染测试** - 空数据/正常数据/部分数据边界条件
7. **集成冒烟测试** - 模拟`dashboard_main.py`的完整加载流程

### 1.3 测试分层

```
L0 - 导入与依赖检查（无数据库依赖）
L1 - 纯函数单元测试（chart_utils.py）
L2 - 数据库单元测试（database.py）
L3 - Tab模块渲染测试（需mock streamlit）
L4 - 集成冒烟测试（完整加载链路）
```

---

## 二、测试矩阵总览

| 层级 | 测试文件 | 用例数 | 依赖 |
|------|----------|--------|------|
| L0 | `test_config.py` | 5 | 无 |
| L0 | `test_imports.py` | 15 | 无 |
| L1 | `test_chart_utils.py` | 25 | numpy, pandas, plotly |
| L2 | `test_database_new.py` | 12 | 真实数据库 |
| L3 | `test_tab_render.py` | 22 | mock streamlit + 真实数据库 |
| L4 | `test_integration.py` | 4 | 全链路 |
| **合计** | | **83** | |

---

## 三、L0 - 导入与依赖检查

### 3.1 配置模块完整性（`test_config.py` - 5个用例）

| 用例 | 验证内容 |
|------|----------|
| `test_core_constants_exist` | DATABASE_PATH, INDEX_CODES, SECTOR_COLORS, ETF_CATEGORIES 已定义且类型正确 |
| `test_database_path_points_to_file` | DATABASE_PATH 指向的 .db 文件存在（或表已创建） |
| `test_index_codes_format` | INDEX_CODES 键格式为 sh/sz + 6位数字，值非空字符串 |
| `test_sector_colors_complete` | ETF_CATEGORIES 中所有 sector 值都在 SECTOR_COLORS 中有对应颜色 |
| `test_project_root_valid` | PROJECT_ROOT 是有效目录且包含 data/、tabs/ 子目录 |

### 3.2 模块导入验证（`test_imports.py` - 15个用例）

| 用例 | 导入目标 |
|------|----------|
| `test_import_config` | `config.settings` |
| `test_import_database` | `src.utils.database` |
| `test_import_chart_utils` | `src.utils.chart_utils` (10个函数可调用) |
| `test_import_helpers` | `tabs._helpers` |
| `test_import_dashboard_main` | `dashboard_main` |
| `test_import_tabs_package` | `tabs` (11个render_tab*函数) |
| `test_import_tab1` ~ `test_import_tab11` | 逐个导入每个Tab模块 |

---

## 四、L1 - chart_utils 纯函数测试

### 4.1 函数清单与边界

| 函数 | 边界条件 |
|------|----------|
| `downsample(df, max_points)` | n<=max_points原样; n=0空表; 自定义date_col |
| `_add_min_max_annotations(fig, x, y)` | 全NaN; 单点; 空数组; subplot row/col |
| `_cleanse_daily_returns(df, threshold)` | 全异常值; 无异常值; 空表; stats字典键 |
| `_fmt(v, suffix, dec, inv)` | 正/负/零/NaN/非数值字符串 |
| `_fmt_cell(val, suffix, decimals, invert)` | 同上 |
| `_sig(val, bull, bear, warn)` | bull/bear/warn集合/未知值 |
| `_rsi_c(v)` | 边界30/50/70/80 |
| `_boll_c(v)` | 布林位置边界 |
| `_atr_c(v)` | ATR百分比边界 |

### 4.2 用例（`test_chart_utils.py` - 25个）

```
downsample (5):
  test_downsample_no_reduction / test_downsample_exact / test_downsample_preserves_boundaries
  test_downsample_empty / test_downsample_custom_col

_annotations (4):
  test_annotations_basic / test_annotations_all_nan / test_annotations_single_point
  test_annotations_subplot

_cleanse (4):
  test_cleanse_normal / test_cleanse_with_outliers / test_cleanse_empty
  test_cleanse_stats_keys

_fmt/_fmt_cell (5):
  test_fmt_positive / test_fmt_negative / test_fmt_zero / test_fmt_nan
  test_fmt_cell_non_numeric

_sig (4):
  test_sig_bull / test_sig_bear / test_sig_warn / test_sig_unknown

_rsi_c/_boll_c/_atr_c (3):
  test_rsi_c_boundaries / test_boll_c_boundaries / test_atr_c_boundaries
```

---

## 五、L2 - 数据库测试

### 5.1 表结构与索引（`test_database_new.py` - 6个用例）

| 用例 | 验证内容 |
|------|----------|
| `test_database_file_exists` | portfolio.db 文件存在 |
| `test_all_four_tables_exist` | portfolio_snapshots, portfolio_summary, index_quotes, etf_technical |
| `test_snapshots_schema` | 包含 date/code/name/quantity/cost_price/current_price/market_value/pnl/pnl_rate/beta 列 |
| `test_index_quotes_schema` | 包含 date/code/name/close/change_pct/volume/amount 列 |
| `test_etf_technical_schema` | 包含 date/code/ma_signal/macd_signal/rsi_value/rsi_status/kdj_signal/bollinger_position/atr_pct/trend 列 |
| `test_indexes_exist` | 6个索引（idx_snap_date, idx_snap_code_date, idx_summary_date, idx_idx_quote_code_date, idx_tech_date, idx_tech_code_date） |

### 5.2 DatabaseManager CRUD（6个用例）

| 用例 | 验证内容 |
|------|----------|
| `test_init_creates_tables` | DatabaseManager初始化不报错 |
| `test_save_and_load_snapshot` | 保存持仓快照后可读取，字段一致 |
| `test_save_and_load_summary` | 保存汇总后可读取 |
| `test_save_index_quotes` | 保存指数行情后可查询 |
| `test_save_technical_indicators` | 保存技术指标后可查询 |
| `test_get_portfolio_history_order` | 历史数据按时间正序返回 |

---

## 六、L3 - Tab模块渲染测试

### 6.1 测试策略

Tab模块重度依赖`streamlit`（st.columns, st.plotly_chart, st.markdown等），使用`unittest.mock`将streamlit替换为MagicMock。仅验证渲染过程不抛异常，不验证UI输出细节。

### 6.2 数据fixture

| 场景 | positions | summary | index_quotes |
|------|-----------|---------|-------------|
| 全空 | `DataFrame()` | `DataFrame()` | `DataFrame()` |
| 最小 | 1行(9列) | 1行(4列) | `DataFrame()` |
| 正常 | 3行(9列) | 30行(4列) | 10行(7列) |

### 6.3 用例（`test_tab_render.py` - 22个）

```
空数据边界 (11个，每个Tab 1个):
  test_tab1_empty ~ test_tab11_empty

正常数据渲染 (5个，选关键Tab):
  test_tab1_normal / test_tab2_normal / test_tab3_normal
  test_tab5_normal / test_tab7_normal

部分数据边界 (6个):
  test_tab3_summary_one_row / test_tab4_empty_calendar / test_tab6_no_technical
  test_tab8_empty_advice / test_tab9_empty_custom / test_tab10_empty_flow
```

---

## 七、L4 - 集成冒烟测试

### 7.1 用例（`test_integration.py` - 4个）

| 用例 | 验证内容 |
|------|----------|
| `test_full_import_chain` | dashboard_main -> tabs -> chart_utils -> config 全链路无错 |
| `test_load_positions_real` | 通过database查询portfolio_snapshots，返回非空DataFrame |
| `test_load_summary_real` | 通过database查询portfolio_summary，返回30行数据 |
| `test_render_tab1_real_data` | 用真实数据调用render_tab1，不抛异常 |

---

## 八、执行计划

```powershell
cd C:\Users\HUAWEI\Documents\lingxi-claw\portfolio_tracker
.\venv\Scripts\activate

# 快速反馈（L0+L1，无DB依赖，<5秒）
pytest tests/test_config.py tests/test_imports.py tests/test_chart_utils.py -v

# 数据库测试（L2）
pytest tests/test_database_new.py -v

# Tab渲染测试（L3）
pytest tests/test_tab_render.py -v

# 集成测试（L4）
pytest tests/test_integration.py -v

# 全量运行
pytest tests/ -v --tb=short
```

---

## 九、现有测试修复清单

| 文件 | 问题 | 修复 |
|------|------|------|
| `conftest.py` | `conn.close()`会关闭缓存连接 | 移除close调用 |
| `test_database.py` | `from ... import get_connection` | 改为 `get_db_connection` |
| `test_simple.py` | `import dashboard`已不存在 | 改为 `import tabs` + `import dashboard_main` |
| `test_technical.py` | `from src.analysis.technical import calc_ma` 模块不存在 | 标记skip，注明原因 |

---

## 十、预期结果

| 层级 | 用例数 | 预期通过 | 预期失败原因 |
|------|--------|---------|-------------|
| L0 | 20 | ~18 | 路径/配置差异 |
| L1 | 25 | 25 | 纯函数，输入输出确定 |
| L2 | 12 | 10 | 依赖数据库实际状态 |
| L3 | 22 | ~18 | mock可能不完整 |
| L4 | 4 | 3 | 真实数据边界 |
| **合计** | **83** | **~74** | |
