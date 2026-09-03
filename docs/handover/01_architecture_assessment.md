# 架构交接评估（Architecture Handover Assessment）

- 评估对象：`lingxi-claw/portfolio_tracker`（Streamlit + SQLite 量化分析仪表盘）
- 评估人：首席架构师 高见远（mvp-dev-expert-team-architect-2）
- 评估性质：**仅评估，未改动任何生产代码与数据库**
- 工程真实根目录（已用代码验证，非旧空壳）：
  `D:\HuaweiMoveData\Users\HUAWEI\Documents\lingxi-claw\portfolio_tracker`
- 生产库：`data\database\portfolio.db`（约 116MB，27 张表）
- 评估方法：Glob / Read / Grep 静态核查 + 临时 Python 脚本统计行数（脚本位于 `D:\temp\count_lines.py`，未写入项目）；`git ls-files` 仅做只读核查（规避 `git status/add` 在 D 盘偶发的 `err_win_to_posix`）。

---

## 0. 健康度总评

| 维度 | 评级 | 一句话结论 |
|------|------|-----------|
| 分层与依赖方向 | B+ | 路由层 / 服务层 / 数据层单向清晰，无循环依赖 |
| 代码规模健康度 | B- | 分层合理，但存在 5 个 >1000 行 god file，最大 2036 行 |
| 配置 / 环境变量化 | B | env 机制完善，但残留机器专属绝对路径 |
| 数据层设计 | B | Schema 集中注册 + DatabaseManager 集中访问，迁移靠幂等 ALTER；部分分析模块直连 DB |
| 技术债 / 接手风险 | B- | 依赖未锁版本、Docker 与 venv Python 版本不一致、配置悬空项 |
| **综合** | **B（7/10）** | **可接手、中低风险；建议限期治理 god file 与依赖锁定** |

总体判断：架构骨架健康、分层意图清晰、质量工具链（pre-commit）与测试安全网（conftest DB 隔离）到位，属于**"能接手、但需要定向瘦身与加固"**的成熟项目。没有阻断性架构缺陷。

---

## 1. 目录分层与依赖方向

### 1.1 分层结构（实测）

```
入口层 (Entry)
  dashboard.py            ← Streamlit 主入口，编排 + 概览渲染 + Tab 注册
  run_analysis.py         ← 定时任务入口（采集/分析/报告流水线，1136 行）
  run_morning.py / run_supplemental.py  ← 补充批处理入口

UI 层 (Presentation)
  sidebar.py              ← 仅侧边栏 UI 与自定义 CSS（无 Tab 逻辑，已验证）
  tabs/  (tab1..tab17 + tab3 子面板 + gold_components/)
    └─ 每个 tab 暴露 render_tabN()，通过 TAB_REGISTRY 注册

服务层 (Service / 事实上)
  data_loader.py          ← 数据加载与计算引擎（2036 行，被 dashboard 与全部 tab 依赖）

领域 / 分析 / 数据 / 工具层 (src/)
  src/models.py           ← 领域值对象（MonteCarloResult / RebalanceSuggestion / RiskMetrics 等）
  src/analysis/           ← advisor / signal_backtest / rebalance_engine / nav_engine / portfolio_risk / etf_position / factor_attribution / predictor/ ...
  src/data_sources/       ← base(ABC) / sina / akshare_ds / DataSourceManager / fund_flow / market_events / macro_daily / etf_fundamental / neodata_valuation ...
  src/utils/              ← database(DatabaseManager) / db_schema / monitor / notification / news_fetcher / enhanced_report / data_quality ...
  src/report/             ← risk_report / excel_report / smart_report

配置层
  config/settings.py      ← 环境变量化配置中心
```

### 1.2 路由层如何分发到 tabs（实测）

`dashboard.py` 用**数据驱动的 Tab 注册表**集中管理 17 个 Tab，而非散落的 `st.tabs` 调用：

- 注册表：`dashboard.py:1078-1095`（`TAB_REGISTRY = [("标签", "tabs.tabN", "render_tabN"), ...]`），覆盖 `tabs.tab1_net_value` … `tabs.tab17_etf_position`。
- 渲染循环：`dashboard.py:1102-1104` 遍历 `TAB_REGISTRY`，按 `module_path` 动态导入并调用 `func_name`。
- `sidebar.py` 经 Grep 验证**不含任何 Tab 注册/渲染逻辑**（仅 `_inject_custom_css` / `_render_sidebar`），职责分离清晰。

> 注意（非阻塞）：`TAB_REGISTRY` 的标签字符串含 emoji 图标（`dashboard.py:1079-1095`）。这是生产代码现状，不在本评估修改范围；建议后续统一收口到图标规范。

### 1.3 依赖方向验证（无循环依赖）

- `tabs/*` → `data_loader`：`tabs/tab1_net_value.py:15`、`tabs/tab8_advice.py:17`、`tabs/_helpers.py:464` 等均以 `from data_loader import ...` 单向依赖服务层。
- `data_loader.py` 向上依赖：`config.settings`、`src.models`、`src.analysis.signal_score`（`data_loader.py:19,29,39`），**不反向 import tabs**。
- `dashboard.py` 编排层依赖：`data_loader` + `sidebar` + 各 `tabs`（导入点在 `dashboard.py:13,30-39,40`）。

结论：**UI 层 → 服务层(data_loader) → 领域/分析/数据/工具层(src/*) → 配置层(config)**，方向单向、无环。这是本项目最大的架构资产。

### 1.4 业务逻辑是否下沉到 service 层

- **已下沉（正向）**：大部分计算逻辑集中在 `data_loader.py`（被所有 tab 复用），以及 `src/analysis/*`、`src/data_sources/*`。`run_analysis.py` 也复用同一批 `src/*` 模块，说明"交互入口(dashboard)"与"批处理入口(run_analysis)"共享同一服务层，未各自造轮子。
- **未完全下沉（负向）**：`data_loader.py` 同时 `import streamlit as st`（`data_loader.py:15`）并使用 `@st.cache_data`，即"服务/计算引擎"与 Streamlit 缓存装饰器耦合——它既是 service 层又是缓存边界，职责偏重。tab 内仍有较重逻辑（见 §2）。

---

## 2. 代码规模与健康度

### 2.1 总量

- 共 **237 个 `.py` 文件**，**65,553 行**（不含 `venv313/` 与 `.git/`）。
- 其中 `tests/` 约 60 个测试模块，说明测试资产占比可观（测试健康度由 QA 专项评估，本文不展开）。

### 2.2 God file 清单（>1000 行，按行数降序，已用脚本实测）

| 行数 | 文件 | 性质 / 风险 |
|------|------|------------|
| 2036 | `data_loader.py` | 事实上的 service 层；且 import streamlit（§1.4） |
| 1939 | `src/analysis/advisor.py` | SmartAdvisor 单体，逻辑过密 |
| 1706 | `tabs/_helpers.py` | tab 共享辅助函数堆；应拆为 `src/utils/` 或 `components/` |
| 1677 | `tabs/tab8_advice.py` | 单 Tab 文件过大，建议按子面板拆分 |
| 1399 | `src/analysis/signal_backtest.py` | 回测引擎单体 |
| 1178 | `tabs/tab5_advanced.py` | 单 Tab 过大 |
| 1136 | `run_analysis.py` | 批处理编排单体（可接受，但偏长） |
| 955 | `tabs/tab2_position.py` | 单 Tab 过大 |
| 873 | `scripts/backfill/backfill_full_history.py` | 脚本类，影响较小 |
| 822 | `tabs/tab1_net_value.py` | 单 Tab 较大 |
| 809 | `src/data_sources/market_events.py` | 数据采集单体 |
| 792 | `src/data_sources/fund_flow.py` | 数据采集单体 |

判定：超过 300 行硬性门槛的文件大量存在；**最该先治理的 4 个热点**是 `data_loader.py`、`advisor.py`、`tabs/_helpers.py`、`tabs/tab8_advice.py`（均 >1500 行，且后两者是"辅助/单 Tab"性质，最容易安全拆分）。

### 2.3 tabs/ 行数分布（36 个文件，已实测）

```
1706  tabs/_helpers.py
1677  tabs/tab8_advice.py
1178  tabs/tab5_advanced.py
 955  tabs/tab2_position.py
 822  tabs/tab1_net_value.py
 696  tabs/tab10_fund_flow.py
 601  tabs/tab7_news.py
 598  tabs/tab14_market_events.py
 591  tabs/tab4_calendar.py
 514  tabs/tab3_risk_alerts.py
 438  tabs/tab15_trade_review.py
 423  tabs/tab16_risk_outlook.py
 413  tabs/tab6_technical.py
 380  tabs/tab12_macro.py
 379  tabs/gold_components/gold_utils.py
 365  tabs/tab3_risk_warnings.py
 359  tabs/gold_components/international_comparison.py
 343  tabs/gold_components/technical_signals.py
 335  tabs/tab9_custom.py
 320  tabs/tab13_data_quality.py
 296  tabs/tab17_etf_position.py
 276  tabs/tab3_risk_attribution.py
 234  tabs/gold_components/correlation.py
 224  tabs/gold_components/realtime_quotes.py
 181  tabs/tab3_risk_dashboard.py
 175  tabs/gold_components/gold_portfolio_correlation.py
 134  tabs/gold_components/central_bank_trends.py
 127  tabs/gold_components/price_comparison.py
 124  tabs/gold_components/seasonality.py
 123  tabs/gold_components/supply_demand.py
 117  tabs/tab11_gold.py
 115  tabs/gold_components/reserve_analysis.py
  72  tabs/tab3_risk.py            ← tab3 主协调器（小），逻辑下沉到子面板
  71  tabs/gold_components/gold_preloader.py
  26  tabs/__init__.py
   1  tabs/gold_components/__init__.py
```

观察：`tab3_risk.py` 仅 72 行、作为协调器把逻辑下沉到 `tab3_risk_{alerts,attribution,warnings,dashboard}`——这是**良好的拆分范式**，可作为 `tab8_advice.py` / `_helpers.py` 拆分的参照样板。

---

## 3. 配置 / 环境变量化管理

### 3.1 配置中心（`config/settings.py`）

- 统一 env 读取：`env(key, default)`（`settings.py:34-36`），优先级 `环境变量 > .env > 默认值`；`_load_env_file()` 在模块加载时读取项目根 `.env`（`settings.py:10-42`）。
- 已环境变量化：
  - `DATABASE_PATH`（`settings.py:57`，默认 `data/database/portfolio.db`）
  - `BACKUP_DIR`（`settings.py:58`）
  - 邮件：`EMAIL_ENABLED / EMAIL_SMTP_SERVER / EMAIL_SMTP_PORT / EMAIL_USERNAME / EMAIL_PASSWORD / EMAIL_RECIPIENTS`（`settings.py:237-243`）
  - 企业微信：`WECHAT_ENABLED / WECHAT_WEBHOOK_URL`（`settings.py:245-248`）
  - 告警/智能分析：`ALERT_DEDUP_INTERVAL_HOURS / STALE_THRESHOLD_DAYS / ADVICE_ENABLED` 等（`settings.py:284-292`）
- 模板覆盖度：`.env.example` 存在（1695 字节），覆盖了上述主要变量，注释清晰。

### 3.2 硬编码 / 机器专属路径（接手风险点，已实测定位）

| 位置 | 内容 | 影响 |
|------|------|------|
| `config/settings.py:64` | `TDX_EXPORT_DIR` 默认值 `r"C:\zd_zsone\T0002\export"` | 机器专属绝对路径（可用 env 覆盖，但默认值仅作者机有效） |
| `src/data_sources/neodata_valuation.py:29` | `NEODATA_SKILL_DIR = Path("C:/Users/HUAWEI/.workbuddy/skills/neodata-financial-search")` | 依赖本机 WorkBuddy 技能目录，换机即失效 |
| `scripts/backfill_sector_change.py:25-26` | 硬编码 `C:/Users/HUAWEI/.workbuddy/.../query.py` 与 workbuddy python 二进制 | 脚本级机器绑定 |
| `scripts/fetch_market_data.py:20-22` | `ROOT` 已改 D 盘，但 `PY`/`QS` 仍指向 `C:/Users/HUAWEI/.workbuddy/...` | 半迁移状态：ROOT 正确、依赖路径未改 |
| `scripts/import_aug_2026.py:41` | `PDF_PATH = r"C:/Users/HUAWEI/Downloads/20260801-20260831.pdf"` | 一次性导入脚本写死个人下载路径 |

### 3.3 C→D 迁移残留空壳路径（已核查）

- 旧空壳路径 `C:\Users\HUAWEI\Documents\lingxi-claw\portfolio_tracker` **仅出现在 `archive/` 下 4 处**（`archive/start_dashboard_final.py:6`、`archive/temp_start.py:8`、`archive/test_import.py:3`、`archive/_start_dash.py:5`）。`archive/` 已被 `.gitignore` 忽略，非生产代码，**不构成运行风险**。
- **生产代码（非 archive）未发现旧空壳路径引用**——C→D 迁移在运行路径上是干净的。

---

## 4. 数据层设计

### 4.1 Schema 集中管理（正向）

- 27 张表 DDL 全部集中在 `src/utils/db_schema.py:TABLE_DEFS`（`db_schema.py:16-540`），格式 `(table_name, ddl, [index_sql...])`，含核心交易、资金流、宏观/情绪、新闻、监控、指标/回测、市场事件、黄金、NAV、预测底座等。
- 质量监控元数据 `QUALITY_CHECK_TABLES`（`db_schema.py:548-571`）集中登记了每张表的 `date_col/code_col/label`，供 `DataQualityChecker` 使用。
- 访问入口集中：`src/utils/database.py:DatabaseManager`（`database.py:13`），`_init_db()` 统一调用 `init_all_tables`（`database.py:20-27`）。

### 4.2 迁移机制（中性偏负）

- 幂等迁移函数 `_ensure_columns()`（`db_schema.py:594-605`）与 `ensure_etf_features_v2_columns` / `ensure_etf_forward_returns_risk_columns`，在 `init_all_tables()`（`db_schema.py:618-631`）末尾对**已存在表**执行 `ALTER TABLE ADD COLUMN` 补齐新增列。
- **问题**：无独立迁移脚本/框架（如 Alembic），无迁移版本号与历史；schema 演进靠在代码里追加 `ALTER`。对 SQLite 单库可接受，但接手后若需回滚/审计迁移，缺乏可追溯性。

### 4.3 DB 访问泄漏（负向，已实测定位）

`DatabaseManager` / `get_db_connection` 是既定访问入口，但以下**生产分析模块直接 `sqlite3.connect(DATABASE_PATH)` 绕过**，且未设置 `check_same_thread=False`（而 `data_loader.get_db_connection` 设置了，见 `data_loader.py:69`）：

| 文件:行 | 说明 |
|--------|------|
| `src/analysis/etf_position.py:57` | `return sqlite3.connect(db_path)` |
| `src/analysis/nav_engine.py:41` | `return sqlite3.connect(str(DATABASE_PATH))` |
| `src/analysis/portfolio_risk.py:281` | `conn = sqlite3.connect(str(DATABASE_PATH))` |
| `src/analysis/rebalance_engine.py:561` | `conn = sqlite3.connect(db_path)` |

风险：连接参数（线程安全、WAL 模式等）无法在单点统一管控；Streamlit 多线程下 `check_same_thread` 不一致可能引发偶发线程错误。建议统一收敛到 `get_db_connection`。

### 4.4 双源容灾（EM + 新浪）组织方式（实测）

- 基类 `src/data_sources/base.py:BaseDataSource`（ABC，`base.py:17`）提供带重试的 `_request()`（`base.py:31-49`）。
- 故障转移由 `src/data_sources/__init__.py:DataSourceManager` 实现：`get_quote()` / `get_kline()`（`__init__.py:46-57`）按 `source_order`（按 `DATA_SOURCES` 的 `priority` 排序，`__init__.py:32-35`）**顺序尝试，遇异常自动切换下一源**，全部失败抛 `DataSourceError`。
- 额外的"新浪兜底"散落在具体采集模块：`src/data_sources/etf_fundamental.py:116-123`（`fetch_etf_spot_sina_fallback`）、`src/data_sources/fund_flow.py:303`（新浪日 K 线 fallback）。

**配置/代码不一致（接手注意）**：`config/settings.py:DATA_SOURCES` 声明 3 个源（sina 优先级1 / eastmoney 优先级2 / akshare 优先级3），但 `DataSourceManager._init_sources`（`__init__.py:26-29`）**只注册 `sina` 与 `akshare` 两个类**，全仓检索确认**无 `eastmoney.py`**。即 `eastmoney` 是悬空配置项——实际容灾链为 `sina → akshare`（akshare 底层走东方财富接口），而非配置所暗示的三源。建议在 `settings.py` 注释澄清，或显式注册 EM 源。

### 4.5 生产库未入版本控制（已验证，正向）

- `git ls-files | grep -ic '\.db$'` 结果 **0**；`git ls-files` 对 `data/database` / `.env` / `venv313` 均无命中。
- `.gitignore` 已忽略 `data/database/`、`data/*.db`、`*.db`、`.env`（` .gitignore:23,59,85,5-6`）。
- 根目录 `portfolio_tracker.db` 与 `data/backups/*` 为未迁移副本，同样被 `.gitignore` 排除，不会误提交。

> 接手注意：根目录存在 `.env`（1836 字节，含真实凭据）与残留 `.env.QA_RESIDUE_20260805`（41 字节，QA 遗留）。`.env` 已 gitignore（不泄漏），但接手时应清理 `.env.QA_RESIDUE_20260805` 这类遗留文件，并确认 `.env` 不随镜像/备份外泄（Dockerfile 第 19 行 `COPY . .` 会带入构建上下文中的 `.env`，需靠 `.dockerignore` 或构建时不包含 `.env` 来规避）。

---

## 5. 技术债与接手风险

| # | 风险 | 证据 | 严重度 |
|---|------|------|--------|
| R1 | 依赖未锁版本，无 lockfile | `requirements.txt` 全为 `>=`（如 `pandas>=2.0.0`、`streamlit>=1.30.0`、`plotly>=6.0.0`、`akshare>=1.11.0`）；无 `requirements.lock` / `poetry.lock` / `pip-tools` | 高（复现风险） |
| R2 | Docker 镜像 Python 与本地 venv 不一致 | `Dockerfile:5` `FROM python:3.12-slim`；本地 `venv313` 为 Python 3.13 | 中 |
| R3 | data_loader 既是 service 层又耦合 streamlit | `data_loader.py:15` `import streamlit` + `@st.cache_data` | 中 |
| R4 | 分析模块直连 DB，绕过集中访问 | `etf_position.py:57` / `nav_engine.py:41` / `portfolio_risk.py:281` / `rebalance_engine.py:561` | 中 |
| R5 | 机器专属硬编码路径 | §3.2 表（neodata_valuation / backfill_sector_change / fetch_market_data / import_aug_2026 / settings:64） | 中（换机失效） |
| R6 | 配置悬空项 | `DATA_SOURCES.eastmoney` 无对应类（§4.4） | 低 |
| R7 | 迁移无版本化/框架 | `db_schema.py` 仅靠 `ALTER` 幂等补齐（§4.2） | 低 |
| R8 | pre-commit 已配置但未确认是否 install；mypy 弱化 | `.pre-commit-config.yaml` 存在；`pyproject.toml:30` `ignore_missing_imports=true` | 低 |
| R9 | 多根入口编排脚本 | `dashboard.py` / `run_analysis.py`(1136) / `run_morning.py` / `run_supplemental.py` 并存 | 低（设计上可接受） |

### 正向资产（接手信心来源）

- 分层单向、Tab 注册表集中（`dashboard.py:1078-1095`）——路由可维护。
- `src/models.py`（8264 字节）提供领域值对象，类型边界清晰。
- 测试安全网强：`tests/conftest.py:114-164` 对 `sqlite3.connect` 做**守卫拦截**，凡指向生产库的连接一律改道临时副本——直接防止测试误写生产 DB。
- 质量工具链到位：`.pre-commit-config.yaml` 含 black / isort / flake8+bugbear / mypy / bandit（`pyproject.toml` 配 line-length=120）。
- DB 与 `.env` 正确排除于 git（§4.5）。

---

## 6. 架构层面接手建议（按优先级）

### P0（接手第一周，先止血/防坑）
1. **锁定依赖版本**：基于当前 `venv313` 生成 `requirements.lock`（或引入 `pip-tools` / `poetry`）；把 `requirements.txt` 的 `>=` 改为锁定区间。目的：保证接手机与生产环境可复现，避免 `plotly>=6` / `streamlit>=1.30` 大版本漂移引发静默破坏。
2. **统一 DB 访问入口**：将 §4.3 的 4 处 `sqlite3.connect(DATABASE_PATH)` 收敛到 `get_db_connection`，统一 `check_same_thread` 与 WAL 设置，消除 Streamlit 多线程隐患。
3. **清理机器专属路径**：把 §3.2 的 `.workbuddy` / `C:/Users/HUAWEI/Downloads` 绝对路径改为相对路径或 env 注入；删除根目录 `.env.QA_RESIDUE_20260805`。
4. **对齐 Docker / 本地 Python 版本**：`Dockerfile` 改为 `python:3.13-slim`（与 `venv313` 一致），或反之统一到 3.12；并补 `.dockerignore` 排除 `.env`、`data/`。

### P1（接手首月，定向瘦身）
5. **拆分 `tabs/_helpers.py`(1706) 与 `tabs/tab8_advice.py`(1677)**：参照 `tab3_*` 的协调器+子面板拆分范式（§2.3），把通用辅助下沉到 `src/utils/` 或 `components/`，单 Tab 按子面板拆文件，目标单文件 ≤ 600 行。
6. **解耦 `data_loader.py` 的 streamlit 依赖**：将 `@st.cache_data` 缓存边界与纯计算函数分离（缓存留在调用侧/适配层），使计算引擎可在批处理（run_analysis）与测试中被无 Streamlit 依赖地复用。
7. **澄清数据源配置**：在 `DATA_SOURCES` 注释中说明实际容灾链为 `sina→akshare`，或显式实现并注册 `eastmoney` 源，消除悬空配置。

### P2（持续治理）
8. **引入迁移版本化**：对 SQLite 采用轻量迁移记录表（`schema_migrations`）或在 `db_schema.py` 维护有序迁移清单，便于审计/回滚。
9. **推动 pre-commit 真正生效**：在 CI 中运行 `pre-commit run --all-files`，并考虑收紧 mypy（移除 `ignore_missing_imports` 或仅对第三方放宽）。
10. **god file 长期计划**：`advisor.py`(1939) / `signal_backtest.py`(1399) 按职责（建议生成 / 置信度评估 / 回测）继续拆分。

---

## 附录 A：关键证据索引（file:line）

| 主题 | 证据 |
|------|------|
| Tab 注册表 | `dashboard.py:1078-1095`（注册）、`dashboard.py:1102-1104`（渲染循环） |
| sidebar 无 Tab 逻辑 | Grep `sidebar.py` 对 `st.tabs\|import tab\|TAB_LIST` 无命中 |
| tabs→data_loader 单向依赖 | `tabs/tab1_net_value.py:15`、`tabs/tab8_advice.py:17`、`tabs/_helpers.py:464` |
| data_loader 耦合 streamlit | `data_loader.py:15` `import streamlit`；`data_loader.py:69` `get_db_connection` |
| 领域模型 | `src/models.py`（8264 字节；`data_loader.py:29-35` 引用） |
| Schema 集中注册 | `src/utils/db_schema.py:16-540`（TABLE_DEFS 27 表）、`db_schema.py:548-571`（QUALITY_CHECK_TABLES） |
| DB 集中访问 | `src/utils/database.py:13` DatabaseManager、`database.py:20-27` `_init_db` |
| 幂等迁移 | `src/utils/db_schema.py:594-605` `_ensure_columns`、`db_schema.py:618-631` `init_all_tables` |
| DB 直连泄漏 | `src/analysis/etf_position.py:57`、`nav_engine.py:41`、`portfolio_risk.py:281`、`rebalance_engine.py:561` |
| 双源容灾 | `src/data_sources/__init__.py:46-57`（优先级失败转移）、`__init__.py:26-29`（仅注册 sina+akshare） |
| 无 eastmoney 类 | 全仓检索 `**/eastmoney*.py` 无结果 |
| 配置 env 机制 | `config/settings.py:34-36` `env()`、`settings.py:57-58` 库路径可覆盖、`settings.py:237-243` 邮件 |
| 机器专属路径 | `settings.py:64`、`neodata_valuation.py:29`、`backfill_sector_change.py:25-26`、`fetch_market_data.py:20-22`、`import_aug_2026.py:41` |
| C→D 残留空壳路径 | 仅 `archive/` 4 处（gitignored）：`archive/start_dashboard_final.py:6` 等 |
| 依赖未锁版本 | `requirements.txt`（14 行全 `>=`） |
| Docker 版本 | `Dockerfile:5` `python:3.12-slim`；`venv313` = Python 3.13 |
| pre-commit | `.pre-commit-config.yaml`（black/isort/flake8/mypy/bandit）；`pyproject.toml:30` mypy `ignore_missing_imports` |
| DB 未入 git | `git ls-files | grep -ic '\.db$'` → 0；`.gitignore:23,59,85` |
| 测试 DB 隔离 | `tests/conftest.py:114-164` 守卫 `sqlite3.connect` 改道临时副本 |
| 总量 | 237 个 .py / 65,553 行（脚本 `D:\temp\count_lines.py` 实测） |

## 附录 B：范围声明

- 本评估**未运行**项目、**未修改**任何生产代码与 `portfolio.db`。
- 行数统计通过独立临时脚本 `D:\temp\count_lines.py` 完成，未写入项目目录。
- 代码健康度（测试通过率/覆盖率）由 QA 专项评估，本文仅引用其测试资产规模与 DB 隔离安全网作为架构信心依据。
