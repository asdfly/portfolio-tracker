# 架构交接评估 — Portfolio Tracker

评估人：高见远（首席架构师） | 日期：2026-08-05 | 基线：master @ 24574df

结论先行：系统**业务逻辑健康、工程交付链断裂**。代码本身分层清晰、1465 个测试（本地基线）通过；但它**无法从源码重建**，且全部资产（代码 + 数据 + 备份）单点存放于一台 Windows 机器。接手第一周的任务不是开发功能，是止血。

---

## 一、当前架构分层与依赖方向

依赖单向向下，无循环依赖。

```
入口层    dashboard.py(1104) │ run_analysis.py(904) │ *.bat
            │                      │
路由/编排  TAB_REGISTRY 动态 __import__ 15 个 tab   五阶段流水线
            │                      │
UI 层     tabs/tab1..15  →  tabs/_helpers.py(1706) │ components/ui
            │
数据访问   data_loader.py(1964)  ← 事实上的 God Module，11 个 tab 共用
            │
分析引擎   src/analysis/  (advisor / portfolio_risk / multi_factor_score ...)
            │
采集层     src/data_sources/  (sina / akshare / fund_flow / market_events)
            │
存储       SQLite  data/database/portfolio.db (30 表, 88MB)
```

- `config/settings.py` 被各层直接 import，无依赖注入——改配置影响面全局。
- `data_loader.py` 与 `tabs/_helpers.py` 是两个横向"重力井"，占非测试代码约 7%，任何改动波及面最大。

## 二、技术债与风险清单

### 严重

| # | 问题 | 证据 |
|---|------|------|
| S1 | **130 个 commit 从未推送**，最早可追溯 2026-06-08；备份仅 1 份且与主库同盘 | `git status -sb` → `master...origin/master [ahead 130]`；`data/backups/` 仅 portfolio_db_20260803.db |
| S2 | **无法从源码自举**：db_schema.py 仅定义 22 表，实库 29 表。空库初始化缺 advice_history / etf_fundamental / etf_industry_alloc / etf_top_holdings | `src/utils/db_schema.py:478 init_all_tables`；实测新建库仅 22 表 |
| S3 | **无迁移框架**：`_migration_version` 表存在但 0 行，且全代码库零引用 | grep `_migration_version` 无命中；db_schema.py 无 ALTER TABLE |
| S4 | **构建不可复现**：13 个依赖全用 `>=`；snownlp/jieba/pillow 未声明；Dockerfile 无 .dockerignore 却 `COPY . .` | `requirements.txt:1-13`；`Dockerfile:19` |

> S2 是 S1 的根因：因为库无法重建，库本身成了构建产物，才被迫塞进 git。两者必须一起解。

### 中

| # | 问题 | 证据 |
|---|------|------|
| M1 | **情感分析已静默失效**：代码运行时注入硬编码 `venv/Lib/site-packages`（Windows 布局），依赖缺失被 except 吞掉返回 0.5 | `src/utils/news_fetcher.py:20-45`；实测 2026-08 共 66 条新闻 sentiment_score **100% = 0.5**（7 月仅 10.4%） |
| M2 | **Python 版本五分裂**：生产 3.10（硬编码绝对路径）/ venv 3.10.11 / CI 3.12 / Docker 3.12 / lint 目标 3.9 | `scheduled_run.bat:6`；`ci.yml:18`；`pyproject.toml:3,27` |
| M8 | **CI 是"假保护"：不仅 2 个月没跑，而且跑了也必红约 67 个**。① 停滞：origin/master 停在 7cbada1 (2026-06-05)，落后 130 commit / 181 文件 / +24836/-12025，新增 32 个测试文件从未进 CI；requirements.txt 已变（弃 selenium 加 playwright），**CI 从未装过 playwright 浏览器**。② 更致命：`ci.yml:28` 设 `DATABASE_PATH=":memory:"`——空内存库无任何表，所有直查真实表的测试 `pandas.errors.DatabaseError` 全线失败（实测带 CI env → 67 failed / 1405 passed；A/B/C 对照锁定该变量）。本意"CI 无库给安全默认"，却与"测试直查真实表"正面冲突。这比"没跑"更强：CI 是注定失败的配置，且因 2 个月没跑无人知晓。③ `.gitignore:23/85` 忽略 `data/database/` 与 `*.db`，但库已被 `git add` 跟踪（忽略对已跟踪文件无效），每跑一次测试就产生 84MB 二进制 diff | `git rev-parse origin/master`；`git diff --shortstat origin/master..master`；`.github/workflows/ci.yml:28`；`.gitignore:23,85` |
| M9 | **4 个测试文件依赖真实 portfolio.db，但仅 1 个会真红，另 3 个是"假绿"**。① `test_fix_data_quality.py::TestD4Metrics` 4 个用例裸 `cur.execute()` 无保护（:22/35/42/49）→ 空库缺表时 OperationalError 直接抛 → **真红（约 3-4 个）**。② `test_d1_margin_research_block.py::TestD1Integration` 3 个用例调用 `advisor._analyze_margin_data/_institution_research/_block_trade`，三处 `except (... sqlite3.OperationalError ...) → logger.warning → return []`（`advisor.py:925/1053/1260`），测试只断言 `isinstance(result, list)` → **假绿**。③ `test_market_event_signals.py`：`market_event_signals.py:61` `except Exception` 吞掉 5 个分析器，`test_signal_fields` 另有 `if signals:` 护栏 → **假绿**。④ `test_p2_reports_utils.py:63`：`save_report()` 只写文件不查库，`db_path` 仅用于推导目录 → **假绿**。根因同 M5：硬编码 `C:\Users\HUAWEI\...` 在 Linux CI 是合法文件名（反斜杠为普通字符），sqlite 在 CWD 造同名 0 字节文件。但"这些测试从未被 CI 验证"的更准确说法是：**库虽在 HEAD（commit 24574df）已被 `git add` 跟踪（实测 `git ls-files` 命中 `data/database/portfolio.db`），DevOps 一 push 便随代码上 GitHub，CI 拿到的就是这份库；然而 `ci.yml:28` 强制 `DATABASE_PATH=":memory:"` 把默认库路径整个覆盖，sqlite 转而建 0 字节空内存库，4 文件全部对着空库跑——test_fix_data_quality 真红、另 3 个假绿。故"移出 git 会致 4 测试红"的担忧在当前状态下并不成立：它们红是因为 `:memory:` 覆盖而非库缺失；只有当未来撤掉 `:memory:` 覆盖、且库已移出 git 时，本地无 env 跑这 4 个文件才会真正因缺库而红（届时必须配套 fixture 库）** | `test_fix_data_quality.py:8,15,22,35,42,49`；`test_d1_margin_research_block.py:9,131-162`；`advisor.py:925,1053,1260`；`market_event_signals.py:61`；`test_p2_reports_utils.py:63` |
| M3 | **文档化的定时任务已死**，实际靠手工触发；失败仅落日志无告警 | `logs/scheduled_run.log` 最后写入 06-01，而 `portfolio_20260805.log` 是新的 |
| M4 | CI 无 lint 门禁：pre-commit 配了 black/isort/flake8/mypy/bandit，CI 只跑 pytest | `.github/workflows/ci.yml:32` |
| M5 | **多数据库歧义（机制已定位）**：根目录 4 个 .db（3 个 0 字节），2 个被 git 跟踪；今日日志报 `no such table: stock_block_trade`，而主库该表有 9255 行 | 实测 `sqlite3.connect()` 对不存在路径**静默创建 0 字节文件**，随后查询即报该错——与日志报错、0 字节残留文件同签名。`git ls-files \| grep .db`；`logs/portfolio_20260805.log` |
| M6 | 巨文件超标：data_loader.py 1964 / _helpers.py 1706 / tab8_advice.py 1635 行 | `wc -l` |
| M7 | `get_db_connection` 双实现，后导入覆盖前者；ARCHITECTURE.md 宣称的"统一连接管理"不成立 | `dashboard.py:30` 与 `:31`；`data_loader.py:50` |

### 低

- L1 文档漂移：README 称"413 用例"实为 1476；ARCHITECTURE.md:104 称"集中定义 30 张表"实为 22。5 个 `test_d11_version_release` 因此红。
- L2 根目录污染：30+ PNG、日志等散落根目录（评估期 QA 临时生成的 `_qa_cov.py` 已清理，其造成的 `test_d6_root_cleanup` 第 8 个误报随之转绿，真实本地基线为 7 红而非 8）。根目录污染本身仍属待清理的低优先项。
- L3 `config/settings.py:12` 的 `'PROJECT_ROOT' in dir()` 恒为 False（dir() 在函数内只返回局部名）——属死逻辑可清理，但 else 分支算出的路径恰好正确，**不是 test_d5 红的根因**。QA 实测那 2 个 test_d5 红是 WorkBuddy 沙箱拦截 `unlink()`（`OSError ... windows-sandbox-recycle-bin-unavailable`，栈顶 `sitecustomize.py:440`），属其环境产物，Linux runner 无此拦截会过；CI 则因 `:memory:` **新增** `test_database_path_default` 红（断言 `'portfolio.db' in DATABASE_PATH`，CI 给 `:memory:`，`tests/test_d5_env_config.py:60-62`）。
- L4 `NOTIFICATION`(:182) 与 `NOTIFICATION_CONFIG`(:221) 两套通知配置并存。
- L5 `trades` 表确认废弃：0 行、代码零引用，可安全 DROP（现行为 `trade_records` 1157 行）。
- L6 `etf_industry_alloc`(257) / `etf_top_holdings`(300) 的 updated_at **100% NULL**，无新鲜度追踪。

**实测测试状态**：1476 用例，本地真实基线 7 failed / 1465 passed / 4 skipped（耗时 166s，已剔除评估期 QA 临时文件 `_qa_cov.py` 造成的第 8 个误报）。7 个失败全部是上述 L1/L2/L3 三类工程债，无业务逻辑失败。CHANGELOG 仅承认跑"关键子集 136 passed"。**但 1465 passed 含未知比例的"假绿"集成用例（见 M9）**——`test_d1/test_market_event_signals/test_p2` 对着 0 字节空库也会给同色绿，验证的是"没抛异常"而非"结果正确"，故该通过数**不能当回归兜底证据**。更关键的是：**带 CI 环境（`DATABASE_PATH=":memory:"`）实跑必红约 67 个（67 failed / 1405 passed，见 M8）**——当前 130 个 commit、32 个新测试文件从未进过 CI，这个 ~67 是从未验证过的真实风险面。交叉验证带：CI 首跑落 65-75 红说明根因判断准；显著低于 65 多半有未模拟因素；远高于 75 多半是 Linux 路径/依赖问题。

## 三、依赖与版本风险

**不可复现，Docker 镜像无法稳定重现当前系统。**

- `akshare>=1.11.0` 实装 1.18.80。akshare 封装的是爬取端点，小版本间破坏性变更频繁，重装大概率取到不兼容版本，导致整个采集层批量失败。
- 缺失声明：snownlp、jieba（新闻情感）、pillow（dashboard.py:26 导入，现靠 matplotlib 传递依赖侥幸可用）。
- Dockerfile 未执行 `playwright install chromium`（DEPLOYMENT.md 要求手工装），镜像内截图/PDF 导出必然失败。
- 无 `.dockerignore` + `COPY . .` → 88MB 数据库、Windows venv、30+ PNG 全部进镜像。
- `settings.py:56` 通达信路径 `C:\zd_zsone\T0002\export` 为 Windows 强绑定；Linux 下静默 fallback 到 `data/raw/positions.tsv`，容器里持仓将永远是旧快照且无告警。

## 四、新主人必须知道的事

1. **数据库是唯一真相源，不是代码。** 在 S2 修复前，删库等于灭项目。
2. **不可乱动的核心**：`data_loader.py` 中 `compute_monthly_returns`（daily_return 连乘）、`compute_position_advice`（相对比例）、`load_technical`（日期回退）——都是 v2.6 刚修完的金融口径 bug，改动必须配套测试，否则收益率会再次算错（历史上曾把月收益算成 142.74%）。
3. **数据管线怎么触发**：实际是手工跑 `run_analysis.bat` / `run_all.bat`，不是文档说的 `scheduled_run.bat`。顺序：备份 → 阶段1 基础持仓 → 2 风险 → 3 告警 → 3.2 资金流 → 3.5 新闻 → 建议 → 报告。任一阶段失败只写日志，无人通知。
4. **配置从哪读**：`config/settings.py` 单一入口，优先级 环境变量 > `.env` > 默认值。`DATABASE_PATH` 可被环境变量覆盖（CI 即用 `:memory:`）。

## 五、建议的前 3 个技术动作

| 优先级 | 动作 | 工作量 | 理由 |
|--------|------|--------|------|
| P0-1 | **止血**：推送 130 个 commit；`git rm --cached` 数据库并转 LFS/对象存储；portfolio.db 异盘 + 云端备份 | 0.5 天 | 当前任何硬盘故障 = 两个月工作 + 全部历史数据归零。注意 88MB blob 已在历史中，且库以约 20MB/月增长，逼近 GitHub 100MB 硬限——越晚处理越要改写历史。**前置处理成本已从"改 4 个测试"降为"给 1 个文件加守卫"**（M9 复核结论：仅 `test_fix_data_quality.py` 真红），可更快落地 |
| P0-2 | **恢复可自举**：补齐 7 张漂移表进 db_schema.py，启用 `_migration_version`，**仅 `test_fix_data_quality.py` 需加 skip 守卫或换 fixture 库**（M9 中唯一真红文件），另 3 个假绿文件登记为技术债即可、不阻塞 push，验证 空库 → init → run_analysis 全绿 | 1-2 天 | 这是把库移出 git 的前置条件，也是灾备可恢复的前提。不解决，S1 只能治标。**注意排序**：`test_fix_data_quality.py` 守卫应先于"把库移出 git"——但原因不是"移除会让 CI 红"（CI 已在 `ci.yml:28` 强制 `:memory:`，4 测试本来就会红，见 M9），而是本地无 env 跑该文件需要真实库、一旦库移出 git 本地也会红；且 `:memory:` 覆盖意味着该测试在 CI 上从未验证过真实数据质量。守卫把它钉到独立 fixture 库（或 skip + TODO）即可解耦，与"库是否留在 git"脱钩（其余 3 个假绿文件本就不依赖真实结果，无需前置处理） |
| P1 | **锁定环境**：`pip freeze` 产出 requirements.lock，补 snownlp/jieba/pillow，加 .dockerignore，Dockerfile 补 `playwright install chromium`，删 news_fetcher.py:22-25 的 sys.path 注入，**统一 Python 3.12** 并给 CI 加 lint job | 1 天 | 消除"我机器上能跑"，同时顺手修复 M1 情感分析静默失效 |

---

## RoleVerdict

```
verdict: fail
```

**blocking**

| 违反项 | 证据 | 期望 |
|--------|------|------|
| 源码无法重建系统 | db_schema.py:478 仅建 22 表；实库 29 表；实测新建库缺 4 张业务表 | 空库 init 后 run_analysis 可全流程跑通 |
| 130 commit 未推送 + 备份同盘 | `git status -sb` → ahead 130；data/backups/ 仅 1 文件 | 代码推送至 origin；数据库异盘/异地备份 |
| 构建不可复现 | requirements.txt 全 `>=`；snownlp/jieba/pillow 未声明；无 .dockerignore | 版本锁定文件 + 完整依赖声明 + Docker 可重复构建 |
| 无 schema 迁移机制 | `_migration_version` 0 行且代码零引用 | 可版本化的迁移脚本，支持存量库演进 |

**advisory**

| 建议项 | 理由 |
|--------|------|
| 拆分 data_loader.py(1964) / _helpers.py(1706) | 单文件应 ≤300 行；当前两文件是全项目改动风险最高处 |
| **统一 Python 版本至 3.12**（经与 DevOps 复核后修正，原建议 3.10） | 3.10 EOL 2026-10（约 2 个月后）且 3.10.11 是最后一个 Windows 二进制版；3.12 EOL 2028-10，且 CI/Docker 本就是 3.12，改动面仅 scheduled_run.bat + pyproject.toml 两处。已实测 186 个项目 .py **零处**使用 3.12 移除的 stdlib（distutils/imp/asyncore/asynchat/smtpd/telnetlib），无硬阻断。**但不要跟进 3.13**：venv 内 nltk 仍用 3.13 已移除的 `cgi` |
| 迁移 3.12 需设验证闸门，不可当作两行改动 | CI 自 2026-06-05 未运行过，"3.12 已验证"不成立（详见 M8）。首次 push 后的 CI 结果才是真验证 |
| 修复 M1 情感分析静默失效 | 8 月数据已 100% 退化为中性 0.5，Tab7 与建议引擎正在消费失真输入 |
| 补 updated_at 与告警 | 两张 ETF 表新鲜度不可观测；管线失败无人知晓 |
| DROP 废弃 `trades` 表 | 0 行、零引用，与 trade_records 并存易误用 |
| 清理根目录与文档漂移 | 7 个失败测试（真实本地基线）全部源于此；但 CI 不会因清理而全绿——`ci.yml:28` 的 `:memory:` 仍会让 ~67 个直查真实表的测试红，须配 fixture 库（见 M8/M9）才真绿 |
| 3 个 DB 集成测试是"假绿"，通过数含未知比例空转用例 | `advisor.py:925/1053/1260` 与 `market_event_signals.py:61` 宽泛 except + 测试只断言 `isinstance(x,list)`，使集成测试结构性丧失失败能力（第 6 例静默降级反模式，攻击的是我们的判断能力）。1465 passed 不能当回归兜底，CI 重跑须以"真红用例清单"而非总数评估 |
| UI 图标改用 SVG 图标库 | TAB_REGISTRY 15 个 tab 全部使用 emoji 作功能图标，不符合团队规范 |
| CI `DATABASE_PATH=":memory:"` 必须改为指向真实 fixture 库（见 M8/M9） | `ci.yml:28` 的 `:memory:` 使 67 个直查真实表的测试在 CI 上线即红、且 4 个集成测试永远假绿；若"修绿"只把 `:memory:` 回退到默认无库路径，又会触发 M5 的"静默建 0 字节空库"——正确做法是初始化一个带 30 表的 fixture 库供 CI 用，让集成测试真正命中真实表 |
| test_d5 本地 2 红是评估环境产物，非代码缺陷，不计入真实 7 红基线 | WorkBuddy 沙箱拦截 `unlink()`（OSError windows-sandbox-recycle-bin-unavailable），Linux runner 无此拦截会过；CI 反而因 `:memory:` 新增 `test_database_path_default` 红（`test_d5_env_config.py:60-62`）——属评估期观测偏差，已从报告中厘清 |

**evidence**

| artifact | line | 说明 |
|----------|------|------|
| src/utils/db_schema.py | 478 | init_all_tables 仅覆盖 22 表 |
| dashboard.py | 30-31 | get_db_connection 重复导入，后者覆盖前者 |
| dashboard.py | 1078-1092 | TAB_REGISTRY，15 个 emoji 图标 |
| data_loader.py | 1964(总行数) | God Module |
| src/utils/news_fetcher.py | 20-45 | 硬编码 venv 路径 + 静默吞异常 |
| config/settings.py | 12 / 56 | dir() 判断恒假；通达信路径 Windows 强绑定 |
| scheduled_run.bat | 6 | 硬编码 Python 3.10 绝对路径 |
| Dockerfile | 19 | `COPY . .` 且无 .dockerignore |
| .github/workflows/ci.yml | 32 | 仅 pytest，无 lint |
| logs/portfolio_20260805.log | — | `no such table: stock_block_trade`（主库该表 9255 行） |
| src/analysis/advisor.py | 925,1053,1260 | 三处 except 吞 OperationalError → return []（3 个假绿测试的根因） |
| src/analysis/market_event_signals.py | 61 | `except Exception` 吞掉 5 个分析器（假绿根因） |
| tests/test_fix_data_quality.py | 8,15,22,35,42,49 | 裸 `cur.execute` 无保护 → 唯一真红文件 |
| .github/workflows/ci.yml | 28 | `DATABASE_PATH=":memory:"` 空内存库无表，是 CI 必红 ~67 且 4 测试假绿的根因 |
| tests/test_d5_env_config.py | 60-62 | `test_database_path_default` 断言 `'portfolio.db' in DATABASE_PATH`，CI 给 `:memory:` 必红（CI 新增红） |
| tests/test_d5_env_config.py | 110,122 | `env_file.unlink()` 被 WorkBuddy 沙箱拦截，是本地 2 红的环境产物（非代码缺陷） |
| data/database/portfolio.db | — | commit 24574df 已 `git add` 跟踪（实测 `git ls-files` 命中），库随 push 上 GitHub；CI 有库但被 `:memory:` 覆盖 |
| .gitignore | 23,85 | 忽略 `data/database/` 与 `*.db`，但对已跟踪库无效，每跑测试 84MB diff |
