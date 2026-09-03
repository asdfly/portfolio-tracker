# 测试健康度交接评估 — portfolio_tracker

> 评估人：测试工程师（严过关）｜评估类型：**只读交接评估**（未改动生产代码/数据库，仅新建此文档 + 跑现有测试 + 只读校验生产库）
> 工程路径：`D:\HuaweiMoveData\Users\HUAWEI\Documents\lingxi-claw\portfolio_tracker`
> 运行环境：`venv313\Scripts\python.exe`（Python 3.13.14, pytest 9.1.1）

---

## 0. TL;DR 健康度评级

**综合评级：B−（条件健康 / Conditionally Healthy）**

| 维度 | 评级 | 依据 |
|------|------|------|
| 测试体量 | A | 85 个文件、1498 个测试函数定义（1571 个 collected，含参数化） |
| 断言真实性 | A− | 2050 条 `assert`，80/83 测试模块有真实断言；非空跑 |
| 隔离**设计** | A | conftest P0 隔离机制设计扎实，能**主动检出**生产库被碰 |
| 隔离**执行** | **C（不及格）** | 全量运行实测**改动了生产库 mtime**（数据未损、guard 已报警），且**两次全量仅一次复现 → 非确定性 flaky 泄漏**（比确定性泄漏更危险） |
| 覆盖率水位 | C+ | 实测 ~50.6%；核心分析/数据源路径大面积盲区（0–13%） |
| 稳定性 | B− | 2 失败（1 确定性陈旧测试 + 1 **flaky 生产库泄漏**）、4 跳过、7 无害告警 |
| 作为回归基线 | **条件可信** | 分析/指标层立即可信；UI/数据源层需补断言+覆盖后才可信 |

**一句话结论**：测试**套件本身有料、断言真实、隔离设计优秀**，但当前**不能作为"永不触碰生产库"的可信基线**——
(a) 全量运行会**非确定性地**真实改动生产库（run#1 改了 mtime、run#2 未改，数据均完好，conftest 仅 run#1 报警拦截）→ **flaky 生产库触碰，CI 可能漏检**；
(b) 有 1 个陈旧断言把套件染红；
(c) 覆盖率 ~50% 且核心 analysis/data_sources 大面积盲区。
修复 (a)(b) 并对核心盲区补测后，方可作为回归基线。

---

## 1. tests/ 目录结构与命名统计

### 1.1 体量
- **测试文件：85 个**（含 `__init__.py`、`conftest.py`、`_debug_sqlite3.py`；真实测试模块约 82 个）
- **测试函数定义：1498 个** → pytest 实际 collected **1571 个**（部分因参数化展开，如 `test_runtime_safety.py::test_no_function_over_300_lines[tabX]`、`test_financial_metrics.py::TestBetaAlpha::test_tracking_error_formula`）
- **断言总数：2050 条**；80/83 测试模块含 ≥1 条 assert

### 1.2 命名规律与覆盖意图（按形态归类）

| 形态 | 数量 | 代表文件 | 覆盖意图 |
|------|------|----------|----------|
| `test_dN_*`（d1–d15 改进清单） | 12 | test_d1_margin_research_block, d3_closed_loop_feedback, d4_alert_diversification, d5_env_config, d6_root_cleanup, d8_log_rotation, d9_backtest, d10_docker_ci, d11_version_release, d13_tab3_refactor, d14_backup, d15_code_quality | 对应"15 点改进路线图"交付物（闭环反馈/告警分散/环境配置化/Docker CI/版本发布/代码质量/备份等） |
| `test_pN_*`（优先级回归） | 14 | test_p0_critical, p0_multi_factor, p1_advisor_feedback, p1_features, p1_position_valuation, p1_subfunctions, p2_analysis, p2_features, p2_news_peer_cross, p2_reports_utils, p2_strength_stability, p2_subfunctions, p3_erp_dca_boom_alert, p3_subfunctions | 按 P0–P3 优先级沉淀的缺陷回归 |
| `test_tabN_*`（UI/tab 层） | 17 | test_tab1_pure … test_tab15_trade_review, test_tab_render, test_tab12_tab13 | 每个 tab 的单元测试；`_pure`=纯逻辑抽取测试（不依赖 streamlit），其余=用 mock streamlit 跑 render 冒烟 |
| `test_<module>_*`（模块功能） | 32 | test_financial_metrics(91!), test_risk, test_equity_risk_premium, test_gold_*, test_etf_position, test_data_quality, test_database_new, test_db_schema, test_models, test_chart_utils, test_candle_patterns, test_market_event_signals, test_rebalance_engine, test_predictor_*, test_signal_score, test_trading_calendar, test_macro_daily, test_integration | 针对具体算法/工具模块的单测，覆盖面最广 |
| `test_phaseN*` / `test_bugfix_*` | 3 | test_phase7d, test_phase8, test_bugfix_round4 | 分阶段交付 / bug 修复回归 |
| `test_regression_*` / 基础设施 | 1+ | test_regression_db_isolation, test_imports, test_imports_completeness, test_config, test_runtime_safety | 隔离回归 + 导入完整性 + 运行时安全（AST 静态检查） |

**归纳**：命名高度规整，明显是"路线图驱动 + 优先级回归 + 模块单测 + tab 冒烟"四层结构，工程纪律好。最大头是 `test_financial_metrics.py`（91 个用例），指标层最厚实。

---

## 2. 实际运行测试结果（真实跑过）

### 2.1 验证 harness 可用（子集）
```bash
venv313\Scripts\python.exe -m pytest tests/test_imports.py tests/test_etf_position.py -q -p no:cacheprovider
# 44 passed in 3.17s   ✅ harness 正常
```

### 2.2 全量运行（真实结果，跑过 2 次）
```bash
venv313\Scripts\python.exe -m pytest tests/ -q -p no:cacheprovider
# RUN#1: 2 failed, 1565 passed, 4 skipped, 7 warnings in 142.64s  (collected 1571)
#        ↳ 失败 = test_advisor_function_count + test_production_db_not_modified(生产库被碰, guard 报警)
# RUN#2: 1 failed, 1566 passed, 4 skipped, 7 warnings in 151.73s  (collected 1571)
#        ↳ 失败 = test_advisor_function_count 仅此一项；生产库未被碰(guard 未报警)
# 关键差异: 生产库触碰仅 run#1 出现 → 非确定性 flaky(见 §4.3)
```
（为排除 pytest cache 偶发 `Permission denied` 干扰，已加 `-p no:cacheprovider`；该告警无害，不影响判定。）

### 2.3 失败明细

**失败 1 — `test_d15_code_quality.py::TestAdvisorHelpers::test_advisor_function_count`**
```
E   AssertionError: assert 32 == 23
```
- **性质：测试卫生问题（误报），非生产缺陷。** 该用例硬编码 `assert len(fns) == 23`，但 `src/analysis/advisor.py` 现已演进到 **32 个顶层函数**（注释显示 2026-08-13 删除了 `_check_rebalance_needs`，又陆续新增了 `_analyze_block_trade`、`_query_*` 等）。测试未同步更新。
- **处置**：改为 `>=23` 或范围断言，或直接删除该函数计数用例（脆弱且无业务价值）。

**失败 2 — `test_regression_db_isolation.py::TestProductionFilesUntouched::test_production_db_not_modified`** ⚠️
- 单独跑该用例 **通过**；但在**全量运行 run#1** 中 conftest 的 `pytest_sessionfinish` 主动打印并报错：
```
[P0] 测试污染了真实文件：
  - 生产数据库 (...\data\database\portfolio.db):
      before=(1788420648644959400, 127696896) after=(1788425053090267900, 127696896)
```
- 即：**全量运行期间生产库 mtime 改变、字节数不变（127,696,896）** → 有测试真实触碰了生产库文件，但只写回相同内容（或触发了 WAL checkpoint / 无操作写）。**尺寸不变 ⇒ 数据内容完好**（已只读校验，见 §4.2）。
- **该泄漏是非确定性的（flaky）**：第二次全量运行 run#2（`1 failed, 1566 passed, 4 skipped, 151.73s`）**未复现**——`grep -c "测试污染了真实文件" _pytest_full_run2.log` = 0，且生产库 mtime 停留在 run#1 的时间戳（16:44:13）未再变化。**比确定性泄漏更危险**：CI 可能恰巧不触发而漏检，随机某次却会碰到真库。
- 这是本次评估最关键的风险点。

### 2.4 跳过与告警
- **4 skipped**：`test_gold_snapshots.py`（快照缺失/过期）、`test_runtime_safety.py`（架构度量：某 tab 含 >300 行函数时按非阻断 skip）、`test_regression_db_isolation.py`（可选隔离校验）— 全部非阻断、理由明确。
- **7 warnings**：`numpy invalid value encountered in divide`（已处理）、`PytestRemovedIn10Warning: Class-scoped fixture`（fixture 写法）、`news_fetcher.py` 日期解析 `DeprecationWarning` — 全部无害。

---

## 3. 覆盖率真实水位与盲区

来源：`coverage.json`（最近一次测量快照）。`.coveragerc` 标注 `source = src`，但快照实际覆盖了 `src/tabs/config/components`（说明历史某次测量未按 coveragerc 收敛）。

### 3.1 总体
- **行覆盖率 50.63%**（6427 / 12693 语句），**81 个文件**被测量。
- 分档：0% ×1 ｜ 1–25% ×20 ｜ 25–50% ×12 ｜ 50–75% ×16 ｜ 75–100% ×25。

### 3.2 分层
| 层 | 测量文件数 | 平均覆盖 | 中位数 |
|----|-----------|----------|--------|
| tabs（UI 层） | 32 | 57.9% | 69.5% |
| src（逻辑层） | 33 | 47.6% | 50.0% |

### 3.3 关键模块（lead 关注点）
| 模块 | 覆盖 | 说明 |
|------|------|------|
| **src/analysis/advisor.py** | **68.5%**（501 语句，缺 158） | 受关注模块，覆盖尚可 |
| src/analysis/portfolio.py | **13.4%**（缺 206） | ⚠️ 核心分析，重度盲区 |
| src/analysis/indicator_backtest.py | **10.6%** | ⚠️ 回测，盲区 |
| src/data_sources/fund_flow.py | **8.8%**（341 语句缺 311） | ⚠️ 数据源，几乎无测 |
| src/data_sources/monitor_push2his.py | **0%** | ⚠️ 数据源，零覆盖 |
| src/utils/news_fetcher.py | **16.7%**（227 语句缺 189） | ⚠️ 网络 IO，盲区 |
| src/utils/enhanced_report.py / backfill.py | 10% / 11% | 盲区 |
| src/report/chart_generator.py | **0%** | ⚠️ 报告生成，零覆盖 |
| src/report/excel_report.py | 11.9% | 盲区 |
| tabs/tab14_market_events.py | **10.6%**（311 语句缺 278） | ⚠️ tab，盲区 |
| run_analysis.py | 16.6% | 盲区 |
| sidebar.py | 14.0% | 盲区 |
| dashboard.py | 35.9%（507 语句缺 325） | 中等 |

### 3.4 最大盲区结论
1. **核心 analysis 路径**：`portfolio.py` 13%、`indicator_backtest.py` 11% — 组合分析/回测几乎是黑盒。
2. **数据源 IO**：`fund_flow.py` 9%、`monitor_push2his.py` 0%、`news_fetcher.py` 17% — 外部数据获取几乎无行为测试。
3. **报告生成**：`chart_generator.py` 0%、`excel_report.py` 12% — 产出物无保障。
4. **⚠️ 高行覆盖 ≠ 高断言质量**：多数 tab 显示 90%+ 行覆盖（tab3 93%、tab4 93%、tab6 94%、tab12 99%），但**由 render 冒烟测试达成**（调用 `render_tabX()` 仅断言"不抛异常"）。UI 层真实置信度低于数字所示 —— 见 §5 建议。

---

## 4. 能否信任现有套件作为回归基线

### 4.1 断言真实性 ✅
- 2050 条 `assert`；80/83 测试模块含真实断言。仅 3 文件零 `assert`（`test_tab_render.py`、`test_interactive_branches.py`、`test_runtime_safety.py`），经阅码确认：
  - `test_tab_render.py` / `test_interactive_branches.py` 为 **render 冒烟**（执行即断言不崩溃）—— 弱断言但非空跑；
  - `test_runtime_safety.py` 用 `pytest.fail()`（AST 静态核查），断言真实。
- **无"测试完整性反作弊"信号**：全仓仅 13 处 `skip/xfail`，全部合法（快照缺失/可选隔离/架构度量）；未发现删测试、弱化断言、`.only`、篡改框架配置等作弊痕迹。
  - 注：以上为**当前快照**判定；建议交接时以"已知良好提交"为基线做一次 `git diff` 对照，确认历史无作弊。

### 4.2 生产库隔离机制 —— 设计 A，执行 C ⚠️
`conftest.py` 的 P0 隔离设计**非常扎实**：
1. 会话最早期把 `DATABASE_PATH` 重定向到**生产库副本**（tempfile 真复制，独立 inode，非软/硬链）；
2. **monkeypatch `sqlite3.connect`**：任何指向生产库路径的连接一律改道到副本（覆盖测试侧硬编码绝对/相对路径、以及 `src/report/smart_report.py:61` 写死相对路径等）；
3. `_repatch_loaded_modules`：修正隔离前已绑定生产路径的模块常量；
4. `pytest_sessionfinish` **指纹校验**：会话结束比对生产库 + `.env` 指纹，被改即 `exitstatus=1` 并红字报警。
→ **该 guard 在本次全量运行中确实生效并报警**（见 §2.3 失败 2），设计值得肯定。

**但执行层存在真实泄漏**：全量运行改动了生产库 mtime（尺寸不变、数据完好）。根因高概率假设：
- `config/settings.py:57`：`DATABASE_PATH = Path(env('DATABASE_PATH', str(DATABASE_DIR / "portfolio.db")))` —— 在 import 期从 `os.environ` 计算，**若此时 `DATABASE_PATH` 不在 env 中则回退到生产默认路径**；
- `tests/test_d5_env_config.py::TestEnvIntegration` 显式 `importlib.reload(config.settings)`（其 fixture 注释已自述此为危险区）；一旦 reload 时 `DATABASE_PATH` 不在 env，模块级路径回退到生产，任何**按调用时读取** `config.settings.DATABASE_PATH` 的代码即指向生产；
- sqlite3 重定向器依赖**路径解析等价**（`Path(candidate).resolve() == PRODUCTION_DB.resolve()`），对 CWD 偏移下的相对路径打开存在漏判窗口。

**只读校验生产库完整性（未写入）**：
```python
sqlite3.connect("file:.../data/database/portfolio.db?mode=ro")
PRAGMA integrity_check          -> ('ok',)
表数 40；advice_history 2429 行、alerts 82、daily_news 7880、...  ---- 数据完好
```
→ 本次评估未损坏生产数据；但**套件本身在当前状态下运行会触碰生产库文件**，CI 直接连真库跑不安全。

### 4.3 Flaky（稳定性）
- 失败 1（`test_advisor_function_count` 断言 32==23）：**确定性失败**，两个全量 run 均失败，不改测试/代码会一直红。
- 失败 2（生产库 guard / `test_production_db_not_modified`）：**非确定性 flaky** —— run#1 触发（conftest 报警 + 该用例失败），run#2 **未触发**（该用例通过、日志无"污染"字样）。即生产库被触碰是**概率性**的。
  - 后果：CI 可能恰好不触发而报绿，但任意一次全量运行都有几率真实触碰生产库文件（mtime 已实测被 run#1 改动一次，数据完好）。
  - 因非确定性，**根因无法靠单次 bisection 稳定复现**，需用 `--forked` / `--randomly-seed` 或固定 test_d5 在前的顺序做概率性复现来定位（高概率在 `config/settings.py:57` 的 `DATABASE_PATH` 回退 + `test_d5_env_config.py` 的 `importlib.reload(config.settings)` 耦合处，见 §4.2）。
- 两次全量均为 1565/1566 passed、**0 网络层崩溃**，说明套件整体稳定；不稳定点仅集中在上述两处。

---

## 5. 接手后测试策略建议（优先级排序）

### P0 — 必须先修（否则套件不可信为"不碰生产"基线）
1. **堵住生产库隔离泄漏**：
   - 方案 A（推荐）：重定向器按**文件名 + 解析归一**匹配，且无论 CWD 如何都把"指向 data/database/portfolio.db 的任何连接"改道到 conftest 副本；
   - 方案 B：`config.settings.DATABASE_PATH` 在 conftest 接管后**永不回退到生产默认**（env 置位期间 reload 也只取副本）；`test_d5` 的 reload 前后确保 `DATABASE_PATH` 始终在 env 中；
   - 修复后用"全量运行 + conftest guard 干净通过"作为验收门禁。
2. **修掉陈旧断言** `test_d15_code_quality.py::test_advisor_function_count`：把 `==23` 改为 `>=23` 或删除该脆弱用例，恢复套件全绿。

### P1 — 补齐核心盲区（行为级，非冒烟）
3. **analysis 核心**：为 `portfolio.py`(13%)、`indicator_backtest.py`(11%) 补**行为断言**单测（输入→确定性输出），而非仅覆盖行数。
4. **数据源 IO**：`fund_flow.py`(9%)、`monitor_push2his.py`(0%)、`news_fetcher.py`(17%) 用 **mock HTTP/akshare** 固化"正常/超时/脏数据/空响应"路径。
5. **报告生成**：`chart_generator.py`(0%)、`excel_report.py`(12%) 补产出物结构断言。

### P2 — 提升 UI 层真实置信度
6. **tab 测试从"不崩"升级为"对"**：在 `test_tab*_pure.py` 基础上加**值级断言**（如某指标数值、DataFrame 列、图表 trace 数量），当前 90%+ 行覆盖掩盖了弱断言。

### P3 — 工程化
7. **锁定依赖并重测覆盖**：`venv313` 为 Python 3.13.14，建议出 `requirements.lock`；当前 `coverage.json` 可能滞后于代码演进（advisor 函数数已变），修复后重跑 `pytest --cov` 刷新基线。
8. **Tag 已知良好提交**为回归基线锚点，并在 CI 禁止对真库跑测试（统一用 `:memory:` 或副本）。

### 能否现在作为回归基线？
- **分析/指标层（`test_financial_metrics` 91 例、`test_risk`、`test_equity_risk_premium`、`test_rebalance_engine`、`test_etf_position`、`advisor` 68%）→ 立即可信**，可马上纳入回归。
- **整仓作为"不碰生产"门禁 → 暂不可信**，须先完成 P0 两项。
- **UI/数据源层 → 弱可信**，须完成 P1/P2 后才可信。

---

## 附录：实测命令与产物
- 全量 run#1：`venv313\Scripts\python.exe -m pytest tests/ -q -p no:cacheprovider` → `2 failed, 1565 passed, 4 skipped, 7 warnings, 142.64s`（日志 `_pytest_full.log`）
- 全量 run#2：同上 → `1 failed, 1566 passed, 4 skipped, 7 warnings, 151.73s`（日志 `_pytest_full_run2.log`）；生产库污染 grep = 0 → **泄漏为 flaky**
- 子集验证：`tests/test_imports.py tests/test_etf_position.py` → `44 passed in 3.17s`
- 生产库只读校验：`PRAGMA integrity_check == ok`，40 表数据完好（未写入）；mtime 在 run#1 被改为 16:44:13 后未再变（run#2 未触碰）
- 隔离泄漏证据：`_pytest_full.log` 第 96–97 行 `[P0] 测试污染了真实文件` + 生产库 mtime 变更（仅 run#1）
- 本评估新增文件：`docs/handover/02_test_assessment.md`（本报告）；未改动任何生产代码/数据库；
  评估过程中因全量运行触发了套件自身的隔离缺陷，生产库 mtime 被改动一次（数据完好，已只读校验）
