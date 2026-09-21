# 15 · 项目状态全面评估报告

> **状态**：评估已完成（**诊断与判定分节**）。本轮**只读**，未改任何生产代码、未写生产库、未跑线上日报管线。
> **评估对象**：`lingxi-claw/portfolio_tracker`（真实工程目录 `D:\HuaweiMoveData\Users\HUAWEI\Documents\lingxi-claw\portfolio_tracker`）。
> **基线（实测）**：评估日 `2026-09-20 23:39` ~ `2026-09-21`；所测 commit **`96a2967`**（`origin/master` 同步，ahead/behind = 0/0）；环境 `venv313` / Python **3.13.14**；生产库全程 `file:data/database/portfolio.db?mode=ro` 只读；DB 43 张表。
> **方法**：8 个维度并行只读调研；每条结论须带 `file:line` 或只读 SQL 实测；**不采信 2026-09-03 交接评估（00~06）的任何数字**；无法验证的一律标「未验证」，不以估算冒充实测。
> **方案来源**：`docs/handover/14_project_status_assessment_plan.md`。

---

## 0. 执行摘要

| # | 维度 | 判定 | 高风险的项 |
|---|---|---|---|
| 1 | 代码架构与健康度 | 待改进 | DB 接入层**有缺陷**（5 处绕过统一入口直连生产库） |
| 2 | 测试健康度 | 待改进 | 全量回归**恒红 exit=1**；CI 与本地口径分叉 |
| 3 | 数据完整性 | 待改进（偏有缺陷） | `etf_price_history` **覆盖断崖且完全静默** |
| 4 | 运维就绪度 | **有缺陷** | 退出码被邮件覆盖；备份同盘无异地无演练；调度脚本失效 |
| 5 | 文档准确性 | 待改进 | `13` 与 `07` 活源口径冲突；Tab 数 15/17/18 三处并存 |
| 6 | 产品/功能完整性 | 待改进 | tab8 导出 100% 崩溃；`#140` 哨兵换形态存活；advisor 闭环断裂 |
| 7 | 安全（静态） | 待改进 | 仓库外 2 份 mirror 仍含真实生产库 |
| 8 | 性能/可扩展性 | 待改进 | `journal_mode=delete` 从未开 WAL；阶段 3.5 无超时预算 |

**总判定：待改进**，但含 **9 项须优先处置的高风险**（见 §9）。

### 本轮被实测推翻的旧结论（重要）

| 旧结论 | 实测 | 出处 |
|---|---|---|
| 「历史从未有 .db blob」 | 路径层确入库过 3 条 `.db`，但 blob 全为 `e69de29…`（**0 字节**，09-03 filter-repo 剥离内容） | 维 5 / 维 7 |
| `03:537`「`database is locked` 实测 0 次」 | 实际 **2 次**（09-15 12:06），且导致 `advice_history` 写入静默丢失 | 维 8 |
| 「`scripts/backup_db.py` 从未被自动化调用」 | **已过期**：`run_analysis.py:942-968` 阶段零无条件调用，09-16/17/18 连续 3 天有自动产物 | 维 4 |
| 全量回归 1881/1877/4/0 | 现为 **1908/1903/4/1**，`e957464` 之后新增 1 个真实失败 | 维 2 |
| `13` 立论基础「A 族 5 例需回填」 | A 族经 09-20 重测为 **0 例**，`13` 未同步 | 维 5 |
| 「数据新鲜」（本报告 Phase 0 初判） | **错误**：只查 `MAX(date)` 未查每日 code 计数，漏掉 09-18 整天 0 行 | 维 3（自我纠正） |

> **方法论教训（已入台账）**：判「数据新鲜」必须查**每日行数/只数与基线是否一致**，不能只看 `MAX(date)`。

---

## 1. 代码架构与健康度 —— 待改进

**诊断（证据）**

- **God file**：`data_loader.py`（**仓库根**，2073 行 / 58 函数 / 85 处 SQL / **181 处引用，全仓第一**），且 `:15 import streamlit` ⇒ **所有无头管线为拿一个 DB 连接必须加载 Streamlit**。另 `src/analysis/advisor.py` 2035 行、`tabs/tab8_advice.py` 1751、`tabs/_helpers.py` 1706、`src/analysis/signal_backtest.py` 1399。统计范围 `src/`+`tabs/` = 113 个 .py / 44,158 行。
- **DB 接入层（本维度判为有缺陷）**：所谓「统一入口」`src/utils/database.py:246` 只是**两跳转发**，底层是 `data_loader.py:64/72` 的裸 `sqlite3.connect`。全仓 `get_db_connection` 有 **4 个不同定义**（`data_loader.py:64`、`src/utils/database.py:246`、`factor_attribution.py:36` 合规、`nav_engine.py:55` 绕过）。
- **5 处生产代码绕过统一入口直连生产库**，其中 4 处在无人值守管线上：
  | file:line | 连接串 | 在管线 |
  |---|---|---|
  | `src/analysis/nav_engine.py:57` | `sqlite3.connect(str(DATABASE_PATH))` | 是（`run_analysis.py:1426`） |
  | `src/analysis/etf_position.py:57` | `sqlite3.connect(db_path)` | 是（`:346/:409/:440`） |
  | `src/analysis/portfolio_risk.py:795` | `sqlite3.connect(str(DATABASE_PATH))` | 是（`:307`） |
  | `src/analysis/snapshot_gate.py:136` | `sqlite3.connect(s)` | 是（契约 1/2 闸门） |
  | `src/analysis/rebalance_engine.py:833` | `sqlite3.connect(db_path)` | 否（`__main__` 演示块） |
- **连接属性不一致**：全仓只有 `snapshot_gate.py:137` 设了 `PRAGMA busy_timeout=15000`；8 个索引只在走 `src/utils/database` 那条路时才建 ⇒ 绕过路径完全不过索引初始化。
- **同文件内重复定义 7 个符号**（`load_peer_penetration` 定义 3 次），Python **静默取最后一个**；`data_loader.py` 另有 2 段紧跟 `return` 之后、永不执行的函数体级字符串块 ⇒ **改错位置不报错、只是没效果**。
- 分层：`src→tabs` 逆依赖 **0 处** ✅；但 **14 个 `src/**` 模块反向依赖根级 `data_loader`**；`tabs/` 63 处内嵌 SQL，其中 `tabs/tab8_advice.py:1354` 直接 `UPDATE advice_history`。
- 循环导入：Tarjan 实测 **0 个强连通分量**（含函数内延迟 import 也算 0），但靠延迟 import 维持，属脆弱平衡。
- 孤儿模块 5 个：`src/data_sources/{base,sina,akshare_ds,monitor_push2his}` + `predictor/tier0.py`（约 548 行）；`src/report/risk_report.py` 仅测试引用。

**判定**：待改进；**DB 接入层单项 = 有缺陷（高）**。后果：任何「在连接层统一加只读化 / 熔断 / 审计」的改造**都无法一处生效** —— 与 `12_` §17.11「判据正确性 ≠ 判据可达性」是同一类病。

---

## 2. 测试健康度 —— 待改进

**诊断（证据）**

- 全量实测 `pytest tests/ -q -p no:cacheprovider`：**1908 collected / 1903 passed / 4 skipped / 1 failed**，152.9s，**exit=1**（基线 `e957464` 为 1881/1877/4/0）。
- 唯一失败：`tests/test_split_merge_guard.py::test_factor_attribution_excludes_split_day` → `PermissionError: [WinError 32] … pytest-274/…/portfolio.db`。
  - 两轮全量**同样失败同样报错** ⇒ 确定性，非 flaky、非超时掐断；单独跑该文件 `14 passed`。
  - **业务断言其实全过**（日志已打印「因子归因已剔除 1 个拆分/合并伪收益日，60→59 观测」）⇒ **测试卫生缺陷，非产品缺陷**。
  - 根因：`_build_temp_risk_db()` `:298 return conn` 返回**未关闭**连接，调用点 `:307`/`:327` 忽略返回值；叠加 Python 3.12+ sqlite 连接 LRU 缓存使 `close()` 不立即放句柄。本仓 `tests/test_etf_features_qfq_split.py:18-30` 已有正确范式 `_cleanup_db()`（`gc.collect()` 后重试），新用例未沿用。
- **CI 与本地口径分叉（高）**：`.github/workflows/ci.yml:28` 设 `DATABASE_PATH=":memory:"`，实测按此跑 → `no such table: macro_daily/portfolio_snapshots/portfolio_summary/execution_logs`，**4 failed**。⇒ CI 若真跑就是红的；本地全绿靠 conftest 复制 144MB 生产库。
- 隔离守卫**健全** ✅：conftest 三层防护（`:63` 复制隔离、`:92` 重 patch、`:117` `sqlite3.connect` 硬兜底）；本轮全量**无 `[P0] 测试污染了真实文件`**、`[DB-隔离]` 硬兜底 **0 次触发**；两个守卫口径不一致的历史问题已修复（`conftest.py:270-292`）。
  - 残留（中）：8 处硬编码生产库路径靠运行时改道兜底，其中 `src/report/smart_report.py:61` 写死 `data/database/portfolio.db` 且执行 CREATE TABLE + INSERT。
- 门禁 `tests/test_imports.py` **17 passed** ✅（但只覆盖 17/1908 ≈ 0.9%，仅验证能 import）。
- 脆弱断言：`test_advisor_function_count` 已重构为白名单存在性，**与实现一致（实测 35 个函数）**；`tests/test_runtime_safety.py:556-559` 用 `pytest.skip` 隐藏「函数超 300 行」违规（2 个）；`tests/test_gold_snapshots.py` 2 个因数据过期静默 skip 约 3 个月。
- 覆盖率**未测**：venv313 未装 coverage/pytest-cov；现存 `.coverage` mtime 2026-06-16 已过期；`.coveragerc:19 fail_under=30` 阈值过低。

**判定**：待改进（高）。「全绿」这个信号已失真：全量恒红 + CI 分叉 + skip 被当通过。

---

## 3. 数据完整性 —— 待改进（偏有缺陷）

**诊断（证据）**

- 🔴 **高：`etf_price_history` 覆盖断崖且完全静默**。每日覆盖只数 `09-14 = 23` → `09-15/16/17 = 14/14/13` → **`09-18 = 0`**。缺 9 只 `159267 159770 159796 159819 159949 159992 515010 515120 561910`，全走 TX 源（`akshare_stock_zh_a_hist_tx_qfq`），**EM 回退源未接管**。
  - 连带 `etf_features` 特征 NULL 率 `09-14 16.2%` → `09-17 31.7%` → **`09-18 50.9%`**（ret_1d/5d/20d/60d、mom、kdj、atr、parkinson_vol 全空）。
  - **静默性**：`data_quality_issues` 197 行**无一条**关于行情缺口；`collection_retry_queue` 28 行**全是 stock_margin**。
  - 时间线与 `execution_logs` 的 `2026-09-15 15:30 failed: All arrays must be of the same length` **完全重合**，此后未见补采。
- 快照：09-18 **34 行/34 只**，字面符合铁律，但**成因不对** —— 周五应为 35，`027293` 末次 09-11 已缺 5 个交易日；`alerts` 09-18 有 `otc_nav_missing` error「13 只当日净值缺失，其中 1 只当日快照缺行 ⇒ 整篮子未落库」。
- OTC 净值 09-18 **冻结 2 个交易日**（基线 08-01 起 385 行：滞后 0 日 89.9% / 1 日 7.3% / 2 日仅 2.9%）⇒ 尾部偏离，是真缺陷；`880013` 恒 1.0 属货币基金设计约定，不计。
- 13 例折算：判别器 `classify_split_merge` 扫全库命中 **7 例**（与 A/B 族完全一致），C 族 6 例判 False ✅；**伪收益已清零**（`ABS(daily_return)>25%` = **0 行**）。
  - ❌ 但 `is_split_merge` 列在**生产 `etf_features` 与 `portfolio_nav` 均不存在**（`PRAGMA table_info` 计数 0）。schema `db_schema.py:506` 已声明、`features.py:341` 有幂等 ALTER，但 etf_features 末次重建 09-20 **09:15** 早于代码落地 `c61d9b0` **15:45** ⇒ 闸门从未落到生产数据。实证：`510500` `2015-04-15` 的 `ma5/ma20` 仍是台阶原值 `(1.3487352, 2.3611350)`。实际只影响 1 行（其余 6 例 qfq 已覆盖）。
- ✅ 守住：复制行守卫 **133 行/11 只/06-15~09-18**、被 void 的 ETF = **0**、`C(22,2)=231`；fund_flows 09-16 那 20 行 NULL **全保留**、`confidence` 已由 1.0 诚实化为 **0.0**、未置 0 未删；`etf_predictions` 封存零新写入。
- `daily_return` 口径：参与篮子实测 = 22 只 ETF + 880013（分母 939,112.79 精确对上）；覆盖度 **61.25%**（运行期实测，未硬编码）；计算侧**无与 total_value 相乘的误用**（误乘会高估 **63.2%**）。展示侧：`dashboard.py:1075` 把 34 只口径 `total_value` 与 22 只口径 `daily_return` 并排展示、**无口径脚注**（中偏低）。

**判定**：待改进（高，偏有缺陷一侧）。守住的核心不变量不少，但行情断崖 + 静默是当前最实质的生产风险。

---

## 4. 运维就绪度 —— **有缺陷**

**诊断（证据）**

- 🔴 **退出码被邮件步骤覆盖**（最高危，`scheduled_run.bat:15/18/20`）：`exit /b %ERRORLEVEL%` 取的是**最后一条命令（邮件脚本）**的返回值。09-15 实测日志链：`[ERROR] analysis failed, rc=1` → `[EMAIL] 已重生今日报告` → `[EMAIL] 已推送日报 2026-09-15` → `[OK] daily report email sent` ⇒ **整体 exit 0**，Windows 任务计划 Last Result 显示 **0x0 成功**。⇒ **失败日既不告警，还把"重生"的报告当当日日报推送出去**。（`03:529` 早已提出，未落地。）
- 🔴 **备份：同盘 + 无异地 + 无恢复演练**。生产库与 `data/backups/` **同在 D 盘**；26 个脚本**无 restore 脚本**；`tests/test_d14_backup.py` 只测备份/清理、不测恢复；RTO/RPO 无定义。⚠️ `03:645` 给的"异地"方案 `cp … "D:/backup_$(date).db"` **还是 D 盘**。
- 🔴 **`setup_scheduler.ps1` 不可用**：`:12` 硬编码 `C:\Users\HUAWEI\…`（工程在 D 盘）⇒ `:15 Test-Path` 必失败 exit 1；`:38` 触发器 **15:10** 与实际 15:30 矛盾；`:34` 注册动作是 `run_analysis.bat` 而非 `scheduled_run.bat` ⇒ 照此建任务会**断掉日报邮件链路**（`03:369` 已记录）。
- 🟠 顶层备份实测 **15 份 / 2.0GB**，超文档声明的 7 份/0.93GB。根因：`backup_db.py:44-60` `cleanup_old_backups` 是**纯年龄判据**（7 天 + keep_min=3），**无条数上限** ⇒ 15 份全 ≤4 天龄，一份都不会删。（文档里的"7 个"是 09-17 人工 prune 的结果，非机制稳态。）
- 🟠 09-20 全天零备份覆盖（最新备份 mtime 09-19 23:41 vs 生产库 09-20 09:15，缺口 ~14h）。
- 🟠 事故物证被覆盖：`run_report_2026-09-16.json` 实测 `mode=backfill` / `generated_at=09-17 09:06` / `run_status=ok` ⇒ 09-16 事故当天原始账本已被回填洗白（呼应「运行账本非不可变物证」那条旧债）。
- 🟠 CI 装**未锁** `requirements.txt` + Python **3.12 ≠ 生产 3.13.14** ⇒ CI 绿 ≠ 生产能跑；CI 不跑 lint/类型/安全，pre-commit 未纳入 CI 可被 `--no-verify` 绕过。
- 🟠 Dockerfile：3.12 / **无 `.dockerignore`**（会把 venv313、2GB 备份打进镜像）/ 无 volume / CMD 只跑 streamlit ⇒ 容器内定时任务一个都跑不起来。
- ✅ `.bat`/`.ps1` 全部纯 CRLF（裸 LF = 0）；`requirements.lock` 与 venv313 **92 包 0 漂移**（本机可复现）；`.gitignore` 对 .env/备份/DB 覆盖到位。
- **未验证**（需用户本机）：① 计划任务实际注册状态（`schtasks /query /tn PortfolioDailyAnalysis /fo LIST /v`，沙箱内无输出）；② 06-02~08-04 日志零记录原因；③ 15:10 重复任务是否已卸载；④ CI/Docker 是否曾成功。

**判定**：**有缺陷**（3 项高已实际造成后果，不是"待改进"）。

---

## 5. 文档准确性 —— 待改进

**诊断（证据）**

- ✅ **`7d49cb1` 那轮整改本身达标**：8 横幅全在位、横幅内 4 条事实逐条实测为真、9 文件归档干净（git 记 R100）、2 处断链已修、全仓反查旧路径**无遗留**；**全量 174 个 md 的 Markdown 语法链接断链 = 0**。
- 🔴 **H1（最紧急）：`13` 仍按已被推翻的「A 族 5 例」立论**。`13_issue12_backfill_feasibility.md:41`「A 族 5 例的 market_value 在快照侧仍是错误的整倍数放大」、`:146`「仅 5 行 A 族快照需改」；而 `07:1644` 已定稿三族（**A 族 09-20 重测为 0 例**）、`07:1715`/`07:1868` 明写实测推翻原分类。`13:5` 自己声明「以 07 为准」却未同步 ⇒ **照 `13` 执行"回填 5 行 market_value"是一次已被论证为无收益的写库动作**。
- 🔴 **H2：README Tab 数失真 17 → 实际 18**。`README.md:20`/`:264` 写 17；实测 `dashboard.py:1080-1099` TAB_REGISTRY = **18**（`:1098` tab18_watchlist「👀 清仓观察」，`89e6254` 引入），README 全文 **0 次**出现 18/watchlist/观察。（历史债 15→17 已修，Tab18 未同步。）
- 🔴 **H3：`ARCHITECTURE.md:55` 仍写「15 个 Tab」**，文件最后更新 `81d16aa`（**2026-08-03**），**无日期、无状态横幅**。
- 🔴 **H4：归档 README 与横幅自相矛盾**。`docs/_archive/2026-09-03_initial_assessment/README.md:7` 称 `handover/00~04` 是「**持续维护的规范版本**」，但 `00:3` 等横幅写的是「09-03 快照，内容已过时，请勿据此判断当前状态」。⇒ 正确表述应为「00~04 是**规范体例版**，但同为 09-03 快照；铁律/判据以 `12_` 为准」。
- 🟠 M1 `07:3-4` 头部「状态：✅ 已修复」与内容打架（`07:1863`「未闭合项」、`:1877`/`:2169`「尚未排期」）；M2 ETF 数 23 → 实测 `ETF_CATEGORIES`=**34** / `etf_technical` distinct=**35**（`07:57` 是活源）；M3「30 张表」→ 实测 **42**；M4 `DEPLOYMENT.md:85/94` 仍是 C 盘路径；M5 CHANGELOG 停在 09-03，之后整轮修复一条未记。
- ⚠️ 横幅「无任何 `.db` blob」措辞略绝对（历史确有 3 条 `.db` 路径，blob 为 0 字节空对象）⇒ 建议改「无任何**含内容的** .db blob」。
- ⚠️ **本报告的方案稿 `14:43` 自身也写了「17 个 Tab」** ⇒ 印证「计数类断言一律现算」铁律。

**判定**：待改进（4 高）。整改解决了「09-03 快照误导」，但**未覆盖「活文档随代码漂移」**。

---

## 6. 产品 / 功能完整性 —— 待改进

**诊断（证据）**

- **基线纠正**：注册的是 **18 个 Tab，不是 17**。`dashboard.py:1102` 注释仍写「15 个 Tab」⇒ 15/17/18 三处口径并存。
- **18/18 全部真取数真渲染，零空壳、零「待开发」占位** ✅（mock streamlit + 只读连接逐个调用 `render_tabN()`；全仓 tabs/*.py 扫 TODO/FIXME/待开发/敬请期待 = 0 命中）。tab18 极薄（WATCHLIST_CODES 仅 159732，纯 metric 无图表）。
- 🔴 **P0-1（高）：tab8「导出 Excel」100% 崩溃** —— `src/report/excel_report.py:386` `fill = VOID_FILL if is_void else (s.ALT_FILL …)`，`VOID_FILL` 定义在 `Styles` 类内（`:48`），此处漏 `s.` 前缀 ⇒ `NameError`。触发条件：`is_void=True` 行存在，实测 2026-09-18 有 **11 只** ⇒ 点击即崩。**一行可修**（`VOID_FILL` → `s.VOID_FILL`）。*(本报告作者已独立 grep 复核确认)*
- 🟡 P0-2（中）：tab5 导出/截图依赖 playwright chromium，本机未安装；`src/utils/screenshot.py:4` 声称"缺失时优雅降级"，实测未降级而是抛出 ⇒ 声明与行为不符。
- 🔴 **`#140` 残留哨兵（高），换了形态仍存活**：
  - ① 资金流列：10 只场外在 `fund_flows` 中**一行都没有**，`fund_flow_net` 默认 `0.0`、`fund_flow_asof` 默认 `""`；UI 判据 `tab8_advice.py:1644` 在 `asof=""` 时**短路不打标注** ⇒ 显示裸 `+0`，与「当日净流入恰好为 0」**逐字符相同**。对比：001323/002152 有数据但滞后 47 天反而**有** `(as-of 2026-08-05)` 标注 ⇒ **没数据的不标、陈旧的才标，逻辑颠倒**。
  - ② 风险评分列：`risk_available` 恒 True（`compute_etf_risk_scan` 任何输入都返回 dict）；实测 **13/34** 标的各维度实际「数据不足」（用 `score=50` 兜底），却显示 51.4「中等风险」等**看起来正常的数字**；`_score_downside` 还会凭空产出「均线 中性；MACD 中性；KDJ 中性；趋势 震荡整理」这类像真实研判的文案。纯空输入实测 → **46.9「中等风险·关注」**。
  - ⇒ **用户无法分辨**（真中性 50 与假 50 逐字符相同）。
- tab17 估值闸门 F2：实测**拦截 0 只**（估值因子可用 20/20 权益 ETF，`VAL_MIN_DAYS=250` 全部通过）⇒ 闸门逻辑正确、当前数据充分。但文档三处矛盾：`tabs/tab17_etf_position.py:14-15` 仍写「因历史不足 <250 日**自动禁用**」（已过期）、`:287` 写「19/20」、实测 20/20。另 tab17 只覆盖 23 只（12 只场外完全不在评估集）且**UI 无提示**。
- **D12 是唯一未落地项**（中）：仅出现在 3 个文档且全是「缺失」描述，**无 `tests/test_d12*`、无代码引用**，自 09-03 至今未处置。D1-D11/D13-D15 均有落地证据。
- 市场事件「待补采」空洞 —— **实测已修复**：5 张事件表均有真实数据（stock_margin 279,753 / stock_institution_research 27,544 / stock_block_trade 11,763 / stock_lhb 10,421 / stock_holder_change 4,331）；`collection_retry_queue` 27 done + 1 skipped_non_trading，**僵尸项 = 0**。新发现小空洞：`research_reports` **0 行**（采集器写了但从未接入 `run_analysis.py`）、`trades` 死表、`market_breadth` 仅 18 行。
- 🔴 **advisor 闭环：前半通，人工反馈段是死的**。`advice_history` 2,692 条 / `advice_outcome` 2,692 条（2,273 settled）⇒ 自动归因在跑；未来函数红线实现正确（`run_analysis.py:754`）。但：
  - ① 人工反馈从未发生：`status` **2,692/2,692 全 'pending'**，`action_taken`/`feedback`/`resolved_at` **全 NULL** ⇒ D3「建议→执行→效果」只到「出建议 + 事后机器归因」。
  - ② 归因基准列被污染：`bench_return_*` 用 `total_value` 比值，实测均值 **+18.8% / +25.3% / +34.9%**，最大 **+142.7%**，**42/2574 条 > +100%**；同期 `fwd_return_*` 仅 −0.67% / −1.62% / −2.79%。实例：advice_date 2026-06-02，total_value 06-02 = 686,012.9 → 06-17 = **1,385,156.74（+101.9%）** ⇒ 是口径切换造成的**假收益**，「跑赢/跑输基准」在 6–7 月窗口不可用。

**判定**：待改进（高）。功能面完整度实质**高于**历史文档口径，但 1 个可复现崩溃 + 哨兵残留 + 闭环断裂需处置。

---

## 7. 安全（静态扫描，用户拍板只做静态）—— 待改进

**诊断（证据）**

- ✅ **真凭证入库 = 0**：扫 368 个已跟踪文件，命中 166 处逐条人工判定，全为变量名/占位符/注释/环境变量读取。关键：`config/settings.py:203` `"password": ""`、`:241` `env('EMAIL_PASSWORD','')`、`.env.example` 全注释态占位且有 `tests/test_d5_env_config.py:150` 守住。常见密钥前缀（sk-/ghp_/AKIA/AIza/Bearer…）**0 处真凭证**；高熵字符串 1135 个候选全为标识符；唯一 32-hex 命中是东方财富 push2 公开固定参数（假阳性）。
- ✅ **生产库数据未进版本控制**：`git ls-files '*.db'` = 0、`ls-tree -r HEAD` 368 文件无 .db、`rev-list --objects` .db 实体 = 0、`fsck --unreachable` 无残留、无 `refs/original/`。**远端未泄露**（`03`-记载 87.9MB blob 仅本地可达；当前 origin/master 已同步到 96a2967）。
  - ⚠️ 措辞修正：路径层确入库过 3 条 `.db`（`f42fa76 portfolio.db`、`c6d4310 data/portfolio.db`、`d497aa1 portfolio_data.db`），`git cat-file -s` 全为 **0 字节**空 blob（`e69de29…`）⇒ 09-03 filter-repo **剥离了内容、只留路径占位**。
- ✅ **（已收口 · 2026-09-21）仓库外 2 份 mirror 含完整真实生产库**：原 `_backup_portfolio_tracker_git_mirror_20260903.git`（33.3MB）与 `_backup_pt_git_mirror_20260903_2310_pre_filterrepo.git`（33.4MB）中的 `data/database/portfolio.db` blob 实测 **87,949,312 B（83.9MB）**。已于 2026-09-21 经用户明确授权**永久删除**（精确路径 `shutil.rmtree` / `rmdir /s /q`，无 glob、无回收站模糊删除），仓库父目录 `_QUARANTINE_LEAK_20260921/` 一并移除。详见 §12。
- 🟠 机器专属硬编码路径 51 处，运行时真问题 5 处：`setup_scheduler.ps1:12/35`（C 盘，脚本已坏）、`config/settings.py:64` `TDX_EXPORT_DIR` 默认值跨机**静默降级到旧快照且无告警**、`scripts/import_aug_2026.py:42`、`audit/_verify_replica_guard.py:13`、`requirements.lock:8`。已整改清零的：`.workbuddy` 路径已改 `Path.home()`、`scripts/fetch_market_data.py` 已用 `__file__` 推导。
- 🟠 `.env` 本机明文 + `st_mode=0o100666`（未收紧 ACL）；`data/risk_lgb_v2_oos.csv` 产物入库未 ignore（低）；个人 QQ 邮箱 `asdfl@qq.com` 硬编码进版本库（低）。
- ✅ `.env` 未被跟踪且被 ignore；`.streamlit/secrets.toml` 被排除；CI 无真密钥（`DATABASE_PATH=:memory:`、`EMAIL_ENABLED=false`）。

**判定**：待改进（无高；中-高 1 项）。

---

## 8. 性能 / 可扩展性 —— 待改进

**诊断（证据）**

- 日报耗时（66 次有耗时记录）：2026-04 avg 25.5s → 05 70.8s → 08 283.2s → **09 avg 347.8s / median 322.5 / max 540.2s**。最慢阶段恒为 **阶段 3.5「行业新闻」**（09-18 占 48.3%，142.9s；p90 183s / max 238s），**无超时预算、无降级开关** ⇒ 单点外源故障可把日报顶到任务时限（1h）。
- **关键归因：耗时增长不是数据库的锅**。含 DB 读写的「阶段一」月度中位数 4 月 18.1s → 9 月 **17.4s 持平**（同期持仓 23→34）；增长 **100% 来自后加的联网阶段**（3.5 新闻 / 3.2 资金流 / 3.25 预测底座 / 阶段 0 场外净值）。
- 🔴 **`journal_mode = delete`，全项目从未开 WAL**（grep `journal_mode` **0 命中**）；`synchronous=2 (FULL)`。`busy_timeout` 仅 5 处；主连接 `data_loader.py:72` 既不设 WAL 也不设 busy_timeout（仅 Python 默认 5s）⇒ delete 模式下任何写事务加**全库排他锁**，读也被阻塞。
- 🔴 **`database is locked` 实测 2 次**（不是旧文档的 0 次）：`logs/portfolio_20260915.log` 12:06:20 / 12:06:26，`src/report/smart_report.py:88-102` 写 `advice_history` 失败**只 warning 不重试 ⇒ 建议历史静默丢失**。当天该 log 有 ~44 次 `run_analysis` 重叠执行。⇒ `03:537` 结论已过期。
- **查询侧无瓶颈** ✅：全表扫描 36,275 行仅 42ms；主表索引齐全且 EXPLAIN QUERY PLAN **全部 SEARCH**（`portfolio_snapshots(date)`、`etf_features(date,code)` 等均有）。
- 🟠 小问题：`portfolio_snapshots` 有**重复冗余索引** `idx_snapshot_date` 与 `idx_snap_date`（同为 `(date)`）；`advice_history` 等 **7 张表零索引**（advice_history 是每日写入表）；`stock_margin` **279,753 行 + 3 索引 = 44.2MB，占 DB 32%**，~2000 行/日累积。DB 体积 ~+0.75MB/日（09-16 134.48MB → 09-20 137.46MB）。
- 并发窗口：15:30 主流水线（最坏 15:39:38 结束）与 16:30 补采（43 份 `supplemental_*.json` 的 `run_at` 实测全在 16:31:07~16:32:09，**周末也跑**）**不冲突**（隔 ~51min）；但 **16:30 与 16:40 仅隔 8~10min**。16:40 的 `extract_portfolio.py:7` 以**读写模式**打开（虽只 SELECT）⇒ 叠加 delete 模式会在写事务期间被阻塞。
- **未测出**：16:40/17:00 自动化的真实触发时刻（`schtasks` 被沙箱拦、`C:\Windows\System32\Tasks` WinError 5，仅有 `03:44-46` 二手文档证据）；写事务持锁时长；Dashboard 并发影响；阶段五之后 `send_report_email.bat` 独立进程耗时。

**判定**：待改进（中）。

---

## 9. 整改优先级建议（供立项，本次未改任何代码）

### P0 — 立即处置（均有明确 file:line，改动小、收益大）

| # | 项 | 位置 | 改动量 |
|---|---|---|---|
| 1 | **tab8 导出崩溃** `VOID_FILL` → `s.VOID_FILL` | `src/report/excel_report.py:386` | 一行 |
| 2 | **退出码被邮件覆盖**：保存 `run_analysis.bat` 的 ERRORLEVEL 再 exit | `scheduled_run.bat:15/18/20` | 一行 |
| 3 | **测试全量恒红**：`_build_temp_risk_db` 自行 close + 改用 `_cleanup_db()` | `tests/test_split_merge_guard.py:298/361` | 极小 |
| 4 | **`13` 与 `07` 口径冲突**：`13` 按 A 族 0 例重写结论（防无收益写库） | `docs/handover/13_…:41/85/89/90/124/146` | 纯文档 |
| 5 | **清理/加密仓库外 2 份 mirror**（含 83.9MB 真实库） | `…/_backup_*_git_mirror_20260903*.git` | 运维 | **（已收口 2026-09-21：经授权永久删除，见 §12）** |
| 6 | **`etf_price_history` 缺口**：定位 09-15 后补采为何未恢复 → 回填 09-15~09-18 → 加 DQ 条目 + 重试入队 | 数据/采集 | 中 | **（已收口 2026-09-21：自愈验证 + 新增缺口闸门与自动回补入口，见 §12）** |
| 7 | **`#140` 哨兵**：资金流列无数据时改显式「无数据」；风险列 `risk_available` 判据修正 | `tab8_advice.py:1644`、`etf_risk_scan.py` | 小 |
| 8 | 备份加**条数上限**（保留最新 7，与年龄判据取交集） | `scripts/backup_db.py:44-60` | 小 |
| 9 | **开 WAL + 统一 busy_timeout** | `data_loader.py:72` 等 | 小 |

### P1
- `setup_scheduler.ps1` 三改（路径 `%~dp0`、15:10→15:30、目标改 `scheduled_run.bat`）；加 `scripts/restore_db.py` + 恢复演练用例；真异地副本（**目标盘不能是 D:**）。
- CI 改用 `requirements.lock` + Python 3.13；把依赖真实数据的用例 mark `integration` 并在 CI deselect。
- DB 接入层收敛：连接工厂下沉 `src/utils/database.py`，5 处绕过改回统一入口并统一 PRAGMA。
- `is_split_merge` 落生产（需全量重算 `etf_features` / `portfolio_nav` 后复核）。
- advisor：`bench_return_*` 换用不含资金进出的基准；人工反馈段落库通道。
- 文档：README/ARCHITECTURE Tab 数改 18；`ARCHITECTURE`/`DEPLOYMENT` 加状态横幅或按实测重写；`07` 头部状态改；归档 README 措辞；横幅 `.db` 措辞。
- 阶段 3.5 加超时预算与降级。

### P2
- God file 拆解 `data_loader.py`；删 7 个同文件重复定义 + 2 段悬空字符串块；`src/report/risk_report.py` 与 `utils/risk_report.py` 二选一；5 个孤儿模块处置。
- `stock_margin` 保留期/分区；`portfolio_snapshots` 删冗余索引；7 张零索引表补索引。
- D12 立项或显式关闭；`research_reports`/`trades`/`market_breadth` 三个空/薄表处置。
- 覆盖率工具安装并重设 `fail_under`。

---

## 10. 未验证清单（沙箱 / 权限限制，不做臆断）

1. Windows 计划任务的实际注册状态、触发时间、动作目标（`schtasks` / `Get-ScheduledTask` 在沙箱内无任何输出）⇒ **需用户本机执行** `schtasks /query /tn PortfolioDailyAnalysis /fo LIST /v`。
2. 06-02 ~ 08-04 两个月 `scheduled_run.log` 零记录的真实原因。
3. 15:10 那个历史重复任务是否已卸载。
4. CI 是否曾成功运行 / 当前是否绿（需 GitHub Actions 端确认）。
5. Dockerfile 是否曾构建/运行成功。
6. 16:40 / 17:00 自动化的真实触发时刻（仅二手文档证据）。
7. 写事务持锁时长 / 锁等待分布（只读环境无法复现）。
8. 本机是否存在 D: 以外的备份副本。
9. 代码覆盖率（venv313 未装 coverage/pytest-cov）。

---

## 11. 方法论自审

- **本评估自身的已知缺陷**：方案稿 `14:43` 的「17 个 Tab」与 Phase 0「数据新鲜」两处**已被实测推翻**，说明计数/新鲜度类断言必须现算、且不能只查 `MAX(date)`。本报告正文中已就地纠正并保留痕迹，不静默修改。
- **各维度均要求**：证据优先、生产库只读、不采信 09-03 数字、无法验证标「未验证」。8 份回报中「未验证」项已集中列于 §10。
- **未做的事**：未改任何生产代码、未写生产库、未跑 `run_analysis.py` / `scheduled_run.bat`、未做依赖 CVE 审计（用户拍板安全只做静态）、未 commit 除 `14` 以外的任何变更。
- **两条最高可操作断言已由报告作者独立复核**：`excel_report.py:386` 裸 `VOID_FILL`（grep 确认定义在 `Styles` 内 `:48`）、`scheduled_run.bat:15/18/20` 的 `exit /b %ERRORLEVEL%`（读文件确认）。

---

## 12. P0 整改收口记录（2026-09-21，评估后执行）

> 本节为评估（只读）完成后的**整改执行记录**，非诊断内容。所有结论均带 commit / 文件锚点。

### 12.1 九项 P0 集中收口（commit `126b45b`）

九项 P0 改动由并行 worker 落地、lead 逐条复核 diff + 重跑测试后，于 `126b45b` 集中提交（显式 pathspec，11 文件；前置报告 15 自身已在 `1c637ec` 提交推送）：

| # | 项 | 落点 | 验证 |
|---|---|---|---|
| 1 | `VOID_FILL` → `s.VOID_FILL` | `src/report/excel_report.py:386` | `-k excel` 3 passed |
| 2 | 退出码捕获 | `scheduled_run.bat`（存 `analysis_rc` 作最终退出码） | 逻辑复核 |
| 3 | 测试连接泄漏 | `tests/test_split_merge_guard.py`（try/finally close） | 14 passed，无遗留 .db |
| 4 | `13` 口径订正 | `docs/handover/13_…`（A 族 0 例） | 全文无矛盾断言 |
| 5 | 仓库外 mirror | 先隔离至 `_QUARANTINE_LEAK_20260921/`，**本步骤 §12.2 永久删除** | 见 §12.2 |
| 6 | `etf_price_history` 缺口 | 自愈验证 + **本步骤 §12.3 加 DQ 闸门与回补入口** | 见 §12.3 |
| 7 | `#140` 哨兵三列 | `tab8_advice` / `etf_risk_scan` / `technical` / `pre_post_market` / `backfill_full_history` | 30 tests green + import OK |
| 8 | 备份条数上限 | `scripts/backup_db.py`（`MAX_BACKUP_COUNT=10` + keep_min 保护） | 3 场景验证 |
| 9 | WAL + busy_timeout | `data_loader.py`（统一 `busy_timeout=5000` + WAL，只读/:memory: 跳过） | 冒烟全过 |

### 12.2 P0-5 · 仓库外 mirror 永久删除（用户授权）

- **授权**：用户于 2026-09-21 明确指示「镜像永久删除」。
- **执行**：精确路径 `shutil.rmtree`（首只）+ `cmd rmdir /s /q`（残留大仓，rmtree 在沙箱被 SIGTERM 中断，换用内核级递归删除）+ 空父目录 `rmdir`，**无 glob、无回收站模糊删除**。
- **对象**：
  - `D:/…/lingxi-claw/_QUARANTINE_LEAK_20260921/_backup_portfolio_tracker_git_mirror_20260903.git`
  - `D:/…/lingxi-claw/_QUARANTINE_LEAK_20260921/_backup_pt_git_mirror_20260903_2310_pre_filterrepo.git`
  - 父目录 `_QUARANTINE_LEAK_20260921/` 一并移除。
- **复验**：删除后 `os.path.exists` 三项均 `False`，数据泄漏面彻底消除。

### 12.3 P0-6 · `etf_price_history` 缺口根因防护（新增 DQ 闸门）

**根因**（非表象）：缺口曾**完全静默**——既无告警也无重试队列，直到并发进程巧合自愈才被发现（呼应 §3「只查 MAX(date) 漏掉每日只数」的教训）。故防护的核心是**让缺口可见 + 可闭环**，而非一次性回填。

**新增** `src/analysis/price_history_gate.py`（与 `snapshot_gate` 同口径）：
- `detect_etf_price_gaps(conn)`：只读；以「活跃标的中覆盖最完整的那只的日期集合」为参考交易日历；**活跃 = 近 30 天有数据且属最新快照在册持仓**（已清仓标的如 `159732` 排除，避免永久误报 error），对每只活跃标的统计窗口内缺失交易日；参考日取 `max(etf_price_history 全局最新日, portfolio_snapshots 最新日)`，并单列**系统级断崖**（全局最新日落后参考日 ≥4 自然日 ⇒ 全市场源中断）。
- `check_etf_price_history_gaps(db_path, auto_repair=…)`：warning（缺 1~2 天）→ 仅日志 + `_reporter.alert("warning")`，**不写 alerts 表、不降级**；error（缺 ≥3 天或系统级）→ 写 `alerts` 表 + `_reporter.alert("error")` ⇒ `run_status=degraded` ⇒ 真实断崖时拒发基于陈旧价的日报（与 snapshot_gate 契约2 同构，且不会像 09-17 那样把 T+1 常态误判成 error 每日拦死）。
- `ETF_GAP_AUTOREPAIR=1` 时显式增量回补（仅 qfq 源、INSERT OR REPLACE 幂等），**绝不静默触发**；CLI `python -m src.analysis.price_history_gate --check / --repair`。

**接线**：`run_analysis.py` 阶段 3.25（预测底座增量维护、etf_price_history 刷新）之后插入「阶段 3.25b」调用闸门，失败仅降级为 warning 不影响主流程。

**只读复测（生产库 `?mode=ro`）**：闸门现可检出
- `159732`：**error**，缺 3 天（2026-09-17~2026-09-21）；
- 13 只：**warning**，各缺 2 天（2026-09-18 / 2026-09-21，停在 09-17）；
- 9 只当前标的：无缺口。
语法 + error 级 `alerts` 写路径已在**库副本**上验证通过（副本写 1 行、生产库零写入）。

**闭环执行（2026-09-21，lead 自执行，不派工）**：
- **备份**：写库前落 `data/backups/portfolio_20260921_141902_before_gap_repair.db`（144MB，integrity 双 OK）。
- **闸门增强**：`detect_etf_price_gaps` 新增「当前持仓」口径过滤——仅对最新快照日 `portfolio_snapshots` 在册的 code 判缺口，**已清仓的 `159732` 不再被误判为活跃**，根除「清仓标的永久 error ⇒ 每日日报 `run_status=degraded` 被闸门拦死」的 09-17 式灾难（复测：`GAP_CODES_NOT_HELD` 由 `['159732']` 变为 `[]`）。
- **回填**：直接调用 `backfill_etf_price_history`（sources=("em","tx")，增量从 `MAX(date)` 起、INSERT OR REPLACE 幂等）回填 **13 只在册缺口标的**（159732 因已清仓自动排除）。09-18 真实断崖对 13 只**全部补齐**；其中 11 只（含 516160 经重试）补齐至 09-21。
- **残留（benign，待 15:30 管线自愈）**：`512810`、`588000` 仅缺 **09-21 当日盘中数据**——em 源对这 2 只持续 `ProxyError`、tx 源无今日盘中数据；属 1 天 warning（**不拦日报**），今日 15:30 管线增量路径会正常补齐。**无任何 error 级缺口，日报不再被闸门拦死**。
