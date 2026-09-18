# 12 · 工程铁律与坑位总表（portfolio_tracker）

> **本文件与 `~/.workbuddy` 会话记忆的分工**
> 会话记忆 `MEMORY.md` 每次会话自动注入、有长度预算，超限会被**静默截断**（截断后的部分等于不存在）。
> 因此：**只有「一句话就能救命」的铁律留在 MEMORY.md**；台账、实测数字、代码行号、判据演化史全部放本文件，MEMORY.md 用指针引用。
> **维护规则**：新增坑位先写本文件，只有确属「不看会立刻犯错」的才上提到 MEMORY.md。
>
> 生成于 2026-09-16，随项目演进更新。

---

## 1. 环境与工具链铁律

| 项 | 规则 | 违反后果 |
|---|---|---|
| Python | 一律 `venv313/Scripts/python.exe` | 系统 python 缺依赖 / 版本不符（需 3.13） |
| Git Bash coreutils | `grep`/`head`/`tail`/`wc`/`ls`/`cat` **常 command not found** | 管道命令静默失败、`Exit Code: 127`；改用 python 或 `git --no-pager` |
| 路径写法 | 用 `D:/...` | `/d/...` 形式部分命令不认 |
| 库副本落点 | 一律 `data/backups/` | 落 `data/database/` 会让 worker 连错库 |
| 计划任务 | 沙箱内 `schtasks` / `Get-ScheduledTask` **禁用** | 计划任务详情只能用户本地自查 |
| ⚠️ 时钟 | **本机时钟才是准的**，会话头 `<current_time>` 不可信（曾差 5 小时） | 误判时限已过/未到；涉时限先 `python -c "import datetime;print(datetime.datetime.now())"` |

### 1.1 批处理文件
- **`.bat` 必须 CRLF**。LF 会让 cmd.exe 解析错乱 → 日志显示假成功、邮件静默未发。
- Write 工具产出 LF ⇒ 写完必须转 CRLF 并用 `od -c` 核验。

---

## 2. Git 作业规程

### 2.1 三条禁令
| 禁令 | 原因 |
|---|---|
| **禁** `git status` / `git add -A` / `git add .` | 全量遍历触发 Git-for-Windows `err_win_to_posix` 崩溃 |
| **禁** `git gc` / `git repack` / `git fsck` | 删旧 pack 必崩，**曾连损 3 次** |
| **禁** 不带 pathspec 的 `git commit` | 见下 2.2 |

- 查状态用 `git diff --name-status HEAD`；暂存用 `git add <具体文件>`。
- 自动维护已关闭（`gc.auto=0` + `maintenance.auto=false`）：此前每次提交 git 2.55 会后台跑 `geometric-repack`，撞上 filter-repo 残留失效对象（`bad tree object 4570cdb5`）反复失败。**不要重新打开**。仓库有缺失对象但 HEAD 链完整，commit/push 正常。

### 2.2 🔴 误卷事故台账（commit 带上别人的文件）
**`git add <具体文件>` 不保证 commit 只含自己的文件** —— 多人共用同一工作区，他人已 `git add` 未提交的文件会被一并卷进你的 commit。

| 提交 | 声称内容 | 实际装入 |
|---|---|---|
| `05c7288` | `docs(correlation):` | 多装 128 行代码 |
| `8a7173c` | `docs(handover):` | 多装 502 行（gp-4 两文件）；**已推送 ⇒ 不可拆**（拆 = force-push） |

**作业纪律**：`git commit -m "..." <显式路径>` → 提交前 `git diff --cached --name-only` 核对 → 提交后 `git show --stat HEAD` 自核。

### 2.3 filter-repo
`filter-repo --force --no-gc ... --invert-paths`（**必须 `--no-gc`**）→ 会删掉 `origin`，收尾需 `git remote add` + `push --force`。mirror 备份一律 `--no-hardlinks`。

### 2.4 推送出口
沙箱对 GitHub 写出口受限（https push reset / 代理 502 / gh 超时）。**唯一稳定写通道 = SSH-over-443**（`~/.ssh/config` github.com→ssh.github.com:443），远端 `git@github.com:asdfly/portfolio-tracker.git`。
⚠️ `git push` **无输出返回实为未成**，须显式核验远端 hash（`git rev-parse origin/master`）。

---

## 3. 数据层不变量

### 3.1 表结构陷阱
- **`etf_features` PK = `(date, code)`，`feat_version` 不入键** ⇒ 升版本号**无法**隔离新旧量纲。任何改量纲/语义的改动**必须全表重算**；增量会造成「同列两套尺度」且事后无法区分。
- `etf_price_history.adj_close == close`（**零差异**）⇒ 该列**不含复权**。对 sina 源标的，「用了 qfq 复权」的说法不成立。
- `database.get_price_history(code, days)` 返回 `date/current_price/market_value/pnl`，**`ORDER BY date DESC`**（用前须升序）。

### 3.2 量纲（volume）
- **`etf_price_history.volume` 全表口径 = 「手」**（2026-09-16 归一）。
- tx 段 7,018 行原由 akshare `stock_zh_a_hist_tx`（走 newfqkline）写入时 **×100 成「股」**，已 ÷100 修正。
- 腾讯 **raw** 端点 `web.ifzq.gtimg.cn/appstock/app/fqkline/get` 原生即「手」，与 EM 逐日精确相等。
- 落盘 fetcher 走 raw 端点 ⇒ **代码里零转换，禁止再乘除 100**。

### 3.3 覆盖缺口（结构性）
- **12 只场外基金在 `etf_price_history` 零行**，占持仓约 **33%** ⇒ 任何「按行数对齐」的算法都会在这里踩坑。
- 交易日历 `src/utils/trading_calendar.py`：`_HOLIDAY_RANGES` 只内置 **2024/2025/2026**，覆盖外年份**静默退化**（快照回溯到 2012）。**每年初必须补下一年。**

### 3.4 🔴 `etf_features.ret_1d` 双向落盘伪收益
5 例与 `r−1` 差 **`0.000e+00`**（同一事件同时污染 `ret_1d` 与 `ret_1d_lag`）：

| 幅度 | 方向 |
|---|---|
| +255.8% / +221.3% / +176.3% / +248.6% | 向上跳 |
| **−73.9%**（`512010`） | **向下跳** |

⇒ 量纲/折算改动后**必须全表重算**，不能指望增量补齐。

### 3.5 实测行数快照（2026-09-16，勿重数）
| 表 | 行数 | 最新日期 |
|---|---|---|
| `portfolio_summary` | 3477 | 09-14 |
| `portfolio_snapshots` | 36173 | 09-15 |
| `etf_price_history` | 31515 | 09-15 |
| `etf_technical` | 35100 | 09-15 |
| `index_quotes` | 50850 | 09-15 |

---

## 4. `portfolio_snapshots` 复制行（fill-forward）= 活跃泄漏

### 4.1 事实基座
存在「**行上日期 D、实际装更早真实值**」的复制行。已证实链条：

- `2026-06-15~06-29` 十只场外连续 **10 行** `(current_price, market_value)` 恒等；
- 值**逐个精确等于官方 `06-12` 单位净值**；
- `06-30` 行 = 官方 **`06-29`** 净值；
- ⇒ 那根 **+15~18%** 是 **`06-12→06-29`（17 自然日）收益被贴上「1 个交易日」标签**；
- 官方 `06-30` 当日实际只 **+3.59%~+4.97%**。

**教训：β「幅度可达」不能证明基期合法。** 必须证明分子分母的日期归属。

### 4.2 🔴 写入方仍存活
08-03 采集器上线**之后**仍发生 **17 段**，最近一次 = `2026-09-14→09-15`（13 只场外中 **12 只**）。`portfolio_snapshots.id` `38408~38419` **连续一码一行** ⇒ 存在另一处写入路径。**守卫是唯一防线，写入方未断源。**

仓库内 `INSERT ... portfolio_snapshots` 共 3 处：
- `src/utils/backfill.py:187`
- `src/utils/database.py:41`
- `scripts/backfill/backfill_full_history.py:288`

### 4.3 判据（两级，勿退回旧写法）
> **禁止**退回「相邻两行同价即非观测」—— 那会误删 ETF 真实观测。

**Tier 1 · 段长**：`(round(price,2), round(market_value,2))` 同值连续行数 **≥5** ⇒ **整段含段首**非观测。
- 数据驱动，**不加 `is_otc_fund` gate**。
- 依据：ETF 侧全历史自然同价段**最长 4 行、`≥5` = 0 段**。

**Tier 2 · 横截面广度**（仅场外篮子内）：某日「与各自上一行同值」的场外标的**占比 ≥50% 且绝对数 ≥3** ⇒ 该日这些标的非观测。
- 用于覆盖只有 2 行的全市场事件。

**分侧段长实测（判据的判别力来源）**：

| 段长 | 场外 | ETF |
|---|---|---|
| ≥3 | 19 | 47 |
| ≥4 | 11 | 5 |
| **≥5** | **11** | **0** |

（污染段 10 行；分布无重叠）

### 4.4 两条禁忌
① 复制位必须置 **NaN，不能置 0** —— `0.0` 是「看起来完全合法的零收益」，会被 `np.isfinite(rets).sum()` 计入 `valid_returns` 并进入 `df.corr`。
② **禁止另立「零收益观测即丢弃」判据** —— 会误杀真实平价日 / 无成交日。

### 4.5 必须按「有效基期」算 gap
- 复制行**内部** `gap = 1`，正好从 `MAX_SINGLE_SESSION_GAP_DAYS=12` 闸门**下方通过**；
- 假跳变 `|log_ret| = 0.17 < SPLIT_SPIKE_LOG_RET = 0.30` 也躲过尖端闸门；
- ⇒ 必须维护 `last_real_date`，按有效基期算间隔。

### 4.6 🔴 `round(2)` 是承重构件
`market_value` 存在**亚分位浮点抖动**：`2026-06-19` 行 `001407` `55498.0→55497.997`、`166301` `81672.52→81672.51680000001`（quantity 逐位不变）。

若逐位比较 ⇒ 在此断链 ⇒ 基期被刷新到 06-19 ⇒ 06-30 伪收益 `gap` 只剩 **11 天** ⇒ 恰好被 `MAX_SINGLE_SESSION_GAP_DAYS = 12` 放行。

量化差异：不取到分 `average_correlation` = **0.3728**；取到分 = **0.4113**。

### 4.7 守卫顺序不可交换
`2026-06-30→07-31` 的**真实**跨期收益全部 > 0.30：

| 标的 | log_ret |
|---|---|
| `001437` | −0.4598 |
| `001407` | −0.3910 |
| `166301` | −0.3797 |
| `001323` | −0.3339 |
| `519770` | −0.3107 |

⇒ 尖峰判定若先跑，会把这 5 只场外**误标成「折算」**。**跨期判定必须在前。**

### 4.8 阈值口径二分（0.30 原理上无法区分）
| 事件 | `|log_ret|` | 应否保留 |
|---|---|---|
| `159949` 2024-10-08 ±18% | 0.14~0.17 | ✅ 真行情，必须保留 |
| `2026-06-30` 场外 +15.7%~+18.5% | 0.14~0.17 | ❌ 不是收益，应排除 |

⇒ 单一阈值**原理上无法分开**，只能靠「有效基期」规则。

### 4.9 判别力不变量（防回归，必须持续成立）
- `100032` 单只同价段（08-04/05、08-20/21、08-31/09-01，**段长 2**）**不得被 void**；
- `09-14→09-15` 那 **12 只必须被 void**；
- **ETF×ETF 对数 = 231 = C(22,2)**（曾误报 462，把无序对当有序数）—— 相关性改动最干净的对照组。

### 4.10 守卫代码现状
文件 `src/analysis/portfolio_risk.py`｜HEAD `ba96ce8`｜blob `f8aaf95cd143`（= `6434f05` = `198dee8`）

| 位置 | 内容 |
|---|---|
| `:39` | `MAX_SINGLE_SESSION_GAP_DAYS = 12` |
| `:55` | `SPLIT_SPIKE_LOG_RET = 0.30` |
| `:125` | `COPY_RUN_MIN_LEN = 5` |
| `:156` | `_scan_replica_rows` |
| `:198` | `run_len >= COPY_RUN_MIN_LEN` |
| `:223` | `if c * 2 < n:`（Tier 2 判定，**硬编码**） |
| `:500` | `head_hit` |
| — | `zero_variance`（`np.ptp == 0` ⇒ 显式丢弃并登记） |

留痕键：`unreliable_codes` / `cross_period_voided` / `skipped_codes`
reason 枚举：`rows_lt_2` / `rows_le_min_overlap` / `zero_variance` / `valid_returns_lt_min_overlap` / `window_too_narrow` / `series_lt_2` / `columns_lt_2`

blob 历史：`5f7c6a3e4797`（`401a39f`，无守卫）/ `0a6e815b75fe`（`9de2800`，B2 版，602 行）/ `673590464107`（`05c7288`，两级落地，763 行）/ `f8aaf95cd143`（已验收）。

### 4.11 ✅ 已处理：`COPY_BREADTH_MIN_RATIO` 死常量（#87 裁定 B）
- 判定用**硬编码** `c*2 < n`，常量**只在日志被读** ⇒ 旋钮无效（死常量）。
- `b034f9a` 曾修为 `c / n < COPY_BREADTH_MIN_RATIO`，随后 `ba96ce8` **回滚**（裁定：保持已验收 blob）。
- 最终裁定 **B：删常量**，**已执行**（提交 `c698916`）：
  - 删除 `src/analysis/portfolio_risk.py:127` 的定义；日志行 `:227` 的 `COPY_BREADTH_MIN_RATIO * 100` 改为字面 `50.0`；
    两处注释（`:114` 顶部 Tier2 说明、`:223` 判定行）改为「同值占比 50%」。
  - 同笔清理悬空符号引用：`07_known_data_issues.md`（问题十一 行 899 / 两档实现 1250 / 1259 / 1312 / 1330-1332）、
    `10_correlation_guard_decision_log.md`（误卷事实清单 22-23 / Tier2 参数表 56）、
    `11_measurement_conventions.md`（常量定义 613-614 / 推论1 633 / 635-636）。
  - 行为不变：独立复现验证 `audit/_verify_replica_guard.py`（4 用例：100% 命中 / 25% 不命中 / n<3 不命中 / 50% 边界恰命中）全部通过。

### 4.12 不可达分支裁定
`_calculate_metrics:122` 的 `total_value` 差分 fallback 裁定**不可达**：最近 60 行 `daily_return` 60/60 非零；全历史最长连续全 0 = 1 行；触发需 ≥61 连续全 0。
⇒ **不加守卫、只加告警绊线。**

`src/analysis/risk.py::calculate_correlation_matrix`（`:288`）**勿动**（崩溃修复已验证）。

---

## 5. 静默失败目录

> **准则：测出来的数必须要么显式标记、要么显式拒绝，不许静默。**

### 5.1 已实证的失败模式

| # | 模式 | 实证 | 为什么危险 |
|---|---|---|---|
| 1 | **阶段崩溃后静默** | 2026-09-15 15:30 在阶段一/步骤5 崩（`All arrays must be of the same length`）⇒ 阶段二/三/四**全未执行** | 产物存在、看着正常 |
| 2 | **进程硬死** | `2026-09-03`：备份→起跑→写快照 22 条→进入「步骤4 计算技术指标」→**进程硬死** | 无 failed 终态、无 run_report、无告警；缺口躺 **13 天**无人报，并污染 09-04 的 `daily_return` |
| 3 | **DQ 门禁失明** | 09-15 那次失败仍写出 `run_report_2026-09-15.json`，`dq_score=100, alerts=0` | **`dq_score` 不能作为「今天跑通了」的证据** |
| 4 | **邮件仍然推送** | stale 守卫「现场重生今日报告」，产物**章节可能缺失** | 09-15 报告比 09-14 **少「相关性分析」章节**，155,387 vs 158,787 字节。**收到报告 ≠ 管线跑通** |
| 5 | **`run_status` 闸门从未生效** | `data/reports/` 27 份 run_report **全缺该字段** | 闸门实际只靠 summary 日期判据兜着 |
| 6 | **看门狗盲区** | `collect_core.py:167` 的 `os._exit(1)` **绕过 `finally`** | run_report / execution_logs 终态写不出；唯一存活痕迹是 `:162` 的 `logger_.error` |
| 7 | **`etf_features.ret_1d` 双向伪收益** | 见 3.4，5 例差 0.000e+00 | 同一事件污染两列 |

### 5.2 🔴 正确的「跑通了」判据
- **必须看 `logs/scheduled_run.log` 的 rc / 阶段日志**，或 `[EMAIL] 已推送日报` 行。
- **不能用** `dq_score`、不能只看出没出报告、不能只看报告能不能打开。

### 5.3 ⚠️ 测试与生产共用日志与报告目录（读日志时的头号误判源）
pytest 会把隔离空库的错误大量写进 `logs/portfolio_YYYYMMDD.log`：
- `no such table: portfolio_snapshots` / `no such table: etf_price_history`
- 并向 `data/reports/` 写 `test_p2_report.html`

**读到这些不代表生产库坏了。**

### 5.4 ⚠️ 15:30 跑的是工作区，不是 git HEAD
`run_analysis.bat` 只做 `cd /d "%~dp0"` + `%PYTHON% run_analysis.py`，**不 git pull、不 checkout**。
⇒ 「上已提交的 X」**不自动成立**；改完代码必须**冻结工作区**才算真上了版本，且要记录工作区 diff 指纹作为证据。

---

## 6. 测试约定与防污染

### 6.1 🔴 单文件单 writer
两个 worker 先后写同一测试文件 ⇒ 重复 `import` + **同类名定义两次**（`TestCrossPeriodGuard` 在 433 / 742 各一份）⇒ Python **静默屏蔽第一个类，其用例永不执行**。

**症状：测试在跑、数量看着正常、实际有一批没跑。**

**处置**：声明单文件单 writer + 新增 **AST 收集完整性守卫**（断言无重复类/函数名、AST 计数与 `pytest --collect-only` 数量一致）。

### 6.2 生产库污染检测（勿退回旧写法）
- **禁止**用整库 `(mtime, size)` 变化判定污染：conftest 已把进程内所有连接改道到隔离副本，运行期观测到的 mtime 变化**必然来自外部进程**（多人并发写库是常态）⇒ 判定 fail **必为假红**。该类比对一律降级 `warnings.warn`。
- **真污染检测走零假阳性的正向断言**：`TestProductionDbHasNoTestArtifacts`。
- ⚠️ **加防污染断言前必须先证明它读到了正确目标**：曾因直接用 `sqlite3.connect` 实际读到副本，写出**永远不命中的空测试** —— 比假红更危险。
- 🔑 **观测真实生产库的唯一通道**：`sqlite3.connect._pt_real_connect`，且必须传 **URI 形式** `file:<prod>?mode=ro`（普通路径会被 conftest 硬兜底改走副本）。
- `WB_ALLOW_PROD_WRITE=1` 可降级授权写库的告警；真实 `.env` 不受该逃生口覆盖。
- pytest cache NTFS ACL 损坏（C→D 后遗症，已 gitignore）⇒ 加 `-p no:cacheprovider` 规避。

### 6.3 崩溃类修复的硬要求
**给崩溃类修复补回归测试 + 反证（旧实现确实抛错）是硬要求。**
反面教材：`e1610fd` 修 09-15 崩溃时**无任何测试**（`grep tests/ min_overlap|_analyze_correlations` 零命中）。

---

## 7. 数据排障顺序与已知误判台账

### 7.1 顺序（多次验证有效）
1. 报「某标的取不到数据」前，先区分是**数据源真没有**还是**封装层坏了**。
2. 报「某处有 bug」前，先 grep **下游读取点** + 对齐**时间口径**（避免跨停更期比较）。
3. 判断量纲/单位分歧时，**必须对撞数据源端点原值**，不能比「包装器写入库后的值」。

### 7.2 误判台账
| 案例 | 误判 | 真相 | 教训 |
|---|---|---|---|
| `880013` | 「无净值源」 | akshare `fund_money_fund_info_em` 列名硬编码 14 个 vs 货币型返回 13 列；绕开直连东财 `api.fund.eastmoney.com/f10/lsjz` 即可日更 | **结论先行会把活源误标成死源 = 给未来真实断流埋免责声明** |
| 「手→股」量纲 | 判反 | 必须对撞腾讯 raw 端点原值 | 不能只比包装器写入库后的值 |
| 「EM 被代理墙挡」 | 外部故障 | `fund_flow.py` 早已绕代理，绕后直连 200 通 | 见 §8 |

---

## 8. push2his（东财资金流）阻尼

- **旧结论「EM 被代理墙挡」是错的。** `fund_flow.py` 早已绕代理（弹 `*_proxy`、`trust_env=False`、`ProxyHandler({})`），绕后直连 200 通。
- **真相**：push2his 对**高频直连**做阻尼，**20~40%** 请求被掐断或回 `data: null`。与标的无关，是请求频率的函数。⇒ **重试是刚需**（`31ff35b` 已加 3 次 + 指数退避）。
- ⚠️ **度量陷阱**：连续压测会把自家 IP 打进阻尼（健康度 6/10 → 3/8），**不是修复变坏**。评估必须对比**两个正常交易日 15:30** 的 `scheduled_run.log`。
- **兜底链三层都不要删**：逐只 push2his → `fetch_etf_fund_flow_batch`（datacenter-web，不受阻尼）→ `backfill_etf_fund_flow_from_kline`。

---

## 9. 重构类提交的高危模式

两轮「fine-grained exception handling」重构把 `_urllib_get_json` 的 `except Exception` **误收窄成 `except sqlite3.OperationalError`** ——

- HTTP 请求**永不抛 SQLite 异常** ⇒ 网络异常漏网、重试与告警**双双失效**；
- 只在生产日志留下「push2his 不可用」这种**看起来像外部故障**的假象，**误导排查数月**。

**判据：把 `except Exception` 收窄成具体类型时，必须核对该调用实际能抛的异常谱系；「网络/IO 调用」不能被收窄成 DB 异常。这类 bug 不会让测试变红。**

---

## 10. ETF → 跟踪指数 PE 覆盖

- `index_pe_history` 约 **2.8 万行**，csindex 全量（2018→），**权益 ETF 估值因子已全覆盖**。
- ⚠️ 映射代码需在 `etf_position.py` 与 `etf_fundamental.py` **两处同步**。曾因只改一处 + 早期填错代码（`930006` / `h11118`）而误判「指数无数据」；正确为 `H30590`（中证机器人，`159770`）/ `930914`（港股通高股息，`159220`）。
- **判据「中证 / 国证」不同源**：`399673`（创业板50）是**国证指数（深交所体系），非中证** ⇒ csindex 不发布、akshare 无函数、国证官网只有行业级 PE。2026-09-15 用**乐咕 legulegu** 补齐（`48d779d`）：
  - `index_pe_history` 228 行 / `2009-10-30~2026-09-15`
  - `PE_SOURCE_CONFIDENCE` legulegu = 1.0
  - `VAL_MIN_DAYS` 改**来源感知闸门**（月频 `VAL_MIN_MONTHS = 120`）
  - **这是中证类接口取不到国证指数的根因。**

---

## 11. 建模结论（三条已定型，勿重复试错）

| 目标 | 结论 | 证据 |
|---|---|---|
| **方向预测** | **死路，不上线** | Tier0 命中率 46-50%；Tier1（LightGBM+Ridge，walk-forward 5 折 + embargo 60 + HAC t）v1/v2 均 **6/6 VETO**（IC 0.006） |
| **风险 / 波动率预测** | **强达标，已落地** | OOS R² 0.44–0.89、IC 0.66–0.94、AUC 0.90–0.97 ⇒ `tabs/tab16_risk_outlook.py` + `src/utils/risk_report.py` |
| **回撤幅度预测** | **不达标，降级** | R² 全负、AUC 0.63–0.68 ⇒ Tab16 降级为「历史回撤参照」 |

- 数据底座三表：`etf_price_history` / `etf_features`(v3) / `etf_forward_returns`。
- **无未来函数红线。**

### 11.1 🔴 引用上表时必须连带的面板范围与 OOS 覆盖限定
（2026-09-16 由 quant-analyst 补记，原文见 `07_known_data_issues.md`:765-775）

| 限定 | 内容 | 为什么危险 |
|---|---|---|
| **面板范围** | 原面板含 **2012–2017 的 4,091 行**，而 `etf_price_history` 自 **2018-01-02** 才有数据 ⇒ 这些行 OHLC/volume 派生特征**结构性全 NULL** ⇒ 触发 P1-6 缺失率护栏，**门禁跑不到第一折**。收窄到 `>=2018-01-02`（**30,309 行**）后方可运行（task #84） | 裸报「6/6 VETO」而不报面板范围，等于隐去「该结论在什么样本上成立」 |
| **结论随范围变** | 收窄后的结果是 **5/6 VETO**（仅 `w=20 ridge` PASS）。与 6/6 的差异来自**面板范围 + 数据延伸**，**与 volume 手/股修复无关**（同面板重建污染前后逐折 verdict **逐位相同**） | **勿跨范围直接比较「6/6」与「5/6」** |
| **OOS 覆盖缺口** | `walkforward_splits` 的 OOS **只覆盖到 2025-11-06**，其后约 **1/6 数据（含整个 2026）从未进入 OOS**（task #83） | ⇒ 结论只基于 **2025-11 之前**的数据；**任何 2026 年的数据污染结构性无法在 OOS 里体现** |

**判据：引用/对照本表任何结论时，必须连带引用面板范围与 OOS 覆盖区间。** 只看命中率/IC/R² 数字就下结论，属于今天反复出现的「样本未钉死」伪影。

> 相关工具：`scripts/verify_gate_on_refreshed.py` 已把「面板起点对齐数据源覆盖」与「每窗口 OOS 区间 + 末端缺口（超 30 日历日即告警）」做成**显式打印**，禁止「最近一段没被验证」隐形。这是「要么显式标记、要么显式拒绝」准则的正面落地样本。

---

## 12. 标的状态约定

- **`159732` 消费电子 ETF**：已清仓（末次快照 `2026-07-30`，市值 6,210 **非 0**，是「停更」非「归零」）。`is_delisted()` 将其排除出持仓 / 再平衡 / 预测底座（**保留**），同时进 `WATCHLIST_CODES` 保留行情与技术面，前端 Tab18「清仓观察」展示。
  ⇒ **原则：给它数据，不给它决策权。**
- **`880013` 天添利**：货币型，**不参与分析、但计入总持仓**（9,451 元 / 0.607%），靠「现金管理不在 `SECTOR_TARGET_WEIGHTS` → 保持当前占比」实现。用户明确**保持现状**；其 `price ≡ 1.0` 是货币型**设计约定非 bug**（改市值增长算收益会把申赎当收益）。
- **组合口径**：36 只全量 1,564,199.58（含 `159732` 陈旧估值）；**正确口径 = 35 只 / 1,557,989.58**。
- **ETF 数量（实测勿重数）**：**36 = 23 场内 + 13 场外**；23 == `etf_technical` == `config.ETF_CATEGORIES`；`etf_fundamental` = 25 含 2 条脏数据不可用。

---

## 13. 收益 / 风险口径（2026-09-16 定论）

- 头条 TWR 与风险三列**都依赖口径选择，不是客观量**。三条序列实测（窗口口径）：

| 口径 | 窗口收益 | `sharpe_60` |
|---|---|---|
| **A 现库** | **−0.6295%** | **+0.7291** |
| B 补场外 | −10.5488% | −0.8511 |
| C' 区间摊销 | −10.6409% | −1.1346 |

- **「夏普转正」不稳健** ⇒ 只能表述为「**在现库口径下**转正」。
- **裁定：不采纳 B**（补场外 = 用水平换伪尖峰）。A 丢水平保日序列，B 补水平毁日序列，C' 补水平保形态但压深回撤。**三者都不是「真」。**
- 场外口径缺口的根因：`portfolio.py` 用 `WHERE date = prev_dt` **精确匹配** ⇒ 仅月末落行的场外基金**当日被整只踢出收益**。
- 08-03 后那 1.03% 的差**不是**真实日频补齐，而是 `027293` 只按周落行所致的**陈旧价兜底**伪影。
- ⚠️ **全历史头条 `max_drawdown` 不受口径影响**（A = B = 51.30%）。
- **TWR 链严格相等**：quantity 在分子分母约掉 ⇒ `1 + r_t = V_t / V_{t-1}` **精确** ⇒ **补历史缺口不改任何已发布 TWR**（`twr_cumulative` 既有行 0 变动，差 < 1e-7）。
- ⚠️ **`_SUSPECT_DIVERGENCE = 0.30` 单位错**：`md` / `r` 都是小数 ⇒ 实为 **30 个百分点**。且若有入金不在 `trade_records` ⇒ `net_flow = 0` 会带偏 `md`。

---

## 14. 已知债务清单

| 项 | 说明 |
|---|---|
| 文档债 | 前端实际注册 **17 个 Tab**，README 仍写 15 |
| P2-8 持仓舆情 | 需 westock / neodata，非 akshare 栈，暂缓 |
| 其他 P2 | 波动率目标化、IOPV 历史表、分散化相关性 |
| neodata 凭证 | 12h 有效，仅会话内可采，**生产 `scheduled_run` 调不到** |
| NAV 恒等式 | `portfolio_nav.total_units` / `total_value` 恒等式破损 |
| `_save_snapshot_from_kline` | 债务（行情表统一 qfq 后**不可再跑**） |
| 告警被吞 | `run_analysis.py:1405-1406` 告警被 `except Exception` 吞 |
| rc 被覆盖 | `scheduled_run.bat:20`；周末 rc=1 假失败 |
| `etf_pe_backfill.py` | csindex 分支**无交易日过滤** |
| `tab8` periodic | 无 `last_rebalance_date` 来源 |
| `fetch_etf_ohlcv_sina` | `adj_close` 为**休眠路径** ⇒ 裁定 (a) 维持现状 + 登记到 `11_measurement_conventions.md` |
| `recompute_summary_window.py` | **结构上补不了缺口**（问题 `#65`）：`:70-72` 日期清单取自 summary 自己（实测只覆盖 7 天）；`:197-203` `o = old[dt]` 对新增日期 `TypeError` 崩在写库前。已修（`8a7173c`：改 `resolve_dates()` 快照∪汇总并集 + `old.get(dt)`），**未执行** |
| 报告库漂移 | `08_report_library_drift.md`：95 份 HTML vs 库 MATCH 57 / DIFFER 35 / NO_DB_ROW 5；08-03~09-14 窗口 29 条**全部漂移**（差值恒负 −53.6万~−65.7万）。10 个历史改写入口，真正有「写前阻断」的只有 1 个（`recompute_summary_window.py`）⇒ **唯一每天无人值守自动改写历史的入口零校验** |

---

## 15. 取证与验证纪律（2026-09-16 由当日多次翻车事故沉淀）

> 本节的每一条都对应一次**实际发生过的错误**，不是理论。今天同一个 lead 在**一小时内以同一种方式误读两次**（详见 15.2），所以这些纪律不是建议。

### 15.1 独立复算必须连「口径」一起独立
- 「**精确吻合**」有两种性质，必须分开：
  - ① **反推钉死口径**（如 26 日窗口由 5/5 吻合反推确认）—— **有益**，吻合本身携带信息；
  - ② **仅确认双方用了同一口径**（如两方给出同一份列集）—— **中性**，**不能当作「口径正确」的互证**。
- ⇒ **形式化判据：独立复算必须连口径一起独立，否则「同错」不算互证。**
- 实证：本轮两方各有一处「同错」（一方混入 `rsi_14`、另一方列集含两个占比高的列），**都是靠「把每个自由度单独钉死」才暴露的**。

### 15.2 🔴 状态类断言必须当轮实测，不得沿用上一轮上下文
- **典型翻车（本日 lead 自犯 2 次）**：
  1. 拿上一轮自己的**摘要**里「某 worker 有 3 文件未提交」当事实基座，去反推「工作丢失」并发出 P0 质询 + 硬时限 —— 实际那份修复**早已在 `de517bf` 里**（`git log -S'("em","tx")'` 一查即知）。
  2. 把某 worker 报的「**暂存区已就绪、待放行**」读成「**已落库入仓**」—— 实际文件在会话目录 `audit/_stage_tools_audit/`，仓库 `tools/` 从未创建（`tools exists = False`）。
- **为什么隐蔽**：第 5 条伪影（参照量未现算）是「我编了个数」，**这条是「我复述了一个曾经为真、现已失效的结论」**，读者无法从语气上分辨。**摘要、上一轮结论、他人转述，全都是「上一轮上下文」。**
- **判据：涉及「现状/是否已在盘上/是否已生效」的断言，必须当轮用命令现取，不得引用记忆、摘要或他人转述。**

### 15.3 「盘上没有」不等于「有人虚报」——下结论前先穷举三种可能
发现东西不在预期位置时，**按顺序排除**，不得直接判为虚报：
| 可能 | 排查手段 |
|---|---|
| (A) 从未落盘（只报了计划） | 看汇报原文用的是「已落库」还是「已就绪/待放行」 |
| (B) 落到了别的路径 | **跨盘搜索**（会话目录在 C 盘、仓库在 D 盘，`Path.rglob` 两盘都要扫） |
| (C) 落盘后被覆盖 | `git stash list` + `git --no-pager reflog -20` |
- **本日实证**：两个「盘上没有」的案例，**全部是 (A/B)，零虚报**。⇒ 团队没有诚信问题，**是 lead 的读法与验证路径有问题**。
- ⚠️ 连带教训：**交付物放 SESSION_DIR、验收也读 SESSION_DIR** —— 只读仓库根会把「已就绪」误判成「不存在」。

### 15.4 worker 回报「完成」必须带盘上证据
- 合格证据：**commit hash + `git show --stat` 输出**（证明只含预期文件与行数）；或 **文件绝对路径 + 字节数**。
- 不合格：「已落库」「已完成」「已就绪」这类**无坐标**的措辞 ⇒ 必然产生 15.3 那类误判。
- 配套：**`git commit <paths>` 默认是 `--only` 语义**（只提交指定路径，忽略暂存区其他内容）⇒ 精确提交的支柱 = `git diff --cached --name-only`（提交前）+ `git show --stat HEAD`（提交后）双核。
- ⚠️ 但**不要把 `git status --porcelain` 里出现 `A ` 状态直接判为「有人偷偷 add 了文件」** —— 那可能正是 lead 自己在为「未跟踪文件 + pathspec 提交」做准备（paths 语法对未跟踪文件会报 `did not match any file(s) known to git`，必须先 `git add`）。

---

## 16. 🔴 组合市值的静默失真（2026-09-16 实测，最高危）

### 16.1 判据：`Σmarket_value` ≡ `total_value`（逐日恒等）

`portfolio_summary.total_value` **恒等于**当日 `portfolio_snapshots.market_value` 之和
（最近 12 日逐日实测，比值 **1.0000**）。

**⇒ 推论（本节的根）**：**快照缺行 ⇒ `total_value` 必然静默少算**，且不会产生任何报错。

**⇒ 强制前置检查**：判「组合市值可信」之前，**先查当日快照行数是否等于常态基线** ——
当前基线 = **34**（周五含周频的 `027293` = **35**）。这不是附加检查，是**唯一**能事先发现缺口的手段。

### 16.2 实证：09-16 缺整篮子场外

| 日期 | 快照行数 | 其中场外 | `Σmarket_value` |
|---|---|---|---|
| 2026-09-14 | 34 | 12 | 1,514,541.29 |
| 2026-09-15 | 34 | 12 | 1,507,806.18 |
| **2026-09-16** | **22** | **0** | **933,195.10** |

- 缺失 = 整篮子场外：`001194` `001323` `001407` `001437` `001765` `002152` `007994` `008269` `100032` `166301` `519770` `880013`
- `total_value(09-16) = 933,195.1` vs 应 ≈ **1,513,845** ⇒ **少 574,611（−38.1%）**
- **id 结构坐实「从未写入」而非「被删」**：09-16 = `id 38420..38441` **连续块、纯 ETF**；
  而 09-15 的 12 只场外是 `id 38408..38419` **另一个连续块**（另一条写入路径）。

### 16.3 根因：「无新值 ⇒ 不写行」把当日持仓打穿（**不是采集失败**）

```
2026-09-16 15:30:16 [阶段0/5] 场外基金净值采集
2026-09-16 15:30:17   [001194] 最新净值 2026-09-15 | 待插入 0 行      ← 13 只逐一同形
2026-09-16 15:30:25   场外净值: 成功 13 只, 失败 0 只, 跳过(无净值源) 0 只, 新增 0 行
```

场外净值源**当日只到 D−1**（T+1 披露），而库内 D−1 行已存在 ⇒ 采集器判「**待插入 0 行**」
⇒ 当日无场外行 ⇒ 快照主流程只为「**有当日价格的标的**」写行 ⇒ **整篮子场外被跳过**。

⇒ **修复方向必须二选一，不许静默跳过**：(a) fill-forward 自 D−1 值并显式标记 `stale`；
(b) **拒绝写入并告警**。两者都必须在报告侧可见。

### 16.4 🔴 `daily_return` 的基期由「summary 行是否存在」决定，而非交易日历

`src/analysis/portfolio.py:482-484`：
```sql
SELECT date FROM portfolio_summary WHERE date < ? ORDER BY date DESC LIMIT 1
```
⇒ 基期 = **`portfolio_summary` 表的上一行**，**不是**上一交易日、**也不是**上一快照日。
⇒ **summary 缺任何一天 ⇒ 次日自动变成「两日收益」**，且盘上无标记。

**实测（09-16）**：存储 `daily_return = -0.07`
- 基期 `09-14`（代码实际行为 ⇒ 两日）⇒ 复算 **−0.074570** ⇒ `round(2)` = **−0.07** ✅ 精确复现
- 基期 `09-15`（正确口径 ⇒ 一日）⇒ 复算 **+0.651314**
- ⇒ **日报显示「跌 0.07%」，实为「涨 0.65%」—— 符号相反**，且存储值被 `round(2)` 截到 2 位有效数字。

⇒ **验收任何 `daily_return` 前，先确认基期日的 summary 行存在。**

### 16.5 🔴 告警与「运行有效性」完全解耦（闸门失效的精确机制）

| 通道 | 是否含那条 error |
|---|---|
| `alerts` **表** | ✅ `id=124` `('total_value_drop','error','总市值大幅下降(>-5.0%): -38.4%','2026-09-16T15:30:41',0)` |
| 独立**告警邮件** | ✅ **确实送达** —— `15:30:42 src.utils.notification - 邮件发送成功: [ERROR] 投资组合告警 - total_value_drop` |
| `run_report.alerts` | ❌ **没有它**（只有一条 `stale_over_threshold` **warning**） |
| `run_report.run_status` | ❌ **`ok`** |
| `execution_logs` | ❌ **`success`** |
| 日报 | ❌ **照发**（15:34:57） |

- `total_value_drop`（15:30:41）**早于** run_report 生成时刻（15:34:52）⇒ **不是时序漏掉，是根本没进这条链**。
- ⇒ **`run_status` 只看「阶段是否都执行」+ `dq_score`，完全不消费 `alerts` 表。**
- ⇒ **修复判据**：**error 级告警必须进入 `run_status` 的判定**，否则「字段加对了、判据取错了源」。

**同源缺陷的对照组**：`run_report_2026-09-15.json` 仅 **770 B**、**无 `run_status` 字段**、
`stages` 只有 `otc_nav`/`watchlist` **两个**、`dq_score = 100`、`alerts = []`（**空**）。
⇒ 两日对照坐实：**`dq_score` 与 `run_status` 都不能证明运行有效。**

### 16.6 日志判据的适用范围（哪个文件可信）

- `logs/portfolio_*.log` —— **混有 pytest 输出**（`pytest-of-HUAWEI` 路径、`no such table: ...`、
  `market_breadth` 缺失、`All arrays must be of the same length`）⇒ 引用时必须**按 logger + 时间戳筛**，
  只采用 `__main__` logger 且时间戳在生产窗口内的行。
- `logs/scheduled_run.log` —— **实测干净**（关键词 `pytest` / `no such table` / `pytest-of-HUAWEI` **命中均为 0**）
  ⇒ 读它判断「跑通没有」**判据可信**。
- `data/reports/` —— 确有测试产物（`test_p2_report.html`、`test.html`，均 13 B）⇒ 见 §5.3。

### 16.7 复核工具（会话目录 `audit/`，均只读）

| 脚本 | 作用 |
|---|---|
| `_lead_0916_baseday.py` | 复算 09-16 的 `daily_return` 基期（两候选对比 + 复现存储值，含 `round(2)` 验证） |
| `_lead_0916_rowgap.py` | 快照行数缺口归属 + 场外/ETF 构成 + `Σmarket_value` 对账 + 全历史行数基线 |

（按 §2 规程，落库 `tools/audit/` 由 lead 逐文件串行提交。）

---

## 17. 溢出回流：`MEMORY.md` 迁出的详版（2026-09-17）

> 背景：`MEMORY.md` 每次会话自动注入，**超长会被静默截断、截断部分等于不存在**。09-17 把「不看就会立刻犯错」的条目压回 `MEMORY.md`，**把展开说明迁到本节**。本节与 `MEMORY.md` 冲突时，以本节的**实测数**为准。

### 17.1 产物重生成定式（09-16 实战）
**四个报告产出方**（改报告前先确认改的是哪一个）：

| 产物 | 产出方 | 备注 |
|---|---|---|
| `enhanced_report_YYYYMMDD.html` + `latest_report.html` | `EnhancedReportBuilder` | 纯读 DB；**同一次 `save_report(...)` 会同时写这两个** |
| `email_report_*_light.html` | 同 builder，`theme="light"` | 走邮件通道 |
| `smart_report_*.md` | `SmartReportGenerator` | 需 `combined_data` |
| `组合大盘综合视角_*` | `scripts/gen_combo_report.py` | automation 18:00 |

- 报告日期锚点 `report_date = MAX(portfolio_summary.date)` ⇒ **库修对后独立重渲染会自动锚到正确日**，不需要手工改文件名里的日期。
- 🔴 **禁止**用 `scripts/send_report_email.py` 重生成产物：**它没有 dry-run**，`main()` 只认 `--theme`，`today_str` 取**本机今天** ⇒ 等于把「发邮件」当副作用绑上重生成动作。要 light 版就复刻 `:380-388` 两行逻辑直写文件名。
- ⚠️ `save_report(...)` 的 `news_data` 形参**未被内部使用**；传 `None` 会**丢掉「行业资讯」板块**（`impacts`/`rotation` 不落库）⇒ 重生成时若走 `None`，**必须显式声明这一差异**，否则会被当成"报告变短了"。
- ⚠️ 产物命名**两种日期格式并存**：`enhanced_report_20260916.html`（**无横线**）vs `run_report_2026-09-16.json`（**有横线**）⇒ glob 写错会误判「产物不存在」。

### 17.2 `daily_return` 口径 B 的完整定义（09-17 丹哥裁定，durable）
- **定义 =「当日价新鲜标的」口径**：D 日若某标的 `value_date < D`，则该标的**不进分子也不进分母**；当前纳入范围 = **场内 22 只**。
- **覆盖度 = 被纳入标的的前日市值之和 / 全持仓前日市值之和**。⚠️ **不得硬编码**：以运行时算出的数为准。09-16 实测 = **61.08%**（只读生产库复算 `933195.10 / 1527928.85`）；曾一度按 `61.3%` 转述，**已更正**。
- **报告必须并列印出** `daily_return`（22 只 / 61.08%）与 `total_value`（34 只 / 全量），并声明 **「两者口径不同，不可相乘」**（读者拿 `1,527,929 × daily_return` 会算出错金额；实测当日盈亏 `+¥20,123` 而 `1,527,929 × 1.33% = 20,321 ≠ 20,123`）。
- ✅ **标注已落地**：`src/utils/enhanced_report.py` 新增模块级 `_fmt_dr_coverage()`，并**无条件**在「当日盈亏」卡片与其后印出收窄口径声明。读不到覆盖度时印 **「覆盖度未记录」**，不许静默省略 —— 因为**声明本身就与数据可得性无关**（见 §17.6 第 1 条：邮件路径拿不到数字）。
- 🔴 **T+1 回填不得把某日 `daily_return` 改写成「全 34 名」口径** —— 否则单调序列会断在那里，裁定 B 即失效。
- 落地位置 = 工作区 `src/analysis/portfolio.py` 的 **两层收敛**（口径收窄只在这两层）：
  - 第一层 `comparable_codes = set(curr_codes.keys()) & set(prev_snapshots.keys())`（当日持仓 ∩ 前日快照）；
  - 第二层 `common_codes = comparable_codes & fresh_codes`（再收窄到「当日价新鲜」）。
  ⚠️ **不得把裸锚 `common_codes = set(curr_codes.keys()) & ...` 当定位手段**——那行在 HEAD 里已改名/拆分，grep 命中 0；真实锚是上面两句**连左值一起给**。同时 `:569-571` 是价格异常告警，与口径 B 无关，引用时不要带上「不动 :569-571 分母分支」这类误指。
- ⚠️ **待证的推论**（勿引用为结论）：由 09-16 反证 A（22 只 `total_value = 1,513,844.88`）反推 `w_ETF·r_ETF = −0.05%`、`w_OTC·r_OTC = +1.3846pp`，取 `w_OTC = 0.388` ⇒ `r_ETF ≈ −0.08%`、`r_OTC ≈ +3.57%`。**若**成立，则 09-16 存值 `+1.334557%` 属**全 34 名**口径、B 的首个断点就在 09-16。**尚待 `audit/_res1_return_decomp.json` 证伪或证实。**

### 17.3 `#135` 对 `#124` §7.4 的机制更正（重要，结论不变）
「把 `'etf_flow'` 改对成 `'etf'` 而不同时补 NULL 过滤，会**立刻崩**」—— **实测否定**。真实因果是 **pandas dtype 推断**，不是 SQL：

| 输入形态 | 中间态（只改字面量、无 NULL 过滤） | `today_flow` dtype |
|---|---|---|
| **生产库真实数据**（116 行，含 sector） | **不抛**，但 **20/116 行 `today_flow = nan`、`flow_change = nan`** 静默流入报告 | `float64`（NULL 被折成 `NaN`，`float(nan)` 合法） |
| 只有 ETF、最新一行全 NULL | 抛 `TypeError` | `object`（`None` 原样保留） |
| 只有 ETF、且全表都 NULL | 抛 `TypeError` | `object` |

⇒ **静默 NaN 比崩溃更坏**：崩溃会拦下来，NaN 会一路流到展示层。「必须同时补 NULL 过滤」**保留**，理由换成这一条。
⇒ **误判类型**：与 `#127` 的 `13/8` 撤回**同型** —— **把一个数据的巧合（dtype）当成了必然机制**。
⇒ 连带发现（`#130` 未收口部分）：守卫 1 的作用域是「**payload 里给出的**指标列全空」，**不是「关键列 `net_inflow` 为空」**。
  ⚠️ 措辞精确化（09-17，`verify-p1-batch` 指出）：**不是**「12 个指标列全空」—— `fund_flow.py:544-547` 的 `metric_names` 只收 `row.index` 里**存在**的列 ⇒ 只带 `{"net_inflow": NaN}` 的 payload 与 12 列全 NaN 的 payload **落到同一条 `all(v is None)` 路径**。这个语义差正是「A 例被判据 1 放行、必须靠守卫三兜住」的原因。⇒ `{net_inflow: NaN, buy_amount: 1000.0}` 仍可造出「自称 `confidence=1.0` 的空值行」。09-17 裁定 = **拒绝 INSERT + 落同一告警**（不采纳「降级标签」，因为降级行**仍占 `id`/`created_at` 时间线**，而那是 `#124` 取证唯一的线索）。

### 17.4 文档/证据纪律的加严（09-17 沉淀）
- 🔴 **空锚（静默指向）**：引用指向**不存在的内容**且**不报任何错**。实例 `audit/_v130_verdict.md` 两处写「核对 SQL 见 §5」，而 §5 实际是「我的质询」、全文无任何 SQL 语句 ⇒ 若读者不核，D 会**以「SQL 已备好」为由被跳过**。**严重度高于行号漂移**：行号漂移会 `grep` 失败而**显式报错**，空锚完全静默。
  ⇒ **硬规则：每一处都必须有可 `grep` 的原文锚；行号只能在原文锚旁边作导航，不能是唯一定位手段。**
- 🔑 **「可写 / 不可写」边界**：**能逐行读到的代码事实可以写**（含引代码自述文案）；**涉及未知定义的因果一律不写**。写「当前口径下**未重算**」是**事实**（可核日期）；写「因为 X 所以该数**已过期**」是**推断**（定义都不知道，方向甚至可能相反）。09-17 在同一份 `07` §八 里连抓三处把两者写进同一句。
- 🔑 **`numstat` 与 `diff` 必须在同一次 shell 调用里取**：同一 turn 内先测 numstat、后读 diff，文件被并发写入 ⇒ 两个观测量互相矛盾（09-17 实测：我据此报了 `13/8`，实际 `17/10`，被 worker 驳回）。
- 🔑 **查回归必须先核 `collected`**：09-17 `audit/_v130_pytest.txt` 在写到 97% 时被读到，**无 `===== N passed =====` 汇总行**；只看"没报错"会把「跑了一半」当「全绿」。
- 🔑 **`&&` 链断裂会静默跳过后续全部命令、但任务仍报 `completed`**（三踩）⇒ 前置动作后先 `echo "chain OK"` 验链。
- 🔑 **数两个集合的并集时，先问「我数的是不是同一个东西」**（09-17 实测）：`tests/` 内 12 例 `test_report_caliber_note.py` 创建于 `10:45:40`，而第二轮全量产物落于 `10:43:06`（起跑约 `10:39:28`）⇒ **该次 `collected 1838` 根本不含它**。「1838 已含 12」与「1838 + 12 = 1850」**两个方向都错**。⇒ **终局计数一律以「实测 `collected` = `passed` + `skipped` 自洽」为准，不用任何加法推算。**
- 🔑 **反证要分强度级：探针级 < 用例级 —— 声称「某用例是反证」必须给用例级实测**（09-17，`verify-p1-batch` 提出并实测）：

  | 级别 | 做法 | 为什么不够 / 够 |
  |---|---|---|
  | **探针级**（弱） | 自己写脚本**复刻**判据逻辑，再把守卫那几行摘掉重跑 | 探针可能与**用例本身**不一致（判据复刻走样、夹具差异）⇒ 只能当旁证 |
  | **用例级**（强） | 摘掉守卫后**直接调用 CI 里那个用例函数本身** | 断言体真的被执行 ⇒ 「这是反证」这句话才被验证过 |

  实测（`#130` 守卫三，4 例）：用例级 `audit/_g3_pytest_falsify.py` → `.txt`（做法 = `module.save_fund_flows = broken` 后直接调用用例函数，**不改源码**）⇒ **A / E 红、B / C 绿**。
  ⚠️ 同批实测的**「做不到」也要记**：把 `src/` 换成坏版再跑 pytest 在本机走不通（其观察：真实代码库是 grep 出来的、不是 import 包）⇒ **不能拿「跑不了」当「只做探针就够了」的理由**，那会让自称反证的用例**从未被真正证伪过**。
  ⇒ 配套纪律：`#130` 的 4 例已在各自 docstring 里**显式标注自己属于哪一级**（A/E = 反证；B/C = 「不误伤」型，**不得当反证引用**），并注明判别力出处。
- 🔑 **探针的保真度决定结论的措辞**（09-17，同批提出）：若探针**无法调用实现本身**、只能**自己复刻**判据逻辑（`verify-p1-batch` 的原话：探针"是另一个副本"），则其结论**只许**表述为「**复刻行为一致**」，**不得**表述为「**实现如此**」。
  ⇒ 这是「复用上游判据时不得留旧代码副本」的同型问题：**副本一旦与实现分叉，结论就不再指向实现**。⇒ 优先做成「导入实现 / 直接调用用例函数」；做不到时，结论降级 + 显式说明降级原因。
- 🔴 **不要在「引用对象所在的文件」里复制它的原文 —— 否则锚点必然自指**（09-17 实测，我连踩两次）：
  一旦注释/文档照抄了同文件里的代码行，`grep` 那句原文就命中**两处**（引文 + 真码），读者可能**停在引文上** ⇒ 与本节开头那条「空锚」同型，只是更隐蔽（`grep` **成功**、不报错）。
  ⚠️ **更强的一层（第二次踩）**：改用「某条注释作唯一锚点」也不行 —— 只要把**那条注释**写进 docstring，它自己就变成不唯一（实测 3 处命中）。
  ⇒ **硬规则**：**引用同文件时只作文字描述，不复制代码原文、不复制注释原文、不写行号**；要读者看实现就直说「看本函数体」。
  ⇒ 实测支撑（行号为什么必须去掉）：同一段 `metric_names` 在本轮内漂两次（`:544`→`:549`→`:554`），**两次都源于该 docstring 自身的增删**。
- 🔴 **跨编码管道读源码做中文匹配 = 静默假阴性**（09-17，`verify-p1-batch` 实测）：同一份源码经 **PowerShell 管道（GBK 解码）** 读时中文匹配失败，遂使它对**我报的行号**先怀疑再撤回；用 **Read（UTF-8）** 复核则行数与 blob hash 全对。
  ⇒ **自检线索：连纯 ASCII 的行数也变少** ⇒ 不是"内容不同"，是**解码坏了**。
   ⇒ **双证**：`行数` + `git hash-object` 同时给。凡"我读不到某行"的结论，**先证自己读到的是同一份字节**。
- 🔑 **锚会改名（静默）比行号漂移更危险**（risk-manager 提出，lead 于 09-17 独立核实）。行号漂移至少让 `grep` 失败并显式报错；若只锚右值（如 `common_codes = set(...)` 的右半边），而作者把左边的变量名改了（`common_codes → comparable_codes`），该锚就会在 **HEAD 下命中 0**，且**没有任何报错**，读者会被静默带到错误位置或放弃追踪。**硬规则：引用赋值语句时必须连左值一起作为原文锚**——`comparable_codes = set(curr_codes.keys()) & set(prev_snapshots.keys())`，`common_codes = comparable_codes & fresh_codes`。仅凭右值做锚 = 留一个会静默断裂的引用。
- 🔑 **核对锚的唯一性必须在 HEAD 下实测**（不是在工作区）。工作区可能被并发修改，引用目标应为 `git show HEAD:<file>` 输出的字节。

### 17.5 `portfolio_summary.daily_return` 有**不止一个写入者**，且精度不同（09-17 实测）

**只读生产库实测**（`SELECT date, daily_return FROM portfolio_summary WHERE daily_return IS NOT NULL`，共 **3479 行**）：

| 小数位 | 1 | 12 | 13 | 14 | 15 | 16 | 17 | 18 | 19 |
|---|---|---|---|---|---|---|---|---|---|
| 行数 | 14 | 1 | 5 | 57 | **501** | **2074** | **731** | 88 | 8 |

⇒ **≤2 位仅 16 行**（1 位的 14 行 + 其它）；**2026 年 170 行 >2 位 / 2 行 ≤2 位**。最近 8 行（09-07~09-16）小数位 **14~17 位**，含 09-16 的 `1.33455691515695`（14 位）。

**逐行读到的代码事实**：
- `src/analysis/portfolio.py:1005-1006` —— `'daily_pnl': round(daily_pnl, 2)`、`'daily_return': round(daily_return, 2)`。
  ⇒ **管线路径产出的 `daily_return` 必然是 ≤2 位**。
- `scripts/recompute_summary_window.py:14-17`（docstring 自述）—— 「前 8 列……复刻 `src/utils/backfill.py:86-146`，用 `common_codes`（前后两日共同持仓、`quantity` 取前一日）算 `daily_return`」。
  ⇒ 该口径**没有「当日价新鲜」过滤**，**与 09-17 裁定 B 不同**。

**⇒ 事实**：绝大多数 `portfolio_summary` 行的 `daily_return` **不是** `_calculate_summary` 的 `round(2)` 路径写的。09-16 存值 `1.33455691515695` 是 14 位小数 ⇒ **它不是当天 15:30 管线写的**。
**⇒ 补充实测（data-engineer，09-17）**：按该 `common_codes` 口径算 09-16 得 `1.3345662`，与存值 `1.3345569` 相差 `9.29e-06` ⇒ **非逐位相同**（故俗称的「残差 0.138」**不是真实量**，它是 `0.140055` 的另一种投影，源自 `880013` 数量变动 + 逐行 2 位舍入）。

**调度事实（实测 glob `*.bat` + `scripts/*.py`）**：
- `recompute_summary_window.py` **默认 dry-run**（`:30-31`），落地须显式 `--apply`（`:34`）；
- `scripts/` 内**唯一**引用它的文件是它自己；`scheduled_run.bat`、`run_analysis.bat` **均不含** `recompute` 字符串；
- `run_all.bat` 有**交互菜单项 7** → `goto backfill` → `run_backfill.py all`（**需人工选择**）。
⇒ **它不在无人值守链上。** ⚠️ 本节 §16 附近的旧表述「唯一每天无人值守自动改写历史的入口零校验」**与这两条实测冲突**，引用前先按原文锚核对，**不要直接引用那句**。

🔴 **不许写的推断**：不得据此断言「所以 09-16 的值是错的」。**口径不同 ≠ 错误**；裁定 B 只约束**新写入**，未回溯历史行。凡涉及「历史行该不该按 B 重算」的因果，属未知定义，**一律不写**（见 §17.4）。

### 17.6 09-17 登记、**明确不做**的 backlog

| # | 事项 | 为什么现在不做 |
|---|---|---|
| 1 | `daily_return_coverage` **不落库** ⇒ 邮件路径永远印「覆盖度未记录」（数字只在同进程传 summary 时可见）。想带数字须改持久化通道：写进 `run_report_<date>.json` 或给 `portfolio_summary` 加列 | 属**写库侧**改动；本轮裁定「写库侧今天一律不碰」，且它**不阻塞**「不可相乘」这个必要声明（后者已无条件印出） |
| 2 | `portfolio_summary` 至少**两个写入者 + 两套精度**（管线 `round(2)` vs 回填全精度）⇒ 序列存在精度断点 | 改 `round(2)` 是**口径改动**，与裁定 B 同类，须先拍板；且**不能**用「历史都是 17 位」当理由去动它 |
| 3 | 守卫基线口径：`expected_universe` 取 30 日回看交集，跨扩编边界会**回落旧朝代**；`2026-07-31` 因已清仓的 `159732` **误拒** | 属闸门语义，非阻断项；今天改会让闸门在无人值守时段行为变动 |
| 4 | `docs/handover/07_`、`11_`、`12_` 的**行号层重写**（`#115`/`#116` 已提交，文档里「工作区行号」与「`+20`/`+234` 位移」整套先验已失效） | 纯机械活；**锚点以可 `grep` 原文为准**这一条已先行保证可读，故不紧急 |
| 5 | **运行账本 `run_report_<date>.json` 会被回填就地覆盖**（详见 §17.7） | 改名/加字段会**打断邮件闸门**（`send_report_email.py:112` 按 `run_report_<today>.json` 读 `run_status`）⇒ 属写产物侧 + 兼容性改动，须先拍板 |

### 17.7 🔴 运行账本被回填**就地覆盖**，且文件名看不出换了来源（09-17 实测）

**逐行读到的代码事实**：
- `src/data_sources/collect_core.py:540-543` —— `RunReporter(date_str, mode="daily", reports_dir=None, run_date=None)`；`self.date = date_str  # 业务日期(目标日/回填日)`；`self.run_date = run_date or datetime.now()...`。
- `:765-767` —— **落盘名只用业务日期**：`path = os.path.join(out_dir, f"run_report_{self.date}.json")`，紧跟 `open(path, "w", ...)` ⇒ **截断式覆盖，无备份、无 run_id、无 mode 后缀**。
- `:742` —— 文件**内容里**有 `"run_date": self.run_date`；`mode` 也写进了内容（`run_analysis.py:1005` 取 `mode=("backfill" if backfill_date else "daily")`）。
  ⇒ **内容能区分来源，文件名不能。**

**实测现场**（`git diff HEAD -- data/reports/run_report_2026-09-16.json`，`09-17`）：

| 字段 | HEAD（09-16 `daily` 运行） | 工作区（09-17 `backfill` 运行） |
|---|---|---|
| `run_date` | `2026-09-16` | **`2026-09-17`** |
| `mode` | `daily` | **`backfill`** |
| `dq_score` | `94.1` | `97.6` |
| `data_quality_issues` | `spot_stale` / `n_affected=20` / `action=flagged` | **`spot_historical` / `n_affected=25` / `action=rejected`** |
| `alerts` | `[stale_over_threshold: 源数据滞后 20 行(≥阈值5)]` | `[]` |
| `retry_queue_pending` | `1` | `2` |
| `generated_at` | `2026-09-16 15:34:52` | `2026-09-17 09:06:04` |

⇒ **事实**：对历史日期 `D` 跑一次 `--date D` 回填，会以 `mode="backfill"` 覆盖 `run_report_D.json`；该日 `daily` 运行的账本**在磁盘上不复存在**（此例仍可由 git 取回：`index e1087dc..265d197`，即 HEAD blob `e1087dc`）。

🔴 **它对本仓既有论证的影响（必须一并记住）**：
- 「读 `run_report_<date>.json` 的 `dq_score` 判断那天跑通没有」这一用法，**前提是「那天之后再没人对它跑过回填」** —— 该前提**没有任何机制保证**（本例就是反例：09-16 曾被回填）。
- 因此**不得**把某日的账本内容当作「那一天的不可变物证」；引用时必须同时给 `run_date` + `mode` + `generated_at`，**只引用 `run_report_<date>.json` 这个名字是不合格引用**（同 §17.4「空锚」类）。

🔴 **修法方向（本轮不做，仅登记）**：候选 ① 文件名加 `mode` 或 `run_date` 后缀；② 写入前检测同名账本已存在且 `mode != 当前 mode` ⇒ 拒绝或另存；③ 引入 `run_id`。
**三条都会触及 `send_report_email.py:112` 的 `run_report_<today_str>.json` 读取点**（那是邮件闸门读 `run_status` 的唯一通道）⇒ 属兼容性改动，**须先拍板再动**，不能今天顺手改。

### 17.8 已知的「用户可见」变化（**勿当成回归**）

| 变化 | 位置 | 机制（实测） |
|---|---|---|
| 盘后复盘「资金流变化」表 **91 行 → 116 行** | `tabs/tab8_advice.py` 的 `_load_fund_flow_changes`，`"FROM fund_flows WHERE category='etf' AND date >= date('now','-7 days')"` | 该查询此前用的字面量**从未取到 ETF 资金流**（全库 `fund_flows` 只有 `sector`/`etf`/`main_fund` 三种 category）。改对后**新纳入 ETF 行**。数字自洽性我已独立复核：`sector` 有 **91 个不同 code**、`etf` 有 **25 个** ⇒ **91 + 25 = 116**，与实测行数吻合。**非缺陷**。 |
| 报告新增「覆盖度」行 + 口径声明 | `src/utils/enhanced_report.py`（`f053ccb`） | 「当日盈亏」卡片下多一行「覆盖度未记录 / 覆盖 N/M 只…」；卡片组后多一段「两者口径不同，不可相乘」。**预期内**。 |
| Tab8 持仓信号预览出现资金流 as-of 日期标注 | `tabs/tab8_advice.py`（`f09983a`，已完成） | 现值 ≠ 当日时标日期，否则「D-2 的资金流」在 UI 上与「今天的」逐字符相同。盘前「持仓信号预览」与盘后「资金流向变化」两处均已标注。**预期内**。 |
| 组合大盘日报里 `etf_price_history` 的措辞会**随数据新鲜度变化** | `scripts/gen_combo_report.py`（见 §17.10） | 以前**永远**印「最新仅至 2026-08-19（滞后）」（写死）；现在按库实时计算：落后时才说「滞后至 <真实日期>（N 个交易日）」，已覆盖时说「已覆盖至 <日期>」。**表面看是文案变了，实为删掉一条假陈述**。另：正文里的当日回报由 14 位原值改为统一 2 位小数（与卡片一致）。**预期内，勿当回归**。 |

⚠️ **留痕缺口（本轮自曝）**：上表第 1 条的副作用**没有写进 `1864589` 的提交信息**（当时那条信息只覆盖 `#77` 的部分）。**用户可见的变化必须写进提交信息**，否则日后必被当成回归 —— 故在此补记，并由本表承担留痕职责。

### 17.9 🔴 D 修复：`fund_flows` 的「自称完全可信的空值行」已诚实化（09-17 执行并验证）

#### (1) 事件与根因

生产库 `fund_flows` 里，`date='2026-09-16'` 且 `category='etf'` 的 **20 行**（`id` 30796–30815）
**12 个指标列全部 NULL**，却带着 `source='em_spot' / is_estimated=0 / confidence=1.0`
—— 元数据在宣称「这是完全可信的真值」，而值已被清零。全库仅此 20 行如此
（按 `source` 分组统计「12 列全 NULL」：**只有 `em_spot` 命中，且恰为 20**）。

🔑 **根因是一个 NaN-blind 的「or 0」惯用法**（已用 `venv313` + pandas **实测复现**，非推断）：
写方 `fetch_etf_fund_flow_spot_em`（`src/data_sources/fund_flow.py`）对每个指标列都写成
`float(row.get('<列名>', 0) or 0)`。而 **Python 里 `NaN` 是 truthy**，故：

- 列**不存在** ⇒ `row.get(col, 0)` 得 `0` ⇒ `or 0` 得 `0.0`（**这是该写法唯一生效的情形**）；
- 列**存在但为 NaN** ⇒ `row.get(col, 0)` 得 `nan` ⇒ `float(nan) or 0` **返回 `nan`**（不是 0）；
- `save_fund_flows._float(nan)` 映射为 `None` ⇒ **NULL 落库**。

实测输出：`float(pd_row.get(col,0) or 0)` 在 NaN 单元格上得 `nan`，`_float()` 得 `None`。
⇒ **「or 0」只防「列缺失」，完全防不住「列存在但为 NaN」**。EM 当日尚未发布资金流
（或该 ETF 无资金流）时返回的正是 NaN，于是整行 12 列全空却被贴上 `confidence=1.0`。

⚠️ **本条的处置是「文档化 + 守卫兜住」，不是改 `or 0`**：把它硬改成「NaN→0.0」会**更坏**
（伪造一个看起来合法的「零流量」真值）。当前「NaN→NULL→被守卫 3 拒绝 + 落 error 告警」
才是诚实结果。**故 `or 0` 保持原样，属已知机制**。

#### (2) 修复目标态与裁定理由

| 列 | 原值 | 新值 | 理由 |
|---|---|---|---|
| `source` | `em_spot` | `em_spot_empty` | 命名真实条件（EM spot 返回了行但指标为空）；关键是**从此可与真 `em_spot` 行区分** |
| `is_estimated` | `0` | `1` | 保持本表既有不变量：`is_estimated=0 ⇒ confidence ∈ {NULL, 1.0}`（实测该不变量在本表**当前严格成立**）。若留 0 会造出全表唯一一个「`is_estimated=0` 且 `confidence=0.0`」的组合，任何未来「取真实数据」的过滤都会把它扫进去 |
| `confidence` | `1.0` | `0.0` | 信任旋钮归零 |
| 12 个指标列 | NULL | **仍 NULL** | 🔴 **严禁置 0**：`0.0` 是「看起来合法的零流量」，会被下游当真值 |

🔴 **这 20 行必须保留，绝不 DELETE**：其 `id`（30796–30815）已被写方自己的 docstring
当作事故取证引用（`save_fund_flows` 的 docstring 内明写该 id 区间），删掉即毁证。

#### (3) 可复现的规范 SQL（`audit/` 不入库，故命令在此留档）

```sql
-- 目标：恰 20 行；rowcount 必须 == 20，否则回滚
UPDATE fund_flows
   SET source='em_spot_empty', is_estimated=1, confidence=0.0
 WHERE date='2026-09-16' AND category='etf' AND source='em_spot'
   AND id BETWEEN 30796 AND 30815
   AND net_inflow IS NULL AND buy_amount IS NULL AND sell_amount IS NULL
   AND net_inflow_pct IS NULL AND super_large_inflow IS NULL AND super_large_pct IS NULL
   AND large_inflow IS NULL AND large_pct IS NULL AND medium_inflow IS NULL
   AND medium_pct IS NULL AND small_inflow IS NULL AND small_pct IS NULL;
```

验收查询（四条，必须全部满足）：

```sql
-- ① 目标态：应恰为 (em_spot_empty, 1, 0.0) × 20
SELECT source, is_estimated, confidence, COUNT(*) FROM fund_flows
 WHERE id BETWEEN 30796 AND 30815 GROUP BY 1,2,3;
-- ② 空值行不得再自称 em_spot：应为 0
SELECT COUNT(*) FROM fund_flows WHERE source='em_spot'
   AND net_inflow IS NULL AND buy_amount IS NULL AND sell_amount IS NULL
   AND net_inflow_pct IS NULL AND super_large_inflow IS NULL AND super_large_pct IS NULL
   AND large_inflow IS NULL AND large_pct IS NULL AND medium_inflow IS NULL
   AND medium_pct IS NULL AND small_inflow IS NULL AND small_pct IS NULL;
-- ③ 不变量未被破坏：应为 0
SELECT COUNT(*) FROM fund_flows
 WHERE is_estimated=0 AND confidence IS NOT NULL AND confidence<>1.0;
-- ④ 只动这 20 行：总行数不变 + 计数 −20/+20 配平
SELECT COUNT(*) FROM fund_flows;   -- 27842（前后一致）
SELECT source, is_estimated, confidence, COUNT(*) FROM fund_flows
 GROUP BY 1,2,3;                   -- em_spot 307→287；新增 em_spot_empty 20
```

**实测结果（当轮）**：`rowcount=20`；① `(em_spot_empty,1,0.0,20)` ✓；② `0` ✓；③ `0` ✓；
④ 总行数 27842→27842、`em_spot` 307→287 ✓。备份：
`data/backups/portfolio_PRE_D_SPOT_EMPTY_20260917.db`（143,114,240 B，`integrity_check=ok`）。

#### (4) 读中性证明（跑了生产实现的同一入口）

🔑 纪律：**复现某口径的探针必须调用生产实现的同一入口**，不得自己复刻 SQL。
故直接 `import` 并调用 `src.analysis.pre_post_market._load_fund_flow_changes`，
分别在【修复前备份库】与【修复后生产库】上跑并逐字段比较：

**结果：两侧均 116 行，逐行逐字节完全一致 ✓** ⇒ **D 修复是读中性的**。
与「两处读者（`_load_fund_flow_changes`、`_load_etf_signal_previews`）都以
`net_inflow IS NOT NULL` 为硬条件，而这 20 行的 `net_inflow` 从来就是 NULL」的推理一致。
（该 116 = `sector` 91 个 code + `etf` 25 个 code，与 §17.8 第 1 条互相印证。）

#### (5) 🔴 我在本次**自己写错了三条检查器断言**（数据没错，是验证器错）

记下来是因为这两类错**每次都会重犯**：

| # | 我最初写的断言 | 为什么是错的 | 正确写法 |
|---|---|---|---|
| a | 「全库『12 列全 NULL』行数 = 0」 | 与本修复的**设计自相矛盾**：指标列被**刻意保留 NULL**（置 0 会伪造零流量真值）⇒ 永远为 20。**断言在拿一个必然为假的条件去证明成功** | 「`source='em_spot'` 且 12 列全 NULL 的行数 = 0」（即**未标注的空值行**归零） |
| b | 把 `COUNT(*)` 放进 `GROUP BY` 结果做**集合差集** | 纯计数变化（307→287）会同时表现为「新增一条组合」+「消失一条组合」⇒ 误报不符。**集合的身份只应是标签本身** | 身份用**标签三元组**做差集；计数另列相减并断言配平（−20/+20） |
| c | `sorted(deltas.items())` | 计数 key 含 `None`（legacy `source=None` 组，23,090 行）⇒ **混合类型不可比**，`TypeError: '<' not supported between NoneType and str` | `sorted(..., key=lambda kv: str(kv[0]))` |

⇒ 沉淀：**(i) 断言不得与自己的设计意图相矛盾；(ii) 集合的身份与计数必须分离；
(iii) 对含 `NULL` 分组键的结果排序一律加 `str()` 兜底。**
（(ii) 与 §17.4 已有条目「集合计数自洽」同族，本次是它在**检查器反方向**上的重犯。）

⇒ 另一条流程收获：**修复脚本要同时支持「首次写入」与「仅校验」两条路径**。
第一次跑完后再跑一次，走的正是「命中 0 行 ⇒ 不写入、但仍跑全部事后校验」的分支，
才把上面 (b)(c) 两个检查器缺陷暴露出来 —— 若脚本在「无事可做」时直接 `exit(0)`，
这两个错就永远测不出来。

#### (6) 回归

`tests/` 全量：**`1850 passed, 4 skipped, 0 failed`**（`collected 1854` = 1850 + 4，
117.95s，`audit/_lead_pytest_full5_0917.txt`）—— 与本次修复前的基线**逐数一致**，
即 D 修复对测试面零影响。

### 17.10 🔴 `gen_combo_report.py` 的两处「自伤」：写死的滞后日期 + 裸 `{d_ret}` 泄漏 14 位

#### (1) 是怎么发现的

在准备「重生成 09-16 组合大盘综合视角」时，先核对**已重生成的那一版**（09-17 09:31）是否真的修好了。
结果：大项确实修对了（总市值 933,195→1,527,929；当日回报 −0.07%→+1.33%；
盈亏只数 13/9→19/15；Sharpe/回撤 0.73/51.31%→0.60/6.72%），
但**两行没跟着改**：

1. 正文仍写 `项目 etf_price_history 最新仅至 2026-08-19（滞后）`；
2. 同一句把 `daily_return` 印成 **`1.33455691515695%`**（14 位），
   而同篇卡片写的是 `+1.33%` —— **一个产物里两套精度**。

#### (2) 根因（两处都是**模板写死**，不是数据问题）

| 缺陷 | 根因（实测） |
|---|---|
| 滞后断言永不自我纠正 | 该字符串在 `scripts/gen_combo_report.py` 里是**字面量**，共 **4 处**（锚定规则 note、3.2 表的场外未覆盖行、④数据修复的补采建议条、⑤`数据源与可用性`清单行）+ 1 处注释。它在 08 月曾是实情，数据表补采到最新后**变成假陈述**，且永远为假 |
| 正文精度与卡片不一致 | 5 处正文用裸 `{d_ret}%` 插值。Python 对 float 会给出**最短可往返表示** ⇒ `1.33455691515695`；而卡片走 `chg()`（`{:+.2f}`）⇒ `+1.33%` |

🔑 **实测锚**：`portfolio_summary` 09-16 行 `daily_return = 1.33455691515695`；
`etf_price_history MAX(date) = 2026-09-16`（**不滞后**）。

#### (3) 修法

- 新增实时计算块（在 `d_ret` 解包之后）：`EPH_DATE` / `EPH_STALE` / `EPH_LAG_TD`，
  并派生 `EPH_STATE_TXT`、`EPH_SRC_LINE`、`EPH_CAVEAT_884`、`EPH_FIX_BULLET`、
  `EPH_INLINE_PAREN`、`DRET_TXT` 六个文案变量 —— **全篇措辞由同一次运行的状态派生**，不会出现半新半旧。
- **不新鲜才出「滞后」**；新鲜时改说「已覆盖至 <日期>」，且**任何情况下都保留**
  「未用于当日逐 ETF 归因」（该能力本报告未实现，不得因数据变新而暗示已启用）。
- 「④数据修复」的补采建议条在不新鲜时才渲染（`EPH_FIX_BULLET` 为空串即消失）。
- 5 处裸 `{d_ret}%` → `{DRET_TXT}%`（统一 `{:+.2f}`）。
- 注释里那句 `场外净值 T+1 披露，本地亦无其当日涨跌（etf_price_history 滞后至 …）`
  改写成**实测的**真因：`etf_price_history` 只收 ETF 日线，**12 只场外基金在该表 0 行**
  （实测这 12 个 code 合计 0 行；全表仅 23 个 distinct code）⇒ 这是**结构性缺行**，
  与「采集是否及时」无关（原先把它归因于滞后是**误归因**）。

🔴 **滞后天数用库内自有交易日轴（`index_quotes` 的 `DISTINCT date`）数，故意不 import
`src.utils.trading_calendar`**。理由：本脚本由无人值守自动化以
`python scripts/gen_combo_report.py` 调用，此时 `sys.path[0]` 是 `scripts/` 而不是项目根，
`import src.*` **必然失败** —— 我第一版就是这么写的，实测回落成了「若干交易日」。
取不到就保持 `None` ⇒ 措辞回落「若干交易日」，**宁可不给数字，也不臆造**。

#### (4) 验证（沙箱，两个分支，15/15 断言通过）

🔴 **该脚本会写生产库**（`INSERT OR REPLACE INTO sector_daily_change`），且是顶层脚本，
故验证必须沙箱化（沿用 §17.1 的副本法，但本脚本的 DB 是模块常量，故改为
「读源码 → 把 DB 常量与输出目录改写成沙箱路径 → `exec`」）：

- 沙箱副本 `data/backups/_combo_sandbox.db`；输出重定向到 `audit/_combo_scratch_*/`；
- 🔑 **在沙箱里「种」一个 14 位的 `daily_return`**（`1.33455691515695`）——
  库内 09-17 真值恰为 `-0.38`（本来就是 2 位），**用它根本区分不出**
  `{d_ret}` 与 `{d_ret:+.2f}`，验证会得出「无法判定」。不种就没法压到这条修复。
- **场景 A**（删掉 `etf_price_history` 当日行 ⇒ 落后）：出现真实日期 `2026-09-16`
  + **真实天数 `（滞后 1 个交易日）`**（未走回落）；卡片与正文**同为 `+1.33%`**；
  产物不含 `2026-08-19`、不含任何 14 位数字。
- **场景 B**（补一行当日 ⇒ 覆盖）：全篇 `滞后` 出现次数 **= 0**；改说
  `已覆盖至 2026-09-17`；仍保留 `未用于当日逐 ETF 归因`。
- **生产库 sha256 前后一致**（`638c9e0c…`）⇒ 沙箱隔离成立。
- 回执：`audit/_combo_fix_verify.txt` / `_combo_fix_verify_stdout.txt`；
  产物留档 `audit/_combo_scratch_A|B/`。

#### (5) 关于「09-16 组合大盘」这版产物：**决定不再重生成**

原计划是「只生成一次，与口径 B 一起」。但实测：该产物已于 **09-17 09:19–09:31** 重生成过
（原始坏版保留在 `audit/pre_rg2_0916/`），而口径 B(报告标注) 落在 **10:50** 之后 ⇒
**已生成的 09-16 产物本就不含口径 B 标注**（实测「覆盖度未记录／不可相乘」均 absent）。

**现在再重生成 09-16 会把它改坏，而不是改好**：该脚本的基准日取自
`SELECT MAX(date) FROM portfolio_summary`，如今已是 `2026-09-17`，
用 `--date 2026-09-16` 跑出来的会是「09-17 数据、09-16 文件名」的错配产物。
⇒ **决定：不重生成**。09-16 产物保留其已知缺陷（滞后断言 + 14 位精度），
作为**历史产物**存在；修复的价值体现在**从今晚起**的产物上。
（口径 B 是**面向未来**的报告特性，不回填历史产物 —— 历史产物应保持其被投递时的样貌。）

---

### 17.11 🔴 日报全站停发：契约1 判级误报 + 评估时刻错位（2026-09-17 实测，当日两次修复）

> **一句话**：18 个阶段全绿、五份产物全齐、`Σmarket_value ≡ total_value` 成立 ——
> 但 `run_status=degraded` ⇒ 邮件闸门拒发日报。**误报的代价等于漏报**：
> 把结构性的 T+1 常态判成 `error`，等于**从今天起每天停发**。
>
> **本节所有代码/日志位置一律给「可 `grep` 的原文锚」，不给行号** ——
> `logs/scheduled_run.log` 行号实测会漂（同一段原文，先读到约 1.69 万行处、数小时后读到的刻度不同）。

#### (1) 现象（逐条实测，证据可 grep）

| 事实 | 可 `grep` 的原文锚 / 核验方式 |
|---|---|
| 18 个阶段**全部** `status=ok` | `data/reports/run_report_2026-09-17.json` 的 `"stages"`（18 项逐项 `"status": "ok"`） |
| 产物全部正常产出 | `enhanced_report_20260917.html`（170,743 B）、`latest_report.html`、`smart_report_20260917.md`、`组合大盘综合视角_2026-09-17.html`、`data/reports/run_report_2026-09-17.json` |
| 组合汇总正常 | 只读查 `portfolio_summary` 的 `2026-09-17` ⇒ `total_value = 1,524,395.24`、`daily_return = -0.38` |
| 但账本被降级 | 同一账本内 `"run_status": "degraded"`、`"dq_score": null`、`"alerts"` 计 3 条 |
| 真分被**压制**（不是丢失） | `"dq_score_reason": "run incomplete (run_status=degraded); real dq_score 97.6 suppressed"` |
| 日报被拒 | `logs/scheduled_run.log`（**GBK**）原文 `[EMAIL] [CRITICAL] 数据未就绪，拒绝生成/发送今日(2026-09-17)日报` + `run_status=degraded（本次运行不完整）`；紧跟一行 `[WARN] report email send FAILED, rc=1` |
| 浅色版未生成 | `data/reports/` 下**无** `email_report_20260917_light.html` |

⚠️ **「收到邮件」≠「日报发出」**：阶段三的 `src/utils/notification.py` → `send_portfolio_report`
是**另一条通道**，它不读 `run_status`、**不受闸门管** ⇒ 当天用户仍收到**简版摘要邮件**。
**唯一正式日报出口是 `scripts/send_report_email.py`**；判「日报发没发」只能看它，**不能看「有没有邮件」**。

闸门定义（`grep` 点）：`scripts/send_report_email.py` 的
`_RUN_STATUS_BLOCKING = ("partial", "failed", "degraded")`。

#### (2) 根因链（三层，逐层实测 —— **三层缺一不可，只修前两层不生效**）

**① 判级：把结构性常态判成 `error`。**
`src/analysis/snapshot_gate.py` 的 `check_otc_nav_coverage()`（`#116` 契约1，`10d3ec2` 于 09-17 10:48 入库）
最初把「**场外源当日只到 D-1**」**无条件**判成 `error` 级。
而**场外净值 T+1 披露是结构性常态**（15:30 时源永远只到 D-1）⇒ 该 `error` **每个交易日都必然出现**。

采集侧同一事实的原话（勿把「无值可插」读成「采集失败」）：
`场外净值: 成功 13 只, 失败 0 只, 跳过(无净值源) 0 只, 新增 0 行`、
`[519770] 最新净值 2026-09-16 | 待插入 0 行`。

**② 传播：一条 `error` ⇒ 整轮 `degraded`。**
`src/data_sources/collect_core.py` 的 `evaluate_run_status()`，其 docstring 自带判据原文：
`"degraded": 阶段齐全无 error, 但本次运行产出过 error 级告警`。
⇒ 只要有一条 `error` 级告警，`run_status=degraded`；`degraded` 又使 `dq_score` 置 null
（源码原文：`运行不完整时 dq_score 必须为 null`），**真分只写进 `dq_score_reason`**。
⇒ 后果**不是**「今天发不出」，而是**从今天起每天都发不出**。

**③ 🔴 时序错位（真正的致命点）—— 只修①、② 仍不生效。**
契约1 的调用点原本紧跟**阶段0（场外采集）**之后、**阶段一之前**；
而**当日 `portfolio_snapshots` 行是在阶段一里才写入的**
（`src/analysis/portfolio.py` 的「持仓合并」路径，把 12 只场外以**上一可用净值**补位落行；
源码注释原文 `# 保存持仓快照（新持仓数据写入数据库）`）。
⇒ 检查那一刻当日快照**一行都没有** ⇒ 「快照是否已覆盖这些 code」的判据**恒为 `filled=0 / uncovered=12`**
⇒ 修法①给出的 `warning` 分支**在生产里不可达**。

时序证据（`logs/scheduled_run.log`，**只给原文与时钟，不给行号**）：

| 时刻（本机） | 原文锚 |
|---|---|
| `15:30:43,930` | `ERROR - [otc_nav_missing] 场外当日净值缺失 12 只(目标日 2026-09-17)` |
| `15:30:45,386` | `INFO - 保存持仓快照: 2026-09-17, 34条记录`（logger `src.utils.database`） |

⇒ **告警比落库早约 1.5 秒**。契约1 自己的 docstring 已写明该前提：
`filled` 形态只能在同一日的 `portfolio_snapshots` 行**落库之后**才可观测。

⚠️ **「这是误报」的另一半证据（必须一起写，否则等于只说了半句）** ——
**09-16 的真事故形态当天并不存在**：
- 09-17 快照 **34 行**、code 集合与 09-16 **完全相同**（只读核过两日均 34 行）；
- 契约2 `check_snapshot_baseline()`（`src/analysis/snapshot_gate.py`，**真事故主防线**）**通过了**；
- `daily_return` 口径 B 已把这 12 只**从分子/分母排除**（口径 B 定义见 §17.2；
  覆盖度**不落库**，报告里印的是「覆盖度未记录」，见 §17.6 第 1 条与 §17.8）。

⇒ 该 `error` 是**误报**；09-16 那种「整篮子缺行 ⇒ 当日只有 22 行 ⇒ `total_value` 少 38.1%」（§16.2）
**当天没有发生**。

#### (3) 两次修法（都记；**第二次才是对的**）

| 序 | commit | 改了什么 | 判定 |
|---|---|---|---|
| ① | `055bbfe` | 判据由「源当日无净值」改为「**当日快照是否已覆盖这些 code**」并**分流**：`filled`(已落行) ⇒ `alert_level="warning"` + `ok=True`；`uncovered`(缺行 = 09-16 形态) ⇒ `"error"` + `ok=False`；**读快照失败时保守回退为 uncovered** | 判据**本身是对的**；但**评估时刻错** ⇒ 生产不生效 |
| ② | `10ed187` | 把契约1 整段**移到阶段一之后**（`_reporter.stage("basic", "ok")` 与契约2 之后）。**分流逻辑、`evaluate_run_status`、契约2 均未动** | ✅ **这才是修复** |

两次都顺带删掉/修正了契约1 旧文案里那句**假陈述**：
「…这些标的本日**不落快照行**(禁止静默跳过)」（`src/analysis/snapshot_gate.py` 的 docstring 内
以 `← 此句与事实不符` 标注）。**合并路径确实落了行。**
⇒ 教训：**告警文案必须与写入方行为对账** —— 文案是假陈述时，它既误导读者，
也让「判据到底可不可达」无从判断。

#### (4) 验证数字

- **全量回归**：`1858 collected / 1854 passed / 4 skipped / 0 failed`
  （修前基线 `1854/1850/4/0`，**差值全部来自新增用例**）。
- **真实数据两点复现**（调**生产实现的同一入口函数**，不另写 SQL —— 见 §17.9(4) 同一条纪律）：
  旧时刻 ⇒ `ok=False` / `error` / `filled=0` / `uncovered=12`；
  新时刻 ⇒ `ok=True` / `warning` / `filled=12` / `uncovered=0`。
- **反证是「用例级」而非「探针级」**：拆掉判据 / 倒回时序，
  **直接调用 CI 里的用例函数本身**（不改源码）⇒ FAIL。

⚠️ **附一条未复核的记账差异**：`MEMORY.md` 的「当前全量基线」行仍写
`1857 collected / 1853 passed / 4 skipped / 0 failed`，与本节的 `1858/1854/4/0` **相差 1 条**。
**哪个最新未复核**，故两边各自保留原文，引用时勿混用。

#### (5) 三条可复用教训

**教训 1：闸门的判据必须在「数据可观测的时刻」求值，否则分支不可达 —— 而不可达的守卫会以「全绿」的面目存在。**
契约1 的 `warning` 分支写得很对，但它在生产里**永远走不到**。表上看「守卫已就位」，
实际「守卫是装饰」。⇒ **判据正确性 ≠ 判据可达性**，二者要分开验收。

**教训 2：单测若预置了输入状态，就对「评估时刻」完全失明。**
上一轮 3 条新用例**全部预置了目标日快照行** ⇒ 全部走 `filled` 分支 ⇒ 全绿，
恰好把「生产不可达」掩盖掉。**预置状态 = 把被测对象的时间维抽掉。**
⇒ 硬要求：**时序守卫必须独立存在**（断言调用点在快照落库之后），不能靠「用例绿不绿」代替。

**教训 3：误报的代价等于漏报。**
把「结构性常态」判成 `error` ⇒ 日报**永久停发**，且它**伪装成「质量闸门在正常工作」**。
⇒ **分级必须能区分「常态」与「异常」**：判据要能回答「这条告警是不是每个交易日都会出现」。

**附带（跨层推演纪律，已由源码坐实）**：`pipeline_incomplete` 是 `run_status != ok` **派生**的，
不是成因 —— `collect_core.py` 内条件即 `if run_status != RUN_STATUS_OK:` 才追加
`_alert_pipeline_incomplete(...)`。**把它当 `run_status` 的输入会自我循环。**
本案例账本里三类告警同时出现，但因果只有**一条**：
`otc_nav_missing(error)` ⇒ `degraded` ⇒ `pipeline_incomplete(critical)`。


### 17.12 ✅ #140 占位哨兵值 = 合法中立值 ⇒「无数据」与「中立」逐字符相同（已裁定 Option A）

> 注册为 **#140（tab8「持仓信号预览」的 12 行是占位哨兵）**。2026-09-18 裁定 **Option A**：
> 数据层 `EtfSignalPreview` 新增 `rsi_available` / `score_available` / `risk_available` 三标志（`_load_etf_signal_previews` 按「真实行/字典是否存在」置位）；
> 渲染层 `_render_pre_market_panel` 仅在标志为 True 时输出数值、否则输出「无数据」。
> 已落地 `bf77304`；行为对照测试 `tests/test_tab8_advice.py::TestTab8SignalPreviewNoDataDistinction` 双反例通过（真实中立 50 显示「50」/ 占位 50 显示「无数据」）。Option B（仅文档标注为已知限制）未采用。

**病**：`tabs/tab8_advice.py` 的「持仓信号预览」有 **12 行占位哨兵**，而哨兵值恰为**合法中立值 `50`**。
⇒ 在 UI 上，「这只**没数据**」与「这只**就是中立**」**逐字符相同**，人眼无法区分；下游若按 `==50` 判中性会误把「未取到」当「已判定为中性」。同 `docs/handover/11_measurement_conventions.md` §1.4 那个病（指标缺值时用 0 当哨兵，而 0 也是合法值）。

🔴 **铁律（可复用）**：**占位哨兵值不得落在合法业务值域内**。
- 要么用值域外哨兵（如 `-1`、`null`、或独立的 `is_placeholder` 布尔列）；
- 要么在**数据层**就把「占位」与「真实中立」显式区分，UI 只在确有数据时渲染数值；
- **绝不允许**「没数据」与「有数据但中立」共用同一个数值——这是静默歧义，比报错更坏（报错会被看见，歧义不会）。

📌 **判定/诊断分离的同款陷阱**：测试若只断言「界面显示 50」无法区分这两种语义；必须断言**数据来源层**的 `is_placeholder` / 取数状态，而非渲染出的数字。`verify-p1-batch` 已点名此条，但**尚未在 12_ 留铁律**——本条补上。





