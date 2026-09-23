# 16 · worker 批次 relevance 判定表（2026-09-23）

> **性质**：本文件是**只读复核产物**，不是整改记录。本轮**未改任何生产代码、未写生产库、未跑线上管线、未派工、未 push**。
> **背景**：上一轮会话派工约 40 个 worker（P0 修复 + 全面评估批次），全部处于 `paused`；其派工基线为 `96a2967`。本文件判定「在 HEAD=`7a5cfe1` 下，这些事项是否仍 relevant」。
> **实测环境**：commit `7a5cfe125f92d7280e712409378ace42f152f7a7`；本机时钟 `2026-09-23 09:19`；Python `venv313`；生产库全程 `file:data/database/portfolio.db?mode=ro` + `uri=True`。
> **判据来源优先级**：① 当轮只读探针实测 > ② `git log` 提交证据 > ③ 代码 `grep` 原文锚 > ④ 报告 `15_` 的 §12 收口记录。
> ⚠️ **引用纪律**：本文件所有行号均为「实测于 `7a5cfe1`」的导航，会漂；**唯一定位手段是可 `grep` 的原文锚**（见 `12_` §17.4）。

---

## 0. 执行摘要

| 判定 | 项数 | 含义 |
|---|---|---|
| ✅ **已收口**（Resume 即重复劳动） | **21** | 有 commit / 代码 / 库实测三类中至少一类正面证据 |
| 🔴 **仍成立**（值得 Resume 或转新待办） | **5** | 当轮实测仍存在 |
| 🟡 **部分收口 / 已裁定** | **4** | 核心已了，残留为有意保留或非缺陷 |
| ⚪ **UNKNOWN** | **6** | 无 assignment 原文，无法判定「当初派的是什么活」 |
| ⚠️ **本轮新发现（不在原批次内）** | **1** | `etf_price_history` 于 `2026-09-22` 出现新的覆盖断崖 |

**一句话结论**：**约 40 个 paused worker 里，绝大多数（21 项）已无事可做**；真正值得动手的是 **3 项已知待办（`restore_db.py` / `scripts/` 直连收敛 / `integration` 打标）+ 1 项本轮新发现的行情断崖**。在拿到 assignment 原文前，不建议整批 Resume。

**推导前提（实测）**：`96a2967` 是 HEAD 的祖先（`git merge-base --is-ancestor` = YES），自派工基线起已推进 **22 个提交**，其中 `126b45b` 单笔就集中收口了 9 项 P0。

---

## 1. 判定表 · 一、修复类 `fix-*`（18 项）

### 1.1 ✅ 已收口（11 项，均有可 `grep` 证据）

| worker | 判定 | 证据（可 `grep` 原文锚 / 提交） | 置信 |
|---|---|---|---|
| `fix-excel-void` | ✅ 已收口 | `src/report/excel_report.py` 现为 `fill = s.VOID_FILL if is_void else ...`（原裸 `VOID_FILL`）；提交 `126b45b` | 高 |
| `fix-bat-exitcode` | ✅ 已收口 | `scheduled_run.bat` 存 `analysis_rc` 作最终退出码；提交 `126b45b`（报告 `15_` §12.1 第 2 项） | 高 |
| `fix-test-connleak` | ✅ 已收口 | `tests/test_split_merge_guard.py` 改 try/finally close；提交 `126b45b` §12.1 第 3 项（14 passed，无遗留 `.db`） | 高 |
| `fix-doc13-narrative` | ✅ 已收口 | `docs/handover/13_issue12_backfill_feasibility.md` 已按「A 族 0 例」重写；`126b45b` 的 stat 含该文件 38 行改动 | 高 |
| `fix-mirror-backups` | ✅ 已收口 | 仓库外 2 份 mirror 经授权永久删除；提交 `126b45b` + `0df0a7b`（报告 `15_` §12.2） | 高 |
| `fix-sentinel-140` | ✅ 已收口 | `tabs/tab8_advice.py` 三列均走显式分支：`if not es.fund_flow_asof:` → `ff = "无数据"`；`rsi_available` / `score_available` / `risk_available` 三标志齐备；提交 `126b45b` + `bf77304` | 高 |
| `fix-backup-cap` | ✅ 已收口 | `scripts/backup_db.py` 新增 `MAX_BACKUP_COUNT`；顶层上限对齐 7（`cb5cd71`） | 高 |
| `fix-wal-busytimeout` | ✅ 已收口 | `data_loader.py` 的 `_configure_sqlite_connection` 内 `PRAGMA busy_timeout=` 与 `PRAGMA journal_mode=WAL` 均在位；提交 `126b45b` §12.1 第 9 项 | 高 |
| `fix-copy-two-tier` | ✅ 已收口 | 两级判据（`COPY_RUN_MIN_LEN` 段长 + 横截面 50% 广度）在盘并已验收（`ba96ce8`）；死常量已删（`c698916`）；见 `12_` §4.3/§4.11 | 高 |
| `fix-labels-leakage` | ✅ 已收口 | 「标签未来函数」修复见于 `5711fc0`（六项正确性缺陷）；`etf_forward_returns_v2` 已标注为留档表（`a04cab2`） | 中高 |
| `fix-conversion-guard` | ✅ 已收口（**本轮新证据**） | 生产库 `etf_features` 实测 **已有 `is_split_merge` 列**且 `IS NULL = 0`、`= 1` 的行 = 1（总 34,532 行）；`portfolio_nav` 同列亦在。→ 报告 `15_` §3 所称「该列在生产表不存在」**已被事实推翻** | 高 |

### 1.2 🟡 部分收口 / 已裁定（4 项）

| worker | 判定 | 说明 | 置信 |
|---|---|---|---|
| `fix-etf-price-gap` | 🟡 **闸门已建、缺口复发** | 闸门 `src/analysis/price_history_gate.py`（`detect_etf_price_gaps`）已上线并接线至阶段 3.25b（`0df0a7b`+`85a915d`）；09-21 曾回填 13 只。**但 09-22 又出现新的 23→1 断崖**（见 §3.1）⇒ 「建闸门」已收口，「缺口不再发生」未收口 | 高 |
| `fix-pipeline-stale` | 🟡 部分 | 相关修复分散在 `43d06ce`（资金流 as-of 显式化）、`b9ce67d`（去掉写死的「滞后至 2026-08-19」）、`5892d89`（`run_status` 新增 + `dq_score` 诚实置 null）。**「stale」具体指哪一个 stale 不明**，需 assignment 才能定论 | 中 |
| `fix-adj-fallback` | 🟡 **已裁定：非缺陷，维持现状** | `fetch_etf_ohlcv_sina` 的 `adj_close` 属休眠路径；已在 `docs/handover/11_measurement_conventions.md` 登记（`#86`，`8c3fd76`）。按 `12_` §14 裁定 (a)，**不修** | 高 |
| `fix-test-dbguard` | 🟡 src 侧已收敛、tests 侧靠兜底 | `src/` 下裸 `sqlite3.connect` 实测**仅剩 2 处**（`snapshot_gate.py`、`price_history_gate.py`），且为**有意保留**（拒收 `file:` URI、自带 `busy_timeout=15000`）；`559fb69` 已收敛 4 个分析模块。tests 侧 8 处硬编码路径仍靠 conftest 运行时改道兜底 | 高 |

### 1.3 ⚪ UNKNOWN（3 项，无 assignment 无法判定）

| worker | 现状证据 | 为什么判不了 |
|---|---|---|
| `fix-rebalance` | `git log` 命中 5 条：`1864589`（periodic 再平衡「上次再平衡日」必须显式传入、缺前提时拒绝而非静默兜底）、`64c3c95`（交易日历退化留痕）、`559fb69`（直连收敛，含 `rebalance_engine.py`）、`5711fc0` | 存在多条再平衡相关修复，但**原 assignment 指的是哪一条（或是否指别的 bug）未知** |
| `fix-pe-source` | `git log -i` 命中 47 条；`5711fc0` 含「PE污染」修复；`2c730e6` 折算闸门覆盖场外；`12_` §10 已记录 `399673` 走 legulegu 补齐 | 「PE 来源」可能指 PE 估值来源切换、也可能指 `etf_pe_backfill.py` 的 csindex 分支无交易日过滤（`12_` §14 债务）。**两者状态相反**，需 assignment 区分 |
| `fix-report-keys` | 无任何提交或代码证据命中 | ⚪ **UNKNOWN**：仓库内检索不到对应问题的任何留痕 |

---

## 2. 判定表 · 二、验证 / 诊断 / 评估 / 分析师类（约 22 项）

| worker | 判定 | 证据 | 置信 |
|---|---|---|---|
| `verify-p1-batch` | ✅ 已收口 | P1 批次本体（数据层直连收敛）已由 `559fb69` 落地：4 个分析模块改用统一连接工厂；`src/` 裸连接由 5 处降至 2 处（均为有意保留） | 高 |
| `diag-otc-coverage` | 🟡 **核心问题已消失，残余为结构性常态** | 实测最近 8 个交易日，场外 12 只在 `portfolio_snapshots` 中**每日均 12/12 有行**（`16.2` 那种「整篮子缺行」形态**未再发生**）。残余：12 只 `09-22` 与 `09-21` 净值**全部同值**（T+1 披露常态，非缺陷）。`027293` 已由 `09-11` 恢复到 `09-18/21/22` | 高 |
| `assess-*` 八维度（架构/测试/数据/运维/文档/产品/安全/性能） | ✅ **已收口** | 产出本体 = `docs/handover/15_project_status_assessment_report.md`（36,578 B / 248 行，8 维 + 9 项 P0 优先级），提交 `1c637ec` 已推送 | 高 |
| `quant` / `valuation` / `risk` / `data` / `product` 五位分析师 | ✅ 产出已并入 `15_`（**中置信**） | `15_` 的八个维度与五类分析师角色一一对应，且 §12 已记录整改收口。**但**：无法排除其各自另有未入库的独立交付物（旧会话目录在 C 盘，本会话不可达）⇒ 若确有独立产物需另行找回 | 中 |

---

## 3. 🔴 本轮新发现（不在原 40 项内）

### 3.1 `etf_price_history` 于 `2026-09-22` 出现新的覆盖断崖（23 → 1）

只读实测（`audit/_worker_relevance_probe2.py` 段 A / A2）：

| 日期 | distinct code 数 | 行数 |
|---|---|---|
| `2026-09-16` ~ `2026-09-21` | **23** | 23 |
| **`2026-09-22`** | **1** | **1** |

**唯一有行情的那一只是 `159732`** —— 即 `12_` §12 明载的**已清仓标的**（末次快照 `2026-07-30`），而它恰恰是 `price_history_gate` 被特意加上「当前持仓口径过滤」后**排除在外**的那一类。

三点判读（按 `12_` §5「不许静默」准则，逐条给性质）：

1. **这是真缺口，不是查询口径问题**：同表前 5 个交易日稳定 23 只，09-22 只有 1 只，且 09-22 快照为 35 行（管线当日在跑）。
2. **闸门是否拦下，本轮未验**：`price_history_gate` 的判据只覆盖「最新快照在册」的 code，而 `159732` 已不在册 ⇒ 09-22 的 22 只在册标的若全部缺行，理论上应触发 error 级。**但 `alerts` 表最近 7 条 error 全部停在 `2026-09-18` 及更早**（最新 `id=128`，`2026-09-18T15:30:57`）⇒ **09-22 没落 error**。⚠️ 这可能是闸门未被调用、也可能被调用但判级不同 —— **本轮未做判级复现，标 UNKNOWN，不臆断**。
3. **与 `fix-etf-price-gap` 的关系**：闸门已建（收口），但缺口复发 ⇒ 该 worker 若 Resume，活儿应是「查 09-22 为何 22 只全缺 + 为什么没告警」，**不是**「再建一次闸门」。

**建议**：列为下轮第一优先（属 `12_` §16 那类「静默失真」的同族 —— 行情缺口会顺着 `etf_features` 污染特征 NULL 率，见 `15_` §3：09-18 曾达 50.9%）。

### 3.2 `is_split_merge` 已落到生产数据 —— 推翻 `15_` §3 的一条陈述

`15_` §3 写「`is_split_merge` 列在生产 `etf_features` 与 `portfolio_nav` **均不存在**」。本轮实测：**两表该列均存在**，且 `etf_features` 34,532 行中 `IS NULL = 0`、`= 1` 者 1 行。⇒ 该陈述已失效，引用 `15_` §3 时须连本条一起引用（**勿把「列已存在」读成「闸门语义已验证」** —— 本轮只验了列与取值分布，未验判据正确性）。

### 3.3 场外篮子「整篮子缺行」已不再发生

最近 8 个交易日场外 12 只覆盖均为 **12/12**（`16.2` 的 09-16 事故形态未复发）；且 `alerts` 表可见 `summary_refused_snapshot_incomplete`（`id=127`，09-18）**拒绝写入残缺 summary** 的机制确已生效 ⇒ `12_` §16.3 要求的修复方向 (b)「拒绝写入并告警」已在盘。

---

## 4. 🔴 仍成立的硬待办（5 项，下轮可直接接手）

| # | 事项 | 当轮实测 | 处置难度 |
|---|---|---|---|
| 1 | **`scripts/restore_db.py` 仍 MISSING**（P0-F） | `os.path.exists('scripts/restore_db.py')` = **False**；`git log --grep=restore_db` **0 命中**；同目录 `backup_db.py` 存在 | 中（新建 + 恢复演练用例） |
| 2 | **`scripts/` 直连未收敛** | 实测 `sqlite3.connect` 共 **24 处 / 19 个文件**（最多：`fetch_otc_fund_nav.py` 3、`fetch_market_data.py` 3、`verify_gate_on_refreshed.py` 2） | 中（多为一次性脚本/探针，需逐个判定是否值得收敛） |
| 3 | **`@pytest.mark.integration` 零打标** | `tests/` 内 `pytest.mark.integration` 命中 **0**；而 `.github/workflows/ci.yml` 第 33 行仍是 `pytest tests/ -v --tb=short --timeout=120 -m "not integration"` ⇒ **deselect 仍是空操作** | 低→中（需先界定哪些用例属 integration） |
| 4 | **`2026-09-22` 行情断崖**（§3.1 新发现） | 23 → 1，且未落 error | 中（先查「为何缺 + 为何没告警」，再谈回填） |
| 5 | **`snapshot_gate` / `price_history_gate` 两处直连待定** | `src/` 仅剩这 2 处裸 `sqlite3.connect`，均为有意保留（拒收 `file:` URI + `busy_timeout=15000`） | 待丹哥拍板：保留 / 收敛 / 加注释固化 |

> 附带已确认**不需要**再做的：`integration` marker 本身已在 `pytest.ini` 声明（`integration: marks tests as integration tests`）；CI 的 Python 已对齐 **3.13**（`e7b4334`）。

---

## 5. ⚪ 无法判定项汇总（需丹哥提供 assignment 原文）

| worker | 需澄清的问题 |
|---|---|
| `fix-rebalance` | 指 periodic 再平衡的 `last_rebalance_date` 缺失？还是 `rebalance_engine.py` 的直连？还是别的？ |
| `fix-pe-source` | 指 PE 估值数据源切换（`399673` legulegu）？还是 `etf_pe_backfill.py` 的 csindex 分支无交易日过滤？ |
| `fix-report-keys` | 无任何留痕，需原文 |
| `fix-pipeline-stale` | 指 stale 守卫重生报告？还是资金流/净值 as-of 陈旧标注？ |
| 五位分析师 | 是否存在**未入库的独立交付物**（旧会话目录）？ |

---

## 6. 复核工具与留档（只读，均在 `audit/`，不入库）

| 文件 | 作用 |
|---|---|
| `audit/_worker_relevance_probe.py` | 第一轮：列存在性、快照行数基线、场外覆盖、行情 code 数、`fund_flows` 场外覆盖、最近 summary |
| `audit/_worker_relevance_probe2.py` | 第二轮：09-22 断崖明细、`is_split_merge` 取值分布、`alerts` 真实 schema 与最近 error、场外同值粗检 |

运行方式：`venv313/Scripts/python.exe audit/_worker_relevance_probe.py`（全程 `mode=ro`）。

⚠️ 已知探针缺陷（留痕，勿当结论）：第一轮探针用 `alert_type` 列名查 `alerts`，**该列不存在**（真实 schema = `id / rule_name / level / message / created_at / acknowledged`），故第一轮第 7 段报错；第二轮已修正并取到 7 条 error。**第一轮该段数字不可用**。

---

## 7. 下轮开工建议（按性价比排序）

1. **§3.1 的 09-22 行情断崖**：先定位「22 只为何全缺 + 闸门为何没落 error」。这是唯一在**扩大中**的静默缺口。
2. **`scripts/restore_db.py`**：P0-F，挂了多轮，改动可控。
3. **`integration` 打标**：CI 的 deselect 目前是空操作，属「有闸门但没接线」——与 `12_` §17.11 教训 1 同型。
4. 其余（`scripts/` 直连收敛、两处 gate 直连拍板）属债务清理，可并入维护轮。

**不建议**整批 Resume 40 个 worker：21 项已收口，整批重派的主要产出将是「已无需修复」的回报。
