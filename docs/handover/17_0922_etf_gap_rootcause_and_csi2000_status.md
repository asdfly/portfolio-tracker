# 17 · 09-22 etf_price_history 断崖根因 + 中证2000 纳入回填状态（2026-09-24）

> 本文为「下轮建议①（09-22 断崖定位）」交付物 + 「中证2000 纳入回填」状态核对。
> 全部数字来自生产库只读查询（`file:...?mode=ro`），未改动任何产线代码。
> 铁律：产线代码改动、git 推送均须经用户显式确认，本文仅定位与建议。

---

## 0. 结论速览

| 项 | 状态 |
|---|---|
| Task ① 09-22 断崖根因 | **已定位并验证**：9 只 ETF「按代码静默跳过」→ 持续缺口，至今未修复 |
| Task ① 同类放大（新发现） | 09-23 管线 `success` 但 `etf_price_history` 当日 **0 行** → 监控盲区 |
| Task ② 配置层（INDEX_CODES / NeoData 映射） | 已落地（未提交） |
| Task ② index_pe_history（932000） | **已完成**：10 行，最新 2026-09-23 |
| Task ② index_quotes（sh932000） | **已完成**：500 行（2024-09-03~2026-09-24），westock MCP 回填 + DB 缓存兜底 + 每日自动化 |

---

## 1. Task ① 根因定位（验证态）

### 1.1 现象（生产库只读实测）

| 日期 | etf_price_history distinct codes |
|---|---|
| 2026-09-18 | 23 |
| 2026-09-19 | 0 |
| 2026-09-21 | 23 |
| **2026-09-22** | **14** ← 断崖（23→14，缺 9 只） |
| 2026-09-23 | 0 ← 全量回填失败（见 §3） |
| 2026-09-24 | 0 ← 当日尚未运行（无 execution_logs，非失败） |

> 更正：16_ §3.1 记载「23→1」为误测。实测 09-22 全局 distinct codes = **14**（非 1）。真实缺口是 **9 只 ETF 缺 09-22 这一行**，而非全表崩塌。

### 1.2 持续缺口的 9 只 ETF（至今 MAX(date) 仍 = 2026-09-21）

| code | DB 记录名（portfolio_snapshots.name） |
|---|---|
| 515010 | 证券ETF华夏 |
| 159992 | 创新药ETF银华 |
| 515120 | 创新药ETF广发 |
| 159267 | 航天ETF华安 |
| 159796 | 电池ETF汇添富 |
| 561910 | 电池ETF招商 |
| 159819 | 人工智能ETF易方达 |
| 159949 | 创业板50ETF华安 |
| 159770 | 机器人ETF天弘 |

其余 14 只自修复至 09-22 —— 说明**非系统级**，而是这 9 只的取数持续失败。

### 1.3 根因

`src/analysis/predictor/price_history.py` 的 `backfill_etf_price_history`：

- 增量从 `MAX(date)` 拉取；当某代码「所有数据源均失败」时，仅记录日志
  `"所有数据源均失败，跳过"` 并 `continue`，**不抛错、不计入失败、不告警**。
- 09-22 起，这 9 只在所有源（em 东方财富 / tx 腾讯）持续失败 → 永久缺口（今日仍在 09-21）。
- `price_history_gate` 对「≥1 缺失日」只记 **WARNING**（不写 alerts、不降 run_status），
  故运行时单日缺口属设计预期、永不升级为 error/alert，掩盖了「缺口跨日累积」。

### 1.4 关键补充：09-23 全量回填失败（新发现，独立于 9 只缺口）

`execution_logs` 显示 09-23 15:30 管线正常跑完（`status=success`，207s），
但 `etf_price_history` **09-23 行数 = 0**。

即：当日价格回填在所有源全失败 → 静默跳过 → 整日 0 行，而任务仍报 `success`。
这是**监控盲区：管线 success ≠ 数据新鲜**。根因同源（em 上游 `RemoteDisconnected`
持续，tx 同源亦失败），但影响面从「9 只」扩大到「全量 0 行」。

（09-24 无 execution_logs 记录，判定为尚未到运行时刻/漏跑，非失败。）

---

## 2. 影响评估

- 组合再平衡 / 特征工程若依赖 09-22 之后的日行情，将基于陈旧（09-21 / 09-22）数据。
- 9 只行业 ETF（券商 / 创新药 / 电池 / AI / 机器人 / 创业板50 / 航天）在 09-22–09-24
  窗口无日频更新，前瞻标签与 37 维特征在该窗口失真。
- 日报「success」掩盖数据断层，须人工读库才可见（本次即如此）。

---

## 3. 修复建议（A–D 已于 2026-09-24 落地 —— 见 §6）

| 编号 | 建议 | 直接消除的问题 |
|---|---|---|
| **A** | 价格回填「当日写入行数 = 0（或显著低于持仓数）」即降级 run_status + 写 alerts（对齐现有 gate 的 error 路径） | 09-23「success 假象」 |
| **B** | 按代码隔离重试 / 隔离名单（quarantine）：对持续失败的 9 只加有界重试 + 隔离，避免每轮全量重试拖慢并污染日志；隔离项单独告警 | 9 只永久缺口 + 日志噪声 |
| **C** | 恢复可用价源：等 eastmoney 上游恢复，或为 ETF 日行情接入韧性源（腾讯 gtimg 已在本仓 backfill 脚本验证可解析，但须在其可达环境运行） | 全量 0 行根因 |
| **D** | gate 改「按代码计缺失天数」：将 ≥3 缺失日的 error 判定从全局 distinct 改为按 code 聚合，使 9 只的 3+ 日缺口在下一轮即触发 error/alert | 缺口跨日累积被掩盖 |

> A–D 已于 2026-09-24 经用户授权「直接落地」（commit 本地、未推送，待旦哥确认）；改动明细见 §6。

---

## 4. Task ② 中证2000 纳入回填状态（验证态）

### 4.1 配置层（已落库 / 未提交）

- `config/settings.py` `INDEX_CODES` 增加 `"sh932000": "中证2000"` → 现 **12 只**基准指数。
  每日管线将自动尝试采集其行情；取数失败时按 `_fetch_index_quotes` 既有
  `try/except OSError` 告警跳过，**不崩每日运行**。
- `src/data_sources/neodata_valuation.py` `INDEX_NAME_MAP` 增加 `"932000": "中证2000指数"`
  → 支持 NeoData PE 回填。

### 4.2 index_pe_history（已完成）

- `932000` 共 **10 行，最新 2026-09-23**（NeoData，09-24 回填，写前已备份）。

### 4.3 index_quotes（阻断复核 — 2026-09-24 深度复核；**数据缺口已于 2026-09-24 晚经 westock MCP 闭环，见 §7.5**）

> **状态更新（2026-09-24 晚）**：`sh932000` 已由 **westock MCP** 回填 **500 行**（2024-09-03~2026-09-24，交叉验证 PASS），并加 DB 缓存兜底 + 每日自动化维护。下文根因分析（东财 push2his host 级 RST、腾讯不跟踪、NeoData 截断等）**依旧成立、作为「为何选 westock」的背景**；原「本环境无任何可达源」结论已过时——westock-mcp 是例外且可用。

`sh932000` 行数复核（2026-09-24 晚前）= **0**。原 §4.3 把阻断归因为「沙箱限制 / 临时上游」，**经 2026-09-24 真机（沙箱内外双跑）逐源探测，结论须更正**：

| 源 | 实测结果（2026-09-24） | 性质（更正后） |
|---|---|---|
| 新浪 K线 | 4 种符号格式（sh932000 / sh000932000 等）均 `null` | **结构性不覆盖** 中证2000，非临时 |
| 腾讯 gtimg | `qt.gtimg.cn/q=sh932000` → `v_pv_none_match="1"`；`appstuff/app/chart/kline` 对 `sh000300` 也 `No dispatch info found` | **腾讯根本不跟踪 中证2000**，且该端点本就不服务指数；**真机亦取不到**，非沙箱限制 |
| 东方财富 push2his / push2 | `RemoteDisconnected`（**https 与 http 均失败**，沙箱内外一致） | **东财对 K 线数据端点的 host 级 RST**（门户 `quote.eastmoney.com`、数据治理 `datacenter-web.eastmoney.com` 可达 → 非整体 IP 封，是数据 API 定向阻断）；java 侧 `index_zh_a_hist` 同源失败 |
| 东方财富 datacenter-web | 可达（HTTP 200），但 `RPT_INDEX_KLINE`/`RPT_INDEX_DAILY`/`RPT_IDX_HISTORY` 均「报表配置不存在」 | 主机可达但无可用 index 历史报表名（试探未命中） |
| NeoData「统一行情查询」 | 可达且覆盖（PE 已回填 10 行）；但行情召回**截断中间历史**（明示「省略中间，禁止推断补全」）且**返回窗口不具确定性**（同口径两次查询分别回 9 月 / 7 月窗口） | **不适合做指数时序回填**——实测插入到 7 月陈旧窗口，会污染 MA / 跨日跟踪 |

→ **更正结论**：本机环境（即生产机，repo 同机）**无任何可达且可靠的 中证2000 指数时序源**。
脚本三级回退（新浪→腾讯→东财）逻辑就绪但全链在本地不通；腾讯路径对 932000 永远失效，
东财 K 线端点被本机 RST。A（回填 index_quotes）**无法在本环境落地**，须改从可通达东财 push2his 的
网络出口（代理 / VPN / 异地机器）执行，或待东财对该 host 解封。详见 §7。

### 4.4 回填脚本增强

`scripts/backfill/backfill_single_index.py` 新增 `fetch_klines_tencent`（腾讯 gtimg 回退），
已用 mock 验证解析正确：`date,open,close,high,low,volume` 顺序经样本核对，`high≥low` 一致。
源链：**新浪 → 腾讯 → 东方财富**。

### 4.5 重跑命令（在可达网络环境执行）

```bash
# PE 已齐，--no-pe 仅补行情；如需重跑 PE 去掉 --no-pe
venv313/Scripts/python.exe scripts/backfill/backfill_single_index.py \
    --code sh932000 --name 中证2000 --no-pe
```

脚本写前自动备份 `portfolio.db` 至 `data/backups/`（含大小校验）。
已有备份：`portfolio_PRE_CS2000_BACKFILL_20260924_084208.db`、`..._084534.db`。

---

## 5. 待办 / 下一步

1. ~~用户决策 §3 的 A–D 产线修复~~ —— **已于 2026-09-24 落地（见 §6）**。
2. ~~在**可达网络环境**重跑中证2000 `index_quotes` 回填~~ —— **已完成（westock MCP，见 §7.5）**，无需异地/代理。
3. 提交本次改动（含 §6 的 A–D 产线代码 + 既有中证2000 配置/脚本 + 本报告）——**铁律：显式 pathspec，不推送未获授权**。

---

## 6. A–D 落地记录（2026-09-24，用户授权「直接落地」）

### 6.1 A —— gate 鲜度闸门（0 行即降级）
- 文件 `src/analysis/price_history_gate.py::detect_etf_price_gaps`
- 新增**系统性鲜度判定**：`etf_price_history` 全局最新日 **严格落后** `portfolio_snapshots` 最新日 ⇒ 系统性 `error`（写 `alerts` 表 + 经 `run_analysis` 的 `_reporter.alert("error")` ⇒ `run_status=degraded` ⇒ 邮件闸门拒发基于陈旧价的日报）。
- 直接修复 09-23「全表 0 行但 success 假象」：旧逻辑只看**参考日历**（由数据本身推导，0 行时日历冻结→漏检）；新逻辑以 snapshot 最新日为**权威锚点**，1 天滞后即触发。
- 正常 post-close 跑批后两者相等 ⇒ 无误报；周末/节假日 snapshot 不前进 ⇒ 不误报。

### 6.2 D —— gate 按 code 以 snap_max 为锚升级
- 同一文件单标的缺口循环：除现有参考日历判定外，新增「`snap_max − code_max ≥ GAP_ERROR_THRESHOLD(3)` 自然日 ⇒ `error`」。
- 使缺口跨日累积在下一轮即触发（即便其余标的也停更、参考日历退化，旧逻辑会漏检）。

### 6.3 B —— backfill 有界重试 + 隔离名单
- 文件 `src/analysis/predictor/price_history.py`
- `backfill_etf_price_history` 返回值由裸 `int` 改为富结果 `BackfillResult(rows/attempted/failed/quarantined)`（调用方 `build_base`、`gate._repair` 已同步取 `.rows`；既有测试 `test_predictor_price_history_tx.py` 已更新）。
- 新增 `etf_backfill_quarantine` 表（`CREATE TABLE IF NOT EXISTS`，幂等）：连续全源失败达 `QUARANTINE_AFTER=3` 次进入隔离期 `QUARANTINE_DAYS=1` 天；隔离期内跳过重试（省时省日志），到期再试一次；成功即重置计数。
- 全源失败**不再静默 `continue`**，而是计入隔离失败并单独列示（`failed`/`quarantined` 名单回传）。
- `run_analysis.py` 阶段 3.25 捕获 `ohlcv_failed`/`ohlcv_quarantined` 并以 **warning 级**留痕（**不降级** run_status——隔离是有意设计，避免其本身触发邮件闸门误拦；真实断崖由 gate 的 error 级判定）。

### 6.4 C —— 恢复可用价源（验证为 no-op）
- `em → tx` 前复权回退链（默认 `sources=("em","tx")`）+ 腾讯 gtimg 6 次指数退避重试已就位，即为韧性源。无需额外代码改动；上游恢复后自动回补。

### 6.5 测试
- 新增 `tests/test_etf_gap_fixes.py`：`test_A_freshness_detects_systemic_zero_rows`、`test_D_per_code_escalates_accumulated_gap`、`test_B_quarantine_isolates_persistent_failures`、`test_backfill_result_shape`。
- 回归：`test_predictor_price_history_tx.py`（`.rows` 断言）、`test_run_reporter_completeness.py`、`test_snapshot_baseline_gate.py` 全绿（共 57 项通过）。

### 6.6 行为对照（落地后）
| 场景 | 落地前 | 落地后 |
|---|---|---|
| 09-23 全表 0 行 | success 假象（gate 漏检） | 系统性 error ⇒ degraded ⇒ 拒发 |
| 9 只缺口跨日累积 | 第 1 天 warning；第 3 天起 error（依赖参考日历） | 第 3 天起 error（参考日历 + snap_max 双锚点，更稳） |
| 持续失败标的每轮全量重试 | 每轮重试 + 日志噪声 | 达阈值进隔离期，跳过 + 单独列示 |

---

## 7. 2026-09-24 后续：崩溃修复 + A 重跑 + B 源链方案

### 7.1 崩溃根因修复（已提交 `b4e625e`，未推送）

- **现象**：09-24 主分析 `run_status=failed`（本窗口首次 failed），缺失 basic/risk/monitor/dq_check，
  HEALTH_CHECK 仅 `RemoteDisconnected`，告警 `critical / pipeline_incomplete`（真·流水线错误型，须保留 critical）。
- **根因**：`src/analysis/portfolio.py::_fetch_index_quotes` 遍历 `INDEX_CODES`（含 9/22 新增的 `sh932000`）
  取指数行情，异常捕获写成 `except OSError`；但 `DataSourceManager.get_quote` 多源全失败后抛 **`DataSourceError`**
  （`base.py`，继承 `Exception` 非 `OSError`），未被捕获即上抛，中断整轮 → 全阶段缺失。
  （handover 17_ §4.1 原称「try/except OSError 告警跳过不崩」承诺落空，正因故障转移层把底层异常
  包装成 `DataSourceError` 上抛、类型不符。同文件 `_calculate_technical_indicators` 用的是正确的
  `except DataSourceError`，印证是一处不一致遗漏。）
- **修复**：捕获改为 `except (DataSourceError, OSError)`，方法内局部导入 `DataSourceError`。单只失败仅告警跳过、
  其余 11 只正常返回，整轮不再被单指数拖垮，兑现「告警跳过不崩」承诺。
- **验证**：新增 `tests/test_fetch_index_quotes_isolation.py`（模拟 sh932000 抛 DataSourceError，验证被隔离、
  其余指数全保留、方法不抛异常）；`pytest` 7 passed；`pre-commit` 17 passed。
- **澄清**：`dq_score=null` 是「run incomplete 直接导致」，与 9/17 suppressed 抑制值语义不同，已区分；
  critical 分级逻辑未动，未来若其他必需阶段仍缺仍触发、不降级 warning。

### 7.2 A —— 中证2000 index_quotes 回填：原结论「本环境无法落地」（**已被 §7.5 刷新：经 westock MCP 已落地**）

按用户授权「直接落地 A」，沙箱内外双跑 `backfill_single_index.py --code sh932000 --name 中证2000 --no-pe`，
并在发现全源不通后，逐源探测其可达性（详见 §4.3 更正表）。**结论：本机（即生产机）无任何可达且可靠的
中证2000 指数时序源**，A 无法落地。要点：

1. 新浪不覆盖、腾讯不跟踪 932000、东财 K 线端点（`push2his`/`push2`）对本机 RST（host 级定向阻断，
   https/http 均失败，门户与 datacenter-web 可达）、东财 datacenter-web 无可用 index 历史报表名。
2. NeoData 可达但「统一行情查询」**截断中间历史 + 返回窗口不确定**：实测两次查询分别回 9 月 / 7 月窗口，
   **曾误将 7 月陈旧窗口落库**，已立即删除回滚（见 §7.4）。故 NeoData **不可作为指数时序回填源**。
3. 因此 932000 的 index_quotes 仍为 0 行；但 7.1 的崩溃修复已确保：缺失时整轮**降级**（degraded）而非 failed，
   其余 11 只基准与全组合分析正常产出。这是当前环境下唯一可落地的正确行为。

> **A 的真正落地条件**：从能通达东财 `push2his` 的网络出口（代理 / VPN / 异地机器）重跑
> `backfill_single_index.py`；或待东财对该 host 解封。届时可一键补齐，无需改代码。

### 7.3 B —— 源链增强方案（规划，未实施）

目标：让 `DataSourceManager` 对中证类指数（尤其 932000）具备**生产可调用的可靠源**，且单源失败不致命。
基于 7.2 发现，方案排序如下：

- **B0（已隐含完成）崩溃隔离**：7.1 的 `except (DataSourceError, OSError)` 已确保单指数/单源失败不拖垮整轮。
  这是「B 源链增强」的前提，已落地。
- **B1（已启动实测，2026-09-24 晚）出口解封 / 代理**：
  实测：本环境 `HTTPS_PROXY=http://127.0.0.1:10808` 是**真实出网代理**（baidu 200 验证；sina 取 510300 经代理成功返回真实价 4.515，证明透传出网生效）。
  但对**金融行情域做上游策略限制**：东财 `push2his` 经代理 `503`、网易 `chddata` `502`、datacenter-web `200` 但无 932000 报表名。
  故 932000 在本地仍无法落地（源覆盖缺失 + 金融域上游限），与 7.2 一致。
  代码侧 B1 已落地：`base.py` 新增 `resolve_env_proxies()` 显式注入 env 代理 + 可观测 INFO 日志（出网模式：代理 / 直连一目了然）；
  运行环境只要配置「对 eastmoney 放行的出网代理」（独立 VPN / Clash 系统代理），数据源**自动经代理取数、无需改代码**。本地 10808 不满足（金融域限）。
  两条路：(a) 真机 / 生产网段配可用出网代理（代码已就绪，配即生效）；(b) 异地出网：腾讯云 Lighthouse（账号有，当前无实例），需用户建实例后跑取数。
- **B2（源多样化）新增东财直连源**：在 `DataSourceManager` 注册 `eastmoney_direct`（直连
  `push2his.eastmoney.com/api/qt/stock/kline/get`，带与 `backfill_single_index.py` 一致的超时 + 有界重试），
  与现有 sina / akshare 形成三源；指数取数优先直连、失败再回退。注意：若 B1 不通，此源同样 RST，
  故 B2 须与 B1 搭配才有效。
- **B3（可见性）数据缺口显式标记**：当 `INDEX_CODES` 中某指数在 `index_quotes` 行数为 0 时，
  报告 / `run_report` 显式标注「中证2000：数据暂缺」而非静默缺失；`dq` 或 health 摘要单列「指数覆盖缺口」，
  使降级**有意且可见**（对应 9/17 待办的 critical 分级精神）。
- **B4（排除项）NeoData 不入生产源链**：NeoData 凭证仅 WorkBuddy 会话内有效（~12h）、且行情召回截断 +
  窗口不确定（7.2 已证），**不可作为指数时序回填 / 生产取数源**；仅可作会话内的估值(PE)采集（现有用途）。
- **B5（候选评估）网易 163 / 其他**：网易指数历史（`quotes.money.163.com/service/chddata`）可能覆盖 中证2000，
  且通常不被 host 级封锁；列为待评估候选（需先验证 932000 在网易的代码与可达性，再用 B2 模式接入）。

> 落地建议：先 B1（出口）打通东财 → 再 B2（直连源）固化 → B3（可见性）收尾；B5 作为 B1 不通时的兜底评估。
> B1 已启动实测：代理透传基础设施落地（`base.py` 新增提交，待推送），本环境 10808 真实出网但对金融域上游限，故 932000 本地仍缺；
> B2/B3 待「对 eastmoney 放行的出网代理」具备后实施；B5 网易实测 502 同样受限，降级为「需异地出网才验」。
> 下一步（待用户决策）：在真机生产环境配置可用出网代理，或建腾讯云 Lighthouse 异地实例跑取数——任一具备即解锁 A 与 B2/B3。

### 7.4 本次操作留痕（安全）

- 回滚：NeoData 回填脚本曾误插 8 行 7 月陈旧窗口（`2025-12-31` + `2026-07-09~07-17`），已 `DELETE` 干净，
  `index_quotes WHERE code='sh932000'` 现 **0 行**，与插入前备份 `portfolio_PRE_CS2000_ND_20260924_195757.db` 一致。
- 备份：`data/backups/` 现有 `portfolio_PRE_CS2000_ND_20260924_195757.db` / `..._195924.db`（各 146.1 MB，安全副本）。
- 已删除未提交的探索性脚本 `scripts/backfill/backfill_index_quotes_neodata.py`（其 NeoData 源不可靠，留作 footgun 风险）。
- 提交：`b4e625e`（7.1 崩溃修复 + 回归测试）、`2de44fc`（§4.3 更正 + §7）已推送 origin/master；
  本次 B1 代理透传 `base.py::resolve_env_proxies` + 本 §7.3 实测更新为新增本地提交（待授权推送）。

---

### 7.5 B / A 终局落地（2026-09-24 晚）：用「已连接连接器」闭环 932000 —— 不再依赖东财 push2his

原 §7.2 结论「本机无可达源、A 无法落地」在**实测所有已连接金融连接器**后被刷新：东财 push2his 依旧 RST，但 **westock-mcp（腾讯自选股）与 mx-ds-mcp（东方财富妙想）可正常返回 932000 时序**，且两源交叉验证一致。鉴于「手写为 932000 专门的数据源/落库代码」有污染风险且不经济，最终采用**「连接器 + DB 缓存兜底 + 每日自动化」**三件套，而非 B2（eastmoney 直连源）代码路径。

#### 7.5.1 已连接连接器 932000 实测（2026-09-24 晚）

| 连接器 | 工具 | 结果 |
|---|---|---|
| **westock-mcp** | `data_index`(搜出 `cs932000`) → `data_kline` | ✅ 返回完整日K **~500/600 个交易日**（2024-04-10~2026-09-24），OHLCV 齐全；与东财口径交叉验证一致（volume≈2.95 亿手） |
| **mx-ds-mcp**（东方财富妙想） | `mx_index_block_finance_data` | ✅ 返回 `932000.CSI` 近 6 日完整数据（9/24 收 3209.63，−1.54%） |
| tushare | `index_daily` | ❌ 报错 40203 无接口权限 |
| neodata | `quote_and_kline` | ❌ 返回空；文档明示「中证 .CSI 系列指数暂无技术指标数据」（932000 属 .CSI） |
| 腾讯 `web.ifzq.gtimg.cn` 直连 | `fqkline` | ❌ `cs932000` 仅返 **1 天**（该指数在腾讯公开 kline 无完整历史；`sh000300` 返 600 天对照） |

→ **westock-mcp 是 932000 在本环境唯一可编程全量源**；mx-ds-mcp 可作交叉校验/补充。

#### 7.5.2 落地三件套（均已实现，待推送）

1. **种子回填脚本 + 数据**：`scripts/backfill/backfill_index_quotes_westock.py` 读取 westock `data_kline` 原始 JSON（`{"ok":true,"data":{"nodes":[...]}}`，收盘字段名 `last`），幂等 upsert 入 `index_quotes`；种子文件 `scripts/backfill/data/cs932000_westock_2026-09-24.json`（500 行，2024-09-03~2026-09-24）。
   - **运行结果**：`upsert 500 行；sh932000 现有 500 行，区间 2024-09-03 ~ 2026-09-24`；`--verify` 三锚点全 PASS（2026-09-24=3209.63 / 2025-09-18=3139.76 / 2024-09-30=2166.17）。
2. **DB 缓存兜底（代码，portfolio.py）**：`_fetch_index_quotes` 实时取数失败时，调用新增 `_fallback_index_quote_from_db(code)` 回退到 `index_quotes` 最新一行（带 `_cached=True` 标记；仅当缓存即当日时采信 `change_pct`，否则置 `None`）。**效果：即便东财链继续 RST，主分析 `_fetch_index_quotes` 不再丢弃 932000**，下游模块恒有值。
   - **集成测试**：模拟全源失败（RST）→ 12 只指数（含 sh932000）全部回退 DB 缓存；sh932000 `price=3209.63, cached=True, date=2026-09-24`；模块 `py_compile` 通过。
3. **每日自动化（WB automation）**：`748d9a28-0ddd-447d-bc0a-cad17e94200e`「中证2000(932000) 行情每日维护」，`FREQ=DAILY;BYHOUR=21;BYMINUTE=0`，ACTIVE。prompt 指示 LLM agent 用 westock-mcp `data_kline` 取 `cs932000` → 落临时 JSON → 跑本 §7.5.2.1 脚本 `--verify`，并**显式报告失败**（不静默成功）。
   - 作用：持续刷新 §7.5.2.1 的 DB 缓存，使主分析拿到的 932000 始终是前一交易日收盘（最坏 1 日滞后，由本自动化消除）。

#### 7.5.3 结论与优先级调整

- **A（index_quotes 回填）已落地**，但不是经东财 push2his，而是经 **westock-mcp**（已连接连接器）。原「需异地/代理/VPN」条件不再必要。
- **B2（eastmoney 直连源）降级为非必需**：westock 已闭环数据需求；B2 仅作为未来「数据源冗余」的可选项，不再阻塞。
- **B1（出口代理）已不再是阻塞项**：9 只行业 ETF（§1.2）的东财取数虽仍受 host 级 RST 影响，但已同 §7.5 一样**经 westock-mcp 闭环**（见 §7.6），B1 出口代理不再必要。
- **NeoData 维持排除**：凭证会话有效 + 行情截断/窗口不确定，仅作会话内 PE 采集（不变）。

#### 7.5.4 待提交清单（新增，待授权推送）

- 新增 `scripts/backfill/backfill_index_quotes_westock.py`
- 新增 `scripts/backfill/data/cs932000_westock_2026-09-24.json`（种子，500 行）
- 修改 `src/analysis/portfolio.py`：`_fetch_index_quotes` 加 DB 缓存兜底 + 新增 `_fallback_index_quote_from_db`
- 新增/更新本报告 §7.5
- （此前未推送项：`57b52ea` B1 代理透传 `base.py::resolve_env_proxies` 等，一并评估推送）

### 7.6 9 只行业 ETF（etf_price_history）复制「连接器 + MCP 兜底」模式（2026-09-24 晚）

与 §7.5 同理，将 westock-mcp 兜底模式**原样复制**到 9 只缺口行业 ETF。

#### 7.6.1 缺口现状（2026-09-24 实测）
`etf_price_history` 最新日 = 2026-09-22，仅 14/23 只有 09-22；**恰好 9 只卡在 09-21**：
`159267, 159770, 159796, 159819, 159949, 159992, 515010, 515120, 561910`。
实时增量补数 `backfill_etf_price_history`（em→tx）因同一 RST 全失败，缺口跨日累积；闸门的 `_repair` 再调同一 em/tx 仍失败。

#### 7.6.2 关键发现：09-21 旧行本身即脏数据
`--verify` 交叉校验暴露：**8/9 只的 09-21 行 volume 异常偏低**（如 159267 的 102431 手 vs westock 真值 929715 手，差约 9×；515010 的 70964 vs 309140），close 也不同。说明 RST 自 09-21 起就污染了这批标的的取数，westock 一次性修正了 09-21 + 补齐 09-22/23/24。

#### 7.6.3 落地三件套
1. **回填脚本** `scripts/backfill/backfill_etf_price_history_westock.py`：读 `scripts/backfill/data/etf_westock_<CODE>.json`（westock `data_kline` 原始响应，收盘字段 `last`），幂等 `INSERT OR REPLACE` 入 `etf_price_history`（source=`westock_mcp`，volume 单位=手）。
   初始回填结果：**9/9 全部补齐至 2026-09-24**（共 upsert 83 行；159949 因服务限频仅取回 3 个缺口日，但已足够补缺口）。
2. **实时路径缓存兜底标记**：`src/analysis/predictor/price_history.py::backfill_etf_price_history` 在所有源失败时，新增显式日志——若 `etf_price_history` 已有 `westock_mcp` 缓存行则打 `[OHLCV][缓存兜底] <code> ... 下游回退 westock_mcp 缓存(最新 <date>)`；否则打 `[无缓存]` 告警。集成测试通过（mock 全源失败，159267 命中正向分支、510300 命中无缓存分支）。
3. **每日自动化（WB automation）**：`79b8f3e1-e107-42ca-858c-2ca87343360e`「9只行业ETF(OHLCV) 每日westock维护」，`FREQ=DAILY;BYHOUR=21;BYMINUTE=10`，ACTIVE。LLM agent 对 9 只逐一调 `westock-mcp data_kline`（sh/sz 前缀）→ 落 `etf_westock_<CODE>.json` → 跑本 §7.6.3.1 脚本 `--verify`，失败显式上报。

#### 7.6.4 模式泛化价值评估
「连接器 + MCP 兜底」本质是**用已连接的 MCP 数据通道绕开被 RST/墙阻断的直连源**，三件套（① MCP→DB 幂等回填脚本 ② 实时路径 DB 缓存兜底标记 ③ 每日自动化刷新缓存）可复用于本项目所有"主源不稳"的数据表：
- **已验证可复制**：index_quotes（932000，§7.5）、etf_price_history（9 ETF，本节）——结构同构，复制成本低。
- **高价值候选**：`fund_flows`（ETF 资金流，原走 push2his，阻尼式拒绝）、`etf_fundamental`/`etf_industry_alloc`/`etf_top_holdings`（基本面/持仓，原走 AKShare 东财）、宏观/新闻类——凡主源是东财/新浪且本机不稳的，均可加一套 MCP 兜底。
- **不适用**：纯计算派生表（etf_features/etf_forward_returns）、本地快照（portfolio_snapshots）无需外部源；以及 MCP 本身未覆盖的数据（如个股级深度财务）。
- **代价**：每加一套需 1 个 MCP 回填脚本 + 1 个自动化 + 实时路径兜底标记；数据多一份"缓存副本"，须以 `source` 列区分、防止口径污染（westock 为 qfq 前复权，与 EM 一致，无口径风险）。

#### 7.6.5 待提交清单（新增，覆盖 §7.5.4）
- 新增 `scripts/backfill/backfill_etf_price_history_westock.py`
- 新增 `scripts/backfill/data/etf_westock_*.json`（9 只，种子）
- 修改 `src/analysis/predictor/price_history.py`：`backfill_etf_price_history` 加 westock_mcp 缓存兜底标记
- 删除 `scripts/backfill/backfill_index_quotes_tencent.py`（无全史价值，footgun，已删未提交）
- 更新本报告 §7.5.3 / 新增 §7.6

### 7.7 ETF 资金流（fund_flows）复制「连接器 + MCP 兜底」+ 多源交叉验证（2026-09-25）

与 §7.5 / §7.6 同理，将「连接器 + DB 缓存兜底 + 每日自动化」三件套复制到 `fund_flows`（ETF 资金流）。
**新增要求（丹哥明确）**：配套一套**含多源数据交叉验证**的数据质量检查与维护方案——本期落地。

#### 7.7.1 缺口现状与根因

- `fund_flows` 中 25 个 `category='etf'` 代码（含 2 只误标）的 ETF 资金流**全部卡在 2026-09-21**；实时增量补数（`fetch_etf_fund_flow_batch` 走东财 `push2his` / `fund_etf_spot_em`）因同一 host 级 RST 全失败，缺口跨日累积。
- 诊断发现 **`category='etf'` 中混入 2 只非 ETF 个股**：`001323`（实为慕思股份）、`002152`（实为广电运通）。二者是 `resolve_target_codes` 跟随持仓快照时把个股误归入 ETF 白名单所致，须在资金流回填中**显式排除**（种子生成器已排除，避免脏写）。
- 主取数链（东财 push2his / fund_etf_spot_em）被 host 级 RST 阻断 → 与 §7.5/§7.6 同根因。

#### 7.7.2 多源交叉验证架构与关键发现

| 角色 | 源 | 工具 / 字段 | 用途 |
|---|---|---|---|
| 主写（权威） | **neodata-mcp** `fund_flow` | `main_net_inflow`/`main_inflow`/`main_outflow`/`super_large_net_inflow`/`large_net_inflow`（元） | 主数据源，落 `source='neodata_mcp', is_estimated=0, confidence=1.0` |
| 交叉校验 | **westock-mcp** `data_fund_flow` | `MainNetFlow`→`main_net_inflow`、`JumboNetFlow`→`super_large_inflow`、`BlockNetFlow`→`large_inflow` | 独立第二源，验证 neodata 量级 |
| 历史锚点（待核验） | `fund_flows` 现有 `em_spot` / `em_push2his` 行 | 同 code+date 的 `net_inflow` | 旧主源历史值，作为交叉基准 |

**关键发现（量化，来自 `fund_flow_multisource_report.json`）**：

- **neodata vs westock 逐元一致**：抽样 5 只（159220/159949/510300/510500/512810）共 **115 个重叠点，一致率 100%**（容差 ±5% 或 ±1000 元）。两家独立厂商在同一指标上完全一致 ⇒ neodata 口径可信。
- **em_spot 历史锚点严重背离**：与 neodata 重叠 **301 个点，一致率仅 22.3%**。逐只深挖：小盘/行业 ETF（如 `159267` 航天ETF）**16 个重叠点 0 匹配、12 个背离**（如 08-26 主力净流入 neodata=41.2 万 vs em_spot=16.2 万，差 2.5×；部分日期反向）。说明 `em_spot`（akshare `fund_etf_spot_em`）对中小盘 ETF 的主力净流入存在系统性口径偏差，**历史值不可信**。
- **结论**：以 neodata 为权威源；`em_spot`/`em_push2his` 仅作「待核验锚点」，其背离行须经**双源（neodata+westock）确认**后修正。

#### 7.7.3 落地：回填脚本 + 三源验证 + 维护对账

1. **回填脚本** `scripts/backfill/backfill_fund_flows_neodata.py`：读 `scripts/backfill/data/fund_flow_neodata_<CODE6>.json`（主源种子，23 只各 23 行，2026-08-25~09-24）与可选的 `fund_flow_westock_<CODE6>.json`（交叉种子，5 只），幂等 upsert 入 `fund_flows`。
   - **落库纪律（P1-A + #130/#135 守卫）**：`source='neodata_mcp'`、`is_estimated=0`、`confidence=1.0`；复用 `src/data_sources.fund_flow.save_fund_flows`，自动继承其三项守卫（指标全空跳过 / 不以 NULL 覆盖真值 / `net_inflow` 空拒绝 INSERT）。
   - **缺口策略**：仅补齐缺口——已存在可信源行（`em_spot`/`em_push2his`/`neodata_mcp`/`westock_mcp`）不覆盖；仅当现有行是 `kline_est` 估算（`is_estimated=1`）时才用真实值升级覆盖。
   - **初始回填结果**：**163 行补齐 09-22/23/24 缺口 + 51 个 `kline_est` 估算行升级为真实值 + 366 个可信行保留**；23/23 只 ETF 均至 `MAX(date)=2026-09-24`。
2. **多源交叉验证** `--verify`：计算 neodata×westock、neodata×锚点 两对一致率，落盘 `scripts/backfill/data/fund_flow_multisource_report.json`（含逐只 `anchor_match_rate` / `westock_match_rate` 与背离样例）。
3. **维护对账** `--reconcile`（双源确认修正脏历史）：仅当存在 westock 种子（双源确认）时，将「neodata 与 westock 一致、但与 em_spot/em_push2his 锚点背离 >5%」的历史行 UPDATE 为 neodata 真值；执行前**自动备份 DB** 至 `data/backups/portfolio_PRE_RECONCILE_<ts>.db`。幂等、可安全重入。
   - **初始对账结果**：5 只样本（有 westock 种子）的历史 em_spot 背离行已修正为 `neodata_mcp`，DB 已自动备份。其余 18 只待 westock 全量种子就位后自动纳入（见 §7.7.5）。

#### 7.7.4 实时路径缓存兜底标记

`run_analysis.py` `fetch_etf_fund_flow_batch` 返回空分支新增审计标记：若 `fund_flows` 已有 `neodata_mcp` 缓存则打 `[缓存兜底] ETF资金流 EM 全失败，下游回退 neodata_mcp 缓存(最新 <date>)`；否则打 `[无缓存]` 告警（对齐 §7.6.3.2 的 `[OHLCV]` 标记范式）。

#### 7.7.5 每日自动化（WB automation，待创建于 21:20）

排程 **`FREQ=DAILY;BYHOUR=21;BYMINUTE=20`**（接在 `932000@21:00`、`9-ETF westock@21:10` 之后），ACTIVE。prompt 指示 LLM agent：

1. 对 23 只 ETF 逐一调 **neodata-mcp `fund_flow`**（近 ~30 交易日）→ 落 `fund_flow_neodata_<CODE>.json`（脚本兼容归一化与原始响应两种形态）；
2. 对 23 只逐一调 **westock-mcp `data_fund_flow`**（**单码调用、两次间隔冷却 ≥25 秒**规避 `error_type=2 服务限频`）→ 落 `fund_flow_westock_<CODE>.json`；
3. 跑 `backfill_fund_flows_neodata.py --dir scripts/backfill/data --reconcile`（先 upsert 新数据、再以双源确认对账修正历史 em_spot 脏行、最后重跑三源验证刷新报告）；
4. 任一源调用失败仅跳过错过代码、**显式上报失败**，不静默成功。
   - 西构对齐 §7.5.2/§7.6.3：westock 全量种子就位后，`--reconcile` 自动把 18 只剩余 ETF 的历史 em_spot 背离行一并修正，维护方案闭环。

#### 7.7.6 待提交清单（新增）

- 新增 `scripts/backfill/backfill_fund_flows_neodata.py`（含 `--verify` / `--reconcile` / `--dry-run`）
- 新增 `scripts/backfill/_diag_fundflow_neodata.py`（诊断，确认 25 代码全卡 09-21 + 排除 001323/002152）
- 新增 `scripts/backfill/_gen_neodata_fundflow_seeds.py`（拆分 3 个 neodata 超限文件 → 23 个归一化种子，剥离 .SH/.SZ、日期 20260924→2026-09-24）
- 新增 `scripts/backfill/_gen_westock_fundflow_seeds.py`（固化 5 只实测一致的 westock 样本）
- 新增 `scripts/backfill/data/fund_flow_neodata_<CODE>.json`（23 个种子）+ `fund_flow_westock_<CODE>.json`（5 个交叉种子）+ `fund_flow_multisource_report.json`（验证报告）
- 修改 `run_analysis.py`：`fetch_etf_fund_flow_batch` 空分支加 `[缓存兜底]`/`[无缓存]` 标记
- 新增每日自动化 `fund_flows ETF 资金流 每日维护`（21:20）
- 更新本报告 §7.7

> 数据质量根因备注：`em_spot` 历史资金流对中小盘 ETF 系统性背离已量化（见 §7.7.2），其脏历史经 `--reconcile` 双源修正；`category='etf'` 误标个股（001323/002152）建议后续在 `resolve_target_codes` 层修正，避免再次混入 ETF 取数。

### 7.8 复制「连接器 + MCP 兜底」到 etf_fundamental / etf_top_holdings / etf_industry_alloc + 现有数据质量全面检查方案（2026-09-25）

与 §7.5/§7.6/§7.7 同理，把三件套复制到三张东财主源基本面表，并**落地一套「现有数据质量全面检查方案」**（诊断型，只读，可一键复跑）。

#### 7.8.1 范围与缺口现状（2026-09-25 实测）

| 表 | 主源（akshare 东财） | RST 风险 | 缺口现状（落地前） |
|---|---|---|---|
| `etf_fundamental` | `fund_etf_spot_em` | 是 | 23 只 ETF 每日快照卡 **2026-09-21**（与 fund_flows 同期 RST） |
| `etf_top_holdings` | `fund_portfolio_hold_em` | 是 | 300 行，仅 `2026年1季度` 锚点；无最新可得快照 |
| `etf_industry_alloc` | `fund_portfolio_industry_allocation_em` | 是 | 257 行，report_date `2026-03-31`（12 行 `2026-06-30`）；无最新可得快照 |

三张表**落地前均无 `source` 列** → 无法审计来源（已由回填脚本 `ALTER TABLE` 补 `source/is_estimated/confidence`）。

#### 7.8.2 多源交叉验证架构

| 表 | 主写（兜底主源） | 交叉校验 | 历史锚点（待核验） |
|---|---|---|---|
| `etf_fundamental` | **neodata-mcp `fund_quote`**（日K） | **westock-mcp `data_etf`** overview | 现有 `source IS NULL` 的 EM 行 |
| `etf_top_holdings` | **neodata-mcp `fund_holdings`** | （EM Q1 锚点） | `fund_portfolio_hold_em` Q1 2026 |
| `etf_industry_alloc` | neodata-mcp `fund_allocation`(sector) | （EM 最新 report_date 锚点） | `fund_portfolio_industry_allocation_em` |

**关键发现（量化）**：
- **neodata `fund_quote` 完美补 `etf_fundamental` 缺口**：返回 `{trade_date, latest_price, open, high, low, volume, turnover_value, turnover_rate}`，22/23 只正常补齐 09-22/23/24（**159300 沪深300ETF 在 neodata 无返回**，由每日自动化用 westock 补）。并复用 `fund_flows(neodata_mcp)` 已回填资金流字段（JOIN date+code）。
- **neodata `fund_holdings` 的 `stock_price_changeratio` 实为权重%**（列名误标，非涨跌幅）：512010 药明康德 29.20 / 恒瑞 22.02 / 迈瑞 7.81；EM Q1 锚点 19.85 / 20.17 / 8.16。
- **权重漂移本质 = 时效性陈旧，非数据错误**：neodata 为「最新可得」、EM 为 Q1 2026 陈旧披露，故 512010 药明康德 +9.35pct 是正常漂移。验证报告据此把差异分类为 `时效漂移 / 新增持仓 / 退出持仓`（top-10 成分随季更迭，如 510300 把 兆易创新/寒武纪 换入、长江电力/兴业银行 换出）。
- **`etf_industry_alloc` UNIQUE(code, industry) 无时间维度** → 不能平行快照，严禁覆盖 EM 季度披露。**策略：仅当 (code,industry) 在 EM 完全缺失时才 INSERT neodata（补真缺口）；EM 已有行则跳过，仅在 `--verify` 量化权重漂移。** 实测 38 行 neodata 行业中 33 行 EM 已有（验证）、5 行缺失补齐。
- **neodata `fund_allocation`(sector)** 返回 `{allocation_name, allocation_ratio}` 干净；`fund_report_date` 恒为 null → 重仓股标 `quarter='neodata_latest'`、行业配置标 `report_date='neodata_latest'`。

#### 7.8.3 现有数据质量全面检查方案（核心交付物之一）

`scripts/backfill/inspect_data_quality.py`（**诊断型、只读、不写库**）一键扫描 6 张东财主源相关表：

- **逐表**：行数 / 覆盖代码数 / 最新日期 / `source` 分布 / 是否卡点（`max_date < 期望交易日`）。
- **卡点根因区分（关键）**：`EM_PRIMARY_TABLES` 改为 `(em_src, is_em)` 二元组——
  - `is_em=True` 的表（etf_fundamental / etf_top_holdings / etf_industry_alloc / fund_flows / etf_price_history）卡点 → 归入 `em_blocked_tables`（host 级 RST 阻断）；
  - `index_pe_history`（`is_em=False`，主源 csindex）滞后 1 天 → 标「非EM主源滞后（疑似良性 T+1）」，**不误报为 RST**。
- **跨表一致性**：各表最新日期是否收敛（`diverged` / `divergence_days`）。
- **误标污染检测**：`category='etf'` 表混入的非 ETF 个股（`001323` 慕思股份 / `002152` 广电运通）检出并建议 `resolve_target_codes` 层剔除。
- **产出**：`scripts/backfill/data/data_quality_report.json` + 控制台 Markdown。

**本轮实测结论（2026-09-25 重跑）**：
- `etf_fundamental` 卡点已消除（neodata 补齐至 2026-09-24，`neodata_mcp=66` 行）；
- `index_pe_history` 滞后 1 天被正确判为**良性 T+1**（csindex，非 RST）；
- **`em_blocked_tables = 无`**（三张基本面表 + fund_flows + etf_price_history 经 §7.5~§7.8 兜底后均不再卡点）；
- 跨表发散仅 1 天（index_pe_history T+1），良性。

#### 7.8.4 落地：回填脚本 + 三源验证 + 检查方案

1. **`etf_fundamental` 回填** `scripts/backfill/backfill_etf_fundamental_neodata.py`：读 `etf_fundamental_neodata.json`（22 只 ×3 日 = 66 行，固化自 neodata fund_quote，排除 159300），`ALTER TABLE` 加 `source/is_estimated/confidence`，`INSERT OR IGNORE` 补缺口（不覆盖 EM 行），JOIN `fund_flows(neodata_mcp)` 资金流；`--verify` 产 `etf_fundamental_multisource_report.json`（neodata×westock×EM）。初始回填：**3197 行、MAX=2026-09-24、66 行 neodata、588000 09-24 price=1.713/amount=5.42e9/主力净流入=-1.14e9（来自 fund_flows JOIN）**。
2. **`etf_top_holdings` / `etf_industry_alloc` 回填** `scripts/backfill/backfill_etf_holdings_alloc_neodata.py`：读 `etf_top_holdings_neodata.json`（8 只 ×10 = 80 行）/ `etf_industry_alloc_neodata.json`（8 只 ×~5 = 38 行，固化自 neodata fund_holdings / fund_allocation）。
   - 持仓：平行补 `quarter='neodata_latest'` 快照（`INSERT OR IGNORE`，与 EM Q1 不冲突）→ 初始 **+80 行**；
   - 行业：仅补 EM 缺失行业 → 初始 **+5 行**，33 行验证跳过；
   - `--verify` 产 `etf_holdings_alloc_multisource_report.json`：权重差异分类 `时效漂移/新增持仓/退出持仓`，显式声明「漂移由时效性驱动、非数据错误」。
3. **种子生成器** `gen_etf_fundamental_seeds.py` / `gen_etf_holdings_alloc_seeds.py`：把已验证 neodata 响应固化为组合种子 JSON（每日自动化可重写扩展至全 23 只）。
4. **检查方案** `inspect_data_quality.py`（见 §7.8.3）。

#### 7.8.5 每日自动化（WB automation，21:30）

排程 **`FREQ=DAILY;BYHOUR=21;BYMINUTE=30`**（接在 `fund_flows@21:20` 之后），ACTIVE（id `152124e1-199a-4f47-a872-c802d9d927bd`）。prompt 指示 LLM agent：优先用 neodata-mcp 拉全量持仓 ETF 的 `fund_quote`/`fund_holdings`/`fund_allocation` → 固化种子 JSON（neodata 不可达则复用已提交基线）→ 跑 `backfill_etf_fundamental_neodata.py --verify` + `backfill_etf_holdings_alloc_neodata.py --verify` → 跑 `inspect_data_quality.py --expect <最近交易日>` → 输出 `em_blocked_tables` 是否为空、缺口是否补齐、权重漂移是否判为时效陈旧。

#### 7.8.6 待提交清单（新增）

- 新增 `scripts/backfill/inspect_data_quality.py`（现有数据质量全面检查方案，只读诊断）
- 新增 `scripts/backfill/gen_etf_fundamental_seeds.py`
- 新增 `scripts/backfill/backfill_etf_fundamental_neodata.py`（含 `--verify` / `--dry-run`）
- 新增 `scripts/backfill/gen_etf_holdings_alloc_seeds.py`
- 新增 `scripts/backfill/backfill_etf_holdings_alloc_neodata.py`（含 `--verify` / `--dry-run`）
- 新增 `scripts/backfill/data/etf_fundamental_neodata.json`（66 行）+ `etf_fundamental_multisource_report.json`
- 新增 `scripts/backfill/data/etf_top_holdings_neodata.json`（80 行）+ `etf_industry_alloc_neodata.json`（38 行）+ `etf_holdings_alloc_multisource_report.json`
- 新增 `scripts/backfill/data/data_quality_report.json`（检查报告）
- 新增每日自动化 `ETF基本面/持仓/行业 neodata 每日维护`（21:30）
- 更新本报告 §7.8

> 注：`etf_top_holdings` / `etf_industry_alloc` 本轮仅覆盖 8 只代表性 ETF（宽基+军工+医药+成长+创业板）作基线；全 23 只由每日自动化（neodata 实时重写种子）渐进补全，与 §7.7 fund_flows/westock 渐进范式一致。
