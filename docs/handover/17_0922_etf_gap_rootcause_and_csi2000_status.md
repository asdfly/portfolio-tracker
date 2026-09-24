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
| Task ② index_quotes（sh932000） | **阻断**：0 行，本环境四源全不可达 |

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

## 3. 修复建议（须用户决策后落地 —— 铁律：产线代码改动须经确认）

| 编号 | 建议 | 直接消除的问题 |
|---|---|---|
| **A** | 价格回填「当日写入行数 = 0（或显著低于持仓数）」即降级 run_status + 写 alerts（对齐现有 gate 的 error 路径） | 09-23「success 假象」 |
| **B** | 按代码隔离重试 / 隔离名单（quarantine）：对持续失败的 9 只加有界重试 + 隔离，避免每轮全量重试拖慢并污染日志；隔离项单独告警 | 9 只永久缺口 + 日志噪声 |
| **C** | 恢复可用价源：等 eastmoney 上游恢复，或为 ETF 日行情接入韧性源（腾讯 gtimg 已在本仓 backfill 脚本验证可解析，但须在其可达环境运行） | 全量 0 行根因 |
| **D** | gate 改「按代码计缺失天数」：将 ≥3 缺失日的 error 判定从全局 distinct 改为按 code 聚合，使 9 只的 3+ 日缺口在下一轮即触发 error/alert | 缺口跨日累积被掩盖 |

> 当前**未改动** `backfill_etf_price_history` / `price_history_gate` 产线代码（铁律：未获确认不改产线）。

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

### 4.3 index_quotes（阻断，0 行）

`sh932000` 行数 = **0**。阻断原因（本沙箱实测，四源全不可达）：

| 源 | 结果 | 性质 |
|---|---|---|
| 新浪 K线 | `null`（不覆盖中证2000） | **结构性**，非临时 |
| 东方财富 push2his | `RemoteDisconnected` | 临时上游，与 09-22 系统抖动同源（HEALTH_CHECK 已连续多交易日 fail） |
| 腾讯 gtimg | `No dispatch info found`（本沙箱主机不可路由，连 sh000001 也失败） | 本沙箱限制，用户真机应可达 |
| 网易 163 | `502 Bad Gateway` | 本沙箱限制 |

→ **本环境无法取数**；脚本逻辑已就绪（新浪 → 腾讯 gtimg → 东财 三级回退），
须在**可达网络环境**重跑。

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

1. **用户决策 §3 的 A–D 产线修复**（尤其 A 与 D，直接消除「success 假象」与跨日掩盖）。
2. 在**可达网络环境**重跑中证2000 `index_quotes` 回填（命令见 §4.5）。
3. 提交本次未提交改动（`config/settings.py`、`neodata_valuation.py`、
   `backfill_single_index.py` + 本报告）——**铁律：显式 pathspec，不推送未获授权**。
