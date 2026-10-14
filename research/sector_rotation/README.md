# 行业轮动研究资产归集

> 归集日期：2026-10-14 · 来源：A股投研线工作区
> 用途：将投研线的**数据资产**纳入版本控制，防止会话目录清理导致的历史数据永久丢失

## 为什么要有这个目录

投研线工作区 `C:\Users\HUAWEI\WorkBuddy\2026-08-04-15-30-49` **不是 git 仓库**，其 5.9 MB 资产无历史、无备份。其中 `history.json` 含 **45 个交易日 × 31 个申万一级行业**的日度行情与主力资金流，是**不可再生**的抓取成果（历史行情无法重新联网获取），一旦丢失无法恢复。

本目录为上述资产在 portfolio_tracker 仓库内的**受版本控制副本**。

## 目录结构

```
research/sector_rotation/
├── history.json          557 KB · 45 条日度快照（2026-08-03 ~ 2026-10-13）
│                         结构 {meta, snapshots:[{date,weekday,fetchedAt,note,
│                         indices,strip,sectors[31],ladder,gv,northbound,nbTop,sources}]}
│                         sectors 元素 {n:行业名, p:涨跌幅%, f:主力净流入亿, core:是否核心}
├── dashboard.html        1.01 MB · 单文件零依赖仪表盘，SNAPS 含 45 个日期键
├── reviews/              11 份复盘报告（9 周度 + 2 月度）
├── mail_body/            42 个邮件正文（.txt 纯文本 + .html 浅色版，成对）
└── scripts/              canonical 脚本【存档副本，见下方警告】
    ├── _build_mail_body.py
    └── _snap_dump.js
```

## ⚠️ scripts/ 是存档副本，不是运行副本

`scripts/` 下的脚本**仅供版本留存**，**投研线自动化运行时使用的仍是工作区根目录的原始文件**：

| 用途 | 路径 |
|---|---|
| **运行副本**（自动化实际调用） | `C:\Users\HUAWEI\WorkBuddy\2026-08-04-15-30-49\_build_mail_body.py` |
| 存档副本（本目录，仅留存） | `research/sector_rotation/scripts/_build_mail_body.py` |

**修改脚本时必须改运行副本，然后同步复制到本目录**；反之若只改本目录，对自动化无效。此副本的存在是为了防止根目录 58 个一次性脚本中 canonical 脚本被误删或误判缺失（10-12 曾发生误判 `_build_mail_body.py` 缺失的事故）。

经甄别，投研线 canonical 脚本**实际只有 2 个**（`_build_mail_body.py`、`_snap_dump.js`）；原方案所列 `_append_hist_*.py` 系按日期命名的临时脚本，非 canonical，不纳入留存。

## 数据口径（重要）

| 项 | 口径 |
|---|---|
| 行业分类 | **申万一级 31 行业** |
| 主力资金 | 大单 + 特大单差额，全市场加总不恒为零 |
| 自洽校验 | 31 行业 `f` 求和 ≈ 数据宝公布的「两市主力资金全天净流入 X 亿」 |
| 北向 | 自 2024-08-18 起取消每日净买入披露，仅有成交总额 + 十大成交股；未披露标 `null`／待披露，**禁止编造** |
| 六指数 PE | 东财 push2 指数 f9 恒返回 0.0，一律标「—」 |

**与组合线的口径差异**：组合线 `etf_industry_alloc` 使用**证监会门类**（粗，且为 2026-03-31 季报口径），本目录数据为**申万一级 31 行业**（细）。二者映射为一对多且存在信息损失，行业归因须按**证监会门类级**进行（整合方案 D1 决策 = 方案 A）。

## 同步机制（2026-10-14 已接入自动化）

投研线每日自动化在写完 `history.json` 后自动执行两步（幂等，重复执行不产生重复行）：

1. **复制到本目录**：`cp outputs/history.json → research/sector_rotation/history.json`（跨盘备份）
2. **入库**：`sync_rotation_to_db.py --date <当日> --verify` → 写入 `portfolio.db`

入库产出 4 张表（`scripts/research/sync_rotation_to_db.py` 自动建表）：

| 表 | 来源字段 | 行数（截至 10-13） | 说明 |
|---|---|---|---|
| `sector_fund_flow` | `snapshots[].sectors[31]` | **1395** | 申万 31 行业日度涨跌幅 + 主力净流入，`UNIQUE(date, sector_name)` |
| `northbound_daily` | `snapshots[].northbound[]` | **49** | 北向成交总额日度序列（由各快照的滚动序列跨快照去重），`UNIQUE(nb_date)` |
| `style_ladder` | `snapshots[].ladder[5]` | **225** | 规模谱系（上证50→中证2000）日度涨跌幅 |
| `style_growth_value` | `snapshots[].gv[3]` | **135** | 成长/价值指数对涨跌幅与剪刀差 `gap` |

**数据集纪律**：缺值一律写 `NULL`，绝不填 0 或编造。北向 `v:null` 表示未披露（当前 2026-08-20~08-31 共 8 天为 NULL，属真实缺口）。

**不产出 `style_compass` 表**：风格坐标（罗盘 `x/y`）仅存在于仪表盘 HTML 的 SNAPS 中，经核实该字段在 HTML 内无法稳定提取（正则 0 命中，且 SNAPS 段内日期键数异常），故风格维度改由上表 `style_ladder` + `style_growth_value` 的原始可靠数据表达。

**git 提交策略**：自动化**不执行 git commit**（避免每日 churn，与 `data/reports` 治理同源原则），归集目录的入库由人工定期处理。

## 相关文档

- 整合方案总纲：`docs/integration/投研线整合方案.md`
- 投研线项目记忆：`C:\Users\HUAWEI\WorkBuddy\2026-08-04-15-30-49\.workbuddy\memory\MEMORY.md`
