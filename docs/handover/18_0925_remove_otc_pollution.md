# 18_ 剔除误标个股污染（场外基金/已清仓标的误入 ETF 表）

> 日期：2026-09-25
> 关联：handover 17_（ETF 缺口根因 + CSI2000 状态）、07_known_data_issues.md

## 一、问题本质

`001323`（东吴移动互联灵活配置混合A）、`002152`（华宝核心优势混合）及一批场外基金/已清仓 ETF
被**错误地当成了 ETF**，写入了 `etf_fundamental` / `etf_top_holdings` / `etf_industry_alloc` /
`etf_technical` / `etf_price_history` / `fund_flows(category='etf')` 等"场内 ETF 数据表"。

它们本质是**开放式混合基金（场外基金）**，不是交易型 ETF，不应出现在这几张表里：
- 没有追踪指数 → `ETF_TO_INDEX` 映射为空 → 估值/行业/重仓股语义错乱；
- 东财 `fund_etf_spot_em` 等 ETF 接口对场外基金结构性不覆盖或返回错乱数据；
- 会污染 F10 面板、行业暴露、资金流分析等下游展示与监控。

## 二、根因链条（为什么会发生）

1. **配置层（诱因）**：`config/settings.py` 的 `ETF_CATEGORIES`（L319，被当作"ETF 权威分母"，
   供 tab2/tab3/tab5/tab7/tab8/dashboard 等 UI 做持仓分类展示）**把场外基金也放进了分母**
   （含 `001323/002152/001407/001437/001765/007994/008269/100032/166301/519770/880013/001194/027293` 等）。
   ⚠️ 不能从 `ETF_CATEGORIES` 删除这些条目——会改变分母口径、破坏 UI 展示（见 settings.py L339 注释）。
2. **采集层（直接原因）**：多个"展示/监控类"采集入口**直接读 `ETF_CATEGORIES.keys()` 或遍历
   `portfolio_snapshots` 全部 code**，没有做"是否为场内 ETF"的过滤：
   - `run_analysis.py:1290` → `etf_codes = list(ETF_CATEGORIES.keys())` 传给
     `run_etf_fundamental_collection`（覆盖 run_analysis + run_morning 两个调用方）；
   - `scripts/backfill/backfill_full_history.py:509` → `rebuild_etf_technical` 遍历
     `portfolio_snapshots` 全部 code 算技术面；
   - `src/data_sources/fund_flow.py` 的 `fetch_etf_fund_flow_batch` 接收的 etf_codes 同理。
3. **已存在但未生效的防御**：`src/analysis/predictor/build_base.py` 的 `resolve_target_codes`
   + `_assert_etf_domain` 早已用 `_NON_ETF_EXCLUDE={"001323","002152"}` 拦截，但**只覆盖预测底座/
   特征域**（`etf_features`/`etf_forward_returns`），**没有覆盖上面的展示/监控采集路径**。

## 三、治理方案（两层）

### 3.1 数据层（治标，已完成）—— 剔除历史污染

- 备份：`data/database/portfolio.db.pollute_bak_otc_20260925_0910`（删前全量），
  以及更早的 `portfolio.db.pollute_cleanup_bak_2026-09-25`（仅 001323/002152 删前的遗留，可忽略）。
- 用**通用规则** `code ∈ OTC_FUND_CODES ∪ DELISTED_CODES` 一次性清理，不硬编码具体代码：
  - `etf_fundamental` −171、`etf_top_holdings` −90、`etf_industry_alloc` −89、
    `etf_technical` −1465、`etf_price_history` −1233、`fund_flows(category='etf')` −293，**合计 −3341 行**。
  - 验证：上述各表 `WHERE code IN (...) ` 计数全部归零。
- **未动**的真实数据：`portfolio_snapshots`（真实持仓，51×2=102 行）、`trade_records`（真实交易）、
  `stock_lhb` / `stock_institution_research`（个股研究，正常）均保留。
- 注：`159732`（消费电子ETF华夏，已清仓）属 `DELISTED_CODES`，一并清理其历史残留
  （它是真 ETF 但已清仓，不进当前 ETF 口径）。

### 3.2 代码层（治本，已完成）—— 拦截未来写入

复用 `config/settings.py` 已成熟的 `is_otc_fund()`（L402）与 `is_delisted()`（L420），在三个
采集入口加防御性过滤（入口拦截，无论调用方传什么 codes 都不会污染）：

| 文件 | 入口 | 改动 |
|---|---|---|
| `src/data_sources/etf_fundamental.py` | `run_etf_fundamental_collection` | 函数开头 `codes = [c for c in codes if not is_otc_fund(c) and not is_delisted(c)]`，空则跳过；覆盖 run_analysis + run_morning |
| `src/data_sources/fund_flow.py` | `fetch_etf_fund_flow_batch` + 单只 `fetch_etf_fund_flow` | 批量与单只入口均 `is_otc_fund/is_delisted` 返回空 |
| `scripts/backfill/backfill_full_history.py` | `rebuild_etf_technical` | 遍历 `portfolio_snapshots` 时 `if not is_otc_fund(c) and not is_delisted(c)` |

- 验证：`ETF_CATEGORIES`（34 条）→ 过滤后 22 只场内 ETF；`001323/002152/159732` 等均被排除，
  债券 ETF（511520 等）保留；三文件 `py_compile` + import 通过。

### 3.3 回归自检（已完成）

`scripts/backfill/inspect_data_quality.py` 的误标污染检测从"硬编码两只代码"升级为**通用规则**：
扫描 `etf_fundamental/etf_top_holdings/etf_industry_alloc/etf_technical/fund_flows(etf)`，
凡 `is_otc_fund(c) or is_delisted(c)` 即报污染。重跑结果：**无污染告警**，卡点表无，
跨表发散仅 `index_pe_history` 良性 T+1（1 天）。

## 四、验证结论

- 数据层：5 张 ETF 表 + fund_flows(etf) 的场外/清仓残留全部归零（−3341 行），真实持仓/交易保留。
- 代码层：3 个采集入口加 `is_otc_fund/is_delisted` 拦截，未来自动化（21:20 fund_flows、21:30
  fundamental/holdings/alloc、每日主分析）不再把场外基金写入 ETF 数据表。
- 自检：inspect 通用污染检测通过，可作为常态化回归护栏。

## 五、后续提醒

- 新增场外基金持仓时，只要它在 `OTC_FUND_CODES` 中即自动被拦截；若需新增场外基金，
  请在 `OTC_FUND_CODES`（`config/settings.py:385`）登记，无需改采集代码。
- `ETF_CATEGORIES` 仍保留场外基金条目作展示分母，请勿为"治污"而删条目（会破坏 UI 口径）。
- 若 inspect 再次报"非ETF污染"，说明有采集入口绕过或未生效，优先排查上述三个入口。
