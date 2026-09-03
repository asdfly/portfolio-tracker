# P2-8 持仓舆情 × 资讯与评估 tab 整合评估

> 目标：评估「持仓舆情追踪（fund-analysis B3）」如何整合进现有「📰 资讯与评估」tab（tab7_news.py）。
> 生成日期：2026-08-26

---

## 一、现状盘点：资讯与评估 tab（tab7）已有能力

`tabs/tab7_news.py` 共 4 个面板，**全部是「板块级」舆情**：

| 面板 | 数据源 | 粒度 |
|---|---|---|
| 市场资讯 `_render_news_panel` | `daily_news`（按 category 板块分类） | 板块 |
| 综合评估 `_render_comprehensive_assessment` | 收益/风险/技术/健康评分 | 组合 |
| 市场情绪仪表盘 `_render_market_sentiment` | 持仓涨跌分布（pnl） | 组合/板块 |
| 新闻情感分析 `_render_news_sentiment` | `daily_news.sentiment_score`（SnowNLP+金融词典） | 板块 |

配套后端：`data_loader.load_sector_sentiment` / `load_news_sentiment_for_positions` / `load_etf_industry_news`——均为**行业/板块级**情绪。

## 二、P2-8 持仓舆情的本质 = 下沉到「重仓股级」

fund-analysis B3 的「持仓舆情」是跟踪 **ETF 重仓股（top holdings）** 的舆情，而非板块舆情。对 22 只 ETF 而言，即每只 ETF 的前十大重仓股（如科创50ETF → 中微公司/寒武纪/中芯国际…）的新闻情绪，聚合到 ETF 级。

## 三、数据缺口（实测结论）

| 需求 | akshare（生产栈） | 备选 |
|---|---|---|
| ETF 重仓股名单 | ❌ 本版本无 `fund_etf_holding_em`；`fund_etf_fund_info_em` 是分红/基本信息（且报 ValueError）；`fund_report_stock_cninfo(date)` 是全市场按报告期（重、非 ETF 专属） | ✅ **westock `data_etf(aspect='holdings')`**（实测返回 20 只重仓股+比例+披露日） |
| 重仓股个股舆情 | ❌ `daily_news` 是板块级，无个股级情感 | westock `data_news` / neodata 个股新闻 |

**关键约束**：重仓股名单 + 个股舆情，两个数据源都依赖 **westock/neodata 外部连接器**（非 akshare+SQLite 生产栈）。westock 当前已连接，但生产 `scheduled_run.bat` 纯 Python 无法调用——与 neodata 估值源同性质的「会话内采集」约束。

## 四、价值判断：中等偏下（锦上添花）

ETF 是分散一篮子（每只 20+ 重仓股），单个重仓股舆情对 ETF 走势的边际影响有限。重仓股舆情的主要增量价值只在两类场景：

1. **主题 ETF 重仓龙头舆情** → 提前反映板块情绪（如寒武纪/中芯国际负面 → 科创50ETF 预警）；
2. **重仓股集中暴雷** → 前十大重仓股集体负面时提示风险。

而这两类场景，**现有的板块级舆情已能覆盖约 80%**（负面新闻往往会扩散到板块 category）。重仓股级是「更精准」，不是「从无到有」。

## 五、整合方案（三档）

### 方案 A — 轻量聚合（纯现有数据，零新增采集）
在 tab7 新增「持仓板块舆情聚合」视角：把 `daily_news.sentiment_score` 按 `ETF_CATEGORIES` 的 sector 聚合，映射到每只 ETF，展示「ETF → 所属板块情绪 + 近期负面新闻 TopN」。
- 成本：~0.5 天；**增量有限**（与现有「新闻情感分析」面板部分重叠）

### 方案 B — 重仓股名单 + 板块映射代理（推荐，若要做）
1. 经 westock `data_etf(aspect='holdings')` 采集 22 只 ETF 重仓股名单（会话内/WorkBuddy 定时，落库 `etf_holdings` 表）；
2. 重仓股映射到东财/申万行业 → 复用现有板块级 `daily_news` 情绪 → 聚合到 ETF 级「重仓股舆情分」；
3. tab7 新增「持仓舆情（重仓股）」面板：每只 ETF 的重仓股 Top + 所属行业情绪 + 负面重仓股高亮。
- 成本：~1.5 天；数据源约束同 neodata（westock 需连接器在线）

### 方案 C — 完整重仓股舆情（重仓股 + 个股舆情）
在 B 基础上，再采集重仓股**个股**新闻舆情（westock `data_news` / neodata），直接按重仓股名匹配新闻情绪。
- 成本：~2.5 天；价值最高，但两个数据源都依赖外部连接器，且个股新闻量大、噪声高。

## 六、结论与建议

| 项 | 建议 |
|---|---|
| 默认取向 | **方案 A 或直接暂缓 P2-8**——板块级舆情已覆盖 80% 价值，重仓股级是锦上添花 |
| 若要做 | **方案 B**（westock 重仓股 + 板块情绪代理），性价比最优；放 tab7 新增面板，或并入 tab16 风险展望的持仓维度 |
| 红线 | 舆情只作参考提示、不自动调仓；重仓股数据标注披露日期（季度披露，滞后）；westock/neodata 数据源标注「会话内采集」 |

**投入产出比结论**：P2-8 是三档里唯一「价值中等、却要引入外部连接器依赖」的项。建议与其余已落地的 P0/P1/P2-7 分开决策——除非你明确要「重仓股级」精度，否则暂缓，把精力留给更高价值的迭代（如估值分位积累、风险预警联动）。
