# 04 产品交接评估 — Portfolio Tracker（portfolio_tracker）

> 评估人：产品经理（许清楚）｜评估性质：**只读 / 不改动生产代码与数据库**（本报告为新建评估文档）
> 评估对象：`D:\HuaweiMoveData\Users\HUAWEI\Documents\lingxi-claw\portfolio_tracker`
> 评估依据：以代码为唯一事实来源（TAB_REGISTRY、模块导入、测试文件），文档仅作交叉核对
> 当前版本标记：CHANGELOG 顶部 `v2.6 (2026-08-03)`，其后 `[Unreleased]` 为空

---

## 0. 执行摘要（TL;DR）

- **实际注册 Tab 数 = 17**（tab1–tab17），不是 README 声称的「15 个」。tab16（ETF 风险展望）、tab17（ETF 高低位定位）已上线但**既未进 README 功能概览、也未进 CHANGELOG**（v2.6 之后新增，Unreleased 为空）。
- **17 个 Tab 全部为真实可用实现，未发现空壳 Tab**。`_helpers.py` 里的「Stub 函数」注释是**误导性命名**——那是带空数据保护的真实辅助函数，不是占位。
- **项目自有代码中无 `TODO`/`FIXME`/`未实现` 类功能占位标记**（仅 `venv313/` 第三方库有，已排除）。不存在半编码的功能残骸。
- **产品成熟度最高的核心是「操作建议引擎 + 闭环反馈」**：`src/analysis/advisor.py` 1939 行、`tabs/tab8_advice.py` 1677 行，`advice_history` 表 1789 条（README），反馈链路真实打通。
- **真实缺口（需接手方处理）**：① D12 在 D1–D15 路线中**完全缺失**（无测试、无文档）；② tab17 的 F2 估值因子因 `index_pe_history` 不足 250 交易日被**自动禁用**；③ 部分市场事件数据存在「待补采永久残留」空洞；④ 文档严重滞后于代码（15 vs 17 Tab、advisor 17 vs 23 方法、表 26 vs 30）。

---

## 1. 当前功能清单（代码核实）

### 1.1 注册证据（唯一事实来源）

`dashboard.py` 用 `TAB_REGISTRY` 插件式注册，位置 **`dashboard.py:1078-1096`**。实际注册 **17 项**：

```python
# dashboard.py:1078
TAB_REGISTRY = [
    ("📈 净值走势",   "tabs.tab1_net_value",        "render_tab1"),   # :1079
    ("📊 持仓分布",   "tabs.tab2_position",         "render_tab2"),   # :1080
    ("⚠️ 风险分析",   "tabs.tab3_risk",             "render_tab3"),   # :1081
    ("📅 收益日历",   "tabs.tab4_calendar",         "render_tab4"),   # :1082
    ("💠 高级分析",   "tabs.tab5_advanced",         "render_tab5"),   # :1083
    ("📡 技术信号",   "tabs.tab6_technical",        "render_tab6"),   # :1084
    ("📰 资讯与评估", "tabs.tab7_news",             "render_tab7"),   # :1085
    ("💡 操作建议",   "tabs.tab8_advice",           "render_tab8"),   # :1086
    ("🔬 自定义指标", "tabs.tab9_custom",           "render_tab9"),   # :1087
    ("💰 资金动向",   "tabs.tab10_fund_flow",       "render_tab10"),  # :1088
    ("🥇 黄金市场",   "tabs.tab11_gold",            "render_tab11"),  # :1089
    ("🌐 宏观市场",   "tabs.tab12_macro",           "render_tab12"),  # :1090
    ("📊 数据质量",   "tabs.tab13_data_quality",     "render_tab13"),  # :1091
    ("📋 市场事件",   "tabs.tab14_market_events",   "render_tab14"),  # :1092
    ("🔁 交易复盘",   "tabs.tab15_trade_review",   "render_tab15"),  # :1093
    ("🔮 ETF 风险展望", "tabs.tab16_risk_outlook",   "render_tab16"),  # :1094  ← v2.6 后新增
    ("🎯 高低位定位",   "tabs.tab17_etf_position",   "render_tab17"),  # :1095  ← v2.6 后新增
]
```

> 渲染为懒加载（`dashboard.py:1102-1108`）：仅渲染用户选中的 Tab，避免 17 个 Tab 同步执行导致的首屏卡顿。

### 1.2 逐 Tab 能力描述（基于读 `tabs/tabXX_*.py` 的 docstring / 导入 / 章节标题）

| # | 注册名 | 文件 | 能力一句话描述（代码核实） | 体量 / 状态 |
|---|--------|------|---------------------------|-------------|
| 1 | 净值走势 | `tab1_net_value.py` (47KB) | 累计净值曲线、多基准对比、区间收益、年度收益、日收益分布 | 完整 |
| 2 | 持仓分布 | `tab2_position.py` (47KB) | 饼图/行业分布/相关性矩阵/HHI 集中度/Beta 贡献/交易历史（11 种类型）+ ETF 详情抽屉 | 完整 |
| 3 | 风险分析 | `tab3_risk.py` (27KB, **编排器**) | 夏普/索提诺/卡玛/VaR/最大回撤/压力测试/风险预警。拆为 4 子模块：`tab3_risk_dashboard`/`_attribution`/`_warnings`/`_alerts`（均被 `tab3_risk.py:26,30,34,39` 导入） | 完整（非壳） |
| 4 | 收益日历 | `tab4_calendar.py` (26KB) | 年度/月度收益（daily_return 连乘）、日历热力图 | 完整 |
| 5 | 高级分析 | `tab5_advanced.py` (59KB) | 因子归因、Brinson 分解、Monte Carlo、VaR 直方图、再平衡模拟、一键报告导出 | 完整 |
| 6 | 技术信号 | `tab6_technical.py` (19KB) | 雷达图/信号柱状图/布林带·RSI 分布（数据委托 `data_loader.load_technical`） | 完整 |
| 7 | 资讯与评估 | `tab7_news.py` (29KB) | 新闻聚合（SnowNLP+jieba 情感）、市场情绪评估 | 完整 |
| 8 | 操作建议 | `tab8_advice.py` (79KB / 1677 行) | **核心**：智能建议引擎 UI（多因子评分、仓位建议、信号方向+矛盾标注、置信度筛选、反馈面板） | 完整（最核心） |
| 9 | 自定义指标 | `tab9_custom.py` (18KB) | 技术指标回测、K 线形态识别、DB 回测历史 | 完整 |
| 10 | 资金动向 | `tab10_fund_flow.py` (39KB) | 行业/ETF/主力/北向资金流 | 完整 |
| 11 | 黄金市场 | `tab11_gold.py` (6KB, **编排器**) | 导入 10 个 `gold_components` 渲染器（`tab11_gold.py:9-18`：比价/季节性/储备/技术信号/相关性/实时/央行/供需/国际对比/组合相关性）；`gold_components/` 含 13 模块 | 完整（10 子页） |
| 12 | 宏观市场 | `tab12_macro.py` (17KB) | 汇率/国债收益率/金价基准/LPR/Shibor/两融余额等宏观面板（`_load_macro_data` from `macro_daily`） | 完整 |
| 13 | 数据质量 | `tab13_data_quality.py` (29KB) | 新鲜度/覆盖率/回测完整度/综合质量评分环（`DataQualityChecker`） | 完整 |
| 14 | 市场事件 | `tab14_market_events.py` (29KB) | 龙虎榜/融资融券/股东增减持/机构调研/大宗交易深度分析 | 完整 |
| 15 | 交易复盘 | `tab15_trade_review.py` (20KB) | 买卖配对盈亏/定投追踪（11 只）/交易成本/月度资金流向（含天添利·银转存独立归类） | 完整 |
| 16 | ETF 风险展望 | `tab16_risk_outlook.py` (19KB, **2026-08-21**) | 基于 `src.analysis.predictor`（LightGBM）的波动率/回撤预测可视化 + 模型回测（OOS R²/IC/AUC）。**仅作参考，不自动调仓** | 完整（依赖 ML，见 §3） |
| 17 | 高低位定位 | `tab17_etf_position.py` (14KB, **2026-09-03**) | 三因子价格位置量化（F1 价格分布/F3 资金流/F2 估值闸门），位置分 P∈[-100,+100]+置信度+五档标签。**F2 因数据不足自动禁用**（见 §2） | 完整但有 1 因子关（见 §2.1） |

> 后端分析引擎 `src/analysis/` 共 **28+ 模块**（advisor / backtest / factor_attribution / rebalance_engine / predictor/ / etf_position / equity_risk_premium / industry_boom / smart_alert / candle_patterns / dca_backtest …），支撑上述全部 Tab。

### 1.3 与 CHANGELOG v2.6 / README 的一致性核对

| 核对项 | CHANGELOG v2.6 / 实际代码 | README | 结论 |
|--------|--------------------------|--------|------|
| Tab 总数 | **17**（TAB_REGISTRY:1078-1096） | 「15 个分析 Tab」（`README.md:20`），功能概览表仅列 tab1–15（`README.md:22-38`） | ❌ **README 滞后**：tab16/tab17 已上线但未记录 |
| tab16/tab17 是否在 CHANGELOG | v2.6 之后新增，但 `[Unreleased]` 为空（`CHANGELOG.md:39-40`） | 无 | ❌ **两文档都漏记** |
| advisor 方法数 | 23 个函数（断言 `test_d15_code_quality.py:28`，且均 ≤200 行：`:30-33`） | 「17 方法/13 步骤」（`README.md:87`） | ⚠️ 文档数偏旧（17 vs 23） |
| tabs 文件数 | 22 个 `.py` + `gold_components/`(13) | 「33 文件」（`README.md:114`） | ⚠️ 文档数偏旧 |
| 数据库表数 | README 称 30 张（`README.md:10`） | 同 | ⚠️ CHANGELOG v2.3 称 26 张（`:145`）；以 README 30 为准，但与旧 CHANGELOG 不一致 |
| tab3 拆分 | 1 编排器 + 4 子模块（导入见 `tab3_risk.py:26-39`） | 「tab3_risk_*.py 风险分析拆分（5 子模块）」（`README.md:116`） | ✅ 一致（5 个文件 = 1+4） |

**结论**：代码领先文档。最严重的是 **tab16/tab17 完全未文档化**（README 与 CHANGELOG 双漏），接手方极易误判产品边界。

---

## 2. 已知产品缺口 / 残留 TODO（代码扫描）

### 2.1 扫描方法与结果

- 全仓 `**/*.py` 递归扫描 `(TODO|FIXME|XXX|HACK|暂未|待补|未实现|占位|空壳|stub|Stub|TBD|to be done|not implemented)`，**排除 `venv313/`**。
- 项目自有代码中命中的**全部**结果（无功能占位类 TODO）：
  - `scripts/gen_combo_report.py:578` 「暂未共振拖累」— 报告正文自然语言，非缺口。
  - `scripts/import_aug_2026.py:133` 「变动金额可能以 '-' 占位/缺失」— 注释，非缺口。
  - `tabs/_helpers.py:24` 「===== Stub 函数（替代 dashboard.py 中的外部数据加载函数）=====」— **误导性命名**：其下是 `_render_etf_metrics` / `_render_etf_price_chart` 等**带空数据保护的真实辅助函数**（`_helpers.py:31+`），不是占位壳。
  - `src/data_sources/collect_core.py:397,433,436` 与 `market_events.py:425` 「待补采」— 数据采集重试队列逻辑（「max_attempts 仅作占位」为设计说明），非功能缺口。
- **未发现**任何 `pass` 占位空函数、未实现异常、`raise NotImplementedError` 类残骸。

### 2.2 真缺口 vs 已落地未更新文档

**真缺口（需产品决策 / 工程跟进）：**

| 缺口 | 证据 | 性质 | 严重度 |
|------|------|------|--------|
| **D12 缺失** | `tests/` 下只有 `test_d11/d13/d14/d15`，无 `test_d12`；全仓检索 `D12` 无项目级命中（仅 venv）。 | D1–D15 路线中 D12 既无测试也无文档，状态不明 | 中（路线闭环缺口） |
| **tab17 的 F2 估值因子被禁用** | `tab17_etf_position.py:14-15` 诚实声明：「估值因子 F2 当前因 index_pe_history 历史不足(<250 交易日)自动禁用」 | 三因子中 1 个关（数据驱动，会随数据累积自动启用） | 低（已编码自动恢复） |
| **市场事件数据空洞** | `collect_core.py:397`「2026-08-08(周六)被登记为两融待补采、retry 3 次耗尽后永久残留」；`market_events.py:425` 待补采重试 | 部分源数据存在永久缺口，影响 tab14 完整性 | 中（数据完整性） |
| **tab16 模型质量依赖数据** | `tab16_risk_outlook.py:29` 运行时训练 LightGBM；仅展示 OOS R²/IC/AUC，无显式「数据不足」红线告警 | 预测可信度随样本量波动，缺用户侧护栏 | 低-中 |

**已落地但文档未更新（不是缺口，是文档债）：**

- tab16 风险展望、tab17 高低位定位：**代码已上线**，但 README 功能概览与 CHANGELOG 均未记录（§1.3）。
- advisor 实际 23 个方法（测试断言），README 写 17。
- tabs 实际文件数（22+gold_components）与 README「33 文件」不符。
- `predictor/` 已为 package（`predictor/__init__.py` + `models.py` + `features.py` + `labels.py` + `build_base.py` + `price_history.py` + `tier0.py`），tab16 的 `from src.analysis.predictor.models import ...` 可正常解析 → **非断链**。

---

## 3. 产品完整度判断（结合 advisor 真实能力）

### 3.1 能力成熟度矩阵

| 维度 | 判断 | 证据 |
|------|------|------|
| Tab 覆盖 | **17/17 真实可用**，无空壳 | 文件体量均 >6KB，且 tab11/tab16/tab17 有独立后端模块支撑 |
| 数据层 | **已上线、体量大**（README：35 万+ 快照、34K 技术指标、trade_records 1157 条） | 多 Tab 直接读 SQLite |
| 核心引擎 advisor | **最成熟、最中心** | `advisor.py` 1939 行 / 23 函数（≤200 行/函数，`test_d15:28-33`）；含闲置数据查询助手（`_query_recent_block_trades`/`_query_margin_data`/`_query_institution_research`，`test_d15:20-22`） |
| 闭环反馈 | **真实打通** | D3（CHANGELOG v2.2）：advice_history 写入打通；README 称 1789 条；tab8 含反馈面板 |
| 风险/预测 | **可用但带数据依赖** | tab16 LightGBM 运行时训练；tab17 F2 因子禁用（§2.2） |
| 告警/监控 | **已上线** | Monitor 9 条规则（CHANGELOG v2.2 D4），去重逻辑 `_get_recent_alert_rules` |
| 回测 | **5 策略** | backtest.py（CHANGELOG v2.2 D9：3→5）；信号回测 12,296 组（README） |

### 3.2 advisor 真实能力 vs 宣称

- **宣称**（README:87）：「智能建议引擎（17 方法/13 步骤）、多因子评分、仓位管理建议、信号方向+矛盾标注、置信度交互筛选」。
- **实际**：23 个函数、单函数 ≤200 行、可查询块交易/两融/机构调研等「闲置数据」（D1 激活的 12.8 万行）；UI 层 1677 行含反馈闭环。
- **产品结论**：advisor 是**真实且最成熟**的能力，宣称基本属实，仅方法计数文档滞后（17→23）。**但需向接手方确认一个产品问题**：advice_history 1789 条是「只写日志」还是「回灌进评分」？若仅日志，则「闭环」名实不符，应明确产品定位。

### 3.3 半完成 / 风险点

- tab17：三因子中 F2 关（数据驱动自动恢复，低风险但当前不完整）。
- tab16：预测质量依赖样本，缺用户侧「数据不足」红线告警。
- 数据空洞：市场事件「待补采永久残留」会导致 tab14 局部失真。
- **不可静态判断**：tab16/tab17 是否运行时产出非空合理结果，需一次 Streamlit 冒烟测试（本评估不启动服务，仅静态核实导入链路）。

---

## 4. 与早期 D1–D15 改进路线对照

- **原始路线图**（v1.3，2026-04-27，`output/改进计划与路线图.html`）：P0(7)+P1(10)+P2(4) 共 21 项，文档标注「全部完成」。
- **D1–D10**（CHANGELOG v2.2 显式记录，均标完成）：
  - D1 闲置数据激活 / D2 数据源修复 / D3 闭环反馈(advice_history) / D4 告警多样化(5→9) / D5 配置环境变量化 / D6 根目录清理 / D7(并入 D3) / D8 日志轮转 / D9 回测策略(3→5) / D10 容器化 CI。
- **D11–D15**（以测试文件为落地证据）：

| 项 | 测试证据 | 内容 | 状态 |
|----|----------|------|------|
| D11 | `test_d11_version_release.py:1` | 版本发布治理（CHANGELOG/LICENSE/README 校验） | ✅ 落地 |
| **D12** | **无 `test_d12`、全仓无引用** | 未知（既无测试也无文档） | ❓ **缺口/异常** |
| D13 | `test_d13_tab3_refactor.py:1` | tab3_risk 拆分为 4 子模块（与 §1.2 一致） | ✅ 落地 |
| D14 | `test_d14_backup.py:1` | 数据库备份工具（`scripts/backup_db.py`） | ✅ 落地 |
| D15 | `test_d15_code_quality.py:1` | 代码质量（advisor 助手方法 + tab3 结构） | ✅ 落地 |

**对照结论**：D1–D11、D13–D15 均已落地（14/15）。**唯一断点是 D12**——无测试、无文档、无代码引用，建议接手方第一时间澄清 D12 原定范围（是被合并进其他项，还是漏做）。

---

## 5. 产品层面接手建议（优先补什么 / 低风险高价值增强）

> 排序原则：先消除「误导接手方」的文档债（零代码风险、极高 ROI），再补数据/模型护栏（低风险），最后做信息架构优化（增值）。

### P0 — 文档债清零（零代码风险，最高 ROI，半天可完成）
1. **README 功能概览补 tab16/tab17**，并把「15 个分析 Tab」改为「17 个」。
2. **CHANGELOG 补 `[Unreleased]` 条目**：记录 tab16 风险展望、tab17 高低位定位的上线与后端依赖（predictor/、etf_position）。
3. **统一计数**：advisor 方法（17→23）、tabs 文件数（33→实际）、数据库表数（26 vs 30 以 README 为准并回填 CHANGELOG）。
4. **消除 `_helpers.py:24`「Stub 函数」误导注释**，改为「空数据安全的辅助渲染函数」。

### P1 — 路线与数据闭环（低风险）
5. **澄清 D12**：定位 D12 原范围，要么补测试/文档落地，要么正式归档关闭（避免路线表出现空洞）。
6. **tab17 的 F2 因子**：确认 `index_pe_history` 回填进度（需 ≥250 交易日自动启用）。这是已写好的自动恢复逻辑，接手方只需**验证数据回填任务在跑**，即可解锁第三因子。
7. **数据空洞透明化**：tab13（数据质量）已有的「新鲜度/覆盖率」评分，应**按数据源逐源展示缺口**（尤其「待补采永久残留」的 2026-08-08 两融等），让用户知道 tab14 哪些局部失真。
8. **advisor 闭环定性**：确认 advice_history 是「回灌评分」还是「仅日志」。若是后者，产品上应明确「建议追踪」定位，或补齐反馈→评分的回灌（产品增强，非必需）。

### P2 — 模型护栏与信息架构（增值）
9. **tab16 增加「数据充分性红线」**：当训练样本/近期数据不足时，页面顶部显示明确告警（已有 OOS R²/IC/AUC，加阈值即可），防止弱模型被过度信任。
10. **信息架构优化（降低认知负荷）**：17 个 Tab 对单用户偏多。建议在首页/导航增加「今日关注」聚合页（风险预警 + advisor Top 建议 + 数据质量异常），把最高信号前置。纯新增，不破坏现有 Tab。
11. **运行时冒烟测试（接手第一步）**：启动一次 Streamlit，确认 tab16/tab17 实际产出非空合理结果（本评估未启动服务，仅静态核实 `predictor/models.py`、`etf_position.py` 导入链路通畅）。

### 不建议（避免过度工程）
- 暂不为 tab16/tab17 加「自动调仓」——两 Tab 代码已声明「仅作参考，不自动调仓」，且方向预测已被 walk-forward 证伪（tab17 docstring 明述 Tier1 IC<0.02 全线 VETO）。保持描述性定位更符合产品诚信。

---

## 附：关键证据索引（file:line）

- TAB_REGISTRY：`dashboard.py:1078-1096`（17 项）
- 懒加载渲染：`dashboard.py:1102-1108`
- tab3 子模块导入：`tab3_risk.py:26,30,34,39`
- tab11 黄金子页导入：`tab11_gold.py:9-18`
- tab16 依赖：`tab16_risk_outlook.py:11,29-39` → `src/analysis/predictor/models.py`（存在）
- tab17 依赖与 F2 禁用声明：`tab17_etf_position.py:14-15,40` → `src/analysis/etf_position.py`（存在）
- advisor 体量/结构断言：`src/analysis/advisor.py`（1939 行）；`tests/test_d15_code_quality.py:20-33`
- D11–D15 测试：`tests/test_d11_version_release.py:1` / `test_d13_tab3_refactor.py:1` / `test_d14_backup.py:1` / `test_d15_code_quality.py:1`；**无 test_d12**
- 数据空洞：`src/data_sources/collect_core.py:397,433,436`；`src/data_sources/market_events.py:425`
- 误导性 Stub 注释：`tabs/_helpers.py:24`
- 文档滞后：README.md:20（15 Tab）/ :22-38（缺 tab16/17）/ :87（17 方法）/ :114（33 文件）；CHANGELOG.md:39-40（Unreleased 空）
