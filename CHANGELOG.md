# Changelog

所有重大变更均记录在此文件中。格式遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/)。

---
## v2.6 (2026-08-03)

### 数据质量修复
- **定投/申购符号错误修复 (bba9f84)**: 001407/519770/001194 三只基金的定投/申购 `change_amount` 符号翻转（18 条，+¥124,600 → -¥124,600），PDF 提取时符号被反转
- **月度资金流向归类重构 (bba9f84)**: 天添利(880013) 从「申购/赎回」列拆出为独立「天添利净流」列；银行转存独立列；新增「净投资流」（不含银转存+天添利）和「净现金流」（全部）汇总列；修复净现金流重复求和 bug
- **月收益率修复 (8431712)**: `compute_monthly_returns` 从 `total_value` 比值法改为 `daily_return` 连乘法。修正 2026-06 月度收益 142.74% → 4.00%，年度收益 +34.57% → -1.80%。根因：比值法将追加投入误计为收益
- **技术信号日期回退 (0695bf7)**: `load_technical` 新增日期回退逻辑，`end_date` 无数据时取 ≤ 该日期的最近可用日期。修复 8/03 快照与 etf_technical（T+1 采集）日期不匹配导致「暂无技术信号数据」
- **对账单数据全面利用 (0695bf7)**: dashboard 交易统计扩展为含定投/赎回/红利；tab2 交易历史面板支持全部 11 种交易类型
- **tab15 定投追踪修复 (3c5fb51)**: `calc_dca_tracking` 快照回退，11 只基金市值从 ¥0 → ¥517K；月份截取 `str[:6]` → `str[:7]`，月度从 7 → 35/38 个月
- **仓位建议目标占比修复 (264e92c)**: `compute_position_advice` 目标占比从绝对百分点改为相对比例（`target = current × (1 ± pct)`）。修复减仓 2%-8% 时 4.1% → 4.02%（仅减 0.08pp）的 bug

### 功能增强
- **portfolio_summary snapshot_type (7f8c3ee)**: 新增 `snapshot_type` 列标记 full/daily，3,448 行重建
- **tab15 指标卡增强 (bba9f84)**: 月度资金流向新增累计净投资流/净现金流指标卡

### 测试
- tab15 测试: 26 → 28 用例（+test_ttl_separated +test_summary_columns）
- 仓位建议测试: 11 → 15 用例（+4 目标占比正确性）
- 关键测试子集 136 passed

---
## v2.5 (2026-06-24)

### Bug 修复
- **max_drawdown corrected 净值修复**: `portfolio_risk.py:68` calculate_all 第二参数从 `values`(raw total_value) 改为 `prices`(corrected 累积净值)，消除场外基金月度入账导致的回撤虚高（31.77% → 9.83%）(commit 1567d5a)
- **ETF 基本面采集 import 修复**: `run_analysis.py` 阶段三.七b 添加缺失的 `from config.settings import ETF_CATEGORIES`，修复每日 15:12 采集静默失败（NameError），etf_fundamental 数据恢复至 100+ 天覆盖 (commit 1567d5a)

### 数据采集验证
- 12 个主要数据表运行正常，最新数据至 2026-06-24
- daily_return 分布合理（均值 0.038%，标准差 1.507%，n=3,421）
- ETF 基本面数据恢复：spot 20 条/日 + industry 158 条 + holdings 200 条 + valuation 17 条


## [Unreleased]

### 新增
- **Tab16 ETF 风险展望** (13e0c21, d51fc6c, a1f21a8, 6e1344d)：ETF 波动率 / 回撤预测模型落地；walk-forward 验证 OOS R² 0.44–0.89、AUC 0.90–0.97 达标才上线；前端对比 / 三档分类 / 预警 / 导出 + 组合聚合 + 历史回撤参照 + 轨迹下钻
- **Tab17 ETF 高低位定位** (5db0456, d7d2759)：ETF 状态定位器（非方向预测），三因子集成输出统一度量 P∈[-100,+100]、置信度 C∈[0,1]；advisor `_analyze_position_score` + 测试 + 设计文档 §12；F2 估值因子因 250 交易日数据门控自动禁用（index_pe_history 仅积累约 1 个月）
- **市场情绪温度计** (49051eb)：涨停 / 跌停 + 连板 + 主线 + 融资 多维市场情绪评估
- **周线七阶段市场定位** (6b71f71)：周线生命周期七阶段定位 + 配置审查补缺失核心仓位
- **neodata 估值源接入** (0f1bf58)：新增 neodata 估值数据源 + 获利减仓补市值 + 低配加仓机会测算
- **持仓盈亏分层 / 回本测算** (f3d3dab)：持仓盈亏分层（回本测算）+ 估值分位分析接入 advisor
- **波动率预测置信区间** (ff8e0af)：预测置信区间 + 再平衡引擎联动高波动预警
- **通用 SMTP 发信工具** (fa960f0)：标注废弃邮件模块 + 新增通用 SMTP 发信工具（替代专用模块）
- **招商证券 8 月对账单导入脚本** (bdd4265)：import_aug_2026.py

### 变更
- **日报风险块口径同步前端** (096155e)：三档分类 + 组合聚合 + 历史回撤 + 高波动预警
- **日报邮件浅色主题** (461105d, 1b8443c, 9a1f465)：支持 `--theme` 参数切换深 / 浅色 + 纯文本兜底全文（修复前 40 行截断）
- **数据新鲜度阈值放宽** (d0e1523)：股东增减持 / 上海金放宽 STALE 容忍
- **数据新鲜度核对 / 采集健壮性 / 新浪兜底白名单** (47e2eef)
- **生产脚本入库 + 忽略 QA 残留 env** (25390f7)：生产脚本纳入版本管理（P0-C），忽略 QA 残留 env 文件
- **日报建议聚合** (22fb76d)：聚合机会类建议，消除逐标的重复模板、提升信号密度

### 修复
- **黄金比值告警历史遗留** (109305d)：比值一致性检查加 30 天时间窗
- **日报内容质量四项缺陷** (41f2448)
- **日报邮件推送 bat 换行符** (8d967a9)：修复 bat 换行符 LF→CRLF 导致推送静默失败

### 文档
- **四维交接评估文档** (a99ec59)：架构 / 测试 / 运维 / 产品四维交接评估 + 总览

## [v2.5] - 2026-06-24

### 修复
- **max_drawdown 计算参数错误 (1567d5a)**: portfolio_risk.py:68 calculate_all 第二参数从 values (raw total_value) 改为 prices (corrected 累积净值)，消除场外基金入账导致的回撤虚高（31.77% 修正为 9.83%）
- **max_drawdown 历史数据回填**: 213 天 (2025-08-08~2026-06-23) max_drawdown 修正，使用 corrected daily_return 累积回撤算法（60 日窗口），修正后均值 7.59%，最大 12.68%，无 >20% 异常
- **ETF 基本面采集静默失败 (1567d5a)**: run_analysis.py 阶段三.七b 添加缺失的 `from config.settings import ETF_CATEGORIES`，修复每日采集 NameError

### 测试
- **数据一致性验证**: 3417 天全面扫描 daily_return 分布 (mean=0.038%, std=1.51%)，corrected=0 的 2 天均为非交易日，|dr|>5% 的 43 天集中于 2015 极端行情，均为真实市场波动
- **max_drawdown 回填验证**: 213 天修正后与 corrected daily_return 逻辑一致，06-18~06-23 从 31.77% 修正为 9.83%
- **ETF 基本面采集验证**: 06-24 采集 395 条 (spot:20, industry:158, holdings:200, valuation:17, errors:0)

---

## [v2.4] - 2026-06-20

### 修复
- **Monte Carlo /100 量纲 (8ade3af)**: data_loader.py L684 双重 /100，0.5% 日收益 252 天模拟结果从 1.3% 修正为 251%
- **tab5 再平衡 /100 (8ade3af)**: tab5_advanced.py L688 ret_arr 缺少 /100，场外基金净值模拟修正
- **tab1 日收益率 *100 多余 (979658d)**: tab1_net_value.py L133 多余 *100，标准差 103.291% 修正为 1.033%
- **portfolio_risk corrected returns (942a527)**: 用 corrected daily_return 替代 total_value.pct_change，波动率 262% 修正为 17%，夏普 8.86 修正为 -0.58
- **total_value 跳变统计修复 (c49da0e 等 4 commits)**: tab1/tab4/tab7/dashboard/factor_attribution 共 11 处改用 cumprod 累积净值法
- **backtest Sharpe (ef0f106)**: 添加无风险利率扣除 (Rf=2.5%) + ddof=1 样本标准差
- **factor_attribution 残差 (ef0f106)**: residual_std 使用 ddof=1
- **场外基金 pnl None (68a8463)**: _update_realtime_quotes else 分支改用 (current_price - cost_price) * quantity 计算，回填 11 只基金 63 条历史记录
- **compute_extended Sortino (979658d)**: 减去无风险利率 Rf=2.5%
- **tab2 ETF 技术指标展示 (16db702)**: _helpers.py 过时 stub 改用 data_loader 加载 etf_technical
- **dashboard trend_map 英文 key (16db702)**: 映射为中文行业名称

### 测试
- **第4轮 bugfix 回归测试 (98f30a9)**: test_bugfix_round4.py 38 用例覆盖 6 大修复点
  - TestMonteCarloDimension (5): 量纲对比、确定性路径、seed 可复现、零标准差保护
  - TestCleanseDailyReturns (8): 极端值过滤、尾截断、组合过滤、stats 完整性
  - TestComputeMonthlyReturns (4): 月度/年度收益率、几何平均、空数据防护
  - TestOffMarketFundPnl (8): 盈亏计算、None 值防护、dict.get 行为验证
  - TestCorrectedReturns (5): pct_change 虚假跳变、corrected 无虚假、波动率/夏普对比
  - TestBacktestSharpe (5): 无风险利率扣除、ddof=1、零波动防护、Calmar 公式
  - TestFactorAttribution (4): OLS 回归量纲、残差 ddof=1、R-squared 范围、贡献百分比
- **全量测试 1,220 → 1,435 (+215)**: 0 失败

### 变更
- **测试用例**: 1,307 → 1,435 (+128)
- **测试文件**: 74 → 75 (+1 test_bugfix_round4.py)
- **架构报告**: architecture_report_v5.docx（综合评分 87.8 → 88.5）

---

## [v2.3] - 2026-06-17

### 新增
- **Tab15 交易复盘**: 基于 trade_records 的交易统计面板，支持 ETF 交易历史分析、盈亏统计、持仓变化追踪
- **ETF F10 基本面数据集成**: etf_fundamental.py 采集模块，Tab2 持仓分布新增基金基本面数据和交易历史面板
- **P3 ERP 股债性价比**: equity_risk_premium.py 多模型 ERP 估算（Fed Model / 历史均值 / DDM）
- **P3 定投回测对比**: dca_backtest.py 均匀定投 vs 估值定投策略回测
- **P3 行业景气度指标**: industry_boom.py 4 维评分模型（资金30%+估值25%+技术25%+政策20%）
- **P3 智能预警推送**: smart_alert.py 5 维检测+4级信号+汇总推送
- **P2 新闻情绪升级**: 同类 ETF 穿透对比、信号交叉回验
- **P2 仓位管理+估值补全**: 基于技术信号的仓位建议，估值指标补全
- **P0 多因子综合决策引擎**: 综合评分决策模块
- **P0 同类 ETF 横向对比**: 技术信号综合评分，同类基金穿透对比展示
- **P1 单品风险全景**: 个股级别风险分析、资金流向、交易复盘、行业观点聚合
- **P1 盘前/盘后分析助手**: 交易日前后自动分析提示
- **历史对账单补充导入**: 31个PDF(2023-06~2025-12)分5批提取718条交易，trade_records 299→1017行
- **trade_records 数据增强**: 4项功能集成（交易成本压力测试+F10交易历史+dashboard实际交易统计）

### 修复
- **portfolio_summary daily_return 持仓变化校正 (95b9e2d)**: rebuild脚本使用共同持仓（前日qty×当日price）vs前日市值计算corrected daily_return，避免新增/移除/加仓导致total_value跳变被误计为收益
- **run_analysis 写入路径统一 (7ac0d19)**: 3条写入路径（backfill_full_history/portfolio.py/backfill.py）全部使用corrected daily_return共同持仓校正逻辑
- **全Tab显示层修复 (3a6cae3)**: 9处total_value.pct_change()替换为corrected daily_return/100，涵盖_helpers/tab3/tab7/data_loader(5处)/gold_correlation
- **tab1+dashboard指标修复 (2e72987)**: sharpe_ratio/volatility/max_drawdown全部使用预存corrected daily_return
- **场外基金防护 (422b93d)**: advisor/portfolio/risk三文件增加None值和K线缺失防护
- **递归get_db_connection修复 (642569b)**: 4个文件自引用导致RecursionError，改为显式import
- **11个预存测试失败修复 (ce0f3ad/99d722d)**: 方法名修正、DataFrame truth value、异常捕获、mock完善
- **KeyError "note"修复 (2f07f50)**: _helpers.py动态列选择适配trade_records无note列
- **Streamlit兼容性修复 (4 commits)**: width='stretch'、FutureWarning、horizontal参数、CachedWidget
- **BAT脚本管理 (2 commits)**: 清理重复脚本，v2.0统一版本

### 重构
- **P0-P3巨型函数拆分 (f4ea993)**: 9个超300行函数拆分为编排函数+45个子函数
- **dashboard.py拆分 (dc146e0)**: dashboard.py(2521L)→dashboard.py+data_loader.py(946L)+sidebar.py(199L)
- **Tab签名统一 (4772be5)**: 14个Tab全部改为无参数render_tabN()
- **DB连接统一 (63a99cd)**: 72处sqlite3.connect→get_db_connection()
- **Tab注册自动化 (8237fda)**: TAB_REGISTRY插件式注册，删除14个wrapper函数
- **Selenium→Playwright (d5760b5/19d1720)**: 截图/PDF集中到src/utils/screenshot.py
- **UI组件库标准化 (8c25184)**: render_chart(118处)+render_empty_state(22处)+清理死代码
- **数据加载层抽象 (30dbe7a)**: Repository模式，11个tab重复函数委托到data_loader(-603行)
- **配置硬编码清理 (116de58)**: cache_ttl/downsample/days_window统一到settings.py
- **可扩展性提升 (d188b11)**: 5个dataclass模型替代裸dict返回
- **异常处理三轮细化 (b7ec607/b9ced3e/fac5cc8+5ad3940+97200a7)**: 198→144→80→0处宽泛except Exception
- **tab3拆分 (134fee1/9b5548b)**: 拆为4个子模块+alert_center编排函数
- **tab1大函数拆分 (24f3652)**: _render_multi_benchmark_analysis 323L→5子函数
- **src/代码量精简 (94c4ebe)**: 91个unused imports+13个unused variables+pyflakes 110→0

### 测试
- **测试总数 655→1307 (+652)**: 纯函数测试大幅扩充
- **测试覆盖率提升(3轮)**: 50.6%→53%→72%（gold_utils 44%→92%, news_fetcher 17%→66%）
- **P3模块测试 (fc129a0)**: 55用例覆盖ERP/定投/景气度/预警
- **Tab15测试 (722bb32)**: 24用例/6类
- **ETF F10测试 (59b2638)**: 基本面数据集成测试
- **ETF技术信号测试 (26d30a8)**: 48用例

### 变更
- **Dashboard Tab**: 14→15个（新增Tab15交易复盘）
- **数据库表**: 20→26个（新增etf_fundamental/etf_industry_alloc/etf_top_holdings/indicator_backtest_results/trade_records/_migration_version）
- **数据行数**: 320,000+→350,000+
- **代码总量**: ~44,000L→48,500L/180文件
- **浏览器驱动**: Selenium+ChromeDriver→Playwright
- **测试框架**: pytest 1435用例，全部通过

---

## [v2.2] - 2026-06-04

### 新增
- **D1 闲置数据分析激活**: 融资融券/机构调研/大宗交易 12.8 万行数据接入 advisor 分析引擎，新增 3 个分析方法和 20 个测试
- **D2 数据源修复**: 重写股东增减持采集器，修复 4 个 STALE 数据源，增加自动健康检查
- **D3 闭环反馈**: advice_history 表写入打通（13 列 31 行），Tab8 反馈面板 SQL 修复，追踪"建议→执行→效果"完整链条
- **D4 告警多样化**: Monitor DEFAULT_RULES 从 5 条扩展到 9 条（新增数据源中断/数据质量/持仓变化/总市值变化），添加去重逻辑 `_get_recent_alert_rules`
- **D5 配置环境变量化**: settings.py 添加 `_load_env_file()` + `env()` 辅助函数，敏感配置（邮箱密码/webhook/数据库路径等）支持 .env 文件覆盖，创建 .env.example 模板
- **D6 根目录清理**: 20 个根目录 .py 文件缩减至 2 个核心文件，创建 scripts/backfill/（6 个脚本）+ scripts/setup/，统一 backfill 入口 scripts/run_backfill.py，归档 14 个遗留脚本到 archive/
- **D7**: 确认已在 D3 完成（advice_history 闭环写入）
- **D8 日志轮转**: run_analysis.py 和 run_backfill.py 改用 RotatingFileHandler（10MB，5 备份）
- **D9 回测策略扩充**: backtest.py 新增 `backtest_momentum`（动量策略）+ `backtest_mean_reversion`（均值回归策略），策略数 3→5
- **D10 容器化 CI**: 创建 Dockerfile（python:3.12-slim）+ GitHub Actions CI pipeline（pytest on push/PR）
- **54 个 P2 辅助模块测试**: 覆盖 11 个辅助模块（531→585 total）
- **P0+P1 关键测试**: 55 个测试用例覆盖 Monitor/Notification/SmartReport/RunAnalysis 核心链路

### 修复
- 消除全部 15 个 pytest warnings（477 passed，0 warnings）
- 资金流阈值单位校准（亿元→元）和显示格式统一
- advice 反馈 SQL 列名不匹配修复（3 处）
- 融资融券数据单位统一（亿元→元）
- ETF 资金流 keep_cols 缺失列补全（超大/大/中/小流入）
- start_with_venv.bat 引用路径更新
- news sentiment 分析集成 + Plotly range 弃用迁移

### 变更
- 测试总数 421 → 655（+234 tests）
- 数据库表 advice_history schema 扩展（+4 列：status/action_taken/feedback/resolved_at）
- .gitignore 添加 .env/.env.local/archive/ 排除规则

---

## [v2.1] - 2026-05-13

### 新增
- **黄金市场分析 Tab (Tab11)**: 上海金交所金价 K 线走势、实时分时行情、SPDR Gold Trust 持仓趋势、中国黄金储备图表
- **行业资金流历史回填**: 基于同花顺多周期排行差值分解法，自动回填行业历史资金流数据
- **自动增量回填集成**: 行业资金流回填集成到每日定时任务，自动跳过已有日期

### 修复
- 资金流趋势图/热力图过滤数据稀疏行业（最低 10 天数据覆盖要求）
- ETF 资金流查询去掉 LIMIT 2000 截断
- 同花顺资金流数据单位统一（亿元 → 元）
- requests Session monkey-patch 递归 Bug
- Plotly 6.x titlefont 废弃属性迁移为 title_font_color
- 中国黄金储备日期解析支持 YYYY年MM月份 格式

---

## [v2.0] - 2026-05-11

### 新增
- **Phase 5 全面升级**:
  - Tab9 自定义指标工作台（模板回测、信号追踪）
  - Tab10 资金流分析（主力资金/行业资金流/ETF 资金流三维度）
  - 组合归因深化（Brinson 分解、风格因子暴露）
  - 数据导出增强（CSV 一键导出持仓/摘要/指标）
- **数据采集层**: 资金流数据采集双方案 fallback + push2his 封锁监测工具
- **行业资金流**: TOP10 时间趋势折线图、持仓 ETF 合计资金流日净流入趋势图

### 修复
- 夏普比率计算错误及数据质量问题
- 新闻资讯去重 + 减少页面显示条目
- 资金流模块代理拦截 + 行业资金流接口切换
- 指标回测模板条件不匹配（趋势方向/信号类型对齐）
- 多处变量名错误修复（csv1/df_prev1/current_mv1/prev_mv1/bench_prev1）
- 行业涨跌热力图条形不可见问题
- 智能分析报告风险指标为 0 和行业热力图收益率异常

---

## [v1.7] - 2026-05-10

### 新增
- Tab7 资讯与评估面板
- Tab8 操作建议面板
- Tab4 事件日历
- 侧边栏增强、持仓技术信号列、一键报告导出

---

## [v1.6] - 2026-05-09

### 新增
- Tab1 基准对比表
- Tab2 累计盈亏柱状图
- Tab3 风险提示面板
- Tab6 技术信号面板（概览卡片、雷达图、信号柱状图、HTML 详情表、布林带/RSI 分布图）

---

## [v1.5] - 2026-05-09

### 新增
- Tab4 事件日历模块
- Tab6 技术信号模块
- 项目文档

---

## [v1.3] - 2026-04-30

### 新增
- Dashboard 性能优化（缓存/降采样/索引/日期选择器）
- 完整历史数据回填脚本（新浪 K 线接口）
- 收益日历模块
- 持仓相关性矩阵、ETF 详情抽屉、多基准指数选择器
- 截图和 PDF 导出（Selenium + webdriver-manager）
- 模块标题和指标卡片 hover 提示

### 修复
- 每日快照写入防陈旧机制
- 废弃 API 调用和 SQL 注入风险消除
- Dashboard 3 个运行时错误修复
- daily_return 数据不一致修复

---

## [v1.0] - 2026-04-26

### 新增
- 投资组合跟踪分析系统基线版本
- Streamlit Dashboard（11 个标签页）
- SQLite 数据库（20 张表）
- 自动化数据采集与分析流程
