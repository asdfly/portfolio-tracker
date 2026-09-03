# Portfolio Tracker — ETF 投资组合智能分析系统

基于 Python + Streamlit 的 ETF 投资组合自动化跟踪分析系统。覆盖数据采集、技术分析、风险评估、资金流监控、黄金市场分析、智能建议等全链路能力，提供交互式 Dashboard 和每日定时任务。

## 核心数据

| 指标 | 数值 |
|------|------|
| 数据起始 | 2012-05-28 |
| 数据库表 | 39 张 |
| 交易记录 | 1,256 条（2023-06-28 ~ 2026-08-31） |
| 持仓快照 | 35,607 条（2012-05-28 ~ 2026-09-03；全历史 36 只标的 = 23 只场内 ETF + 13 只场外基金，当前持仓 22 只且已全为场内 ETF） |
| 技术指标 | 34,892 条（23 只 ETF，跨 3,447 个交易日） |
| 代码规模 | 227 文件 / 64,758 行 Python（不含 venv313 / archive） |
| 测试用例 | 约 1,570 个 / 85 个测试文件（2026-08-26 快照） |
| Git 提交 | 336 次 |

## 功能概览

### Streamlit Dashboard（17 个分析 Tab）

| Tab | 名称 | 核心功能 |
|-----|------|----------|
| 1 | 📈 净值走势 | 累计净值曲线、多基准对比、区间收益分析、年度收益图、日收益率分布 |
| 2 | 📊 持仓分布 | 饼图、行业分布、相关性矩阵、HHI 集中度、Beta 贡献、交易历史（全部 11 种交易类型） |
| 3 | ⚠️ 风险分析 | 夏普/索提诺/卡玛比率、VaR、最大回撤、压力测试、风险预警 |
| 4 | 📅 收益日历 | 年度/月度收益概览（daily_return 连乘法）、日历热力图 |
| 5 | 💠 高级分析 | 因子归因、Brinson 分解、Monte Carlo 模拟、VaR 直方图、再平衡模拟 |
| 6 | 📡 技术信号 | 雷达图、信号柱状图、布林带/RSI 分布 |
| 7 | 📰 资讯与评估 | 自动新闻聚合、SnowNLP+jieba 情感评分、市场情绪评估 |
| 8 | 💡 操作建议 | 智能建议引擎（17 步骤/13 维度）、多因子评分、仓位管理建议、信号方向+矛盾标注、置信度交互筛选 |
| 9 | 🔬 自定义指标 | 技术指标回测、K 线形态识别、DB 回测历史 |
| 10 | 💰 资金动向 | 行业资金流趋势/热力图、ETF 资金流、主力资金、北向资金 |
| 11 | 🥇 黄金市场 | 10 个子 Tab（金价走势/实时行情/基准价对比/季节性/技术信号/定价因子/储备分析/央行购金/供需平衡/国际对比） |
| 12 | 🌐 宏观市场 | 宏观数据面板（汇率/债市/金价基准/利率/融资融券） |
| 13 | 📊 数据质量 | 数据质量评分环、新鲜度热力图、覆盖度表格、回测摘要 |
| 14 | 📋 市场事件 | 涨停板/融资融券/股东增减持/机构调研/大宗交易深度分析 |
| 15 | 🔁 交易复盘 | 交易历史统计、盈亏分析、定投基金追踪（11 只）、月度资金流向（天添利/银转存独立归类） |
| 16 | 🔮 ETF 风险展望 | 波动率预测（分位 / 高低波动分类 / 回测 AUC）+ 历史回撤参照；walk-forward 验证 OOS R² 0.44–0.89、AUC 0.90–0.97 达标才上线；回撤幅度预测 R² 全负不达标，仅作历史回撤参照（非涨跌预测） |
| 17 | 🎯 高低位定位 | ETF 状态定位器（描述"现在处于历史什么位置"，非方向预测）；三因子集成输出统一度量 P∈[-100,+100]、置信度 C∈[0,1]；F2 估值因子因 250 交易日数据门控自动禁用（index_pe_history 仅积累约 1 个月） |

> **⚠️ ETF 数量有多个口径，不要混用**（数字不一致不是 bug）。以下均为 2026-08-26 实测：
>
> | 数字 | 含义 | 出处 |
> |------|------|------|
> | **36** | 全历史出现过的**全部标的** = 23 只场内 ETF + 13 只场外标的 | `portfolio_snapshots` distinct `code` |
> | **23** | 场内 ETF 全集，等于 `config/settings.py` 的 `ETF_CATEGORIES` | `etf_technical` distinct `code`（两集合实测完全相等） |
> | **22** | 预测底座 / 风险模型覆盖，随调仓自动增减 | `etf_features`、`etf_price_history`（= 23 减去已清仓的 `159732`） |
> | **25** | ⚠️ **不可用作分母**，含 2 只场外混合基金脏数据 | `etf_fundamental`（详见 `docs/handover/07_known_data_issues.md`） |
>
> **两个数数陷阱**（踩过，勿重复）：
> - 用代码正则 `^(5\d{5}|1[56]\d{4})$` 判 ETF 会得到 25 只 —— 假阳性 `166301`（华商新趋势优选混合 LOF）、`519770`（交银优择回报混合A，`519xxx` 是场外代码段）。**注意这个 25 与 `etf_fundamental` 的 25 数值相同但集合不同**，纯属巧合。
> - 用"名称含 ETF 字样"判则得到 22 只 —— 假阴性 `512810`，它是真 ETF（华宝中证军工 ETF），但库里存的是简称"国防军工"。
> - 两个判据各自修掉自己的错之后**都收敛到同一个 23 只集合**，即 `etf_technical`。判定 ETF 请以配置/`etf_technical` 为准，不要现场拍正则。
> - 另外按 `(code, name)` 去重会得到 59 条，是**同一代码存在简称/全称两种写法**造成的假象（例：`159220` 同时存在"港股通红利低波ETF华宝"与"港红利"），不是标的数。

### 数据采集

| 数据类型 | 数据源 | 说明 |
|----------|--------|------|
| ETF 行情 | 新浪财经(主) + AKShare(备) | 日 K 线、收盘价、成交量 |
| 技术指标 | 自动计算 | MA/MACD/RSI/KDJ/布林带/ATR |
| 行业资金流 | 同花顺 | 90 个行业板块主力净流入 |
| ETF 资金流 | AKShare + 估算 | 23 只 ETF 净流入/流出 |
| 主力资金 | 东方财富 | 大单/中单/小单净流入 |
| 北向资金 | 东方财富 | 沪股通+深股通合并 |
| 市场事件 | AKShare | 涨停板、融资融券、股东增减持、机构调研、大宗交易 |
| 黄金行情 | 上海金交所 | Au99.99/Au99.95/Au(T+D) 历史 K 线+实时分时 |
| 黄金持仓 | SPDR/央行 | SPDR Gold Trust 持仓、中国黄金储备 |
| 定价因子 | AKShare | 中美国债收益率利差、CPI、Shibor |
| ETF 基本面 | AKShare | F10 数据（持仓/行业配置/估值/规模） |
| 交易记录 | 通达信对账单(PDF) + 手动导入 | 11 种交易类型（买卖/定投/申购/赎回/红利/股息/银行转存等） |

### 智能分析引擎

- **风险指标**: 夏普比率、索提诺比率、卡玛比率、VaR(95%/99%)、CVaR、Beta/Alpha、最大回撤
- **集中度分析**: HHI 指数、行业暴露度（超限/偏高/正常三级）
- **仓位管理建议**: 多因子评分 → 评分区间映射 → 加仓/维持/减仓建议，目标占比按相对比例计算
- **信号回测**: 4 轮迭代（v1 展示 → v2 置信度 → v3 Per-ETF+组合 → v4 强度分级+滚动窗口），12,296 组回测
- **策略回测**: 5 种再平衡策略（买入持有/定期/阈值/动量/均值回归）
- **再平衡引擎**: 4 种调仓策略（threshold/periodic/equal_weight/layered），默认 `layered` 分层配置——按类别基准权重（宽基35%/债券20%/医药10%…）+ 类内市值占比分配，非等权；含换手率与交易成本估算，T+1 执行
- **告警系统**: 9 条自动监控规则（数据源中断/数据质量/持仓变化/市值变化/回撤/集中度/夏普/波动率/异常），告警去重
- **P3 高级功能**: ERP 股债性价比、定投回测对比、行业景气度指标、智能预警推送

## 项目结构

```
portfolio_tracker/
├── config/
│   ├── settings.py              # 全局配置（支持 .env 环境变量覆盖）
│   └── notification.json        # 通知渠道配置（email / wechat 开关）
├── src/
│   ├── data_sources/            # 数据采集层（12 模块）
│   │   ├── base.py / sina.py / akshare_ds.py
│   │   ├── fund_flow.py         # 资金流采集（行业/ETF/主力/北向）
│   │   ├── market_events.py     # 市场事件采集
│   │   ├── macro_daily.py       # 宏观日度数据
│   │   ├── etf_fundamental.py   # ETF F10 基本面采集
│   │   ├── valuation_percentile.py  # PE 历史分位数
│   │   └── news_fetcher.py      # 新闻资讯抓取+情感分析
│   ├── analysis/                # 分析引擎（12 模块）
│   │   ├── technical.py         # 技术指标计算
│   │   ├── portfolio.py / portfolio_risk.py / risk.py
│   │   ├── advisor.py           # 智能建议引擎（SmartAdvisor 29 方法；analyze_portfolio 编排 19 个分析步骤）
│   │   ├── position_advisor.py  # 仓位管理建议（评分→操作→目标占比）
│   │   ├── multi_factor_score.py # 多因子评分（资金+估值+技术+风险）
│   │   │   ├── backtest.py          # 策略回测引擎（5 种策略）
│   │   ├── rebalance_engine.py  # 再平衡引擎（分层/阈值/周期/等权策略）
│   │   ├── nav_engine.py        # 单位净值（TWR）账本
│   │   ├── stats_utils.py       # 统计工具（Newey-West HAC / FDR 校正）
│   │   ├── factor_attribution.py # 因子归因（OLS 回归）
│   │   ├── candle_patterns.py   # K 线形态识别
│   │   ├── dca_backtest.py      # 定投回测对比
│   │   ├── equity_risk_premium.py # ERP 股债性价比
│   │   ├── industry_boom.py     # 行业景气度（4 维评分）
│   │   └── market_event_signals.py
│   ├── report/                  # 报告生成（Excel/risk/smart）
│   ├── utils/                   # 工具层（15 模块）
│   │   ├── database.py          # DB 连接统一管理（get_db_connection）
│   │   ├── db_schema.py         # DDL 定义
│   │   ├── monitor.py           # 运行监控（9 条规则+去重）
│   │   ├── notification.py      # 通知管理（邮件/企业微信）
│   │   ├── data_quality.py     # 数据质量评估（新鲜度40+覆盖度30+回测度30）
│   │   ├── trade_importer.py    # 对账单 PDF 导入
│   │   ├── position_reader.py   # 通达信持仓读取
│   │   ├── chart_utils.py       # 图表工具
│   │   ├── trading_calendar.py # 本地交易日历（离线，内置 2024-2026 休市）
│   │   ├── backfill.py          # 数据回填
│   │   └── screenshot.py        # Playwright 截图/PDF
│   └── models.py                # 5 个 dataclass（RiskMetrics/MonteCarloResult/...）
├── tabs/                        # Streamlit Tab 渲染层（36 文件）
│   ├── tab1_net_value.py ~ tab17_etf_position.py
│   ├── tab3_risk_*.py           # 风险分析拆分（5 子模块）
│   ├── _helpers.py              # Tab 共享 UI 组件
│   └── gold_components/         # 黄金 Tab 子组件（13 模块）
├── dashboard.py                 # Streamlit 主入口（TAB_REGISTRY 插件式注册）
├── data_loader.py               # 数据加载层（Repository 模式，统一数据入口）
├── sidebar.py                   # 侧边栏导航
├── run_analysis.py              # 定时分析任务（五阶段流水线）
├── scripts/
│   ├── backfill/                # 历史数据回填（6 脚本）
│   ├── backup_db.py             # 数据库备份
│   ├── send_report_email.py     # 收盘日报邮件自动推送（SMTP）
│   └── setup/                   # 通知配置向导
├── tests/                       # 测试套件（85 文件 / 约 1,570 用例，2026-08-26 快照）
├── run_all.bat                  # 交互式启动菜单（9 选项）
├── scheduled_run.bat            # 定时任务入口（备份+分析）
├── pyproject.toml               # black/isort/mypy/bandit 配置
├── pytest.ini                   # pytest 配置
├── requirements.txt             # 运行依赖
└── requirements-dev.txt         # 开发依赖
```

## 快速开始

### 环境要求

- Python 3.10+
- Windows（通达信导出路径依赖）/ Linux / macOS
- 依赖包：见 `requirements.txt`

### 安装

```bash
# 克隆项目
git clone <repo-url>
cd portfolio_tracker

# 安装依赖
pip install -r requirements.txt

# 安装 Playwright 浏览器（截图/PDF 导出用）
python -m playwright install chromium

# 安装 pre-commit hook（开发用）
pip install -r requirements-dev.txt
pre-commit install
```

### 运行

```bash
# 方式一：交互式菜单（推荐）
run_all.bat

# 方式二：直接启动 Dashboard
python -m streamlit run dashboard.py --server.port 8501

# 方式三：定时分析任务
python run_analysis.py

# 方式四：Windows 定时任务
scheduled_run.bat
```

访问 http://localhost:8501 查看 Dashboard。

### 定时任务配置

通过 Windows 任务计划程序配置 `scheduled_run.bat`：

| 任务 | 时间 | 说明 |
|------|------|------|
| #1 盘前数据采集 | 08:00 | ETF 行情+技术指标+资金流 |
| #2 盘后完整分析 | 15:30 | 五阶段流水线（基础+风险+监控+智能+通知） |
| #3 数据库备份 | 15:25 | VACUUM INTO 生成每日备份，保留 7 天 |
| #4 早盘采集 | 08:00 | 补充盘前数据 |

调度按 cron 周一至周五执行。

## 邮件自动推送（收盘日报）

每日 15:30 收盘分析完成后，自动将当日报告推送到邮箱（HTML 可视化报告 + Markdown 摘要双附件），无需人工确认。

配置在 `.env`（已被 `.gitignore` 忽略，不会入库）：

| 变量 | 说明 |
|------|------|
| `EMAIL_ENABLED` | 设为 `true` 启用推送 |
| `EMAIL_SMTP_SERVER` | SMTP 服务器（默认 `smtp.qq.com`） |
| `EMAIL_SMTP_PORT` | 端口（587 = STARTTLS） |
| `EMAIL_USERNAME` | 发件人邮箱 |
| `EMAIL_PASSWORD` | QQ 邮箱授权码（非登录密码，在 QQ 邮箱「设置 → 账户 → 开启 SMTP」生成） |
| `EMAIL_RECIPIENTS` | 收件人列表（逗号分隔） |

调度链路：`scheduled_run.bat` 在 `run_analysis.bat` 之后调用 `send_report_email.bat`，复用现有 Windows 定时任务（15:30 触发），无需新建计划任务。`EMAIL_PASSWORD` 为空时脚本自动跳过（exit 0），不会报错或刷失败日志。

## 数据库

SQLite 数据库位于 `data/database/portfolio.db`，包含 39 张表：

| 分类 | 表名 | 说明 |
|------|------|------|
| 核心 | portfolio_snapshots | 持仓快照（35K 行，含场内+场外） |
| 核心 | portfolio_summary | 组合汇总（3.4K 行，含 snapshot_type） |
| 核心 | trade_records | 交易记录（1,256 条，11 种类型） |
| 行情 | etf_technical | 技术指标（34K 行，23 只 ETF） |
| 行情 | index_quotes | 指数行情（50K 行） |
| 资金 | fund_flows | 资金流（24K 行） |
| 资金 | stock_margin | 融资融券（210K 行） |
| 事件 | stock_lhb / stock_block_trade / stock_holder_change / stock_institution_research | 市场事件 |
| 黄金 | gold_sge_hist / gold_etf_holdings | 黄金行情+持仓 |
| 分析 | signal_backtest_stats / signal_confidence_current | 信号回测 |
| 分析 | advice_history | 建议历史（1,789 条） |
| 系统 | alerts / execution_logs | 告警+执行日志 |

## 测试

```bash
# 全量测试
python -m pytest tests/ -v

# 快速门禁（pre-commit hook）
python -m pytest tests/test_imports.py -v

# 特定模块
python -m pytest tests/test_p1_position_valuation.py -v
python -m pytest tests/test_bugfix_round4.py -v
```

## 架构要点

- **数据加载层**: `data_loader.py` 采用 Repository 模式，11 个 Tab 的重复查询函数统一委托到单一实现
- **Tab 注册**: `TAB_REGISTRY` 插件式注册，17 个 Tab 无参数签名 `render_tabN()`
- **DB 连接**: 统一 `get_db_connection()`，仅 `data_loader.py` 实现内部保留 1 处 `sqlite3.connect`
- **异常处理**: 裸 `except` = 0，宽泛 `except Exception` = 0（三次细化完成），340 处具体异常类型
- **UI 标准化**: `render_chart`（121 处）+ `render_empty_state`（26 处），统一图表/空态渲染
- **Dataclass**: 5 个数据结构替代裸 dict 返回，支持 `__getitem__`/`get`/`keys` 兼容访问
- **配置管理**: 支持 `.env` 环境变量覆盖，`settings.py` 集中管理

## 关键算法说明

- **日收益率**: 共同持仓法（前日 qty × 当日 price / 前日市值），不受追加投入/赎回/快照类型切换影响
- **月度收益**: `daily_return` 连乘法，避免 `total_value` 比值法将追加投入误计为收益
- **最大回撤**: 基于 corrected 累积净值（daily_return 连乘），60 日窗口
- **夏普比率**: 扣除无风险利率 Rf=2.5%，ddof=1 样本标准差
- **仓位建议**: 多因子评分 → 5 档区间映射 → 目标占比 = 当前占比 × (1 ± 调整比例)

## 许可证

私有项目，未开源。许可证见仓库 LICENSE 文件（MIT）。
