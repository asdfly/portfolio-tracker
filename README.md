# Portfolio Tracker — ETF 投资组合智能分析系统

基于 Python + Streamlit 的 ETF 投资组合自动化跟踪分析系统。覆盖数据采集、技术分析、风险评估、资金流监控、黄金市场分析、智能建议等全链路能力，提供交互式 Dashboard 和每日定时任务。

## 核心数据

| 指标 | 数值 |
|------|------|
| 数据起始 | 2012-05-28 |
| 数据库表 | 30 张 |
| 交易记录 | 1,157 条（2023-06 ~ 2026-07） |
| 持仓快照 | 35,101 条（含 22 只场内 ETF + 11 只场外基金） |
| 技术指标 | 34,408 条（23 只 ETF × 1,496 交易日） |
| 代码规模 | 198 文件 / 54,552 行 Python |
| 测试用例 | 1,413 个（75 个测试文件） |
| Git 提交 | 304 次 |

## 功能概览

### Streamlit Dashboard（15 个分析 Tab）

| Tab | 名称 | 核心功能 |
|-----|------|----------|
| 1 | 净值走势 | 累计净值曲线、多基准对比、区间收益分析、年度收益图、日收益率分布 |
| 2 | 持仓分布 | 饼图、行业分布、相关性矩阵、HHI 集中度、Beta 贡献、交易历史（全部 11 种交易类型） |
| 3 | 风险分析 | 夏普/索提诺/卡玛比率、VaR、最大回撤、压力测试、风险预警 |
| 4 | 收益日历 | 年度/月度收益概览（daily_return 连乘法）、日历热力图 |
| 5 | 高级分析 | 因子归因、Brinson 分解、Monte Carlo 模拟、VaR 直方图、再平衡模拟 |
| 6 | 技术信号 | 雷达图、信号柱状图、布林带/RSI 分布 |
| 7 | 资讯与评估 | 自动新闻聚合、SnowNLP+jieba 情感评分、市场情绪评估 |
| 8 | 操作建议 | 智能建议引擎（17 步骤/13 维度）、多因子评分、仓位管理建议、信号方向+矛盾标注、置信度交互筛选 |
| 9 | 自定义指标 | 技术指标回测、K 线形态识别、DB 回测历史 |
| 10 | 资金动向 | 行业资金流趋势/热力图、ETF 资金流、主力资金、北向资金 |
| 11 | 黄金市场 | 10 个子 Tab（金价走势/实时行情/基准价对比/季节性/技术信号/定价因子/储备分析/央行购金/供需平衡/国际对比） |
| 12 | 宏观经济 | 宏观数据面板（汇率/债市/金价基准/利率/融资融券） |
| 13 | 数据质量 | 数据质量评分环、新鲜度热力图、覆盖度表格、回测摘要 |
| 14 | 市场事件 | 涨停板/融资融券/股东增减持/机构调研/大宗交易深度分析 |
| 15 | 交易复盘 | 交易历史统计、盈亏分析、定投基金追踪（11 只）、月度资金流向（天添利/银转存独立归类） |

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
- **告警系统**: 9 条自动监控规则（数据源中断/数据质量/持仓变化/市值变化/回撤/集中度/夏普/波动率/异常），告警去重
- **P3 高级功能**: ERP 股债性价比、定投回测对比、行业景气度指标、智能预警推送

## 项目结构

```
portfolio_tracker/
├── config/
│   ├── settings.py              # 全局配置（支持 .env 环境变量覆盖）
│   └── db_schema.py             # 数据库 DDL 集中定义（30 张表）
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
│   │   ├── advisor.py           # 智能建议引擎（17 方法/13 步骤）
│   │   ├── position_advisor.py  # 仓位管理建议（评分→操作→目标占比）
│   │   ├── multi_factor_score.py # 多因子评分（资金+估值+技术+风险）
│   │   ├── backtest.py          # 策略回测引擎（5 种策略）
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
│   │   ├── backfill.py          # 数据回填
│   │   └── screenshot.py        # Playwright 截图/PDF
│   └── models.py                # 5 个 dataclass（RiskMetrics/MonteCarloResult/...）
├── tabs/                        # Streamlit Tab 渲染层（33 文件）
│   ├── tab1_net_value.py ~ tab15_trade_review.py
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
│   └── setup/                   # 通知配置向导
├── tests/                       # 测试套件（75 文件 / 1,413 用例）
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

## 数据库

SQLite 数据库位于 `data/database/portfolio.db`，包含 30 张表：

| 分类 | 表名 | 说明 |
|------|------|------|
| 核心 | portfolio_snapshots | 持仓快照（35K 行，含场内+场外） |
| 核心 | portfolio_summary | 组合汇总（3.4K 行，含 snapshot_type） |
| 核心 | trade_records | 交易记录（1,157 条，11 种类型） |
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
- **Tab 注册**: `TAB_REGISTRY` 插件式注册，14 个 Tab 无参数签名 `render_tabN()`
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

私有项目，未开源。
