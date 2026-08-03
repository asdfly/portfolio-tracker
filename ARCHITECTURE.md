# 架构文档

## 系统架构概览

```
┌─────────────────────────────────────────────────────┐
│                   用户交互层                          │
│  ┌──────────┐  ┌──────────┐  ┌───────────────────┐  │
│  │Dashboard  │  │run_all.bat│  │scheduled_run.bat  │  │
│  │(Streamlit)│  │(交互菜单) │  │(Windows定时任务)   │  │
│  └────┬─────┘  └────┬─────┘  └────────┬──────────┘  │
│       │              │                  │             │
│  ┌────▼─────────────▼──────────────────▼──────────┐  │
│  │              dashboard.py / run_analysis.py     │  │
│  │         (TAB_REGISTRY 插件式注册/五阶段流水线)    │  │
│  └────────────────────┬───────────────────────────┘  │
├───────────────────────┼─────────────────────────────┤
│              数据加载层 (data_loader.py)               │
│  ┌────────────────────▼───────────────────────────┐  │
│  │  Repository 模式: 11 Tab 统一委托 → 单一实现     │  │
│  │  load_positions / load_summary / load_technical │  │
│  │  compute_monthly_returns / compute_rebalance... │  │
│  └────────────────────┬───────────────────────────┘  │
├───────────────────────┼─────────────────────────────┤
│              分析引擎层 (src/analysis/)                │
│  ┌─────────┬─────────┬──────────┬─────────────────┐  │
│  │风险分析  │技术分析  │智能建议   │因子归因/回测     │  │
│  │risk.py  │technical│advisor   │factor_attr      │  │
│  │portfolio│_py      │_py       │backtest.py      │  │
│  │_risk.py │         │position  │candle_patterns  │  │
│  │         │         │_advisor  │multi_factor     │  │
│  └─────────┴─────────┴──────────┴─────────────────┘  │
├─────────────────────────────────────────────────────┤
│              数据采集层 (src/data_sources/)            │
│  ┌──────┬──────┬──────┬──────┬──────────────────┐   │
│  │新浪   │AKShare│同花顺│东方财富│上海金交所/SPDR   │   │
│  │sina.py│akshare│fund  │market│gold_utils.py    │   │
│  │       │_ds.py │_flow │_event│                 │   │
│  └──────┴──────┴──────┴──────┴──────────────────┘   │
├─────────────────────────────────────────────────────┤
│              存储层 (SQLite)                           │
│  ┌──────────────────────────────────────────────┐    │
│  │  portfolio.db (30 张表)                       │    │
│  │  get_db_connection() 统一连接管理              │    │
│  └──────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────┘
```

## 分层架构

### 1. 用户交互层

| 入口 | 文件 | 说明 |
|------|------|------|
| Dashboard | `dashboard.py` | Streamlit Web 界面，15 个 Tab，`TAB_REGISTRY` 插件式注册 |
| 交互菜单 | `run_all.bat` | 9 选项 Windows 批处理菜单 |
| 定时任务 | `scheduled_run.bat` | Windows 任务计划程序入口，备份+分析 |
| 分析流水线 | `run_analysis.py` | 五阶段：基础持仓 → 风险分析 → 监控告警 → 智能建议 → 通知报告 |

### 2. 数据加载层 (data_loader.py)

采用 Repository 模式，作为所有数据访问的统一入口：

- **持仓数据**: `load_positions()` — 自动回退到最近有数据的日期
- **组合汇总**: `load_summary()` — 含 daily_return、max_drawdown、snapshot_type
- **技术指标**: `load_technical()` — 日期回退机制，T+1 采集兼容
- **月度收益**: `compute_monthly_returns()` — daily_return 连乘法
- **日历数据**: `load_calendar_data()` — 年/月/日/星期/盈亏
- **再平衡建议**: `compute_rebalance_suggestion()` — dataclass 返回
- **仓位建议**: `load_position_advices()` — 多因子评分 → 目标占比

### 3. 分析引擎层 (src/analysis/)

| 模块 | 职责 | 关键算法 |
|------|------|---------|
| `portfolio.py` | 组合分析 | 共同持仓法日收益率 |
| `portfolio_risk.py` | 风险指标 | corrected 累积净值回撤 |
| `advisor.py` | 智能建议 | 17 方法/13 步骤 |
| `position_advisor.py` | 仓位管理 | 评分→操作→相对比例目标占比 |
| `multi_factor_score.py` | 多因子评分 | 资金30%+估值25%+技术25%+风险20% |
| `backtest.py` | 策略回测 | 5 种再平衡策略 |
| `factor_attribution.py` | 因子归因 | OLS 回归，ddof=1 |
| `candle_patterns.py` | K 线形态 | 88% 覆盖率 |
| `equity_risk_premium.py` | ERP | 100% 覆盖率 |
| `dca_backtest.py` | 定投回测 | 均匀/估值策略 |
| `industry_boom.py` | 行业景气度 | 4 维评分 |

### 4. 数据采集层 (src/data_sources/)

| 模块 | 数据源 | 采集内容 |
|------|--------|---------|
| `sina.py` | 新浪财经 | ETF 日 K 线（主数据源） |
| `akshare_ds.py` | AKShare | ETF 行情（备用）+ 宏观数据 |
| `fund_flow.py` | 同花顺/东方财富 | 行业/ETF/主力/北向资金流 |
| `market_events.py` | AKShare | 涨停/融资/股东/机构/大宗 |
| `etf_fundamental.py` | AKShare | ETF F10（持仓/行业/估值/规模） |
| `news_fetcher.py` | 网络 | 新闻抓取+SnowNLP 情感分析 |
| `valuation_percentile.py` | AKShare | PE 历史分位数 |

### 5. 存储层

- **数据库**: SQLite，`data/database/portfolio.db`
- **连接管理**: `get_db_connection()` 统一管理，支持 `db_path` 参数
- **DDL 管理**: `src/utils/db_schema.py` 集中定义 30 张表
- **备份**: `VACUUM INTO` 每日备份，保留 7 天
- **数据质量**: `data_quality.py` 评估新鲜度(40%)+覆盖度(30%)+回测度(30%)

## 代码规范

| 规范 | 工具 | 配置 |
|------|------|------|
| 格式化 | black | line-length=120, target py39 |
| 排序 | isort | profile=black, line_length=120 |
| 类型检查 | mypy | python_version=3.9, ignore_missing_imports |
| 安全检查 | bandit | skip B101 |
| 测试 | pytest | testpaths=tests, -v --tb=short |
| Pre-commit | py_compile + test_imports | 语法检查+导入快速门禁 |

## 异常处理策略

- 裸 `except`: 0（完全消除）
- 宽泛 `except Exception`: 0（三次细化完成）
- 具体异常类型: 340 处（DB: sqlite3.Error, 数据: KeyError/ValueError/TypeError, 网络: ConnectionError/OSError）
- UI 降级: `render_empty_state()` 26 处统一空态展示

## UI 组件标准化

- `render_chart(fig)`: 121 处调用，统一 Plotly 图表渲染
- `render_empty_state(msg)`: 26 处调用，统一空数据展示
- 直接 `st.plotly_chart`: 仅 5 处（特殊场景）
