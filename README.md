# 投资组合跟踪分析系统

## 项目简介

基于Python的投资组合自动化跟踪分析系统，支持多数据源、丰富的技术指标、数据持久化和定时任务。

## 功能特性

### 阶段一已实现功能

- [x] **多数据源支持**: 新浪财经(主) + AKShare(备用)
- [x] **数据源自动切换**: 故障时自动切换到备用源
- [x] **技术指标增强**: 
  - MA均线(5/20日)
  - MACD(12/26/9)
  - RSI(14日)
  - KDJ(9/3/3)
  - 布林带(20日, 2倍标准差)
  - ATR(14日)
  - 成交量均线
- [x] **数据持久化**: SQLite数据库存储历史数据
- [x] **持仓数据读取**: 通达信持仓文件解析
- [x] **自动异常处理**: 请求失败自动重试

### 待实现功能

- [ ] 风险指标计算(夏普比率、最大回撤等)
- [ ] 报告生成(HTML/PDF)
- [ ] 通知机制(邮件/企业微信)
- [ ] Windows定时任务自动部署
- [ ] 策略回测框架

## 项目结构

```
portfolio_tracker/
├── config/
│   └── settings.py          # 配置文件
├── src/
│   ├── data_sources/        # 数据源模块
│   │   ├── base.py          # 数据源基类
│   │   ├── sina.py          # 新浪财经
│   │   ├── akshare_ds.py    # AKShare
│   │   └── __init__.py      # 数据源管理器
│   ├── analysis/            # 分析模块
│   │   ├── technical.py     # 技术指标
│   │   └── portfolio.py     # 组合分析器
│   └── utils/               # 工具模块
│       ├── database.py      # 数据库管理
│       └── position_reader.py # 持仓读取
├── data/
│   ├── raw/                 # 原始数据
│   ├── processed/           # 处理后数据
│   └── database/            # SQLite数据库
├── logs/                    # 日志文件
├── run_analysis.py          # 主入口
├── run_analysis.bat         # Windows批处理
├── setup_scheduler.ps1      # 定时任务配置脚本
└── requirements.txt         # 依赖列表
```

## 安装使用

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置持仓文件路径

编辑 `config/settings.py`:
```python
POSITION_FILE = r"C:\zd_zsone\T0002\export\持仓股20260410.xls"
```

### 3. 运行分析

```bash
python run_analysis.py
```

或双击运行 `run_analysis.bat`

### 4. 配置定时任务（可选）

以管理员身份运行PowerShell:
```powershell
.\setup_scheduler.ps1
```

或在Windows任务计划程序中手动创建：
- 触发器：每天 15:10
- 操作：启动程序 `run_analysis.bat`
- 条件：仅当网络连接可用时

## 配置说明

### 数据源配置

在 `config/settings.py` 中配置:

```python
DATA_SOURCES = {
    "sina": {
        "enabled": True,      # 启用
        "priority": 1,        # 优先级(数字越小优先级越高)
        "timeout": 10,        # 超时时间(秒)
        "retry": 3,           # 重试次数
    },
    "akshare": {
        "enabled": True,
        "priority": 2,
        ...
    }
}
```

### 技术指标配置

```python
TECH_INDICATORS = {
    "ma": {"fast": 5, "slow": 20},
    "macd": {"fast": 12, "slow": 26, "signal": 9},
    "rsi": {"period": 14},
    "kdj": {"k": 9, "d": 3, "j": 3},
    ...
}
```

## 数据库结构

### portfolio_snapshots - 持仓快照
- date: 日期
- code: 代码
- name: 名称
- quantity: 数量
- cost_price: 成本价
- current_price: 当前价
- market_value: 市值
- pnl: 盈亏
- pnl_rate: 盈亏率

### portfolio_summary - 组合汇总
- date: 日期
- total_value: 总市值
- total_pnl: 总盈亏
- daily_pnl: 当日盈亏
- daily_return: 日收益率
- vs_hs300: vs沪深300

### etf_technical - 技术指标
- date: 日期
- code: 代码
- ma_signal: MA信号
- macd_signal: MACD信号
- rsi_value: RSI值
- kdj_signal: KDJ信号

## 日志

日志文件保存在 `logs/` 目录，按日期命名:
- `portfolio_YYYYMMDD.log`

## 注意事项

1. 持仓文件需使用通达信导出功能生成
2. 首次运行会自动创建SQLite数据库
3. 网络异常时会自动重试并切换到备用数据源
4. 技术指标计算需要至少30个交易日的K线数据



## 阶段二新增功能（风险分析）

### 风险指标

| 指标类别 | 具体指标 | 说明 |
|----------|----------|------|
| 收益指标 | 总收益率、年化收益率、胜率 | 收益能力评估 |
| 波动率指标 | 年化波动率、下行波动率、偏度、峰度 | 风险水平评估 |
| 回撤指标 | 最大回撤、当前回撤、回撤天数 | 最坏情况评估 |
| 风险调整收益 | 夏普比率、索提诺比率、卡玛比率 | 风险调整后收益 |
| VaR | VaR(95%)、VaR(99%)、CVaR | 潜在最大损失 |
| Beta/Alpha | Beta系数、Alpha、R²、信息比率 | 相对基准表现 |
| 集中度风险 | HHI指数、等效品种数、行业分布 | 分散化评估 |
| 压力测试 | 多场景模拟损失 | 极端情况评估 |

### 风险预警规则

- **集中度预警**: 单一品种>25% 或 HHI>0.25
- **回撤预警**: 当前回撤>10%
- **波动率预警**: 年化波动率>30%
- **相关性预警**: 存在高相关性品种对
- **夏普比率预警**: 夏普比率<0.5

### 使用方式

风险分析已集成到主分析流程，运行 `run_analysis.py` 即可自动计算。

查看风险分析结果：
```python
from src.analysis.portfolio import PortfolioAnalyzer

analyzer = PortfolioAnalyzer()
results = analyzer.run_daily_analysis()

# 查看风险摘要
risk_summary = results['summary']['risk_summary']

# 查看详细风险分析
risk_details = results['risk']
```
\n## 更新计划

- 阶段二：风险指标计算、集中度分析
- 阶段三：自动化部署、通知机制
- 阶段四：智能建议、策略回测

## License

MIT License


## 阶段三：自动化部署（已完成）

### 功能特性

#### 1. 通知机制
- **邮件通知**: 支持SMTP邮件发送日报和告警
- **企业微信**: 支持Webhook推送消息到企业微信群
- **HTML报告**: 格式化的邮件报告，包含收益概览和风险指标

#### 2. 监控告警
- **自动告警检测**: 实时监控以下指标
  - 单日跌幅超过阈值（默认-3%）
  - 最大回撤超过阈值（默认-10%）
  - 持仓集中度风险（HHI > 0.5）
  - 波动率异常升高（> 30%）
  - 夏普比率偏低（< 0.5）
- **告警分级**: warning / error / critical 三级
- **告警确认**: 支持告警确认机制

#### 3. 运行监控
- **执行日志**: 自动记录每次任务执行状态
- **健康检查**: 系统健康状态实时监控
- **执行统计**: 成功率、平均耗时等统计指标

### 使用方法

#### 启动监控面板
```bash
monitor.bat
```

#### 命令行操作
```bash
# 运行分析
python run_enhanced.py --run

# 查看健康状态
python run_enhanced.py --health

# 查看执行统计
python run_enhanced.py --stats 7

# 查看告警
python run_enhanced.py --alerts 24
```

#### 配置通知
编辑 `config/settings.py`:

```python
NOTIFICATION_CONFIG = {
    'email': {
        'enabled': True,
        'smtp_server': 'smtp.qq.com',
        'username': 'your_email@qq.com',
        'password': 'your_auth_code',
        'recipients': ['recipient@example.com'],
    },
    'wechat': {
        'enabled': True,
        'webhook_url': 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=YOUR_KEY',
    }
}
```

### 数据库表结构（新增）

```sql
-- 告警记录表
CREATE TABLE alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_name TEXT,
    level TEXT,
    message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    acknowledged BOOLEAN DEFAULT 0
);

-- 执行日志表
CREATE TABLE execution_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_name TEXT,
    status TEXT,
    message TEXT,
    duration_seconds REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```


## 阶段四：智能分析（已完成）

### 功能特性

#### 1. 策略回测引擎 (`src/analysis/backtest.py`)
- **再平衡策略**:
  - Buy and Hold (买入持有)
  - Periodic (定期再平衡)
  - Threshold (阈值再平衡)
  - Risk Parity (风险平价)
  - Momentum (动量策略)
- **回测指标**: 总收益、年化收益、夏普比率、最大回撤、卡玛比率、换手率

#### 2. 智能建议引擎 (`src/analysis/advisor.py`)
- **建议类型**:
  - Rebalance (再平衡建议)
  - Risk Management (风险管理)
  - Opportunity (机会识别)
  - Caution (风险提示)
  - Strategy (策略建议)
- **优先级分级**: High / Medium / Low
- **置信度评估**: 0-1 区间量化建议可信度

#### 3. 智能报告生成 (`src/report/smart_report.py`)
- 执行摘要自动生成
- 分级建议展示
- 策略表现汇总
- 风险提示声明

### 使用方法

#### 快速启动
```bash
run_all.bat
```

#### 单独运行智能分析
```bash
python run_smart.py
```

#### 运行完整分析流程
```bash
run_all.bat  # 选择选项 4
```

### 配置说明

编辑 `config/settings.py`:

```python
SMART_ANALYSIS_CONFIG = {
    'advice_enabled': True,      # 启用智能建议
    'backtest_enabled': True,    # 启用回测分析
    'min_confidence': 0.6,       # 建议最小置信度
    'max_advices': 10,           # 最大建议数量
    'rebalance_threshold': 0.05, # 再平衡阈值（5%偏离）
    'momentum_lookback': 20,     # 动量观察期
    'risk_parity_target': 0.2,   # 风险平价目标波动率
}
```

### 报告示例

智能分析报告包含：
1. **执行摘要** - 关键洞察概览
2. **智能建议** - 分级投资建议列表
3. **策略表现** - 当前组合关键指标
4. **风险提示** - 免责声明和风险警示

---

## 系统架构总览

```
投资组合智能分析系统 v1.2
│
├── 阶段一：基础优化
│   ├── 多数据源支持 (新浪财经 + AKShare)
│   ├── 7种技术指标计算
│   └── SQLite数据持久化
│
├── 阶段二：风险分析
│   ├── 夏普/索提诺/卡玛比率
│   ├── VaR风险价值
│   ├── Beta/Alpha分析
│   └── 压力测试
│
├── 阶段三：自动化部署
│   ├── 邮件/企业微信通知
│   ├── 5项自动告警规则
│   └── 运行监控面板
│
└── 阶段四：智能分析
    ├── 5种再平衡策略回测
    ├── 智能建议引擎
    └── 智能报告生成
```

---

## 快速开始

1. **安装依赖**
   ```bash
   pip install -r requirements.txt
   ```

2. **配置持仓**
   编辑 `data/positions/positions.xlsx`

3. **启动系统**
   ```bash
   run_all.bat
   ```

4. **查看报告**
   报告保存在 `data/reports/` 目录

---

## 项目统计

- **代码文件**: 20+ 个 Python 模块
- **功能模块**: 4 个阶段完整实现
- **数据库表**: 6 张核心数据表
- **技术指标**: 7 种常用指标
- **风险指标**: 10+ 项风险度量
- **告警规则**: 5 项自动监控
- **回测策略**: 5 种再平衡策略
- **建议类型**: 5 类投资建议

---

*最后更新: 2026年4月25日*
