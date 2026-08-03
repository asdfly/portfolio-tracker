# 部署与运维指南

## 环境要求

| 组件 | 版本 | 说明 |
|------|------|------|
| Python | 3.10+ | 推荐 3.10.11 |
| OS | Windows 10/11 | 通达信导出路径依赖 Windows |
| 浏览器 | Chromium | Playwright 截图/PDF 导出依赖 |
| SQLite | 3.35+ | Python 自带 sqlite3 模块 |

## 安装步骤

### 1. 安装 Python 依赖

```bash
cd portfolio_tracker
pip install -r requirements.txt
```

核心依赖：

| 包 | 用途 |
|---|------|
| streamlit | Web Dashboard |
| plotly | 交互式图表 |
| pandas / numpy | 数据处理 |
| scipy | 统计计算 |
| akshare | 金融数据 API |
| beautifulsoup4 | 网页解析 |
| playwright | 截图/PDF 导出 |
| openpyxl | Excel 报告 |
| python-docx | Word 报告 |
| schedule | 定时任务 |

### 2. 安装 Playwright 浏览器

```bash
python -m playwright install chromium
```

### 3. 配置环境变量（可选）

在项目根目录创建 `.env` 文件：

```ini
# 数据库路径（默认 data/database/portfolio.db）
DATABASE_PATH=data/database/portfolio.db

# 通达信导出目录（自动查找最新持仓文件）
TDX_EXPORT_DIR=C:\zd_zsone\T0002\export

# 通知配置（可选）
SMTP_HOST=smtp.example.com
SMTP_PORT=465
SMTP_USER=your_email@example.com
SMTP_PASSWORD=your_password
WECOM_WEBHOOK=https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=xxx
```

### 4. 初始化数据库

```bash
python -c "from src.utils.database import get_db_connection; conn = get_db_connection(); print('DB initialized')"
```

数据库会自动创建 30 张表（DDL 定义在 `src/utils/db_schema.py`）。

### 5. 安装 pre-commit hook（开发者）

```bash
pip install -r requirements-dev.txt
pre-commit install
```

## 定时任务配置

通过 Windows 任务计划程序配置以下任务：

### 任务 1：盘前数据采集（08:00）

```
程序: scheduled_run.bat
参数: (无)
起始位置: C:\Users\HUAWEI\Documents\lingxi-claw\portfolio_tracker\
触发器: 每周一至五 08:00
```

### 任务 2：盘后完整分析（15:30）

```
程序: scheduled_run.bat
参数: (无)
起始位置: C:\Users\HUAWEI\Documents\lingxi-claw\portfolio_tracker\
触发器: 每周一至五 15:30
```

`scheduled_run.bat` 执行流程：

1. **Stage 0**: 数据库备份（VACUUM INTO，保留 7 天）
2. **Stage 1-5**: `run_analysis.py` 五阶段流水线
   - 基础持仓分析（行情+技术指标）
   - 风险分析（夏普+回撤+VaR+Beta）
   - 监控告警（9 条规则检测）
   - 智能建议（17 步骤/13 维度）
   - 通知报告（HTML 邮件+企业微信）

## 运行方式

### 交互式菜单

```bash
run_all.bat
```

9 个选项：完整分析 / 快速分析 / 监控面板 / Dashboard / 增强报告 / 通知配置 / 历史回填 / venv Dashboard / 退出。

### Dashboard

```bash
python -m streamlit run dashboard.py --server.port 8501
```

访问 http://localhost:8501

### 命令行分析

```bash
python run_analysis.py
```

## 数据导入

### 通达信持仓导入

1. 通达信 → 报表导出 → 持仓股文件（TSV）
2. 系统自动从 `TDX_EXPORT_DIR` 查找最新文件
3. 导入到 `portfolio_snapshots` 表

### 对账单 PDF 导入

```bash
python -c "
from src.utils.trade_importer import TradeImporter
importer = TradeImporter()
importer.import_pdf('path/to/statement.pdf')
"
```

支持自动提取 11 种交易类型：证券买入/卖出/定投/申购/赎回/红利/股息/银行转存/质押回购等。

### 历史数据回填

```bash
python scripts/run_backfill.py all
```

回填历史 K 线、技术指标、宏观数据等。

## 数据库维护

### 日常备份

自动备份：`scheduled_run.bat` Stage 0 每日执行。

手动备份：

```bash
python scripts/backup_db.py
```

### 数据质量检查

Dashboard → Tab 13 数据质量，或：

```bash
python -c "
from src.utils.data_quality import DataQualityChecker
from src.utils.database import get_db_connection
conn = get_db_connection()
checker = DataQualityChecker(conn)
report = checker.generate_report()
print(report)
"
```

### 查看执行日志

```bash
python -c "
from src.utils.database import get_db_connection
conn = get_db_connection()
rows = conn.execute('SELECT * FROM execution_logs ORDER BY id DESC LIMIT 10').fetchall()
for r in rows: print(r)
"
```

## 测试

```bash
# 全量测试（约 5 分钟）
python -m pytest tests/ -v

# 快速门禁（pre-commit 用）
python -m pytest tests/test_imports.py -v

# 特定模块
python -m pytest tests/test_p1_position_valuation.py tests/test_tab15_trade_review.py -v
```

## 日志

- 定时任务日志: `logs/scheduled_run.log`
- 运行日志: `logs/run_analysis_YYYYMMDD.log`
- Streamlit 日志: 控制台输出

## 故障排查

| 症状 | 可能原因 | 解决方案 |
|------|---------|---------|
| Dashboard 显示旧数据 | Streamlit 进程未重启 | 终止旧进程后重新启动 |
| 「暂无技术信号数据」 | etf_technical 日期不匹配 | 已修复：load_technical 自动日期回退 |
| 月度收益异常高 | total_value 比值法误计追加投入 | 已修复：改用 daily_return 连乘法 |
| 定投基金市值为 ¥0 | 最新快照不含场外基金 | 已修复：calc_dca_tracking 快照回退 |
| 仓位建议目标=当前 | 目标占比按绝对百分点计算 | 已修复：改为相对比例 |
| run_all.bat 乱码 | LF 换行符 | 已修复：改为 CRLF |
| Selenium 报错 | ChromeDriver 版本不匹配 | 已替换为 Playwright |

## 性能优化

- 资金流向查询：sector/etf 加 date 过滤 + 复合索引（23x/13x 加速）
- 数据库连接：`get_db_connection()` 统一管理，避免连接泄漏
- Streamlit 缓存：`@st.cache_data` 装饰器缓存数据加载结果
- 图表渲染：`render_chart()` 统一渲染，避免重复配置
