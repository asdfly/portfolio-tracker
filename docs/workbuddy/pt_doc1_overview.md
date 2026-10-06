# portfolio_tracker 项目总览与架构手册

> 投资组合智能分析系统 · 知识库主文档（归档于 WorkBuddy 资料库「portfolio_tracker 项目知识库」）

## 一、系统定位
投资组合智能分析系统，覆盖 22 只 ETF，提供每日主分析、盘后日报、补采巡检、A股轮动复盘、圆桌投资大师等多条产品线。部署于本机（项目路径 `portfolio_tracker`）与腾讯云 Lighthouse（轻量应用服务器，当前账号下无在用实例）。

## 二、技术栈
- 前端/应用：Streamlit
- 存储：SQLite（生产库 `portfolio.db`，约 146MB，只读分析用 `file:?mode=ro`）
- 数据源：AKShare（主）+ NeoData（估值/PE 备用）；新浪 / 东财 / 腾讯多源容错
- 调度：Windows 任务计划 15:30 主分析 + `watchers/watchdog` 守护；定时任务经 WorkBuddy automation 驱动
- 邮件：smtp-mail-sender skill（读取 `.env` 的 `EMAIL_*` 配置，smtplib 直连 `smtp.qq.com`）

## 三、关键文件
- `config/settings.py` — 指数白名单 `INDEX_CODES`（12 只，含 `sh932000`）、数据源配置
- `src/data_sources/` — 数据源管理器（`base` / `sina` / `akshare`），含代理透传 `resolve_env_proxies`
- `src/analysis/portfolio.py` — 主分析，`_fetch_index_quotes` 取指数行情
- `src/analysis/price_history_gate.py` — 系统性鲜度 / 单标的缺口 gate
- `scripts/backfill/` — 回填脚本（`backfill_single_index.py` 三源链）
- `run_analysis.py` / `run_report` — 主分析入口与邮件报告
- `docs/handover/` — 交接文档（14~17 系列）

## 四、主分析流程
`run_analysis.py` → 取数 → 阶段 `basic` / `risk` / `monitor` / `dq_check` → 生成 `run_report_YYYY-MM-DD.json`（含 `run_status`、`dq_score`、`alerts`）。

`run_status` 语义：
- `ok`：完整成功
- `degraded`：数据陈旧 / 系统性缺口，邮件闸门拒发基于陈旧价的日报
- `failed`：真·流水线错误（缺失 required 阶段），`critical` 告警，不降级为 warning

## 五、数据层概览
- 22/22 ETF 覆盖：OHLCV + 37 维特征 + 前瞻标签
- `resolve_target_codes` 自动跟随最新持仓快照；`MAJOR_ETFS` 白名单仅防御兜底
- 指数基准 12 只（`INDEX_CODES`），含 9/22 新增 `sh932000`（中证2000）
- `index_quotes` 存收盘价序列；`index_pe_history` 存 PE（NeoData 回填）

## 六、已知问题索引
详见《已知问题与根因追踪》：
1. 中证2000（`sh932000`）行情四源不可达
2. 沙箱出网 SMTP RST（WinError 10054）
3. 单指数取数失败曾拖垮整轮（已修复 `b4e625e`）
4. B1 代理透传（代码侧就绪，运行环境出网待配）
