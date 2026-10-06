# portfolio_tracker 项目说明（项目级记忆）

> 本文件由 WorkBuddy 项目级机制自动加载到所有本项目任务上下文。请勿删除。

## 一、项目定位
投资组合智能分析系统：覆盖 **22 只 ETF**（持仓快照驱动，非写死的白名单），含定时日报推送（SMTP 目标 `asdfl@qq.com`）、数据新鲜度验证、再平衡引擎、ETF 高低点评估引擎、23 只 ETF PE 回填等模块。
报告栈：`plotly` 静态 HTML（浅色主题、邮件客户端友好，规避深色主题与内嵌 base64 图片）。

## 二、关键路径与环境（务必用对）
- **项目根**：`/d/HuaweiMoveData/Users/HUAWEI/Documents/lingxi-claw/portfolio_tracker`（D 盘，疑似迁移/外部盘，注意盘稳定性）
- **Python 运行时**：仓库内 `venv313/Scripts/python.exe`
- **真实数据库**：`data/database/portfolio.db`（⚠️ 不是 `data/portfolio.db`，后者是空壳，勿查错）
- **git 远程**：`github.com/asdfly/portfolio-tracker`（推送走 SSH-over-443 通道）

## 三、常用命令 / 入口
- 增量补价：`src/analysis/predictor/price_history.py` 的 `backfill_etf_price_history(conn, codes, sources=("em","tx"))`，返回 `BackfillResult.rows`
- ETF 基本面（neodata 种子消费）：`scripts/backfill/backfill_etf_fundamental_neodata.py --verify`（`INSERT OR IGNORE`，消费 `scripts/backfill/data/etf_fundamental_neodata.json` 冻结种子）
- ETF 基本面（akshare 兜底）：`scripts/backfill/backfill_etf_fundamental_akshare.py`（东方财富 `fund_etf_hist_em` → 新浪 `fund_etf_hist_sina` 兜底，均不复权，`INSERT OR IGNORE` 仅补缺口）
- 两融重试：在 `src/data_sources/market_events.py` 的 `run_pending_margin_retries(conn, date_display, max_attempts)`

## 四、关键陷阱 / 铁律（任何任务都必须遵守）
1. **成交量单位**：`etf_fundamental.volume` = **股(shares)**；`etf_price_history.volume` = **手(lots)**。两表单位不同，禁止混淆；只有在做明确的单位换算时才 `×100`（手→股）。
2. **neodata 在自动化里不可用**：neodata 是平台 deferred tool，自动化会话的 deferred index 未注入 → 自动化中 neodata 调用必失败（真阴性，非代码 bug）。数据补齐已改走 akshare 兜底脚本，**勿在自动化 prompt 里强依赖 neodata**。
3. **git 铁律**：提交必须**显式 pathspec**，绝不 `git add -A` / `git add .`，避免卷走他人/自动化产物；**未获用户明确授权绝不 `git push`**。
4. **跨盘文件操作**：托管 Python 运行时（`~/.workbuddy/binaries/python/...`）的沙箱看不到真实 D: 盘（`/d` 是桩目录）。读写 D: 文件须走 **Git Bash** 或系统 Python（`C:/Program Files/Python310/python.exe`）。
5. **`.gitignore` 第 103 行排除整目录 `.workbuddy/`** → 记忆/自动化本地状态不入库，勿强行 `git add` 它们。
6. **休市日勿误补**：如 2026-09-25 中秋休市，补数函数会正确跳过非交易日，不要误以为"缺口"而补空白交易日。
7. **双数据源容错**：东方财富（主）+ 新浪（`akshare fund_etf_hist_sina`，备）。新浪 `volume` = 股，与 `etf_fundamental` 一致，**无需 ×100**；新浪无 `turnover_rate` 列。

## 五、自动化（定义在客户端/服务端；本地仓库仅存运行时 memory.md，不含定义）
- `152124e1`：每晚 21:30，etf_fundamental 双兜底（neodata 实时 + akshare 推进）
- `a191fb16`：一次性复查任务（已运行）
- 另有 `automation-1785911636011` 等目录（`.workbuddy/automations/` 下仅存每个自动化的运行时 `memory.md`，**自动化定义/cwds 在客户端服务端，不在本仓库**）；新建项目时是否自动归集取决于客户端按工作目录关联的逻辑，请在客户端「项目 → 自动化」面板核对。

## 六、项目工作流范式
并行拉数 → 缺口校验 → 落盘 deliverable → memory append → present；批处理脚本强制 CRLF、用 `venv313`、GitHub 走 SSH-over-443。交付物既要点预测也要内嵌方法论自审（如"指数权重 ≠ 逻辑载体"陷阱）。
