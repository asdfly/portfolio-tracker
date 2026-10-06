# portfolio_tracker × WorkBuddy「项目」功能 接入指引

> 目的：把 portfolio_tracker 工作用 WorkBuddy 真正的「项目」功能组织起来（云端共享指令 / 项目级配置 / 资料库 RAG / 自动化归属）。
> 本文件由 agent 生成；**其中标注「需你在客户端操作」的步骤，agent 无法直接执行**（无项目实体创建权、无项目资产库写入权）。

## A. 在客户端新建项目并关联本仓库（需你在客户端操作）

1. 打开 WorkBuddy 左侧边栏的 **「项目」** 入口，点击左上角 **「+ 新建项目」**。
2. 填写项目配置：
   - **项目名称**：`portfolio_tracker`
   - **指令**（建议粘贴以下摘要，所有任务自动继承）：
     ```
     投资组合智能分析系统，覆盖22只ETF。铁律：git显式pathspec提交、无授权不push；
     etf_fundamental.volume=股、etf_price_history.volume=手，禁止混淆；自动化中neodata不可用，
     数据补齐走akshare兜底；真实库 data/database/portfolio.db；休市日不补空白。
     ```
   - 连接器 / 专家 / 技能：按需添加（如已装相关 Finance 技能可纳入）。
3. 创建后，在项目详情页将**工作目录关联到本仓库根**：
   `/d/HuaweiMoveData/Users/HUAWEI/Documents/lingxi-claw/portfolio_tracker`
4. **现有 5 个自动化**（cwds 已绑定该目录）会自动归属本项目，**无需迁移**，照常每晚运行。
5. 验证：在本项目下新建任务，确认上下文已自动注入项目指令 + `.codebuddy` 规则（见下方 C）。

## B. 上传知识文档到项目资产库做 RAG（需你在客户端操作）

1. 进入项目详情页，顶部标签栏点击 **「资产」**。
2. 上传以下文档（已整理在 `docs/workbuddy/`）：
   - `pt_doc1_overview.md`（系统总览）
   - `pt_doc2_tasks.md`（任务清单）
   - `pt_doc3_issues.md`（已知问题）
   - （可选）探针说明：`scripts/probe_cs2000.py`、`scripts/probe_miaoxiang.py`
3. 上传后，本项目任务即可通过 **RAG** 检索这些项目知识（代码与数据库**不要**上传资料库）。

## C. 验证清单

- [ ] 新任务上下文包含项目指令与 `.codebuddy/` 规则（CODEBUDDY.md + 3 个 rules）
- [ ] 现有自动化（152124e1 等）仍按原计划运行、未报错
- [ ] 项目资料库可检索到上传的文档
- [ ] 本地 git 状态干净（新增 `.codebuddy/` 与本文档，未卷走其他产物）

## D. 已落地的项目级配置（agent 已完成）

仓库内已新增 `.codebuddy/`，对所有任务自动生效：
- `CODEBUDDY.md`：项目架构 / 路径 / 常用命令 / 关键陷阱
- `rules/git-workflow.md`：git 显式提交、不主动 push、跨盘走 Git Bash
- `rules/data-integrity.md`：成交量单位、neodata 限制、休市日、双源容错
- `rules/environment.md`：路径、venv、真实库、托管 Python 读不到 D 盘

> 注：`.codebuddy/` 与既有 `.workbuddy/`（Agent 本地工作空间）并存；`.codebuddy` 优先级「项目级 > 用户级」。
