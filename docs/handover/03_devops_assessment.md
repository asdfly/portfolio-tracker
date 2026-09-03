# 部署 / 交付 / 运维就绪度交接评估

| 项 | 值 |
|---|---|
| 评估对象 | `D:\HuaweiMoveData\Users\HUAWEI\Documents\lingxi-claw\portfolio_tracker` |
| 评估人 | 运维工程师（卜宕机）· MVP 开发专家团 |
| 评估日期 | 2026-09-03 |
| 评估性质 | 只读评估。未修改任何生产代码、配置、数据库；仅新增本文件 |
| 代码基线 | `master` @ `47e2eef`（本地 HEAD），共 334 commits |
| 判定刻度 | `references/01-standards/production-readiness-scorecard.md`（Bronze/Silver/Gold 七维记分卡） |
| 选型对照 | `references/architecture/mvp-stack.md`、`references/cost-models/development-costs.md` |

> 路径说明：`C:\Users\HUAWEI\Documents\lingxi-claw\portfolio_tracker` 是 Huawei「移机数据」重定向后的残留外壳（仅剩 `data/`、`venv313/`，无源码）。**真实工程目录在 D 盘**，本文所有路径均以 D 盘为准。

---

## 1. 总体结论

**记分卡总档：未达 Bronze。**（总档取七维最低档，`发布安全` 维度未达 Bronze）

需要把两件事分开说，否则结论会被误读：

- **「能不能跑」——已被证明。** 15:30 盘后流水线连续运行了 4 个月以上，`data/backups/` 里 8/28、8/31、9/1、9/2、9/3 五份日备份齐整（周末正确跳过），9/3 那次跑到阶段四正常收尾（`logs/scheduled_run.log`）。DB 从 125.2MB 长到 127.7MB，日增约 0.4MB，数据在稳定累积。这是一套真实在产的系统，不是演示品。
- **「能不能交接、能不能回退」——不成立。** 生产链路上有 5 个脚本从未入库，日报编排逻辑存在于 `.workbuddy/`（被 gitignore），依赖全部 `>=` 未锁，158 个 commit 未推送，备份与源库同盘无异地副本，而 `git status` 当前直接崩。任何一次「换台机器 / 回到上周的状态 / 交给下一个人」的动作都会失败。

差距集中在**可回退、可复现、可交接**，不在功能。下面每一条都带文件行号与可执行命令。

---

## 2. 现状：生产实际怎么跑

### 2.1 真实调度拓扑（5 个调度器，文档只写了 1 个）

| # | 触发时间 | 调度器 | 执行内容 | 有文档？ | 入版本控制？ |
|---|---|---|---|---|---|
| 1 | 每日 09:00 | WorkBuddy 自动化 `automation-1785979940493`「投资组合次日补采」 | `run_morning.py` | 无 | 编排无，脚本有 |
| 2 | 工作日 15:30 | Windows 任务计划程序 | `scheduled_run.bat` → `run_analysis.bat` → `run_analysis.py` → `send_report_email.bat` | README/DEPLOYMENT 有（时间写错） | 是 |
| 3 | 每日 16:30 | WorkBuddy 自动化 `automation-1785911636011`「每日巡检补采」 | 补采巡检 | 无 | 编排无 |
| 4 | 每日 16:40 | WorkBuddy 自动化 `automation-1787708904209`「组合+大盘综合视角」 | `scripts/fetch_market_data.py` → `scripts/gen_combo_report.py` → SMTP 推送 | 无 | **编排与脚本均无** |
| 5 | 工作日 17:00 | WorkBuddy 自动化 `automation-1787712904171`「ETF估值数据追加」 | ETF 估值追加 | 无 | 编排无 |

`automation_update list` 实测确认 4 个自动化状态均为 `ACTIVE`。也就是说：**文档覆盖了 1/5 的生产调度，版本控制覆盖了 1/5 的生产编排。** 一个只拿到 git 仓库的接手人，会以为这个系统每天只跑一次 15:30 的分析。

### 2.2 入口链路（15:30 主流水线）

```
Windows 任务计划程序
  └─ scheduled_run.bat            :13  mkdir logs
                                  :15  call run_analysis.bat  >> logs\scheduled_run.log
                                  :18  call send_report_email.bat >> logs\scheduled_run.log
       └─ run_analysis.bat        :5   PYTHON = "%~dp0venv313\Scripts\python.exe" -E
                                  :21  %PYTHON% run_analysis.py
            └─ run_analysis.py         Stage0 备份 → 阶段一~四
                 └─ scripts/backup_db.py:32-38  SQLite 在线备份 API
       └─ send_report_email.bat   :5   同一 venv313
                                  :9   scripts/send_report_email.py
```

两个 `.bat` 均使用 `%~dp0` 相对定位（`run_analysis.bat:4-5`、`send_report_email.bat:4-5`），这是迁移后仍然能跑的直接原因——**这一点做对了**。

### 2.3 Dashboard 与 Docker

- Dashboard 入口：`python -m streamlit run dashboard.py --server.port 8501`（`README.md:170`），`dashboard.py` 52KB 单文件 + `tabs/` 拆分，无 supervisor / systemd / PM2，靠人工起停。`dashboard_stdout.log`（179 字节，4/27）与 `streamlit_debug.log`（5/20）是历史手工启动残留，说明 Dashboard 从未纳入常驻托管。
- **Docker 路径不可用于生产**，判定依据见 §5.4。`README.md` 全文 0 次提到 docker，`Dockerfile` 事实上是孤儿文件。

---

## 3. CI 现状与有效性

`.github/workflows/ci.yml`（713 字节，唯一 workflow）：

| 步骤 | 内容 | 有效性判定 |
|---|---|---|
| 触发 | `push` / `pull_request` → `master`（`:3-7`） | **事实上从不触发**——本地领先 origin/master **158 个 commit**，远端最新 `7cbada1` 停在 2026-06-05 |
| checkout | `actions/checkout@v4`（`:13`） | 有效 |
| setup-python | 3.12（`:15-18`） | 与生产 venv（Python 3.13）不一致，见 §5.5 |
| install | `pip install -r requirements.txt` + pytest（`:20-24`） | 依赖未锁，每次跑到的版本都不同，见 §5.1 |
| test | `pytest tests/ -v --timeout=120`，`DATABASE_PATH=":memory:"`（`:26-32`） | 高概率红。QA 交接评估已定位测试依赖真实 `portfolio.db` 与硬编码绝对路径 |
| lint / build / deploy | **不存在** | `.pre-commit-config.yaml` 里配好的 black / isort / flake8 / mypy / bandit 一个都没进 CI |

**结论：CI 是装饰性的。** 三个月没有为任何一个 commit 提供过反馈，且即使被触发也大概率红。`.pre-commit-config.yaml`（44 行，配置质量不错）与 CI 完全脱节——CI 只跑 pytest，pre-commit 的 5 个 linter 只在本地生效，而本地 hook 又被替换掉了（见 §5.6）。

---

## 4. P0 风险（按修复顺序排列）

修复顺序不是按严重度排，而是按**依赖关系**排：P0-1 不修，后面每一条都没法做（改不了文件、提交不了）。

### P0-1 · git 工作区当前不可用（根因已定位到单个文件）

**现象**

```console
$ git status --porcelain
BUG: compat/mingw.c:156: err_win_to_posix() called without an error!
```

`git status` / `git add` / `git ls-files -o` 全部崩溃。git 2.55.0.windows.3，`core.fscache=false`、`core.longpaths=true` 均无效。

**根因（逐层二分定位）**

1. `git status --porcelain -uno` → 正常返回空 → 崩溃只发生在**未跟踪文件枚举**阶段，跟踪文件索引无损。
2. 按子树递归枚举：`src` / `tabs` / `components` / `scripts` / `tests` / `config` / `docs` / `data` / `venv313` / `report` 全部正常，**只有 `.pytest_cache` 触发 BUG**。
3. 定位到具体文件：

```console
$ cat .pytest_cache/.gitignore
cat: .pytest_cache/.gitignore: Permission denied
```

`.pytest_cache/.gitignore` 的 NTFS ACL 已损坏，进程无读权限。git 进入该目录读取 ignore 规则 → Win32 调用以一种 `err_win_to_posix()` 无法翻译的方式失败 → 断言崩溃。这是 **C→D 移机导致 ACL 丢失的直接后果**，不是 git 本身的问题。

**为什么是 P0**：它让「先看清改了什么，再提交」这个最基本的安全动作无法执行。P0-2 里那 5 个未入库的生产脚本之所以能被漏掉 4 个月，这就是机制层面的原因。

**修复（约 30 秒，可立即执行）**

```bash
cd "D:/HuaweiMoveData/Users/HUAWEI/Documents/lingxi-claw/portfolio_tracker"

# 1. 删掉损坏的缓存目录（pytest 会自动重建，无任何数据价值）
#    若 rm 因 ACL 失败，用 Windows 侧接管所有权：
#    takeown /f .pytest_cache /r /d y  &&  icacls .pytest_cache /grant "%USERNAME%":F /t
rm -rf .pytest_cache

# 2. 补进 .gitignore，防止复发（现有 .gitignore 缺这一条）
printf '\n# pytest 缓存（ACL 易损，务必忽略）\n.pytest_cache/\n' >> .gitignore

# 3. 验证
git status --porcelain | head
```

**修复前的临时绕过**（`-uno` 看改动，pathspec 排除看新增，二者都已实测可用）：

```bash
git status --porcelain -uno                       # 只看已跟踪文件的修改
git status --porcelain -- . ':!.pytest_cache'     # 完整状态（含未跟踪）
```

### P0-2 · 生产链路上 5 个脚本从未入库，且 158 个 commit 从未推送

**未跟踪的生产代码**（用 §P0-1 绕过命令得到的完整清单）：

| 文件 | 大小 | 角色 |
|---|---|---|
| `scripts/gen_combo_report.py` | 58,266 B | **16:40 日报的 HTML 生成器**。全项目第三大源文件（仅次于 `data_loader.py` 72KB、`dashboard.py` 52KB） |
| `scripts/fetch_market_data.py` | 12,100 B | **16:40 日报的数据采集器**（NeoData 查询 → `data/.neotmp/neodata_market.json`） |
| `scripts/backfill_sector_change.py` | 6,248 B | `sector_daily_change` 表历史回溯 |
| `scripts/probe_institution_gap.py` | 1,909 B | 机构调研缺口探查 |
| `scripts/verify_sina_whitelist.py` | 2,630 B | 新浪数据源白名单校验 |

加上被 `.gitignore:88` 排除的 `.workbuddy/`（内含 4 个自动化的执行记忆与运维 runbook），意味着：

- 16:40 日报链路的**脚本和编排都不在版本控制里**。这条链路的产物是每天推送到 `asdfl@qq.com` 的组合日报，是这套系统最直接的用户价值输出。
- `.workbuddy/memory/automations/automation-1785979940493/memory.md:176` 里写着 `run_morning` 卡死时的诊断恢复步骤（`wmic` 找 PID → `Stop-Process` → 测 `csindex` URL 连通性 → 带 `timeout 420` 重跑，并指明根因是 `ak.stock_zh_index_value_csindex` 走 `pd.read_excel` 不走 requests 补丁）。**这是真正的运维 runbook，而它不在仓库里、不在 DEPLOYMENT.md 里、也不在任何备份里。**

**远端状态**

```console
$ git rev-list --left-right --count origin/master...HEAD
0	158
$ git log -1 --format='%h %ad %s' origin/master
7cbada1 Fri Jun 5 13:24:21 2026 +0800 P4: migrate tab1 render from dashboard.py to tabs/tab1_net_value.py
```

远端（`https://github.com/asdfly/portfolio-tracker.git`）落后 158 个 commit、约 3 个月。**代码的唯一有效副本就是这块 D 盘。** 加上备份也在同一块盘（P0-4），单盘故障 = 代码 + 数据 + 运维知识同时归零。

**修复**：先做 P0-3（历史清理会重写 commit，顺序不能反），再按下面提交与推送。

```bash
# 提交未入库的生产脚本（P0-1 修完后 git add 才可用）
git add scripts/gen_combo_report.py scripts/fetch_market_data.py \
        scripts/backfill_sector_change.py scripts/probe_institution_gap.py \
        scripts/verify_sina_whitelist.py
git commit -m "chore: 补入 16:40 日报链路与回溯脚本（此前从未入库）"

# 把 .workbuddy 的运维知识固化进仓库（不解除 .workbuddy/ 的忽略，只导出必要部分）
mkdir -p docs/runbook
cp .workbuddy/memory/automations/automation-1785979940493/memory.md docs/runbook/09-00-次日补采.md
cp .workbuddy/memory/automations/automation-1787708904209/memory.md docs/runbook/16-40-组合大盘日报.md
# 另需人工补写 16:30 / 17:00 两个自动化的 prompt 与恢复步骤
git add docs/runbook && git commit -m "docs: 固化 4 个 WorkBuddy 自动化的编排与恢复步骤"
```

### P0-3 · 本地历史里躺着 87.9MB 真实持仓数据库，尚未推送

**事实（精确到 commit 与字节）**

```console
$ git rev-list --objects --all | git cat-file --batch-check='%(objecttype) %(objectname) %(objectsize) %(rest)' \
    | awk '$1=="blob"' | sort -k3 -nr | head -1
blob 9d713dfc48dafcde44b65f334d1c666c6a42c32f 87949312 data/database/portfolio.db
```

| commit | 日期 | 动作 | 在远端？ |
|---|---|---|---|
| `24574df` | 2026-08-03 | `A data/database/portfolio.db`（87,949,312 B = 83.9 MiB）<br>提交信息：`chore: 提交数据库文件 portfolio.db (84MB, 30表)` | **否** |
| `e0f8d43` | 2026-08-05 | `D` 三个 .db，提交信息：`接手治理: 数据库移出 git 跟踪` | **否** |
| `323ab7c` / `89a4512` / `99d722d` | 05-11 / 05-17 / 06-12 | 另外 3 个 .db 路径 | 前两个在远端，但 blob 均为 **0 字节**占位文件 |

**当前 `.gitignore` 已经堵住了入口**（`:23 data/database/`、`:59 data/*.db`、`:85 *.db`），`git ls-files | grep '\.db$'` 返回空——**跟踪层面是干净的**。`e0f8d43` 那次治理做对了一半。

**但没做完的那一半是关键的一半**：`git merge-base --is-ancestor 24574df HEAD` 返回真，即 87.9MB blob **仍可从 HEAD 追溯到**，仍在 `.git`（35MB pack）里。风险不是「已经泄露」，而是：

> **只要有人执行一次 `git push`，这 158 个 commit 连带 83.9MiB 真实持仓、交易记录、盈亏数据一起进入 GitHub 远端历史。**

而 P0-2 的修复动作就是 `git push`。**两条 P0 是互为扣环的：不先清历史就推送，等于把数据泄露自动化。** 一旦推送，撤销成本从「本地改写」跃升到「联系 GitHub Support 清理 + 假定数据已泄露」。

**现在是清理成本最低的时刻**：因为这 158 个 commit 从未推送，改写历史**不需要任何人 force-pull、不影响任何协作者**。这个窗口在第一次 push 之后就永久关闭。

**修复（`git filter-repo`，比 bfg 更新且官方推荐）**

```bash
# ── 第 0 步：整仓冷备（必做，改写历史不可逆）──────────────────
cd "D:/HuaweiMoveData/Users/HUAWEI/Documents/lingxi-claw/portfolio_tracker/.."
cp -r portfolio_tracker/.git "D:/backup_git_$(date +%Y%m%d)"

# ── 第 1 步：装工具 ────────────────────────────────────────
pip install git-filter-repo

# ── 第 2 步：先演练，确认命中范围 ─────────────────────────────
cd portfolio_tracker
git filter-repo --analyze          # 产出 .git/filter-repo/analysis/ 报告，先读一遍
git filter-repo --path data/database/portfolio.db --path portfolio.db \
                --path data/portfolio.db --path portfolio_data.db \
                --invert-paths --dry-run

# ── 第 3 步：实际改写 ──────────────────────────────────────
git filter-repo --path data/database/portfolio.db --path portfolio.db \
                --path data/portfolio.db --path portfolio_data.db \
                --invert-paths --force

# ── 第 4 步：验证 ─────────────────────────────────────────
git rev-list --objects --all | git cat-file --batch-check='%(objecttype) %(objectname) %(objectsize) %(rest)' \
  | awk '$1=="blob" && $3>5000000'          # 应为空
du -sh .git                                  # 35MB → 预计 10~15MB（估算，非实测）
git log --oneline | wc -l                    # commit 数应仍为 334（只换 SHA，不丢历史）
```

**必须提前知道的三个副作用**

1. **全部 commit SHA 改变。** 所有对旧 SHA 的引用（CHANGELOG、文档、issue、`.workbuddy/memory` 里的记录）失效。改写完建议在 `docs/handover/` 留一份新旧 SHA 映射（`filter-repo` 会在 `.git/filter-repo/commit-map` 生成）。
2. **`filter-repo` 默认移除 `origin` remote**（防误推）。改写后需 `git remote add origin https://github.com/asdfly/portfolio-tracker.git`，并且由于远端 `7cbada1` 及其祖先也被改写，第一次推送需 `git push --force-with-lease origin master`。**推送前务必先确认远端仓库是 private**（`gh` 未登录，本次无法核实可见性——这是本报告唯一未能验证的关键事实，接手第一件事应确认）。
3. **改写要在 P0-2 提交之后、push 之前做。** 顺序：P0-1 修 git → P0-2 提交未入库脚本 → P0-3 清历史 → 确认远端 private → push。

### P0-4 · 备份与源库同盘，无异地副本；且只有 7 天窗口

**现状（实测）**

```
data/database/portfolio.db                    127,696,896 B   (9/3 15:30)
data/database/portfolio.db.backup_phase0       30,642,176 B   (5/21，孤儿)
data/backups/portfolio_20260828~20260903.db    5 份 × ~126MB  = 767MB
data/backups/archive/portfolio_db_20260803.db  83,173,376 B   (一次性)
data/backups/archive/portfolio_db_PRE_QA_20260805.db  88,764,416 B (一次性)
磁盘：D: 554G，已用 255G，可用 300G（46%）
```

备份机制本身**实现得不错**，先说对的部分：

- `scripts/backup_db.py:32-38` 用 **SQLite 在线备份 API**（`source.backup(dest)`），而不是文件 `cp`。这保证了备份的事务一致性，即使备份瞬间有写入也不会拿到撕裂的库。这是正确做法。
- 保留策略有兜底：`cleanup_old_backups(max_age_days=7, keep_min=3)`（`:44-60`），`keep_min` 保证即使全部超期也留 3 份，不会清空。
- 日志实证生效：`Cleanup: 2 old backups removed, 5 retained`、`Backup created: ...portfolio_20260901_153007.db (120.4 MB)`。
- 自动化真实运行：8/28(五)→8/31(一) 之间正确跳过周末。

再说四个缺口：

| 缺口 | 证据 | 后果 |
|---|---|---|
| **无异地/离盘副本** | 备份与源库都在 `D:\` | 单盘故障 = 数据 + 代码 + runbook 全灭。这是全项目**唯一真正不可恢复**的风险 |
| **只有 7 天窗口** | `max_age_days=7`；`data/backups/` 实际只有 8/28~9/3 | 数据损坏若第 8 天才发现 → 无法恢复。`archive/` 里两份 8 月初的一次性备份是手工留的，无机制保证 |
| **零恢复演练** | 全仓无恢复脚本、无演练记录、备份后无 `PRAGMA integrity_check` | 「备份存在」≠「备份能用」。127MB 文件恢复本身只需 <1 分钟（RTO 很好），但从未验证过 |
| **文档与实现不符** | `DEPLOYMENT.md:100` 与 `README.md:189` 都写 **VACUUM INTO**，实际是在线备份 API（`backup_db.py:35`） | 接手人按文档理解会误判备份语义与一致性保证 |

**RPO/RTO 实测判定**：RTO ≈ 1 分钟（拷贝 127MB 文件）——很好。RPO ≤ 24 小时，但要分数据类型看：行情/技术指标/资金流可从外部 API 重采（可恢复）；**`trade_records`（1,157 条，来自对账单 PDF 手工导入）与 `portfolio_snapshots` 的历史通达信快照不可重采**——这部分的真实 RPO 是「上一次备份」，且原始 PDF / TSV 若未单独留存，就是永久丢失。

**修复（按性价比排序，引用 `cost-models/development-costs.md` 的隐性成本模型）**

`development-costs.md` 给的隐性成本是「服务器/云服务 ¥100-500/月」。这套系统**不需要**这笔钱——它是单用户本机分析系统，`mvp-stack.md` 里 SQLite 定位「嵌入式/桌面端，规模上限百万级」，当前 127MB / 27 表 / 单表最大 21 万行（`stock_margin`）离上限还远，日增 0.4MB 推三年约 570MB 仍在舒适区。**结论：不要上云，把钱花在异地备份上——对象存储冷备 1~2GB 约 ¥1-5/月，是这套系统性价比最高的一笔运维支出。**

```bash
# scripts/backup_offsite.sh —— 建议新增（本次评估未创建，属改动生产，留给接手人）
# 1) 校验备份可用性（当前完全缺失的一环）
LATEST=$(ls -t data/backups/portfolio_*.db | head -1)
python -c "
import sqlite3,sys
c=sqlite3.connect(sys.argv[1])
r=c.execute('PRAGMA integrity_check').fetchone()[0]
n=c.execute(\"SELECT count(*) FROM sqlite_master WHERE type='table'\").fetchone()[0]
print('integrity:',r,'| tables:',n)
sys.exit(0 if r=='ok' and n>=27 else 1)
" "$LATEST" || { echo "备份校验失败，中止上传"; exit 1; }

# 2) 压缩后上传（SQLite 压缩比很高，127MB → 约 35MB）
gzip -c "$LATEST" > /tmp/pf.db.gz
# 腾讯云 COS 示例：coscmd upload /tmp/pf.db.gz /portfolio/$(date +%Y%m%d).db.gz
# 最省事的替代：同步到另一块物理盘 / 移动硬盘 / 个人网盘

# 3) 每月 1 份长期归档（补上 7 天窗口之外的时间点）
[ "$(date +%d)" = "01" ] && cp "$LATEST" "data/backups/archive/monthly_$(date +%Y%m).db"
```

接到 15:30 链路的方式：在 `scheduled_run.bat:15` 与 `:18` 之间加一行 `call "%~dp0backup_offsite.bat" >> logs\scheduled_run.log 2>&1`。**同时必须把原始导入件（对账单 PDF、通达信 TSV）一并纳入异地备份**——那才是真正不可重建的资产。

---

## 5. P1 风险

### 5.1 依赖全部未锁 → 不可复现，等于没有回滚能力

`requirements.txt`（13 行）**13/13 全部使用 `>=`**，无一处上界：

```
pandas>=2.0.0     numpy>=1.24.0    requests>=2.31.0   scipy>=1.11.0
matplotlib>=3.7.0 plotly>=6.0.0    streamlit>=1.30.0  beautifulsoup4>=4.9.0
openpyxl>=3.0.0   python-docx>=1.0.0  schedule>=1.2.0  akshare>=1.11.0
playwright>=1.40.0
```

对比 `venv313/Lib/site-packages` 实际安装版本，**已经发生跨大版本漂移**：

| 包 | 声明下界 | 实装版本 | 跨越 |
|---|---|---|---|
| pandas | `>=2.0.0` | **3.0.5** | 跨 1 个大版本 |
| numpy | `>=1.24.0` | **2.5.1** | 跨 1 个大版本 |
| akshare | `>=1.11.0` | 1.18.81 | 7 个小版本 |
| streamlit | `>=1.30.0` | 1.61.1 | 31 个小版本 |
| scipy | `>=1.11.0` | 1.18.0 | 7 个小版本 |
| plotly | `>=6.0.0` | 6.9.0 | 9 个小版本 |

pandas 2→3 和 numpy 1→2 都是有破坏性变更的大版本跳跃。真正的后果：

- **`git checkout <旧 commit>` 恢复不了旧状态。** 代码回到了，依赖回不去——`pip install -r requirements.txt` 装到的是「今天的最新版」。这就是记分卡 `发布安全` 维度「能回退」判定为**未达 Bronze**的直接依据。
- **CI 与生产装到的依赖不同、每次 CI 之间也不同。** 今天 CI 绿、明天上游发个版就红，且无法归因。
- **Docker 构建不可复现。** 同一个 `Dockerfile` 隔一周构建，得到两套不同依赖。

**修复**

```bash
# 方案 A：pip 原生，改动最小（推荐先做这个止血）
venv313/Scripts/python.exe -m pip freeze > requirements.lock.txt
# 保留 requirements.txt 作为「直接依赖 + 宽松范围」，lock 文件用于复现
# CI 与 Dockerfile 改为 pip install -r requirements.lock.txt

# 方案 B：uv（更快、原生支持跨平台 lock，适合后续正规化）
pip install uv && uv pip compile requirements.txt -o requirements.lock.txt

# 顺手清理：schedule>=1.2.0 是无用依赖
grep -rn "import schedule" --include=*.py . | grep -v venv313 | grep -v archive
# 实测返回空 —— 全项目无一处 import，调度实际由 Windows 任务计划 + WorkBuddy 承担
```

### 5.2 部署文档与实际严重脱节

| 位置 | 文档写的 | 实际 |
|---|---|---|
| `setup_scheduler.ps1:12` | `$ScriptPath = "C:\Users\HUAWEI\...\run_analysis.bat"` | 工程在 D 盘。**脚本执行到 `:15` 的 `Test-Path` 就会 `Write-Error` 退出**——这个脚本现在是坏的 |
| `setup_scheduler.ps1:35` | `-WorkingDirectory "C:\Users\HUAWEI\..."` | 同上 |
| `setup_scheduler.ps1:38` | 触发时间 `15:10` | 实际 15:30（备份文件名 `portfolio_20260903_153016.db` 与 `DEPLOYMENT.md:95` 均为 15:30）。三处说法互相矛盾 |
| `DEPLOYMENT.md:85, 94` | 起始位置 `C:\Users\HUAWEI\...` | D 盘 |
| `README.md:185-192` | 4 个任务：08:00 盘前 / 15:30 盘后 / **15:25 备份** / 08:00 早盘；「按 cron 周一至周五」 | 无 15:25 独立备份任务（备份已并入 `run_analysis.py` Stage 0，`scheduled_run.bat:8-10` 明确说明「原 Stage 0 内联备份块已退休」）；无 08:00 任务；真实是 09:00/15:30/16:30/16:40/17:00 五个。且 4 个 WorkBuddy 自动化中 3 个是 `FREQ=DAILY`（**含周末**），与「周一至周五」不符 |
| `DEPLOYMENT.md:67` | 「自动创建 30 张表」 | 实际 27 张表（`README.md:213` 也写 30）。数量口径不一致 |
| `DEPLOYMENT.md:100` / `README.md:189` | 备份用 VACUUM INTO | 实际在线备份 API（`backup_db.py:35`） |

**判定**：`setup_scheduler.ps1` 处于**已损坏**状态（C 盘路径 + 错误时间），照它执行会失败或注册出错误的任务。`DEPLOYMENT.md` 与 `README.md` 的调度章节需要按 §2.1 的真实拓扑重写——现在照文档接手，会漏掉 4/5 的调度、并按错误的时间去排查问题。

**修复**：`setup_scheduler.ps1` 改为自动定位，彻底消除硬编码：

```powershell
$ScriptPath = Join-Path $PSScriptRoot "run_analysis.bat"
$WorkDir    = $PSScriptRoot
$Trigger    = New-ScheduledTaskTrigger -Daily -At "15:30"   # 与实际一致
```

### 5.3 C→D 迁移残留清单（完整）

| 文件:行 | 残留内容 | 影响 | 处置 |
|---|---|---|---|
| `.pytest_cache/.gitignore` | NTFS ACL 损坏，不可读 | **git 完全不可用**（P0-1） | 删目录 + 加 ignore |
| `setup_scheduler.ps1:12,35` | `C:\Users\HUAWEI\...` | 脚本已损坏 | 改 `$PSScriptRoot` |
| `DEPLOYMENT.md:85,94` | `C:\Users\HUAWEI\...` | 误导接手人 | 改文档 |
| `scripts/fetch_market_data.py:20` | `ROOT = r'D:\HuaweiMoveData\...'` 绝对路径写死 | 再迁移即失效 | 改 `Path(__file__).resolve().parent.parent` |
| `scripts/fetch_market_data.py:21,22` | `PY` / `QS` 指向 `C:\Users\HUAWEI\.workbuddy\...`（Python 3.13.12 与 neodata skill 的 `query.py`） | 见 §5.7 | 环境变量化 |
| `venv313/pyvenv.cfg:1,4,5` | `home` / `executable` / `command` 均指向 C 盘 | 实测仍可运行（base Python 确实还在 C 盘 `.workbuddy/binaries`），但 `activate.bat:11` 的 `VIRTUAL_ENV` 是错的 | 建议重建 venv |
| `venv313/Scripts/activate.bat:11` | `VIRTUAL_ENV=C:\Users\HUAWEI\...\venv313` | 手工 activate 后行为异常；`.bat` 走 `%~dp0` 直调 python.exe 绕过了它，故生产未受影响 | 重建 venv |
| `archive/*.py`、`archive/*.bat`（5 处） | C 盘路径 | 无（`archive/` 已被 `.gitignore:2` 忽略且不参与生产） | 不动 |
| C 盘外壳目录 | 残留 `data/` + `venv313/` | 磁盘占用 + 未来误操作时打开错目录 | 确认无独有数据后清理 |

**特别注意 venv 的双重脆弱**：`venv313/` 之所以没被 git 吞掉，靠的是 **pip 在 venv 内自动生成的 `venv313/.gitignore`（内容 `*`）**，而不是项目自己的 `.gitignore`——项目 `.gitignore:18-20` 只列了 `python-env/`、`venv/`、`.venv/`，**没有 `venv313/`**。一旦那个内部 `.gitignore` 被删或重建 venv 时未生成，886MB 就变成可暂存状态。应在项目 `.gitignore` 显式补 `venv313/` 做纵深防御。

### 5.4 Dockerfile 不可用于生产（7 个阻断项）

`Dockerfile`（33 行）写得整洁，但按「拿到就能跑」的交付标准逐条核对，7 项不达标：

| # | 问题 | 位置 / 证据 | 后果 |
|---|---|---|---|
| 1 | **无 `.dockerignore`** | 实测文件不存在 | `COPY . .`（`:19`）会把 `venv313`(886MB) + `data/backups`(767MB) + `data/database`(152MB) + `.git`(35MB) + `logs`(5.6MB) + 20 余张 PNG 全部塞进构建上下文，**上下文近 2GB**，且**把真实持仓数据烤进镜像层**——镜像一旦分享即数据泄露 |
| 2 | **缺 Playwright 浏览器** | `:16` 只 `pip install`，无 `playwright install chromium`；`DEPLOYMENT.md:36-40` 明确列为必需步骤 | 容器内截图 / PDF 导出必然失败 |
| 3 | **无 volume 声明** | 无 `VOLUME`，`:22` 只 `mkdir` | 容器重建 = 数据库归零。对一个「数据就是全部价值」的系统，这是致命项 |
| 4 | **无 HEALTHCHECK** | 全文无 | 编排层无法判活；Streamlit 挂了容器仍显示 running |
| 5 | **只能跑 Dashboard** | `CMD`（`:30-32`）固定 streamlit | 真正产生价值的 5 个定时任务在容器里**一个都跑不起来**（无 cron、无 entrypoint 分支） |
| 6 | **root 运行** | 无 `USER` | 最小权限原则缺失 |
| 7 | **`0.0.0.0` + 无鉴权** | `:31 --server.address=0.0.0.0` | Streamlit 无内建鉴权，暴露即等于公开全部持仓盈亏 |

外加一个结构性问题：`TDX_EXPORT_DIR` 默认 `C:\zd_zsone\T0002\export`（`config/settings.py:64`，实测该目录在本机存在），是 Windows 通达信客户端路径。容器内无法访问，持仓导入链路在 Docker 下天然断裂。

**建议**：不要试图把这套系统容器化。它的定位（`mvp-stack.md`：桌面端 + SQLite）与 Docker 的价值主张（可移植的无状态服务）根本不匹配。两条务实路线选一：

- **A（推荐）**：承认这是本机部署，**删除 `Dockerfile`**，把 `DEPLOYMENT.md` 写成一份诚实完整的 Windows 部署手册（含 §2.1 五个调度器）。
- **B**：保留 Docker 但只用于 **CI 测试环境**（跑 pytest，不跑生产），并在 `Dockerfile` 顶部注释说明「仅用于 CI，非生产部署」，同时补 `.dockerignore`。

无论选哪条，`.dockerignore` 都应立即补上——因为**现状是任何人执行一次 `docker build` 就会把持仓数据烤进镜像**：

```gitignore
# .dockerignore（建议内容）
.git
venv313
data/backups
data/database
data/reports
data/.neotmp
logs
output
archive
.workbuddy
.pytest_cache
*.png
*.log
.env
coverage.json
.coverage
```

### 5.5 Python 版本四处不一致

| 位置 | 声明版本 |
|---|---|
| 生产 venv（`venv313/pyvenv.cfg`） | **3.13.12** |
| `Dockerfile:5` | 3.12-slim |
| `.github/workflows/ci.yml:18` | 3.12 |
| `README.md:141` / `DEPLOYMENT.md:7` | 3.10+（「推荐 3.10.11」） |
| `pyproject.toml:3` black `target-version` | py39 |
| `pyproject.toml` mypy `python_version` | 3.9 |

生产跑 3.13，CI 验 3.12，文档说 3.10，静态检查按 3.9 的语法规则。**CI 通过不能推断生产可用**——3.12→3.13 之间 pandas 3.x / numpy 2.x 的行为差异完全在验证盲区内。建议统一到 3.13 并在 `pyproject.toml` 加 `requires-python = ">=3.13"`。

### 5.6 pre-commit 门禁形同虚设（且是「防大文件」那一条失效）

`.git/hooks/pre-commit` 是**手写 bash 脚本**，不是 pre-commit 框架的 runner。后果一：`.pre-commit-config.yaml` 里的 black / isort / flake8 / mypy / bandit **全部未生效**（`install_pre_commit.sh` 与 `README.md:158-160` 都写着 `pre-commit install`，但实际安装的是自制脚本）。

后果二更严重——第 3 步「防止提交大数据文件」是**空操作**：

```bash
# .git/hooks/pre-commit:32-39
LARGE_FILES=$(git diff --cached --name-only | while read f; do
    if [ -f "$f" ]; then
        SIZE=$(wc -c < "$f")
        if [ "$SIZE" -gt 5242880 ]; then
            echo "[WARN] Large file (>5MB): $f ($SIZE bytes)"    # ← 写进变量
        fi
    fi
done || true)
# ← LARGE_FILES 从此再未被引用，ERRORS 未递增
if [ "$ERRORS" -gt 0 ]; then ...                                  # ← 永远不因大文件触发
```

`LARGE_FILES` 被赋值后从未 echo、从未参与判定，`ERRORS` 也没递增。**5MB 门禁既不报警也不拦截**——它是这个仓库里唯一本该阻止 87.9MB DB 入库的机制，而它一行都没执行到位。（时间线上 hook 建于 8/20，晚于 8/3 那次提交，所以不是它放过去的；但它现在也拦不住下一次。）

后果三：hook 位于 `.git/hooks/`，**不进版本控制**。`clone` 到新机器 = 零门禁。且 `:5` 硬编码 `/d/HuaweiMoveData/...` 的 python 路径，换机即失效。另有 `.git/hooks/pre-commit.exe`（4/26 残留）与之并存，属死文件。

**修复**

```bash
# 1) 立刻修好大文件门禁（把 WARN 变成 FAIL）
#    在 .git/hooks/pre-commit 第 39 行后插入：
#    if [ -n "$LARGE_FILES" ]; then echo "$LARGE_FILES"; ERRORS=$((ERRORS + 1)); fi

# 2) 用框架接管，让 .pre-commit-config.yaml 真正生效
#    check-added-large-files 是官方实现，比自制逻辑可靠
pip install pre-commit && pre-commit install
#    并在 .pre-commit-config.yaml 的 check-added-large-files 加参数：
#      args: [--maxkb=5120]

# 3) 删掉死文件
rm -f .git/hooks/pre-commit.exe
```

### 5.7 16:40 日报链路无法真正无人值守

`scripts/fetch_market_data.py` 的三处外部耦合：

```python
# :20-22
ROOT = r'D:\HuaweiMoveData\Users\HUAWEI\Documents\lingxi-claw\portfolio_tracker'
PY   = r'C:\Users\HUAWEI\.workbuddy\binaries\python\versions\3.13.12\python.exe'
QS   = r'C:\Users\HUAWEI\.workbuddy\skills\neodata-financial-search\scripts\query.py'
```

它以 `subprocess` 调用**另一个 Python 解释器**去跑**一个 WorkBuddy skill 的脚本**（实测该文件存在，8/26 版本）。这意味着这条生产链路依赖：WorkBuddy 安装位置不变 + 该 skill 不被升级改名 + 内部 CLI 契约（`--query` / `--save-token`）不变。三者都不在本项目控制范围内，且都没有版本约束。

更硬的约束是 token：`:8-9` 与 `:289-295` 说明 NeoData token 过期时以 **exit(2) + `TOKEN_EXPIRED`** 退出，需要「自动化 agent 调 `connect_cloud_service` 取凭证 → `--save-token` → 重跑」。而 `.workbuddy/memory/automations/automation-1787708904209/memory.md`（9/1 条目）实录了这一幕：

> NeoData：token 经 connect_cloud_service 重取（**TTL 12h 已过期**）→ --save-token → 4 次查询成功

**TTL 12 小时、日报每天 16:40 跑 → 几乎每次都要刷新 token，而刷新只能由 AI agent 在会话中完成。** 这条链路不是「脚本化的定时任务」，而是「每天需要一个 agent 在场的半自动流程」。同一份 memory 还记录了 8/31 那次「乐享知识库连接器未连接 → 知识库导入未执行」，即**连接器缺失会导致步骤静默跳过**。

这不是必须马上改的缺陷（设计上是有意的：宁可跳过也不编造数据，这个取向是对的），但**必须在交接文档里写明**：16:40 日报不是无人值守任务，它对 agent 可用性与连接器状态有硬依赖。接手人若以为它是纯脚本，某天报告不来时会查错方向。

---

## 6. P2 问题（不阻断，建议顺手清）

| # | 问题 | 证据 | 建议 |
|---|---|---|---|
| 1 | `logs/scheduled_run.log` 无轮转 | 1,105,973 B，`scheduled_run.bat:15,18` 用 `>>` 无限追加 | 加按月切分或用 `logrotate` 等价逻辑 |
| 2 | 日志编码混乱 | `scheduled_run.log` 中 `.bat` 的 echo 是 GBK、Python 输出是 UTF-8，混在一起成乱码（`鎶曡祫缁勫悎` 等） | `.bat` 内 echo 改英文，或统一 `chcp 65001` + `PYTHONIOENCODING=utf-8` |
| 3 | **无任务失败告警** | `config/notification.json` 的 events 是 `source_all_down`/`dq_low_score` 等**数据质量**事件，没有「作业本身失败」事件 | 15:30 链路失败只写进没人看的日志 → 静默失败。应在 `scheduled_run.bat` 末尾判 `%ERRORLEVEL%` 非 0 时发一封告警邮件 |
| 4 | 根目录 4 个孤儿 .db | `portfolio.db`(0B)、`portfolio_data.db`(0B)、`portfolio_records.db`(0B)、`portfolio_tracker.db`(307,200B) | 确认无引用后删除。它们是 §P0-3 里那几个 0 字节 blob 的现世残留 |
| 5 | 根目录 24 张调试 PNG | `verify_*.png`/`final_*.png`/`full_*.png` 等，合计约 1.6MB | 已被 `.gitignore:69-77` 忽略，但污染工作区。移入 `archive/` 或删除 |
| 6 | `.env.QA_RESIDUE_20260805` | 内容 `D5_TEST_NO_OVERRIDE=should_not_override`，8/5 QA 残留，且是未跟踪文件 | 删除 |
| 7 | 无用依赖 `schedule>=1.2.0` | 全项目无 `import schedule`（实测） | 从 `requirements.txt` 移除 |
| 8 | 3 个自动化含周末空跑 | `FREQ=DAILY`（09:00 / 16:30 / 16:40），README 却称「周一至周五」 | 改 `BYDAY=MO,TU,WE,TH,FR`，与 17:00 那个保持一致 |
| 9 | `data/backups/` 目录未显式忽略 | `.gitignore` 只靠 `*.db`(:85) 兜住备份文件 | 补一行 `data/backups/`，防止将来非 .db 产物入库 |
| 10 | `portfolio.db.backup_phase0` | 30,642,176 B，5/21 遗留在 `data/database/` 内 | 移入 `data/backups/archive/` 或删除 |
| 11 | SQLite 单写者与并发窗口 | 16:30 与 16:40 两个自动化写同一个库，仅隔 10 分钟 | **实测 `logs/` 中 `database is locked` 出现 0 次**，风险尚未发生。但 16:30 巡检若超时 10 分钟就会撞上。建议把 16:40 后移到 17:10，或统一开 WAL |

---

## 7. 环境变量与密钥管理

**这一块是全项目做得最好的部分，先给结论：无泄露，机制正确。**

| 检查项 | 结果 | 证据 |
|---|---|---|
| `.env` 是否入库 | **从未入库** | `git log --all -- .env .env.local` 返回空；`git ls-files \| grep '^\.env'` 只有 `.env.example` |
| `.env` 是否被忽略 | 是 | `.gitignore:5-6` |
| `.env.example` 是否含真实密钥 | 否，全部注释占位 | `.env.example:26-28` 的 `EMAIL_USERNAME` / `EMAIL_PASSWORD` 均为注释状态 |
| 已跟踪配置是否含密钥 | 否 | `config/notification.json` 的 `webhook.url` 为空串；`config/settings.py` 无硬编码凭据 |
| 密钥是否环境变量化 | 是 | `config/settings.py:10-36` 手写 `.env` 解析器，优先级「系统环境变量 > .env > 默认值」（`:31` `if key not in os.environ`），语义正确 |
| 是否引入多余依赖 | 否 | 未用 `python-dotenv`，自己 27 行搞定，减少一个依赖，判断合理 |
| 无凭据时的降级 | 优雅 | `EMAIL_PASSWORD` 为空时 `send_report_email.py` 直接 exit 0 跳过（`README.md:209`），不刷失败日志 |
| CI 是否用假值 | 是 | `ci.yml:27-30` 显式 `EMAIL_ENABLED: "false"`、`WECHAT_ENABLED: "false"` |

**两点残余风险**（都不是「泄露」，是「静态存储」层面的）：

1. `.env` 中 `EMAIL_PASSWORD` 为 16 位明文（QQ 邮箱授权码，长度特征吻合）、`EMAIL_USERNAME` 12 位。单用户本机场景下这是可接受的工程取舍，但该文件应确保 ACL 只对当前用户可读——尤其考虑到本次已经发现 `.pytest_cache/.gitignore` 的 ACL 在迁移中损坏，同批文件的权限状态不可假定完好：

```powershell
icacls .env                                  # 先看现状
icacls .env /inheritance:r /grant:r "$env:USERNAME:(R)"   # 收紧为仅当前用户可读
```

2. NeoData token 由 WorkBuddy 侧缓存（`fetch_market_data.py:292-295` 通过 `--save-token` 转交），本项目不落盘，这个设计是对的。但 token 缓存位置在 `.workbuddy` 体系内，**不在本项目的密钥轮换视野中**——交接时需说明「有一个 12h TTL 的外部凭据不由本项目管理」。

---

## 8. 生产就绪记分卡评级

按 `references/01-standards/production-readiness-scorecard.md` 七维打分。**总档取各维最低档。**

| 维度 | 档位 | 判定证据 |
|---|---|---|
| 测试 + 回归 | **Bronze** | 有 tests/ 且规模可观、pre-commit 有快速门禁；但 CI 三个月未真实触发、`:memory:` 下大概率红、QA 交接评估已列 P0 级测试隔离缺陷。未达 Silver（无回归集门禁、回归率非零） |
| 契约 | **Bronze** | 无对外 API；内部契约=DB schema（`src/utils/db_schema.py` DDL）；但无迁移工具、无 schema 版本号，文档表数（30）与实际（27）不符。未达 Silver |
| 安全 | **Bronze** | 密钥不入库 ✓、无硬编码凭据 ✓、`.env` 机制正确 ✓；但历史中 87.9MB 真实财务数据待清理、Dockerfile `0.0.0.0` 无鉴权、无依赖存在性/漏洞核验（`safety`/`bandit` 未进 CI）。未达 Silver |
| 无障碍 | **未评估** | 单用户内部 Dashboard，非本岗位判定范围。Streamlit 默认组件基本可键盘操作，视作满足 Bronze「核心流可键盘完成」。建议由前端/QA 岗补评 |
| 性能 | **Silver** | 资金流查询加 date 过滤 + 复合索引（23x/13x 提速，`DEPLOYMENT.md:231`）、`@st.cache_data` 缓存、`get_db_connection()` 统一连接避免泄漏、日常全流程 2~4 分钟（`.workbuddy/memory` 实录）。未达 Gold（无容量/压测、无性能预算守恒） |
| 可观测 | **Bronze** | 有分级日志、`execution_logs` / `alerts` 表、`src/utils/monitor` 数据质量告警；但日志不轮转、编码混乱、**无作业失败告警**（P2-3）、无 SLI/SLO。未达 Silver |
| 发布安全 | **未达 Bronze** | Bronze 的唯一判据是「**能回退**」，三条同时不成立：① 依赖全 `>=`，checkout 旧 commit 无法复现旧环境（§5.1）② 唯一远端副本落后 158 commit / 3 个月，且 `git status` 当前不可用（§P0-1、P0-2）③ 5 个生产脚本与全部编排逻辑不在版本控制内，无「上一个版本」可回（§P0-2） |

### 总档：未达 Bronze（短板 = 发布安全）

**离 Bronze 全绿差 4 项**（即 §4 的四条 P0）：

1. 修 `.pytest_cache` ACL，恢复 git 可用性
2. 5 个生产脚本 + 4 个自动化编排入库
3. 清 87.9MB DB 历史 → 确认远端 private → 推送 158 commit
4. 备份异地化 + 一次恢复演练

**目标档建议：不追 Silver 全绿，追「Bronze 全绿 + 安全/可观测/发布安全 三维到 Silver」。**

理由：记分卡说「商业级最低 Silver」，但本系统不对外销售、无多用户、无 SLA 承诺，`契约`/`无障碍` 维度冲 Silver 属于为不存在的风险付账。而这套系统真实的失效模式只有两个——**数据丢失**和**交接失败**——它们精确落在 `安全`、`可观测`、`发布安全` 三维上。把资源集中在这三维，是对这个具体系统而言性价比最高的分配。

**留痕**（记分卡第 4 节要求）：档位=未达 Bronze；证据=本文 §4/§5 逐条；责任人=接手运维；目标档=Bronze 全绿 + 三维 Silver；期限=P0 四项 1 周内，P1 两周内。

---

## 9. 交付包现状

按「用户拿到即用」的标准核对——即：`git clone` 之后能不能把这套系统在一台新 Windows 机器上跑起来。**结论：不能。**

| 交付包应有 | 现状 | 缺口 |
|---|---|---|
| `README.md` + 一键启动 | 有（14KB，内容详实） | 调度章节失真（§5.2）；`run_all.bat` 9 选项菜单可用 |
| `.env.example` | **有，且质量好** | 无缺口，可直接 `copy .env.example .env` 使用 |
| `.gitignore` | 有 | 缺 `.pytest_cache/`、`venv313/`、`data/backups/` |
| `DEPLOY.md` 部署 + **回滚方案** | 有 `DEPLOYMENT.md`（5.7KB，故障排查表写得好） | **完全没有回滚章节**；C 盘路径失效；调度表错误 |
| 依赖锁文件 | **无** | 新机器装到的依赖与生产不同（§5.1） |
| 完整生产代码 | **否** | 5 个脚本未入库（§P0-2） |
| 调度编排定义 | **否** | 4 个 WorkBuddy 自动化的 prompt 全在仓库外 |
| 运维 runbook | **否** | 卡死恢复等知识只在 gitignore 的 `.workbuddy/memory` 里 |
| 测试报告 | 有 | `docs/测试交接评估.md`、`data_coverage_report.md` |
| 数据初始化路径 | 半 | `DEPLOYMENT.md:61-67` 给了建库命令，但新库是空的；无脱敏样例数据，接手人无法验证功能 |
| 不应包含的东西 | **包含了** | 根目录 24 张调试 PNG、4 个孤儿 .db、`coverage.json`(467KB)、`.env.QA_RESIDUE_*` |

**新机器部署的真实断点（按遇到顺序）**：

1. `git clone` → 拿不到 5 个生产脚本 → 16:40 日报链路缺失
2. `pip install -r requirements.txt` → 装到未来某版 pandas/numpy → 行为未知
3. `python -m playwright install chromium` → 这步文档有写，OK
4. 建空库 → 无数据，Dashboard 15 个 Tab 全空 → 无法验收
5. 配定时任务 → `setup_scheduler.ps1` 因 C 盘硬编码直接报错退出
6. 4 个 WorkBuddy 自动化 → 无任何文档可依据，**只能靠原作者口述重建**

第 6 条是交付包最本质的缺陷：**系统 4/5 的调度能力无法通过交付物传递。**

---

## 10. P0 修复清单（可直接执行，顺序不可调换）

顺序理由：① 不修 git 就改不了也提交不了；② 不先提交就会在改写历史时丢掉未跟踪文件；③ 不先清历史就推送 = 数据泄露自动化；④ 备份独立于前三步，但必须在「敢删任何东西」之前先有异地副本。

```bash
cd "D:/HuaweiMoveData/Users/HUAWEI/Documents/lingxi-claw/portfolio_tracker"

# ═══ 步骤 0：先冷备，再动手（任何改写前的铁律）═══
cp -r .git "D:/backup_git_$(date +%Y%m%d)"
cp data/backups/$(ls -t data/backups/portfolio_*.db | head -1 | xargs basename) "D:/backup_db_$(date +%Y%m%d).db"

# ═══ 步骤 1：恢复 git 可用性（P0-1，约 30 秒）═══
rm -rf .pytest_cache      # 失败则先 takeown /f .pytest_cache /r /d y
printf '\n.pytest_cache/\nvenv313/\ndata/backups/\n' >> .gitignore
git status --porcelain | head          # 应正常返回，不再 BUG

# ═══ 步骤 2：生产代码与运维知识入库（P0-2 前半）═══
git add scripts/gen_combo_report.py scripts/fetch_market_data.py \
        scripts/backfill_sector_change.py scripts/probe_institution_gap.py \
        scripts/verify_sina_whitelist.py .gitignore
git commit -m "chore: 补入 16:40 日报链路脚本 + 修正 .gitignore（pytest缓存/venv/备份）"
rm -f .env.QA_RESIDUE_20260805 .git/hooks/pre-commit.exe

# ═══ 步骤 3：清理 87.9MB DB 历史（P0-3，务必在 push 之前）═══
pip install git-filter-repo
git filter-repo --path data/database/portfolio.db --path portfolio.db \
                --path data/portfolio.db --path portfolio_data.db \
                --invert-paths --dry-run          # 先演练，读输出
git filter-repo --path data/database/portfolio.db --path portfolio.db \
                --path data/portfolio.db --path portfolio_data.db \
                --invert-paths --force
git rev-list --objects --all | git cat-file --batch-check='%(objecttype) %(objectname) %(objectsize) %(rest)' \
  | awk '$1=="blob" && $3>5000000'                # 必须为空，否则不许 push
du -sh .git                                        # 应从 35MB 明显下降

# ═══ 步骤 4：确认远端可见性后推送（P0-2 后半）═══
# 【人工确认】github.com/asdfly/portfolio-tracker 必须是 Private —— 本次评估未能核实
git remote add origin https://github.com/asdfly/portfolio-tracker.git   # filter-repo 会移除 remote
git push --force-with-lease origin master

# ═══ 步骤 5：备份异地化 + 恢复演练（P0-4）═══
# 5a 校验最新备份可用（当前完全缺失这一环）
LATEST=$(ls -t data/backups/portfolio_*.db | head -1)
venv313/Scripts/python.exe -c "
import sqlite3,sys
c=sqlite3.connect(r'$LATEST')
print('integrity:',c.execute('PRAGMA integrity_check').fetchone()[0])
print('tables:',c.execute(\"SELECT count(*) FROM sqlite_master WHERE type='table'\").fetchone()[0])
"
# 5b 恢复演练：拷到临时位置起 Dashboard，确认 15 个 Tab 有数据（勿覆盖生产库）
# 5c 落异地：对象存储 / 另一块物理盘 / 移动硬盘 任选，并把原始对账单 PDF、通达信 TSV 一起带上
# 5d 接入 scheduled_run.bat，并加每月归档一份
```

**验收标准**（做完逐条核对）：

- [ ] `git status` 不再报 `mingw.c:156`
- [ ] `git ls-files | grep -c '^scripts/'` 包含全部 5 个新增脚本
- [ ] 历史中无 >5MB blob；`.git` 体积明显下降；`git log --oneline | wc -l` 仍为 334
- [ ] 远端 `origin/master` 与本地 HEAD 一致，且仓库为 Private
- [ ] `data/backups` 之外（另一块盘或云）存在至少 1 份可通过 `integrity_check` 的备份
- [ ] 完成 1 次真实恢复演练并记录耗时
- [ ] `docs/runbook/` 下有 4 个自动化的编排与恢复步骤

---

## 11. 不要动的地方

评估中确认这些是有意为之的正确设计，改动只会引入风险：

1. **`.bat` 里的 `%~dp0` 相对定位**（`run_analysis.bat:4-5`、`send_report_email.bat:4-5`、`scheduled_run.bat:3`）——这是 C→D 迁移后生产链路没断的直接原因。不要改成绝对路径。
2. **`config/settings.py:10-36` 自写的 `.env` 解析器**——语义正确（不覆盖已有环境变量）、少一个依赖，不要为了「规范」换成 `python-dotenv`。
3. **`scripts/backup_db.py:32-38` 的 SQLite 在线备份 API**——比 `cp` / `VACUUM INTO` 都更适合有并发写入的场景，`keep_min=3` 的兜底也想得周到。要改的是文档（说成了 VACUUM INTO），不是代码。
4. **`send_report_email.py` 无凭据时 exit 0 跳过**——避免了每天刷失败告警，取舍正确。
5. **`fetch_market_data.py` 的 `TOKEN_EXPIRED` exit(2) 而非编造数据**——数据诚实性优先于流程顺畅，这是金融数据系统该有的取向。要补的是交接文档里的说明，不是改行为。
6. **生产数据库 `data/database/portfolio.db` 与 `data/backups/` 下的一切**——本次评估全程只读，未做任何写入。任何清理动作前先完成步骤 0 与步骤 5。

---

## 附：评估方法与未能核实项

**方法**：以 `Read` / `Grep` / `Glob` 为主，`git` 命令仅用于只读查询（`ls-files` / `log` / `rev-list` / `cat-file` / `branch -r` / `merge-base` / `check-ignore`），未执行任何写操作（无 `add` / `commit` / `filter-repo` / `gc`）。所有结论均可由文中给出的命令复现。

**未能核实的 3 项**（交接后应优先补齐）：

1. **GitHub 仓库 `asdfly/portfolio-tracker` 的可见性（Private / Public）**——`gh` 未登录，无法查询。这直接决定 §P0-3 的紧急程度：若为 Public，则 §P0-3 从「防泄露」升级为「必须在下次 push 前完成，且需评估 origin/master 上 0 字节 .db 之外是否还有敏感文本」。
2. **Windows 任务计划程序的实际注册状态**（任务名、触发器、上次结果）——`schtasks.exe` 被沙箱安全策略拦截。当前的运行结论由 `data/backups/` 时间戳（含周末正确跳过）与 `logs/scheduled_run.log` 反推，证据充分但非直接读取。接手后请执行 `schtasks /query /tn PortfolioDailyAnalysis /fo LIST /v` 核实真实触发时间（15:10 还是 15:30）。
3. **`filter-repo` 后 `.git` 的确切体积**——文中 10~15MB 为按「87.9MB blob 是 35MB pack 中最大单体压缩对象」的推算，非实测。
