# 新会话准备指引 (NEW_SESSION_PREP)

> 用途：在 WorkBuddy 启动**新会话**继续 `portfolio_tracker` 项目前的准备清单。
> 本文件是**简明版**；完整铁律见 `docs/handover/12_engineering_invariants.md`（随仓库走，权威源）。
> 最近更新：2026-09-23。仓库 HEAD **以 `git rev-parse HEAD` 实测为准**（本文件撰写时 HEAD=`a4abd2c`，已推送）。⚠️ 工作区可能含未提交改动（见 §6），勿假定干净。

## 0. 一句话交接
新会话开局第一句：
> 继续 portfolio_tracker，先读 `docs/handover/12_engineering_invariants.md`（铁律全集，随仓库跟踪，权威源），再动手；`.workbuddy/memory/` 仅作本地补充，可能只到 09-18 且缺 MEMORY.md（见 §1）。

## 1. 铁律索引在哪（最重要）
本项目沉淀了重型"铁律索引"，新会话若不加载它，**必重蹈覆辙**（静默改坏生产库、假成功、任务名错配等）。
- **权威源（随仓库跟踪、必读）**：`docs/handover/12_engineering_invariants.md` —— 完整判据/SQL/证据全在这。开局第一件事就是读它。
- **`.workbuddy/memory/` 是两个不同的位置，且都不保证完整**：
  - 仓库内那份 `D:/.../portfolio_tracker/.workbuddy/memory/`，**gitignored、不被跟踪**，实测最新只到 `2026-09-18.md`、无 `MEMORY.md`、缺 09-19~09-22。仅作本地补充，**绝不可当作唯一真相源**。
  - 若重开的是同一个 WorkBuddy 会话工程，其工作区记忆（如 `C:/Users/HUAWEI/WorkBuddy/2026-08-05-09-57-26/.workbuddy/memory/`）才有完整链（含 MEMORY.md 与 09-19~22），但**新开仓库文件夹不会看到它**。
- 结论：交接正确性只依赖 `12_engineering_invariants.md` + 本文件；记忆文件只是锦上添花。

## 2. 起点状态（已实测，直接继承）
- 仓库 HEAD 以 `git rev-parse HEAD` **实测**为准（2026-09-23 实测 = `a4abd2c`）；工作区**不保证干净**，见 §6
- 生产定时任务「投资组合每日分析」：沙箱内**无法自验**（PowerShell 输出被吞，见 §3），状态须由你本地自查（命令见 §6）。勿凭记忆断言 Ready
- `venv313` 在；Python 3.13 已对齐（Docker/CI）；备份上限 = 7；P1-D 直连已收敛；setup_scheduler 已修
- `scripts/restore_db.py` **仍缺失**（P0-F 待办，见 §4）

## 3. 必须继承的"静默失效高发"铁律（摘要）
- 只认 `data/database/portfolio.db`；其余 `.db` 全是副本
- 诊断库用 `file:...?mode=ro` + `uri=True`；写库前先备份
- Git：显式 pathspec（**禁 `git add -A` / `git add .`**）、`git status` 已禁（用 `git diff --name-status HEAD`）、`git push` 须 `dangerouslyDisableSandbox` + `git ls-remote` 逐字验真
- Git Bash **无 coreutils**（ls/cat/head/tail/grep/wc 均 127）→ 用 python / Read / Grep / Glob
- `python -c` 禁反引号；`&&` 断链静默跳过 → 收尾 `echo "chain OK"` 验链
- 数字要么显式标记、要么显式拒答，不许静默；**不信任会话头的 `<current_time>`**，以本机时钟为准
- 测试计数**必须看 `collected`**（基线 1881 collected / 1877 passed / 4 skipped / 0 failed @ `e957464`）；默认 120s 超时会在 ~82% 处 SIGTERM，全量须 `run_in_background` 或加大超时
- 删除操作须**精确文件名清单**，禁前缀/glob
- 全量回归：`venv313/Scripts/python.exe -m pytest -q -p no:cacheprovider`（约 3m20s）
- ⚠️ 沙箱内 PowerShell `Get-ScheduledTask` / `schtasks` **stdout 被吞**（探针 `Write-Output` 也无输出），计划任务状态**无法自验** → 标记 UNKNOWN，请你本地运行并回传（命令见 §6）

## 4. 可直接接手的真待办
1. 给依赖真实 DB/网络的用例补 `@pytest.mark.integration`（目前 CI 的 `-m "not integration"` 因 0 处打标是空操作）
2. `scripts/` 下 20+ 处 `sqlite3.connect`（一次性脚本/探针）尚未收敛到 `get_db_connection`
3. **P0-F 异地副本 + 恢复演练：`scripts/restore_db.py` 仍 MISSING**，需创建
4. `snapshot_gate` / `price_history_gate` 两处直连（busy_timeout=15000，拒收 `file:` URI）待定
5. evolution 台账若干项（`#129/#130` fund_flows 覆盖链、`#140` tab8 哨兵值 `50` 与中性值碰撞等）

## 5. 关键路径速查
| 项 | 路径 |
|----|------|
| 铁律全集 | `docs/handover/12_engineering_invariants.md` |
| 生产库 | `data/database/portfolio.db` |
| Python | `venv313/Scripts/python.exe` |
| 调度脚本 | `setup_scheduler.ps1`（任务名 = `投资组合每日分析`） |
| 备份目录 | `data/backups/`（顶层留最近 7 个） |
| 运行账本 | `data/reports/run_report_<date>.json`（回填会被就地覆盖，非不可变物证） |

## 6. 已知工作区状态（2026-09-23 实测，新会话须复核）
- **未提交改动**：`scripts/gen_combo_report.py` 被修改（+12 / −1），内容为 evolution #30（军工主线顺风 streak 由 >0 转 0 时的显式红色终止提示，并把写死的「（主线未熄火）」改为按状态派生）。归属待丹哥确认（可能是本机编辑，也可能是他人）。
- ⚠️ **时效性风险**：`run_analysis.bat` 只 `cd /d "%~dp0"` 后跑 `run_analysis.py`，**不 pull、不 checkout** ⇒ 每次 15:30 生产运行跑的就是**含这 12 行的工作区版本**。
- 处置三选一（未决前新会话保持只读）：
  - `(a)` 显式 pathspec 提交：`git add scripts/gen_combo_report.py` → `git commit` → `git show --stat HEAD` 自核（如需推送再 `dangerouslyDisableSandbox` + `git ls-remote` 验真）
  - `(b)` 保持现状不动（15:30 会用到它）
  - `(c)` `git stash` 冻结，事后再 `git stash pop`
- **定时任务本地自查命令**（请丹哥在本地 PowerShell 执行并回传，沙箱内读不到）：
  ```powershell
  Get-ScheduledTask -TaskName "投资组合每日分析" | Select-Object TaskName,State | fl
  Get-ScheduledTask -TaskName "投资组合每日分析" | Get-ScheduledTaskInfo | fl LastRunTime,LastTaskResult,NextRunTime
  ```
