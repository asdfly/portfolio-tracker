# 06 - Git 历史清理 Runbook（P0：87.9MB 生产数据库 blob）

> 本文件为**操作手册（草稿）**，由 devops-3 在 2026-09-03 准备。
> 当前阶段**仅做预演与准备**，未执行任何历史重写命令。
> 正式执行需 lead 向用户确认后再动手。所有命令均基于仓库实测事实，非猜测路径。

---

## 0. 背景与现状（基于 2026-09-03 实测）

- **仓库路径**：`D:\HuaweiMoveData\Users\HUAWEI\Documents\lingxi-claw\portfolio_tracker`（D 盘；C 盘同名目录为迁移空壳，勿用）
- **规模**：336 个 commit，5 个 ref，HEAD = `25390f7`（分支 `master`）
- **P0 阻塞项**：历史中躺着一个 **87.9MB 真实生产数据库 blob**，至今仍可从 `master` 历史检出。
- **引入提交**：`24574df`（2026-08-03，作者 asdfly，信息 "chore: 提交数据库文件 portfolio.db (84MB, 30表)"）引入了 `data/database/portfolio.db`（原始 87,949,312 字节）。该文件在 `e0f8d43`（"接手治理: 数据库移出 git 跟踪"）从跟踪中移除，但 **blob 仍留在历史里**。
- **可达性**：该 blob (`9d713dfc48dafcde44b65f334d1c666c6a42c32f`) 仅从**本地未推送的 `master`** 历史可达；远端 `origin/master` 停留在 `7cbada1`（2026-06-05），**不包含该 blob**。即属于"160 个从未推送的 commit"窗口内。一旦把这些 commit push 上去，blob 永久留存于 GitHub，后续清理需联系 GitHub 支持。
- **全历史大对象扫描（>1MB）仅 2 个**：
  1. `data/database/portfolio.db` — 87.9MB（原始）/ 27.8MB（loose 压缩后），**P0 必须清除**
  2. `output/test_export.pdf` — 1.1MB（sha `9ef881941024eb8703895885780adecdcde9250c`），生成物（在 `0d68dc5` 加入、`58dc53c` 移除），建议一并清除
  - 评估中提到的 `data/backups/portfolio_db_20260803.db` (79.3MB) **从未被提交进 git**（扫描无此 blob），只是 commit message 里提到的本地备份，无需处理历史。
- **`.git` 当前体积**：35.0MB（loose 30.7MB + pack 4MB）。其中 87.9MB blob 的 loose 压缩体占 **27.8MB（约 80%）**。
- **清理后预期**：`.git` 约 **7–10MB**（移除 27.8MB 后重新 pack）。
- **工作树生产库**：`data/database/portfolio.db` 当前在磁盘为 122MB（提交后持续增长），**清理只动 git 历史，绝不删除该文件**。
- **完整备份**：`D:\HuaweiMoveData\Users\HUAWEI\Documents\lingxi-claw\_backup_portfolio_tracker_git_mirror_20260903.git`（mirror 克隆，校验通过：336 commit / 5 ref / blob 存在 / 35M）。

---

## 1. 前置检查清单（执行前必须全部满足）

- [ ] **备份已就绪**：mirror 备份存在且校验通过（`git -C <backup> rev-list --count --all` == 336；`for-each-ref` == 5；`cat-file -t 9d713dfc...` == blob）
- [ ] **工作区干净**：`git status --short` 为空。若触发 Git-for-Windows `err_win_to_posix` 崩溃（全量遍历吞输出），改用 `git ls-files --others --exclude-standard` 确认只有预期的那 8 个未跟踪文件
- [ ] **git-filter-repo 已安装**：`git filter-repo --version` 有输出（见 §5 安装方式；本次实测未安装）
- [ ] **`.gitignore` 已封堵**：`data/backups/`、`data/.neotmp/`、`data/risk_backtest.json`、`*.sqlite`、`*.sqlite3` 已加入（本次已补）
- [ ] **已通知所有协作者**：本次重写会改变**全部 commit hash**；任何其它 clone / worktree 将失效，需删除后重新 clone
- [ ] **确认远端状态**：私有仓库 `asdfly/portfolio-tracker`，除执行者外无人已拉取本地未推送历史（远端当前仅到 `7cbada1`，安全）

---

## 2. 精确的清理命令（基于实测路径，非猜测）

### 方案 A（推荐）：清除 87.9MB 生产库 blob + 生成的 PDF
```bash
cd D:/HuaweiMoveData/Users/HUAWEI/Documents/lingxi-claw/portfolio_tracker
git filter-repo --force \
  --path data/database/portfolio.db \
  --path output/test_export.pdf \
  --invert-paths
```

### 方案 B（保守，仅处理 P0 的 87.9MB 库）
```bash
cd D:/HuaweiMoveData/Users/HUAWEI/Documents/lingxi-claw/portfolio_tracker
git filter-repo --force \
  --path data/database/portfolio.db \
  --invert-paths
```
> 说明：`--invert-paths` 表示"**删除**指定的路径"。两个路径均来自 §0 实测的 blob 清单；只关心 P0 时用方案 B。

---

## 3. 执行后验证

```bash
# 3.1 重新扫描大对象：87.9MB blob 必须消失
git rev-list --objects --all \
  | git cat-file --batch-check='%(objecttype) %(objectname) %(objectsize) %(rest)' \
  | awk '$1=="blob" && $3>1000000'
#   期望：方案 A 下为空；方案 B 下仅剩 output/test_export.pdf (1.1MB)

# 3.2 commit 数守恒（应为 336）
git rev-list --count --all

# 3.3 原 blob 应已不存在
git cat-file -t 9d713dfc48dafcde44b65f334d1c666c6a42c32f
#   期望：fatal: not a valid object name

# 3.4 .git 体积（期望 7~10MB，原为 35MB）
du -sh .git

# 3.5 工作树完好：无意外改动
git status --short
#   若触发 mingw 崩溃，改用：git ls-files --others --exclude-standard

# 3.6 跑一次测试，确认源码未被破坏
pytest -q        # 或项目实际测试命令

# 3.7 确认 HEAD 仍是业务最新提交（内容不变，仅 hash 变）
git log --oneline -3
```

---

## 4. 回滚方案（从 mirror 备份完全恢复）

备份位置：
`D:\HuaweiMoveData\Users\HUAWEI\Documents\lingxi-claw\_backup_portfolio_tracker_git_mirror_20260903.git`

```bash
cd D:/HuaweiMoveData/Users/HUAWEI/Documents/lingxi-claw

# 4.1 把可能已改写的仓库移开（保留工作文件作为安全副本，不删除备份）
mv portfolio_tracker portfolio_tracker_ATTEMPTED_$(date +%Y%m%d)

# 4.2 从 mirror 备份克隆出干净仓库（完整历史 + 全部 ref，原始 hash）
git clone _backup_portfolio_tracker_git_mirror_20260903.git portfolio_tracker

# 4.3 还原工作树中的生产数据文件（这些从未进 git，需从副本拷回）
cp -r portfolio_tracker_ATTEMPTED_*/data/database/portfolio.db portfolio_tracker/data/database/ 2>/dev/null
cp -r portfolio_tracker_ATTEMPTED_*/data/backups/*            portfolio_tracker/data/backups/ 2>/dev/null

# 4.4 校验
cd portfolio_tracker
git rev-list --count --all                                              # 期望 336
git log --oneline -1                                                    # 期望 25390f7（原始 HEAD）
git cat-file -t 9d713dfc48dafcde44b65f334d1c666c6a42c32f              # 期望 blob（已恢复）
```

> 注意：mirror 备份是完整保险，任何一步出错都可回到执行前状态。备份本身**不要删除**。

---

## 5. git-filter-repo 安装方式（供决策，本次实测未安装）

实测：`git filter-repo --version` 报 "not a git command"；`venv313/Scripts/python.exe -m pip show git-filter-repo` 报未安装。
**推荐装到用户级或独立环境，不要污染项目 venv313（那是运行环境，保持纯净）：**

- 用户级 pip（Git Bash）：
  ```bash
  python -m pip install --user git-filter-repo
  ```
- 或独立专用 venv（最干净，不影响任何项目）：
  ```bash
  python -m venv C:/Users/HUAWEI/.filterrepo-venv
  C:/Users/HUAWEI/.filterrepo-venv/Scripts/pip install git-filter-repo
  C:/Users/HUAWEI/.filterrepo-venv/Scripts/git-filter-repo --version
  ```
- 或随 Git for Windows 的辅助组件（部分版本自带，可用 `git filter-repo --version` 先验证）

装好后验证：`git filter-repo --version`。

---

## 6. 风险与注意事项

- **全部 commit hash 会改变**（filter-repo 重写整段历史）。
- **任何其它 clone / worktree 会失效**，需删除后重新 clone。
- **首次推送必须是 force push**（`git push --force --all && git push --force --tags`，或 `git push --mirror`），因为远端当前历史（≤`7cbada1`）与本地新 hash 不兼容、非快进。
- 远端为**私有**仓库；确认除执行者外无人已拉取本地未推送历史（远端当前仅到 `7cbada1`，安全）。
- 清理**只移除 git 历史中的 blob**；工作树里的 `data/database/portfolio.db`（122MB 生产库）保持原样、**绝不删除**。
- **切勿在清理前先 push**：一旦 87.9MB blob 上 GitHub，清除需联系 GitHub 支持，极为麻烦。
- 本机坑：项目在 D 盘，`git status` / `git add -A` 全量遍历可能触发 Git-for-Windows `err_win_to_posix` 崩溃（输出被吞 / Exit 3）。遇到就改用 `git ls-files` / `git diff --name-status HEAD`。
- `.pytest_cache` 目录有 NTFS ACL 损坏、删不掉且已 gitignore，不要试图删它。
