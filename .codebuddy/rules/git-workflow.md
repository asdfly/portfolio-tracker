# 规则：Git 工作流铁律（portfolio_tracker）

适用于本项目所有涉及 git 的操作，自动加载生效。

1. **显式提交**：永远使用 `git add <具体路径>` 显式 pathspec 提交；禁止 `git add -A`、`git add .`、`git add -u` 等批量暂存，避免卷走他人改动或自动化产物（如 `data/reports/run_report_*.json`、`backfill_etf_price_history_westock.py` 等）。
2. **不主动推送**：未获得用户明确授权，绝不执行 `git push`。
3. **跨盘操作走 Git Bash**：对 D: 盘文件做读写/合并时，不要用托管 Python 运行时（看不到真实 D:），改用 Git Bash（`/usr/bin/bash` 经 Git 安装）或系统 Python `C:/Program Files/Python310/python.exe`。
4. **勿碰本地状态目录**：`.workbuddy/`（记忆、自动化）已被 `.gitignore` 排除，是本地状态，不要强行加入版本库。
