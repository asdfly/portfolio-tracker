# 规则：环境与路径（portfolio_tracker）

适用于所有文件操作与命令执行，自动加载生效。

1. **项目根目录**：`/d/HuaweiMoveData/Users/HUAWEI/Documents/lingxi-claw/portfolio_tracker`（D 盘，疑似迁移/外部盘，注意稳定性）。
2. **Python 运行时**：优先仓库内 `venv313/Scripts/python.exe`。
3. **真实数据库**：`data/database/portfolio.db`（非 `data/portfolio.db` 空壳）。
4. **托管 Python 读不到 D: 盘**：`~/.workbuddy/binaries/python/...` 的运行时沙箱把 `/d` 当作桩目录，无法访问真实 D: 文件。跨盘读写请用 Git Bash 或系统 Python `C:/Program Files/Python310/python.exe`。
5. **git 远程**：`github.com/asdfly/portfolio-tracker`，推送走 SSH-over-443。
