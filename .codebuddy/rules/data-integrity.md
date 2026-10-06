# 规则：数据完整性铁律（portfolio_tracker）

适用于所有数据拉取、回填、校验任务，自动加载生效。

1. **成交量单位绝不可混淆**：
   - `etf_fundamental.volume` 单位是 **股(shares)**；
   - `etf_price_history.volume` 单位是 **手(lots)**。
   - 仅在显式做单位换算时才 `×100`（手→股）；禁止默认相乘。
2. **neodata 在自动化中不可用**：neodata 是平台 deferred tool，自动化会话未注入其 deferred index，调用必失败（属平台限制，非代码 bug）。数据补齐一律走 akshare 兜底脚本（`scripts/backfill/backfill_etf_fundamental_akshare.py`），**不要在自动化 prompt 里要求调用 neodata**。
3. **休市日不补空白**：补数函数会自动跳过非交易日（如 2026-09-25 中秋），遇到"缺口"先核实是否休市，不要误补空白交易日。
4. **双源容错**：主源东方财富，备源新浪（`akshare fund_etf_hist_sina`）；新浪 `volume`=股，与 `etf_fundamental` 一致，无需 `×100`；新浪无 `turnover_rate` 列。
5. **真实库路径**：所有数据库操作指向 `data/database/portfolio.db`，不要误用到 `data/portfolio.db`（空壳）。
