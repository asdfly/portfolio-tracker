# 20 · 分红数据回填（portfolio_events）规划与实施状态

> 上游：#19 Tab4 收益日历评估。#19 发现事件日历为「通用 A 股日历 + 真实持仓上下文」，
> 预留 `data_loader.load_portfolio_events()` 接口（返回 `[]`，契约稳定）。本任务落地真实分红数据回填，
> 使事件日历按持仓个性化显示真实除息/收益分配事件。

## 一、目标
1. 新增 `portfolio_events` 表，持久化「当前持仓相关」的真实分红事件。
2. 编写幂等回填脚本 `scripts/backfill/backfill_etf_dividends.py`，拉取每只持仓标的分红历史并落库。
3. 把 `load_portfolio_events()` 由「返回空」改为「读表 + 持仓过滤 + 窗口裁剪」的真实实现。
4. 前端零改动：tab4 `_render_event_calendar` 已 `events_list.extend(load_portfolio_events(...))`，契约 key 对齐即可生效。

## 二、数据源（容错双通道，对齐项目「东财主 + 新浪备」惯例）
- **主通道 · 新浪**：`ak.fund_etf_dividend_sina(symbol="sh/sz"+code)` → 列 `日期`/`累计分红`。
  Sina 是项目既有备份源（参考 `fund_etf_hist_sina`），实测 510050→18 行、510300→14 行，0.1–0.2s/次。
  注意：**必须带交易所前缀**（sh/sz），裸 6 位码返回空。
- **回退 · 东财公告**：`ak.fund_announcement_dividend_em(symbol=code)` → 列 `公告日期`/`公告标题`。
  主通道为空时（如场外开放式基金 100032/166301/519770）兜底，按「收益分配公告」接入。
- **剔除**：880013 天添利（货币基金，无除息事件），code[0]=='8' 直接跳过。

## 三、表结构 `portfolio_events`
```sql
CREATE TABLE IF NOT EXISTS portfolio_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    code        TEXT NOT NULL,
    name        TEXT,
    event_type  TEXT NOT NULL,          -- 'dividend'
    event_date  TEXT NOT NULL,          -- 'YYYY-MM-DD'
    per_unit    REAL,                   -- 本次每份分红(元/份)，公告来源为 NULL
    cumulative  REAL,                   -- 累计每份分红(元/份)
    detail      TEXT,                   -- 公告标题 / 备注
    source      TEXT,                   -- 'akshare:fund_etf_dividend_sina' 等
    created_at  TEXT,
    UNIQUE(code, event_type, event_date)
);
```
- 幂等：`INSERT OR IGNORE`，重跑不重复。
- `ensure_portfolio_events_table(conn)` 在 data_loader（读）与回填脚本（写）两处共用，保证读永不因缺表报错。

## 四、回填脚本要点（`backfill_etf_dividends.py`）
- 读 `portfolio_snapshots` 最新快照，按 code 去重取「最长名称」为 canonical name。
- 前缀映射：code[0] ∈ {5,6,9}→`sh`；∈ {0,1,2,3}→`sz`；∈ {8}→跳过。
- 主通道空 → 回退公告通道；逐 code 隔离异常（单只失败不影响其余）。
- `per_unit` = 当前行 `累计分红` − 上一行 `累计分红`（首行 = 累计分红）。
- 汇总：成功/空/错误只数、总事件数、耗时。支持 `--dry-run` / `--verify`。

## 五、读取实现（`load_portfolio_events(horizon_days=90)`）
- `ensure` 表 → 取最新快照去重 codes → 查 `event_date` 落在 `[today-horizon, today+horizon]` 且 code∈持仓 的行。
- 映射为契约 dict：`icon="💰"`、`title="{name} 分红除息"`、`desc="每份 ¥{per_unit:.4f}（累计 ¥{cumulative:.4f}）"`（公告来源用 detail）、
  `date=event_date`、`days_ahead=(event_date-today).days`、`color=#d29922`（分红专属金）、
  `urgency`：未来≤14天「即将除息」/ 未来「N天后」/ 过去「已派息N天前」/ 今日「今日除息」。

## 六、实施状态
- [x] 方案设计（本文件）
- [x] `portfolio_events` 表 + `ensure_portfolio_events_table`
- [x] 回填脚本 `backfill_etf_dividends.py`（主:新浪 + 回退:东财公告）
- [x] `load_portfolio_events` 真实读取 + tab4 持仓卡片文案随接入更新
- [x] 运行回填灌库 + 验证 + 提交推送

## 七、验证结果（实测 2026-09-25）
- `py_compile` 通过；`pytest tests/test_imports.py` **17/17 通过**。
- 回填：35 只去重持仓，22 只有分红数据，**79 条事件**落库，0 异常；货币基金 880013 经双通道剔除（0 行）。
- 口径校验：`portfolio_events` 总行数=79；510300 共 14 行，`per_unit`=累计分红相邻差分（首行=累计），数学无误。
- `load_portfolio_events(90)` 窗口内返回 **3 条真实除息事件**：
  - 中证500ETF南方 2026-07-15（已派息72天前）
  - 港股通红利低波ETF华宝 2026-08-17（已派息39天前）
  - 红利低波ETF易方达 2026-09-11（已派息14天前，每份 ¥0.012）
  - 返回 dict 的 key 与 tab4 渲染契约完全一致（icon/title/date/urgency/days_ahead/color/desc）。
- tab4 持仓卡片文案改为动态：有事件时显示「近90天 N 条真实除息事件已高亮」；无事件时「真实除息日已按持仓接入（近90天暂无）」。
- 已提交并推送（见 git log）。

## 八、后续（可选，不在本次）
- 将回填纳入定时维护：分红低频，季度/半年跑一次即可，脚本幂等（`INSERT OR IGNORE`）可安全调度。
- 若需财报披露日个性化，可再加 `earnings` 类型事件（来源 westock 公告 / fund_announcement_dividend_em 之外）。

## 八、后续（可选，不在本次）
- 将回填纳入定时维护（分红低频，季度/半年跑一次即可，脚本幂等可安全调度）。
- 若需财报披露日个性化，可再加 `earnings` 类型事件（来源：`fund_announcement_dividend_em` 之外或 westock 公告）。
