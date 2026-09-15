# 07 · 已知数据问题与口径陷阱

> 记录日期：2026-08-26
> 状态：**✅ 已修复**（2026-09-03 随提交落地：代码正则 + 生产库脏数据双修，见各节"✅ 已修复"标注）
> 证据获取方式：对 `data/database/portfolio.db` 以 `file:...?mode=ro` 只读连接执行 SQL，未做任何写入。

本文档登记交接评估期间实测发现的数据正确性问题。这些问题**不影响现有功能运行**（不会报错、不会崩溃），但会让统计口径失真、并可能污染下游分析。因为影响面是"数字不对"而非"跑不起来"，容易长期潜伏，故单列成篇。

---

## 问题一：`_is_etf` 代码正则存在假阳性，把场外基金判成 ETF

### 位置

`src/analysis/predictor/build_base.py:23-32`

```python
_ETF_CODE_RE = re.compile(r"^(5\d{5}|1[56]\d{4})$")

def _is_etf(code: str, name: str) -> bool:
    """判定持仓记录是否为 ETF：name 含 'ETF' 或代码符合 ETF 模式。"""
    if name and "ETF" in str(name).upper():
        return True
    if code and _ETF_CODE_RE.match(code):
        return True
    return False
```

### 问题

`5\d{5}` 会吞掉整个 `5xxxxx` 段，但 **`519xxx` 是场外开放式基金代码段**，不是场内 ETF。同理 `1[56]\d{4}` 会吞掉 `166xxx`（LOF / 分级基金）。

实测对全历史 36 只标的逐一判定，正则与"名称含 ETF"两种判据冲突 3 例：

| 代码 | 名称 | 正则判定 | 名称判定 | 真实身份 | 结论 |
|------|------|---------|---------|---------|------|
| `166301` | 华商新趋势优选灵活配置混合型证券投资基 | ✅ ETF | ❌ | 混合型 LOF | **正则假阳性** |
| `519770` | 交银优择回报灵活配置混合A | ✅ ETF | ❌ | 场外混合基金 | **正则假阳性** |
| `512810` | 国防军工 | ✅ ETF | ❌ | 华宝中证军工 ETF | **名称判据假阴性**（库里存简称，无 "ETF" 字样） |

因为 `_is_etf` 是 `name` 判据与 `code` 判据的**或**关系，`512810` 靠正则救回来了（结果正确），但 `166301` / `519770` 被正则错误放行（结果错误）。

### 实际影响：目前为零，但是"侥幸"

`resolve_target_codes`（同文件 L35+）只对**最新持仓快照**跑 `_is_etf`。当前持仓 22 只已全为真 ETF，`166301` / `519770` 早已清仓、不在最新快照里，所以两个假阳性**当前没有进入预测底座**。

这是运气，不是设计。一旦这两只（或任何 `519xxx` / `166xxx` 场外基金）被重新买入并出现在最新快照，它们就会：

1. 被纳入 `resolve_target_codes` 的目标域；
2. 触发 `backfill_etf_price_history` 去东方财富 / 新浪拉**场内 ETF 日 K**——而场外基金没有场内日 K，采集会失败或拉到空数据；
3. 进而在 `etf_features` / `etf_forward_returns` 里产生残缺行，污染风险模型训练集。

### 建议修法

不要继续在正则上打补丁（代码段规则会变）。改为**以配置为准**：

- 首选：判定改为 `code in config.settings.ETF_CATEGORIES`（实测该配置全集 = 23 只 = `etf_technical` 集合，是干净的权威源）。
- 若必须保留正则兜底，至少排除已知场外段：`519xxx`、`166xxx`，即把 `5\d{5}` 收紧为 `5(0|1|2|3|5|6|8)\d{4}` 之类的白名单式写法，并补单元测试固定住 `166301` / `519770` / `512810` 三个样本的期望判定。

### 建议回归用例

```python
# 三个真实样本，把两类错误都钉住
assert _is_etf("512810", "国防军工") is True       # 真 ETF，名称无 ETF 字样
assert _is_etf("166301", "华商新趋势优选混合") is False  # LOF，勿判 ETF
assert _is_etf("519770", "交银优择回报混合A") is False   # 场外，勿判 ETF
```

### ✅ 已修复（2026-09-03，随提交落地）

未采用"查 `config.ETF_CATEGORIES`"方案，改用**收紧正则**：

```python
_ETF_CODE_RE = re.compile(r"^(?!166|519)(5\d{5}|1[56]\d{4})$")
```

理由：查表会把 `512810`（华宝中证军工 ETF，库内存简称"国防军工"、无 "ETF" 字样、且**不在** `config.ETF_CATEGORIES` 代码列表里）误杀——它会因 name 无 ETF、code 不在表而被判非 ETF，反而漏掉一只真 ETF。收紧正则保留 `5\d{5}` 兜住 `512810`，同时用负向前瞻 `(?!166|519)` 排除 `166xxx`(LOF) / `519xxx`(场外) 两个已知场外段，`name含ETF` 仍为主信号。9 例验证全过（含 `512810`→True、`166301`/`519770`/`001323`/`002152`/`001194`→False）。

---

## 问题二：`etf_fundamental` 表混入 2 只场外混合基金

### 现状

`etf_fundamental` distinct `code` = **25**，比真实场内 ETF 全集（23）多出 2 只：

| 代码 | 名称 | `etf_fundamental` 行数 |
|------|------|----------------------|
| `001323` | 东吴移动互联混合A | 19 |
| `002152` | 华宝核心优势混合 | 19 |

这两只是**场外混合基金**，不该出现在一张名为 `etf_fundamental` 的表里。集合关系实测：

```
etf_fundamental - etf_technical = {001323, 002152}
etf_technical  - etf_fundamental = {} （空）
```

即 `etf_fundamental` ⊃ `etf_technical`，多出来的正好是这 2 只脏数据。

### 影响

- 任何直接 `SELECT COUNT(DISTINCT code) FROM etf_fundamental` 当作"ETF 覆盖数"的统计都会虚高 2 只（这正是交接文档一度把覆盖写成 25 的来源）。
- 若有分析逻辑遍历 `etf_fundamental` 做横向对比（如估值/规模排序），这 2 只场外基金会作为不可比标的混入结果。

### ✅ 已修复（2026-09-03，随提交落地）

1. **写入来源已定位且确认安全**：`etf_fundamental` 的写入函数是 `src/data_sources/etf_fundamental.py:445` 的 `save_to_db(conn, "etf_fundamental", ...)`，包裹在 `run_etf_fundamental_collection` 内；该函数**仅被 `scripts/verify_sina_whitelist.py` 调用，不在主采集链**（`build_base` / `run_analysis` 不碰它）。因此历史脏数据是早期一次性写入，不会在后续例行采集中被重新写回——可直接清库。
2. **已执行清理**（生产库 `data/database/portfolio.db`，非 git 跟踪；已备份验证）：
   ```sql
   DELETE FROM etf_fundamental WHERE code IN ('001323','002152');  -- 各 19 行，共 38 行
   ```
   distinct `code` 由 **25 → 23**，与 `etf_technical` / `config.ETF_CATEGORIES` 对齐。复查 `fund - tech == set()`（空集）。
3. 采集期过滤：写入前已可用修好的 `_is_etf`（见问题一）过一遍，杜绝再次混入。

---

## 附：ETF 数量口径速查（防止再次数错）

| 数字 | 含义 | 出处 | 可否作分母 |
|------|------|------|-----------|
| **36** | 全历史全部标的 = 23 场内 ETF + 13 场外标的 | `portfolio_snapshots` distinct `code` | ✅ 全标的口径 |
| **23** | 场内 ETF 全集 | `etf_technical` distinct `code`，实测 == `config/settings.py` 的 `ETF_CATEGORIES` | ✅ **推荐的 ETF 权威分母** |
| **22** | 预测底座 / 风险模型覆盖 | `etf_features`、`etf_price_history` | ✅ 模型覆盖口径（= 23 − 已清仓 `159732`） |
| **23** | `etf_fundamental` 标的数 | 2026-09-03 已清 `001323`/`002152` 脏数据，现与 `etf_technical` 对齐 | ✅ 可用 |
| **25** | 用代码正则判出的 ETF 数 | 含 2 只假阳性 `166301`/`519770` | ❌ **不可用**（与上一行数值相同但集合不同，纯属巧合） |
| **59** | `(code, name)` 去重对数 | 同一代码有简称/全称两种写法 | ❌ 不是标的数 |

> **59 的成因示例**：`159220` 在 `portfolio_snapshots` 中同时存在"港股通红利低波ETF华宝"与"港红利"两种 `name`。按 `(code,name)` 去重会把同一只 ETF 数成 2 只。历史上曾据此误报"48 只 ETF"，实为此类假象。

### 场外标的清单（13 只，供核对）

`001194` 景顺长城稳健回报灵活配置混合A、`001323` 东吴移动互联混合A、`001407` 景顺长城稳健回报灵活配置混合C、`001437` 易方达瑞享灵活配置混合I、`001765` 前海开源嘉鑫混合A类、`002152` 华宝核心优势混合、`007994` 华夏中证500指数增强A、`008269` 大成睿享混合A、`027293` 东吴产业趋势混合A、`100032` 富国中证红利指数增强前端、`880013` 天添利（现金管理类）、`166301` 华商新趋势优选混合（LOF）、`519770` 交银优择回报混合A。

---

## 复核脚本

以下脚本可随时重跑以验证本文档结论（**只读，不写库**）：

```python
import sqlite3, re
con = sqlite3.connect('file:data/database/portfolio.db?mode=ro', uri=True)
cur = con.cursor()
RE = re.compile(r"^(5\d{5}|1[56]\d{4})$")

snap = dict(cur.execute("SELECT code, MAX(name) FROM portfolio_snapshots GROUP BY code").fetchall())
tech = {r[0] for r in cur.execute("SELECT DISTINCT code FROM etf_technical")}
fund = {r[0] for r in cur.execute("SELECT DISTINCT code FROM etf_fundamental")}
feat = {r[0] for r in cur.execute("SELECT DISTINCT code FROM etf_features")}

regex_set = {c for c in snap if RE.match(c)}
name_set  = {c for c in snap if 'ETF' in (snap[c] or '').upper()}

assert len(snap) == 36
assert len(tech) == 23
assert regex_set - tech == {'166301', '519770'}   # 正则假阳性
assert tech - name_set == {'512810'}              # 名称判据假阴性
# 修复后：etf_fundamental 已清脏数据，fund - tech 应为空集
assert fund - tech == set()                         # etf_fundamental 脏数据已清理（2026-09-03）
assert tech - feat == {'159732'}                  # 已清仓，故预测底座少 1
print("全部结论复核通过")
```

---

## 优先级判定

| 问题 | 严重度 | 紧急度 | 理由 |
|------|--------|--------|------|
| 一 · `_is_etf` 假阳性 | 中 | 低 | 当前无实际影响（两只已清仓），但属**埋雷**：一旦买入同类场外基金即触发采集失败 + 训练集污染。建议随下次 predictor 模块改动一并修掉。 |
| 二 · `etf_fundamental` 脏数据 | 低 | 低 | 仅影响统计口径，不影响运行。**必须先定位写入来源再清库**，否则会被采集重新写回。 |
| 三 · `portfolio_nav.total_units` 错位 | 中 | 中 | 字段 100% 错误，当前无下游读取故影响为零，但任何新消费方都会踩坑（见下）。 |
| 四 · `portfolio_nav.mwr_return` 与 TWR 矛盾 | 中 | 中 | **用户可见**：该值已进入 `load_portfolio_nav()` 返回值，展示出来是 −73.5%。 |

前两者均**不构成上线/交接阻断项**，但应写入待办，不要遗忘。

---

## 问题三：`portfolio_nav.total_units` 存的是「前一个交易日的市值」

> 记录时间 2026-09-15 · 发现人 data-engineer · **状态：仅记录，未修复**
> 本轮同时在做「场外净值写入 portfolio_snapshots」，为避免事后分不清是哪个改动引起的，本 bug 不在本轮修复。

### 复现查询

```sql
SELECT date, total_units, total_value,
       LAG(total_value) OVER (ORDER BY date) AS prev_day_total_value
FROM portfolio_nav ORDER BY date DESC LIMIT 15;
```

| date | total_units | 当日 total_value | 前一日 total_value | 是否相等 |
|---|---|---|---|---|
| 2026-09-14 | 933294.5 | 933891.5 | 933294.5 | ✅ |
| 2026-09-11 | 945046.1 | 933294.5 | 945046.1 | ✅ |
| 2026-09-10 | 950179.2 | 945046.1 | 950179.2 | ✅ |
| 2026-09-09 | 950728.9 | 950179.2 | 950728.9 | ✅ |

```sql
-- 全表命中率
SELECT COUNT(*) FROM (
  SELECT total_units, LAG(total_value) OVER (ORDER BY date) pv FROM portfolio_nav
) WHERE pv IS NOT NULL AND ABS(total_units - pv) < 0.01;
-- => 3476   （全表 3477 行，唯一未命中的是首行，因为它没有前值）
```

**结论：3476 / 3477 = 99.97% 命中，是 100% 系统性错位，不是偶发。**

### 根因（源码行号）

`src/analysis/nav_engine.py:222-234`：

```python
rows.append({
    "date": d_str,
    "unit_nav": round(unit_nav, 6),
    "total_units": round(prev_v, 2),      # ← BUG：prev_v 是上一轮的 v
    "total_value": round(v, 2),
    ...
})
prev_v = v                                # ← 在 append 之后才推进
```

`prev_v` 在 `append` **之后**才被赋成当日 `v`，因此写入本行的 `total_units` 恒等于**前一个交易日**的 `total_value`。

### 影响范围（已核实）

- **当前影响 = 0**：全项目 grep `total_units` 只有 3 处，全是写入侧
  （`db_schema.py:448` 建表、`nav_engine.py:226` 赋值、`nav_engine.py:249/266` 建表与 INSERT）。
  `nav_engine.py:294` `load_portfolio_nav()` 的 SELECT 列里**没有** `total_units`，即无任何下游读取。
- **字段语义**（`db_schema.py:448` 注释）：`总份额（无申赎则恒定=初始市值）`。
  正确值应为 `total_value / unit_nav`；现状是滞后一天的市值，语义完全不对。
- **未受污染的部分**：`unit_nav` 是独立计算的（`nav_engine.py:221` `unit_nav = prev_nav * (1 + r_total)`），
  并非 `total_value / total_units`。实测 09-14：`total_value/total_units = 1.0006`，而 `unit_nav = 2.5247`，
  两者无关 —— **所以 unit_nav 序列没有被这个 bug 污染**，不必担心 NAV 曲线滞后一天。

### 建议修法（待排期）

```python
"total_units": round(v / unit_nav, 2) if unit_nav else None,   # nav_engine.py:226
```
改后需全量 `rebuild_portfolio_nav()` 重刷，并补一条断言：`total_units * unit_nav ≈ total_value`。

---

## 问题四：`portfolio_nav.mwr_return` —— MWR 当前不可用（IRR 求解器发散）

> 记录时间 2026-09-15 · 发现人 data-engineer
> **状态：MWR 当前不可用 —— 求解器发散待修；护栏已加、坏值已清（2026-09-15）**
> 结论一句话：**这个字段现在不要用**，值不可信，且每次重建都会变。

### 复现查询

```sql
SELECT mwr_return, COUNT(*) FROM portfolio_nav GROUP BY mwr_return;
-- 2026-09-15 重建前: -0.735121 | 3477
-- 2026-09-15 重建后: 10.444292 | 3477     ← 值变了，但「全表同值」的性质没变
```

### 需要修正的两处误判

**其一**：初判为「从 2026-08-27 起恒为死值」，实测**不是区间问题，是全表问题**——
全表 3477 行（2012-05-28 ~ 2026-09-14）永远是同一个值。

**其二**：这个值**不是固定常数，每次 `rebuild_portfolio_nav()` 都会重算并改变**。
2026-09-15 因场外净值补齐触发重建，`mwr_return` 由 **−0.735121 变为 10.444292**（即 +1044%）。
所以不要把它当成「死值」写进任何断言。

另外「每行相同」本身**不是 bug，是设计**：`nav_engine.py:237-241` 注释写得很清楚——
「全周期 MWR，写入每行便于任意行读取」，即把算出来的全周期 IRR 广播到每一行。

### 真正的 bug：IRR 结果与 TWR 差了一个数量级

| 时点 | `unit_nav`（09-14） | 累计 TWR | `mwr_return` | 折算 |
|---|---|---|---|---|
| 重建前 | 2.524669 | **+152.47%** | −0.735121 | −73.51% |
| 重建后 | 2.626177 | **+162.62%** | 10.444292 | **+1044.43%** |

同一个组合、同一段现金流，TWR 与 MWR 应同号且量级相近。
重建前是 +152% vs −73.5%（差 225pp），重建后是 +162% vs +1044%（差 882pp）——
**两次都错，且重建后更离谱**。说明 `_solve_period_irr(df, cf, div)`（`nav_engine.py:238`）
的解根本不可信，而不是「某次算错了」。
可能原因：现金流符号/日期对齐错误，或 IRR 在无实根/多根时返回了边界值。

### 影响范围（已核实）

- `mwr_return` **有下游读取**：`nav_engine.py:294` `load_portfolio_nav()` 的 SELECT 含该列
  （列清单：`date, unit_nav, total_value, net_flow, twr_cumulative, mwr_return, is_suspect`），
  会被基准对比 / 回撤 / 归因等下游消费 → **用户可见的错误数字**。
- 建议：修复前先在展示侧隐藏该字段，避免 −73.5% 被当成真实收益展示。

### 已做的处置（2026-09-15）

按「先不产生错误数字、再不展示错误数字、最后才修算法」的顺序：

1. **护栏**：`src/analysis/nav_engine.py` 新增 `_MWR_MIN = -0.99` / `_MWR_MAX = 5.0`，
   在 `_solve_period_irr()` 内用 `_guard()` 包裹所有返回路径。
   解非有限值或越界 → 返回 `None` + `logger.warning` 打印输入摘要
   （现金笔数、净流入、首末笔、分红合计、期初期末市值）。
   同时给原本静默的「无实根」分支补了 warning。
   `rebuild_portfolio_nav()` 里 `if mwr is not None` 的分支不变，
   因此返回 `None` 时各行 `mwr_return` 自然写入 `NULL`。

2. **清坏值**：`scripts/purge_bad_mwr.py`（新建，判据与 `_MWR_MIN/_MWR_MAX` 同源）。
   已执行：`UPDATE portfolio_nav SET mwr_return = NULL WHERE ... 越界`，
   **清理 3477 行**，现全表 `mwr_return IS NULL`。

3. **展示侧容错**：`get_nav_series()`（原 `load_portfolio_nav` 位置的读取函数）
   docstring 已注明 `mwr_return` 可能为 NULL/NaN，下游必须显示"暂不可用"，
   **不要**渲染成 nan / None / 0，也不要用 0 参与运算。
   ⚠️ 核实结论：全项目 grep `mwr_return` 只有 `nav_engine.py` 内部 +
   `db_schema.py` 建表 + 本文件，**当前没有任何展示层调用 `get_nav_series()`**，
   所以暂无实际渲染点需要改。未来新增消费方时请回读本条。

4. **端到端验证**：重新 `rebuild_portfolio_nav()` 后确认
   `越界坏值行数 = 0`、`mwr_return IS NULL = 3477/3477`，
   且护栏 warning 正常打印，其余字段（unit_nav 2.626177 / TWR +162.62% / is_suspect 0）不受影响。

### 为什么区间取 [-0.99, 5.0] 而不是更宽

不要用 `mwr < 10` 这类宽区间：+1044% 能被拦，但 +900% 同样会被放行，
而那依然是错的。取 +500% 作为上界，已远超任何合理组合收益，
足以把发散解挡住，又不至于误伤正常值。

### 修法（待排期，属另一个专项）

**本轮不修 IRR 算法本身。** 排查线索（护栏 warning 打印的输入摘要，2026-09-15）：

```
期间 2012-05-28~2026-09-14 (3477 天), 现金流 380 笔, 净流入 1,109,683.60,
首笔 29,000.37@2012-05-29, 末笔 -8,000.00, 分红合计 4,903.79,
期初市值 10,416.00, 期末市值 1,505,090.13   →  解出 10.444292
```

两个可疑点，供后续专项参考：

- **时间权重口径**：`_solve_period_irr` 里 `frac = (t - i) / t`（`t` 为总天数），
  折现因子是 `(1 + r) ** frac`，这把 `r` 定义成**整个持有期的收益率**而非年化。
  但字段名与注释都指向"全周期资金加权收益"，语义与用法是否对齐需先确认。
- **现金流符号**：`dep = -float(cf.get(...))`，净流入 110 万而期末市值仅 150 万、
  期初 1 万，直觉上全期收益应在数十个百分点量级，解出 +1044% 需核对
  `trade_records` 里是否存在符号或口径（如分红、转存）错记。

建议步骤：
1. 先给 `_solve_period_irr()` 补单元测试：构造已知 IRR 的现金流，断言输出。
2. 确认 `frac` 的时间口径（全期 vs 年化）与字段语义一致。
3. 抽样核对 `trade_records` 中若干笔大额的 `change_amount` 符号。
4. 修好后重刷 `portfolio_nav`，并同步更新 README / 日报口径说明。

---

## 问题五：TWR 序列是「历史段场内主导 + 近期全组合」的拼接口径

> 记录时间 2026-09-15 · 发现人 data-engineer · **状态：刻意保留，不修**
> 本条不是 bug，是**口径限制**。读 `portfolio_nav` / `portfolio_summary` 前必读，
> 否则会把不同口径的收益串在一起比较。

### 背景

2026-09-15 用 `scripts/fetch_otc_fund_nav.py` 补齐了场外 13 只基金 2026-08-03 ~ 2026-09-14
的日频净值快照（347 行）。此前场外基金**只有月末快照**，详见本文「场外标的清单」。

### 口径现状

| 时间区间 | 场外 13 只覆盖 | daily_return 实际由谁贡献 |
|---|---|---|
| 2012-05-28 ~ 2026-01-30 | 无 | 场内 |
| 2026-01-31 ~ 2026-07-30 | **仅月末那一天** | 月末日含场外，其余日仍只有场内 |
| 2026-08-03 ~ 2026-09-14 | **日频**（本次补齐） | 全组合（22 场内 + 12 场外） |
| 2026-09-15 起 | 日频（若采集器接入日常链路） | 全组合 |

**即：修正后的 TWR 是一段拼接序列，不是全周期一致口径。**
07-31 及之前，绝大多数交易日的收益只反映场内 22 只（约占总市值 62%）；
08-03 起才反映全组合。跨过 2026-08-03 做区间收益对比时，口径发生了切换。

### 为什么不做全历史回补

**不要**顺手把场外历史也全量回补成日频。原因：

- 场外基金的历史**份额变动不可考**。`portfolio_snapshots` 只在月末那一天有记录，
  中间的申购/赎回无从还原。
- 用「当前份额 × 历史净值」倒推历史市值，会凭空造出从未持有过的资产，
  引入的错误比现在更大。
- 现有月末快照里的 `quantity` 是当天的真实份额，只有那一天的市值是可靠的。

### 若要彻底一致，需要什么

1. 拿到场外 13 只的**完整申赎流水**（券商/基金App 导出的对账单），按日重建份额；
2. 或用 `trade_records` 反推（当前表内是否覆盖场外申赎需另行核实）；
3. 之后才能做全历史日频回补并重刷 `portfolio_nav`。

**在此之前，任何跨 2026-08-03 的收益对比都必须注明口径切换。**
