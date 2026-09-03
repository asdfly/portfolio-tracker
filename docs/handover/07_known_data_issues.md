# 07 · 已知数据问题与口径陷阱

> 记录日期：2026-08-26
> 状态：**已确认、未修复**（本文档为登记，不含代码变更）
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

### 建议修法

1. 先确认写入来源：定位是哪个采集函数把这两只写进 `etf_fundamental` 的（怀疑早期版本按"当时持仓"全量写入、未过滤场内/场外）。**先修来源，再清历史**，否则下次采集会重新写回。
2. 清理语句（需备份后执行，且应走正式变更流程，不要在评估期直接改生产库）：
   ```sql
   DELETE FROM etf_fundamental WHERE code IN ('001323','002152');
   ```
3. 加约束或采集期过滤：写入前用修好的 ETF 判定（见问题一建议）过一遍。

---

## 附：ETF 数量口径速查（防止再次数错）

| 数字 | 含义 | 出处 | 可否作分母 |
|------|------|------|-----------|
| **36** | 全历史全部标的 = 23 场内 ETF + 13 场外标的 | `portfolio_snapshots` distinct `code` | ✅ 全标的口径 |
| **23** | 场内 ETF 全集 | `etf_technical` distinct `code`，实测 == `config/settings.py` 的 `ETF_CATEGORIES` | ✅ **推荐的 ETF 权威分母** |
| **22** | 预测底座 / 风险模型覆盖 | `etf_features`、`etf_price_history` | ✅ 模型覆盖口径（= 23 − 已清仓 `159732`） |
| **25** | `etf_fundamental` 标的数 | 含 2 只场外脏数据 | ❌ **不可用** |
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
assert fund - tech == {'001323', '002152'}        # etf_fundamental 脏数据
assert tech - feat == {'159732'}                  # 已清仓，故预测底座少 1
print("全部结论复核通过")
```

---

## 优先级判定

| 问题 | 严重度 | 紧急度 | 理由 |
|------|--------|--------|------|
| 一 · `_is_etf` 假阳性 | 中 | 低 | 当前无实际影响（两只已清仓），但属**埋雷**：一旦买入同类场外基金即触发采集失败 + 训练集污染。建议随下次 predictor 模块改动一并修掉。 |
| 二 · `etf_fundamental` 脏数据 | 低 | 低 | 仅影响统计口径，不影响运行。**必须先定位写入来源再清库**，否则会被采集重新写回。 |

两者均**不构成上线/交接阻断项**，但应写入待办，不要遗忘。
