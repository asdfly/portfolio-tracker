"""NeoData 指数估值采集模块 — 用自然语言金融搜索补全跟踪指数估值数据。

背景（P0-3 估值分位分析的数据源补强）：
  - akshare `stock_zh_index_value_csindex` 仅返回最近 ~20 个交易日 PE，不足以算 5 年分位；
  - 乐咕乐股 `stock_index_pe_lg/pb_lg` 现已可用（环境修复后），399673 创业板50 的
    长历史 PE-TTM 改由 `etf_pe_backfill.fetch_legulegu_pe` 经乐咕补齐，本模块专注日频当前值；
  - neodata-financial-search 的「指数估值」接口可返回每日 PE/PB/股息率/风险溢价等
    丰富字段（实时，约近 10 个交易日），是可靠的估值数据源。

注意：
  - neodata 鉴权凭证由 WorkBuddy 运行时管理（本地缓存，12 小时有效期），**仅能在
    WorkBuddy 会话内采集**；生产环境（scheduled_run.bat 的纯 Python）无法调用。
  - 因此本模块的 `collect_all` 是「WorkBuddy 会话内一次性/周期性回填」工具，采集结果
    落库后由 advisor 的 `_analyze_valuation` 读取（读库不依赖 neodata）。
  - 历史深度约 10 个交易日，5 年分位仍需持续积累；建议通过 WorkBuddy 定时任务每日
    追加一次，配合 `index_pe_history` 的 UNIQUE(index_code, date) 幂等去重。
"""
from __future__ import annotations

import json
import logging
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional

from src.utils.trading_calendar import is_trading_day

logger = logging.getLogger(__name__)

# neodata skill 的 query.py 路径按当前用户主目录推导，避免硬编码用户名
NEODATA_SKILL_DIR = Path.home() / ".workbuddy" / "skills" / "neodata-financial-search"
NEODATA_QUERY_PY = NEODATA_SKILL_DIR / "scripts" / "query.py"

# 跟踪指数代码 -> 查询名称（16 个去重指数，来自 etf_fundamental.index_code）
INDEX_NAME_MAP: Dict[str, str] = {
    "000300": "沪深300指数",
    "000905": "中证500指数",
    "000852": "中证1000指数",
    "000688": "科创50指数",
    "399673": "创业板50指数",
    "399959": "中证军工指数",
    "399975": "证券公司指数",
    "930713": "人工智能指数",
    "H30590": "中证机器人指数",
    "931743": "消费电子指数",
    "931152": "中证创新药产业指数",
    "399989": "医药指数",
    "399808": "新能源指数",
    "931157": "中证电池主题指数",
    # h11118(中证两岸三地500美元指数) 是早期 159220 的误填映射, 已替换为正确代码 930914
    "930914": "港股通高股息低波动指数",
    "H30269": "中证红利低波动指数",
    "932000": "中证2000指数",
}


def _norm_date(d: str) -> str:
    """'YYYYMMDD' -> 'YYYY-MM-DD'。

    统一日期格式是 index_pe_history 的硬要求：列是 TEXT，SQLite 按字符串排序，
    '-'(0x2D) < '0'(0x30) 会让 '2026-09-03' 排在 '20260911' 之前，
    `ORDER BY date` 取末行时永远拿到紧凑格式的 neodata 值（P0 缺陷根因）。
    """
    d = str(d).strip()
    if len(d) == 8 and d.isdigit():
        return f"{d[:4]}-{d[4:6]}-{d[6:]}"
    return d


def _ensure_valuation_columns(conn) -> None:
    """幂等迁移：index_pe_history 增加 pb / div_yield 列（供 neodata 富字段落库）。"""
    cols = {r[1] for r in conn.execute("PRAGMA table_info(index_pe_history)").fetchall()}
    if "pb" not in cols:
        conn.execute("ALTER TABLE index_pe_history ADD COLUMN pb REAL")
    if "div_yield" not in cols:
        conn.execute("ALTER TABLE index_pe_history ADD COLUMN div_yield REAL")
    # source: 区分数据口径 (csindex 中证官方 / neodata), 供下游按源取数
    if "source" not in cols:
        conn.execute("ALTER TABLE index_pe_history ADD COLUMN source TEXT DEFAULT 'unknown'")
    conn.commit()


def _run_query(query: str) -> Optional[dict]:
    """调用 neodata query.py，返回解析后的 JSON dict；失败返回 None。"""
    if not NEODATA_QUERY_PY.exists():
        logger.warning("neodata query.py 不存在: %s", NEODATA_QUERY_PY)
        return None
    try:
        proc = subprocess.run(
            [sys.executable, str(NEODATA_QUERY_PY), "--query", query],
            cwd=str(NEODATA_SKILL_DIR),
            capture_output=True, text=True, timeout=90,
        )
    except Exception as e:
        logger.warning("neodata 查询 %s 失败: %s", query, e)
        return None
    if proc.returncode != 0:
        logger.warning("neodata 查询 %s 退出码 %s: %s", query, proc.returncode, proc.stderr[:200])
        return None
    try:
        return json.loads(proc.stdout)
    except Exception as e:
        logger.warning("neodata 查询 %s 响应解析失败: %s", query, e)
        return None


def _split_row(ln: str) -> List[str]:
    """拆分行内单元格（去掉首尾 | 并按 | 切分）。"""
    return [c.strip() for c in ln.strip().strip("|").split("|")]


def _iter_tables(lines: List[str]):
    """从 markdown 文本中逐个产出 (表头cells, 数据行cells列表)。

    跳过分隔行（| :---: | ---: | 之类），仅返回数据行。
    """
    i = 0
    n = len(lines)
    sep = {"", ":---:", "---:", ":---", "---", ":--:", "--:"}
    while i < n:
        if lines[i].lstrip().startswith("|"):
            header = _split_row(lines[i])
            data: List[List[str]] = []
            j = i + 1
            while j < n and lines[j].lstrip().startswith("|"):
                cells = _split_row(lines[j])
                if set(cells) <= sep:  # 分隔行
                    j += 1
                    continue
                data.append(cells)
                j += 1
            yield header, data
            i = j
        else:
            i += 1


def _col_idx(header: List[str], *keys: str) -> Optional[int]:
    for i, h in enumerate(header):
        if any(k in h for k in keys):
            return i
    return None


def _to_float(v: Optional[str]) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(str(v).replace(",", "").strip())
    except (ValueError, TypeError):
        return None


def _extract_valuation_rows_old(content: str) -> List[Dict]:
    """旧格式解析：表头含「指数代码」+「市盈率」，代码在首列（000688.SH 形式）。"""
    if not content:
        return []
    lines = [ln.strip() for ln in content.splitlines() if ln.strip()]
    if len(lines) < 3:
        return []

    header_line = None
    for ln in lines:
        if "指数代码" in ln and "市盈率" in ln:
            header_line = ln
            break
    if header_line is None:
        return []

    header = _split_row(header_line)
    c_code = _col_idx(header, "指数代码")
    c_date = _col_idx(header, "交易日")
    c_pe = _col_idx(header, "滚动市盈率PE(TTM)", "滚动市盈率")
    c_pb = _col_idx(header, "市净率PB(LF)", "市净率PB")
    c_dy = _col_idx(header, "股息率")
    if c_date is None or c_pe is None:
        return []

    rows: List[Dict] = []
    for ln in lines:
        cells = _split_row(ln)
        if len(cells) <= max(c_date, c_pe):
            continue
        code_cell = cells[c_code] if c_code is not None else ""
        if "." not in code_cell:  # 数据行首列为「000688.SH」形式
            continue
        pe = _to_float(cells[c_pe])
        if pe is None or pe <= 0:
            continue
        rows.append({
            "code": code_cell.split(".")[0],
            "date": cells[c_date][:10],
            "pe": pe,
            "pb": _to_float(cells[c_pb]) if c_pb is not None else None,
            "div_yield": _to_float(cells[c_dy]) if c_dy is not None else None,
        })
    return rows


def _extract_valuation_rows_new(content: str) -> List[Dict]:
    """新格式解析（neodata 2026-10 起接口变更）：单一「统一估值查询」块，
    首列为「日期」（无指数代码列），PE/PB 与 股息率 拆到两张表：
      - 历史估值-基础指标：| 日期 | ... | PE(TTM) | ... | PB(LF) | PB(MRQ) |
      - 历史估值-销售、现金流与股息指标：| 日期 | ... | 股息率(TTM) | 静态股息率 |
    两张表按日期对齐合并。返回 code=None 的行（由 fetch_index_valuation 按块归属信任）。
    """
    if not content:
        return []
    lines = [ln.strip() for ln in content.splitlines()]
    base_cands = []  # 候选基础指标表（含 日期 + PE(TTM)）
    div_cands = []   # 候选股息率表（含 日期 + 股息率(TTM)）
    for hdr, rows in _iter_tables(lines):
        htext = " ".join(hdr)
        if "日期" in htext and "PE(TTM)" in htext:
            base_cands.append((hdr, rows))
        if "日期" in htext and "股息率(TTM)" in htext:
            div_cands.append((hdr, rows))
    if not base_cands:
        return []

    # 优先选「历史估值-基础指标」表：含 PB(LF)/PB(MRQ)/市净率 且数据行最多；
    # 排除仅 1 行的「实时估值快照」（裸 PB 列、无 PB(LF)）。
    def _base_score(item):
        hdr, rows = item
        htext = " ".join(hdr)
        has_pblf = ("PB(LF)" in htext) or ("PB(MRQ)" in htext) or ("市净率" in htext)
        return (1 if has_pblf else 0, len(rows))

    base_hdr, base_rows = max(base_cands, key=_base_score)
    div_hdr = div_rows = None  # type: ignore
    if div_cands:
        # 股息率表同样优先数据行多者（排除快照的 1 行）
        div_hdr, div_rows = max(div_cands, key=lambda it: len(it[1]))

    c_date = 0
    c_pe = _col_idx(base_hdr, "PE(TTM)", "滚动市盈率")
    c_pb = _col_idx(base_hdr, "PB(LF)", "PB(MRQ)", "市净率PB")
    if c_pe is None:
        return []

    # 股息率按日期建索引
    div_map: Dict[str, float] = {}
    if div_hdr is not None:
        c_ddate = 0
        c_dy = _col_idx(div_hdr, "股息率(TTM)", "股息率")
        if c_dy is not None:
            for r in div_rows:  # type: ignore
                if len(r) > max(c_ddate, c_dy):
                    ds = _norm_date(r[c_ddate])
                    dy = _to_float(r[c_dy])
                    if dy is not None:
                        div_map[ds] = dy

    out: List[Dict] = []
    for r in base_rows:  # type: ignore
        if len(r) <= max(c_date, c_pe):
            continue
        ds = _norm_date(r[c_date])
        pe = _to_float(r[c_pe])
        if pe is None or pe <= 0:
            continue
        pb = _to_float(r[c_pb]) if c_pb is not None else None
        out.append({
            "code": None,  # 新格式无指数代码列，块已绑定查询标的
            "date": ds,
            "pe": pe,
            "pb": pb,
            "div_yield": div_map.get(ds),
        })
    return out


def _extract_valuation_rows(content: str) -> List[Dict]:
    """解析「指数估值」/「统一估值查询」markdown 表格，抽取 交易日/PE/PB/股息率 行。

    兼容两种 neodata 返回格式：
      - 旧格式（含「指数代码」列 + 「指数估值」块）
      - 新格式（2026-10 起，「统一估值查询」块，首列为「日期」，PE/PB 与股息率分表）
    优先旧格式（带代码列可做跨指数防误标），命中则用之；否则回落新格式。

    Returns:
        [{code, date, pe, pb, div_yield}, ...]
    """
    old = _extract_valuation_rows_old(content)
    if old:
        return old
    return _extract_valuation_rows_new(content)


def fetch_index_valuation(index_code: str) -> List[Dict]:
    """采集单个指数的估值时间序列（经 neodata）。

    查询策略：优先用指数代码精确查询（f"{index_code} 指数估值"），规避中文名被
    neodata 实体解析误判（实证 930713/931743 中文名曾误解析为 931071.CS /
    01801085.PT，导致 0 行或串味）。若代码查询无「指数估值」块或 0 行，回退到
    INDEX_NAME_MAP 中文名查询（兜底 000688 等代码查询间歇失败的标的）。

    防误标（修复原 `... or rows` 兜底缺陷）：严格按 index_code 过滤返回行；当
    neodata 把查询误解析为其它指数（返回行代码 != index_code）时，丢弃这些行并
    告警，绝不以 index_code 名义落库他人估值。
    """
    name = INDEX_NAME_MAP.get(index_code, index_code)
    queries = [f"{index_code} 指数估值", f"{name} 指数估值"]
    # neodata 返回块类型：旧「指数估值」或新「统一估值查询」（2026-10 接口变更）
    VALUATION_BLOCK_TYPES = {"指数估值", "统一估值查询"}
    last_resolved = None
    for q in queries:
        data = _run_query(q)
        if not data or data.get("code") != "200":
            continue
        try:
            recall = data["data"]["apiData"]["apiRecall"]
        except (KeyError, TypeError):
            continue
        for block in recall:
            if block.get("type") in VALUATION_BLOCK_TYPES:
                rows = _extract_valuation_rows(block.get("content", ""))
                # 旧格式按 index_code 过滤（防跨指数误标）；新格式 code=None 已绑定查询标的，直接信任
                matched = [r for r in rows if r["code"] is None or r["code"] == index_code]
                if matched:
                    return matched
                if rows:
                    last_resolved = sorted({r["code"] for r in rows if r["code"]})
    if last_resolved:
        logger.warning(
            "[NeoData] %s 全部候选查询返回的「指数估值」行代码 %s 与请求不符，"
            "疑似 neodata 实体解析偏差，丢弃以防误标", index_code, last_resolved)
    return []


def save_valuation(conn, index_code: str, rows: List[Dict]) -> int:
    """将估值时间序列幂等写入 index_pe_history（含 pe/pb/div_yield）。

    落库前用 A 股交易日历过滤非交易日：neodata 的「日期」不是交易日历口径
    （实证 2026-09-12 周六仍吐值），混入会污染 MAX(date) 与分位计算。
    """
    if not rows:
        return 0
    _ensure_valuation_columns(conn)
    n = 0
    for r in rows:
        ds = _norm_date(r["date"])
        try:
            if not is_trading_day(ds):
                logger.warning(
                    "[NeoData] %s 跳过非交易日 %s（neodata 日期非交易日历口径）",
                    index_code, ds)
                continue
        except Exception as e:
            logger.warning("[NeoData] %s 日期无法判定 %s: %s，跳过", index_code, ds, e)
            continue
        try:
            conn.execute(
                "INSERT OR REPLACE INTO index_pe_history "
                "(index_code, date, pe, pb, div_yield, source) "
                "VALUES (?, ?, ?, ?, ?, 'neodata')",
                (index_code, ds, float(r["pe"]),
                 r.get("pb"), r.get("div_yield")),
            )
            n += 1
        except Exception as e:
            logger.debug("写入估值失败 %s/%s: %s", index_code, r.get("date"), e)
    conn.commit()
    return n


def collect_all(conn, indexes: Optional[List[str]] = None) -> Dict[str, int]:
    """采集全部（或指定）跟踪指数估值并落库，返回 {index_code: 写入行数}。"""
    codes = indexes or list(INDEX_NAME_MAP.keys())
    result: Dict[str, int] = {}
    for code in codes:
        try:
            rows = fetch_index_valuation(code)
            n = save_valuation(conn, code, rows)
            result[code] = n
            logger.info("[NeoData] %s 估值采集 %d 行", code, n)
        except Exception as e:
            result[code] = 0
            logger.warning("[NeoData] %s 采集异常: %s", code, e)
    return result


def load_latest_valuation(conn, index_code: str) -> Dict:
    """读取某指数最新一行估值（pe/pb/div_yield + 历史行数）。"""
    row = conn.execute(
        "SELECT date, pe, pb, div_yield FROM index_pe_history WHERE index_code=? "
        "ORDER BY date DESC LIMIT 1", (index_code,)).fetchone()
    cnt = conn.execute(
        "SELECT COUNT(*) FROM index_pe_history WHERE index_code=?", (index_code,)).fetchone()[0]
    if not row:
        return {"history_count": cnt, "pe": None, "pb": None, "div_yield": None, "date": None}
    return {
        "date": row[0], "pe": row[1], "pb": row[2], "div_yield": row[3],
        "history_count": cnt,
    }


if __name__ == "__main__":
    import logging as _logging
    _logging.basicConfig(level=_logging.INFO)
    from config.settings import DATABASE_PATH
    from src.utils.database import get_db_connection

    _conn = get_db_connection(str(DATABASE_PATH))
    try:
        _res = collect_all(_conn)
        for _c, _n in _res.items():
            print(f"  {_c}: {_n} 行")
        print(f"合计 {sum(_res.values())} 行")
    finally:
        _conn.close()
