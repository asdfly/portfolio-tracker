"""NeoData 指数估值采集模块 — 用自然语言金融搜索补全跟踪指数估值数据。

背景（P0-3 估值分位分析的数据源补强）：
  - akshare `stock_zh_index_value_csindex` 仅返回最近 ~20 个交易日 PE，不足以算 5 年分位；
  - 乐咕乐股 `stock_index_pe_lg/pb_lg` 长期 SSL 连接失败；
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

logger = logging.getLogger(__name__)

# neodata skill 的 query.py 路径（WorkBuddy 运行时环境）
NEODATA_SKILL_DIR = Path("C:/Users/HUAWEI/.workbuddy/skills/neodata-financial-search")
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
    "930006": "中证机器人指数",
    "931743": "消费电子指数",
    "931152": "中证创新药产业指数",
    "399989": "医药指数",
    "399808": "新能源指数",
    "931157": "中证电池主题指数",
    "h11118": "恒生港股通高股息低波动指数",
    "H30269": "中证红利低波动指数",
}


def _ensure_valuation_columns(conn) -> None:
    """幂等迁移：index_pe_history 增加 pb / div_yield 列（供 neodata 富字段落库）。"""
    cols = {r[1] for r in conn.execute("PRAGMA table_info(index_pe_history)").fetchall()}
    if "pb" not in cols:
        conn.execute("ALTER TABLE index_pe_history ADD COLUMN pb REAL")
    if "div_yield" not in cols:
        conn.execute("ALTER TABLE index_pe_history ADD COLUMN div_yield REAL")
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


def _extract_valuation_rows(content: str) -> List[Dict]:
    """解析「指数估值」markdown 表格，抽取 交易日/PE(TTM)/PB(LF)/股息率 行。

    Returns:
        [{code, date, pe, pb, div_yield}, ...]
    """
    if not content:
        return []
    lines = [ln.strip() for ln in content.splitlines() if ln.strip()]
    if len(lines) < 3:
        return []

    # 定位表头行（含「指数代码」与「滚动市盈率」）
    header_line = None
    for ln in lines:
        if "指数代码" in ln and "市盈率" in ln:
            header_line = ln
            break
    if header_line is None:
        return []

    def _split(ln: str) -> List[str]:
        return [c.strip() for c in ln.strip().strip("|").split("|")]

    header = _split(header_line)

    def _col(*keys: str) -> Optional[int]:
        for i, h in enumerate(header):
            if any(k in h for k in keys):
                return i
        return None

    c_code = _col("指数代码")
    c_date = _col("交易日")
    c_pe = _col("滚动市盈率PE(TTM)") if _col("滚动市盈率PE(TTM)") is not None else _col("滚动市盈率")
    c_pb = _col("市净率PB(LF")
    c_dy = _col("股息率")
    if c_date is None or c_pe is None:
        return []

    rows: List[Dict] = []
    for ln in lines:
        cells = _split(ln)
        if len(cells) <= max(c_date, c_pe):
            continue
        code_cell = cells[c_code] if c_code is not None else ""
        if "." not in code_cell:  # 数据行首列为「000688.SH」形式
            continue

        def _f(idx: Optional[int]) -> Optional[float]:
            if idx is None or idx >= len(cells):
                return None
            v = cells[idx].replace(",", "").strip()
            try:
                return float(v)
            except ValueError:
                return None

        pe = _f(c_pe)
        if pe is None or pe <= 0:
            continue
        rows.append({
            "code": code_cell.split(".")[0],
            "date": cells[c_date][:10],
            "pe": pe,
            "pb": _f(c_pb),
            "div_yield": _f(c_dy),
        })
    return rows


def fetch_index_valuation(index_code: str) -> List[Dict]:
    """采集单个指数的估值时间序列（经 neodata）。"""
    name = INDEX_NAME_MAP.get(index_code, index_code)
    data = _run_query(f"{name} 指数估值")
    if not data or data.get("code") != "200":
        return []
    try:
        recall = data["data"]["apiData"]["apiRecall"]
    except (KeyError, TypeError):
        return []
    for block in recall:
        if block.get("type") == "指数估值":
            rows = _extract_valuation_rows(block.get("content", ""))
            return [r for r in rows if r["code"] == index_code] or rows
    return []


def save_valuation(conn, index_code: str, rows: List[Dict]) -> int:
    """将估值时间序列幂等写入 index_pe_history（含 pe/pb/div_yield）。"""
    if not rows:
        return 0
    _ensure_valuation_columns(conn)
    n = 0
    for r in rows:
        try:
            conn.execute(
                "INSERT OR REPLACE INTO index_pe_history (index_code, date, pe, pb, div_yield) "
                "VALUES (?, ?, ?, ?, ?)",
                (index_code, r["date"], float(r["pe"]),
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
