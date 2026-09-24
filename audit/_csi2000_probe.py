"""只读探针：核查 portfolio.db 是否含中证2000指数（及代码 932000）。

铁律：生产库只读。连接串强制 mode=ro + uri=True，全程不写。
"""
import sqlite3

DB = "file:D:/HuaweiMoveData/Users/HUAWEI/Documents/lingxi-claw/portfolio_tracker/data/database/portfolio.db?mode=ro"
PATTERNS = ["%中证2000%", "%932000%", "%2000%"]
# 同时覆盖可能的别名/英文
PATTERNS += ["%csi2000%", "%中证二〇〇〇%"]

con = sqlite3.connect(DB, uri=True)
con.row_factory = sqlite3.Row
cur = con.cursor()

# 1) 列所有表及行数
print("=== TABLES ===")
cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
tables = [r["name"] for r in cur.fetchall()]
for t in tables:
    try:
        cur.execute(f'SELECT COUNT(*) AS c FROM "{t}"')
        n = cur.fetchone()["c"]
    except Exception as e:
        n = f"ERR({e})"
    print(f"  {t:32s} {n}")

# 2) 找出可能含证券名/代码的列（TEXT 类，或列名含 name/code/symbol/index/fund/etf/security）
print("\n=== CANDIDATE TEXT COLUMNS ===")
cand = []  # (table, col)
for t in tables:
    try:
        cur.execute(f'PRAGMA table_info("{t}")')
        cols = cur.fetchall()
    except Exception:
        continue
    for c in cols:
        cname = c["name"]
        ctype = (c["type"] or "").upper()
        is_text = any(k in ctype for k in ("CHAR", "TEXT", "CLOB", "VARCHAR", "STR"))
        is_namey = any(k in cname.lower() for k in
                       ("name", "code", "symbol", "index", "etf", "fund", "security",
                        "secu", "title", "label", "kw", "key"))
        if is_text or is_namey:
            cand.append((t, cname))

# 3) 对候选列跑 LIKE 三模式
print("\n=== SEARCH HITS (中证2000 / 932000 / 2000) ===")
hit_count = 0
for (t, c) in cand:
    for p in PATTERNS:
        try:
            cur.execute(f'SELECT COUNT(*) AS c FROM "{t}" WHERE "{c}" LIKE ?', (p,))
            n = cur.fetchone()["c"]
        except Exception:
            n = 0
        if n > 0:
            hit_count += 1
            print(f"  HIT table={t} col={c} pattern={p!r} count={n}")
            # 取样
            try:
                cur.execute(f'SELECT "{c}" FROM "{t}" WHERE "{c}" LIKE ? LIMIT 5', (p,))
                for row in cur.fetchall():
                    print(f"       sample: {row[c]!r}")
            except Exception:
                pass

if hit_count == 0:
    print("  (无命中)")

# 4) 顺带：列出 22 只 ETF / 持仓标的名（若可定位证券名表），供丹哥判断篮子构成
print("\n=== SECURITY-NAME SAMPLE (若表存在) ===")
for t in tables:
    tl = t.lower()
    if any(k in tl for k in ("etf", "fund", "holding", "security", "index", "nav", "info", "meta")):
        cur.execute(f'PRAGMA table_info("{t}")')
        cols = [c["name"] for c in cur.fetchall()]
        name_col = next((c for c in cols if c.lower() in
                         ("name", "secu_name", "secu_abbr", "fund_name", "etf_name",
                          "full_name", "symbol_name", "title")), None)
        code_col = next((c for c in cols if c.lower() in
                         ("code", "symbol", "secu_code", "fund_code", "etf_code",
                          "ts_code", "inner_code")), None)
        if name_col or code_col:
            sel = ", ".join(f'"{c}"' for c in (code_col, name_col) if c)
            try:
                cur.execute(f'SELECT {sel} FROM "{t}" LIMIT 30')
                rows = cur.fetchall()
                if rows:
                    print(f"  -- {t} ({code_col}/{name_col}) --")
                    for r in rows:
                        print("     ", dict(r))
            except Exception as e:
                print(f"  -- {t}: ERR {e}")

con.close()
print("\nDONE (read-only, no write performed)")
