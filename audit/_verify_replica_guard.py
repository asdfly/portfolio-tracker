"""Focused behavioral verification of _scan_replica_rows after #87 (delete dead
COPY_BREADTH_MIN_RATIO constant). The guard's only executable reference to the
deleted constant was the log literal `COPY_BREADTH_MIN_RATIO * 100` (== 50.0);
the real comparison is the hardcoded `c * 2 < n`. We extract the three relevant
functions via AST (bypassing module-level numpy/pandas/.risk/config imports) and
confirm the Tier2 branch fires exactly on the documented cases.

Run with managed python (no venv needed).
"""
import ast
import re

SRC = r"D:\HuaweiMoveData\Users\HUAWEI\Documents\lingxi-claw\portfolio_tracker\src\analysis\portfolio_risk.py"

with open(SRC, "r", encoding="utf-8") as f:
    src = f.read()

tree = ast.parse(src)
wanted = {"_calendar_gap_days", "_replica_key", "_scan_replica_rows"}
funcs = []
for node in tree.body:
    if isinstance(node, ast.FunctionDef) and node.name in wanted:
        funcs.append(ast.get_source_segment(src, node))
assert len(funcs) == 3, f"expected 3 functions, got {len(funcs)}: {[f[:30] for f in funcs]}"
func_src = "\n\n".join(funcs)

# Provide stubs in the exec namespace.
class _Logger:
    def info(self, *a, **k): pass
    def warning(self, *a, **k): pass

OTC = set()

def is_otc_fund(code):
    return code in OTC

ns = {
    "is_otc_fund": is_otc_fund,
    "logger": _Logger(),
    "COPY_RUN_MIN_LEN": 5,
    "COPY_BREADTH_MIN_N": 3,
    "Optional": __import__("typing").Optional,
    "Tuple": __import__("typing").Tuple,
    "Dict": __import__("typing").Dict,
    "List": __import__("typing").List,
    "Any": __import__("typing").Any,
}
exec(compile(func_src, SRC, "exec"), ns)
scan = ns["_scan_replica_rows"]


def make_series(codes, dates, key_factory):
    series = []
    for c in codes:
        hist = [{"date": d, "current_price": p, "market_value": m}
                for d, (p, m) in zip(dates, [key_factory(c, d) for d in dates])]
        series.append((c, f"label-{c}", hist))
    return series


def tier2_hits(out, date):
    return {code for code, dmap in out.items() if date in dmap and dmap[date][0] == 2}


# Case A: 12 OTC codes, key(D) == key(D-1)  => VOID D (Tier2)
OTC = {f"OTC{i:02d}" for i in range(12)}
dates = ["2026-09-14", "2026-09-15"]
def key_same(c, d): return (1.0, 1.0)
outA = scan(make_series(sorted(OTC), dates, key_same))
assert tier2_hits(outA, "2026-09-15") == OTC, "Case A failed"
print(f"Case A PASS: {len(tier2_hits(outA, '2026-09-15'))}/12 OTC voided on 09-15 (Tier2)")

# Case B: 12 OTC codes, only 3 match prev-day key => NO void (3/12=25% < 50%)
match3 = {f"OTC{i:02d}" for i in range(3)}
def key_b(c, d):
    if d == "2026-09-14": return (1.0, 1.0)
    return (1.0, 1.0) if c in match3 else (2.0, 2.0)
outB = scan(make_series(sorted(OTC), dates, key_b))
assert tier2_hits(outB, "2026-09-15") == set(), "Case B failed"
print("Case B PASS: 0/12 voided (3/12 = 25% correctly below 50%)")

# Case C: denominator too small (n=2 < COPY_BREADTH_MIN_N=3) => NO void
OTC = {f"OTC{i:02d}" for i in range(2)}
def key_same2(c, d): return (1.0, 1.0)
outC = scan(make_series(sorted(OTC), dates, key_same2))
assert tier2_hits(outC, "2026-09-15") == set(), "Case C failed"
print("Case C PASS: 0/2 voided (n=2 < COPY_BREADTH_MIN_N=3 gated)")

# Case D: boundary 50% (n=4, c=2) => c*2=4 >= n=4 => the 2 matching codes VOID (>=50% fires)
OTC = {f"OTC{i:02d}" for i in range(4)}
match2 = {f"OTC{i:02d}" for i in range(2)}
def key_d(c, d):
    if d == "2026-09-14": return (1.0, 1.0)
    return (1.0, 1.0) if c in match2 else (2.0, 2.0)
outD = scan(make_series(sorted(OTC), dates, key_d))
hitsD = tier2_hits(outD, "2026-09-15")
assert hitsD == match2, f"Case D failed: expected {match2}, got {hitsD}"
assert hitsD.isdisjoint(OTC - match2), "Case D failed: non-matching codes wrongly voided"
print(f"Case D PASS: exactly the 2/4 (50%) matching codes voided (boundary fires correctly)")

print("\nALL TIER2 GUARD CASES PASS — behavior unchanged after #87 constant removal.")
