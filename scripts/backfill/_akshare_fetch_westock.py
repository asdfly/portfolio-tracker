#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Fallback fetcher for etf_price_history backfill.

westock-mcp data_kline is unavailable in this automation session (not connected to
the deferred tool index), so we use akshare's Sina ETF history endpoint, which is
the project's own documented backup source and avoids the East Money push2his RST.

Empirically verified against the westock reference sample (etf_westock_510300.json)
that akshare-Sina yields bit-identical OHLC, and:
  - volume (Sina, in shares) == westock volume (in lots) * 100  -> divide by 100
  - amount  (Sina, in yuan)   == round(westock amount / 10000) * 10000 -> round to 万
So the emitted JSON is byte-compatible with westock-mcp data_kline output.

Single-code, rate-limited fetch with backoff; writes etf_westock_<CODE6>.json
(idempotent), one file per ETF, exactly as the backfill script expects.
"""
import sys
import os
import time
import json

import akshare as ak

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
OUTDIR = os.path.join(ROOT, "scripts", "backfill", "data")

# (prefix+code) as given by the task
CODES = [
    "sz159220", "sz159267", "sz159300", "sz159650", "sz159732", "sz159770",
    "sz159796", "sz159819", "sz159949", "sz159992", "sh510300", "sh510500",
    "sh511380", "sh511520", "sh512010", "sh512100", "sh512810", "sh515010",
    "sh515120", "sh516160", "sh561910", "sh563020", "sh588000",
]

START = "2026-09-20"
END = "2026-10-09"
MAX_RETRIES = 8


def norm_date(s):
    s = str(s).strip()
    if "/" in s:
        y, m, d = s.split("/")
        return f"{int(y):04d}-{int(m):02d}-{int(d):02d}"
    return s[:10]


def fetch_one(symbol):
    last_err = None
    for attempt in range(MAX_RETRIES):
        try:
            df = ak.fund_etf_hist_sina(symbol=symbol)
            if df is None or len(df) == 0:
                return None, "empty response"
            return df, None
        except Exception as e:  # noqa: BLE001
            last_err = repr(e)
            if attempt < MAX_RETRIES - 1:
                time.sleep(12 + (attempt % 4))  # 12~15s backoff
    return None, last_err


def main():
    results = {}
    for sym in CODES:
        code6 = sym[2:]  # strip sz/sh/bj
        df, err = fetch_one(sym)
        if df is None:
            print(f"[SKIP] {sym}: {err}")
            results[sym] = "SKIP"
            time.sleep(2)
            continue

        nodes = []
        for _, row in df.iterrows():
            d = norm_date(row["date"])
            if d < START or d > END:
                continue
            vol = row.get("volume")
            amt = row.get("amount")
            nodes.append({
                "date": d,
                "open": float(row["open"]),
                "last": float(row["close"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "volume": round(float(vol) / 100.0) if vol is not None else 0,
                "amount": (round(float(amt) / 10000.0) * 10000) if amt is not None else None,
            })

        if not nodes:
            print(f"[SKIP] {sym}: no rows in [{START},{END}]")
            results[sym] = "NODATA"
            time.sleep(2)
            continue

        out = {"ok": True, "data": {"nodes": nodes}}
        path = os.path.join(OUTDIR, f"etf_westock_{code6}.json")
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(out, fh, ensure_ascii=False)
        print(f"[OK] {sym} ({code6}): {len(nodes)} nodes, max={nodes[-1]['date']}")
        results[sym] = "OK"
        time.sleep(2)  # gentle between codes

    ok = [k for k, v in results.items() if v == "OK"]
    bad = {k: v for k, v in results.items() if v != "OK"}
    print(f"\nSUMMARY ok={len(ok)} bad={len(bad)}")
    if bad:
        print("BAD:", bad)


if __name__ == "__main__":
    main()
