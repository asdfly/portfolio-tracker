#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Fetch real 中证2000 (932000) OHLCV from 中证指数公司 official source (akshare)
and emit westock data_kline isomorphic JSON:
  {"ok":true,"data":{"nodes":[{"date","open","last","high","low","volume","amount"}, ...]}}
This is a transparent REAL-DATA alternative used when westock-mcp is not connected.
Explicit start/end dates are required (akshare defaults to a stale 2018-2024 window).
"""
import os, json
from datetime import date
import akshare as ak

OUT = r"D:/HuaweiMoveData/Users/HUAWEI/Documents/lingxi-claw/portfolio_tracker/scripts/backfill/data/cs932000_latest.json"
START = "20240101"
END = date.today().strftime("%Y%m%d")  # 动态到今日，避免硬编码过期窗口

def main():
    df = ak.stock_zh_index_hist_csindex(symbol="932000", start_date=START, end_date=END)
    print("rows:", len(df))
    print(df.head(2).to_string())
    print(df.tail(3).to_string())
    nodes = []
    for _, r in df.iterrows():
        date = str(r["日期"])
        try:
            date = date[:10]
        except Exception:
            pass
        nodes.append({
            "date": date,
            "open": float(r["开盘"]),
            "last": float(r["收盘"]),
            "high": float(r["最高"]),
            "low": float(r["最低"]),
            "volume": float(r["成交量"]),
            "amount": float(r["成交金额"]),
        })
    nodes.sort(key=lambda n: n["date"])
    doc = {"ok": True, "data": {"nodes": nodes}}
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(doc, f, ensure_ascii=False)
    print(f"WROTE {OUT} nodes={len(nodes)} range={nodes[0]['date']}~{nodes[-1]['date']}")

if __name__ == "__main__":
    main()
