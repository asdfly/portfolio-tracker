#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""探针：测试多个候选源能否取到 中证2000(sh932000) 行情，用于落地 A 与规划 B。"""
import os, json, requests
for k in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
    os.environ.pop(k, None)
os.environ["NO_PROXY"] = "*"; os.environ["no_proxy"] = "*"

S = requests.Session()
S.headers.update({"User-Agent": "Mozilla/5.0", "Referer": "https://gu.qq.com/"})

def show(label, url, params=None, timeout=20):
    try:
        r = S.get(url, params=params, timeout=timeout)
        txt = r.text[:400]
        print(f"[{label}] HTTP {r.status_code} len={len(r.text)}")
        print("   ", txt.replace("\n", " "))
    except Exception as e:
        print(f"[{label}] ERR {type(e).__name__}: {e}")

print("=== 控制组：已知指数 sh000300 验证端点是否对指数生效 ===")
show("T-gtimg-300", "https://web.ifzq.gtimg.cn/appstuff/app/chart/kline",
     {"param": "sh000300,day,,,320,qfq"})
show("T-qt-300", "https://qt.gtimg.cn/q=sh000300")

print("\n=== 中证2000 探针 ===")
# 1) 腾讯实时报价（验证 932000 是否为有效腾讯符号）
show("T-qt-932000", "https://qt.gtimg.cn/q=sh932000")
# 2) 腾讯 kline 现有端点（复现失败）
show("T-gtimg-932000", "https://web.ifzq.gtimg.cn/appstuff/app/chart/kline",
     {"param": "sh932000,day,,,320,qfq"})
# 3) 腾讯 kline 备用 host
show("T-proxy-932000", "https://proxy.finance.qq.com/ifzqgtimg/appstuff/app/chart/kline",
     {"param": "sh932000,day,,,320,qfq"})
# 4) 东财直连 kline（不依赖 akshare，secid 1.932000）
show("EM-direct-932000",
     "https://push2his.eastmoney.com/api/qt/stock/kline/get",
     {"secid": "1.932000", "fields1": "f1,f2,f3,f4,f5,f6",
      "fields2": "f51,f52,f53,f54,f55,f56,f57,f58", "klt": "101",
      "fqt": "1", "beg": "20250101", "end": "20500101", "lmt": "5"})
# 5) 东财直连 指数列表校验 secid 是否对
show("EM-direct-300",
     "https://push2his.eastmoney.com/api/qt/stock/kline/get",
     {"secid": "1.000300", "fields1": "f1,f2", "fields2": "f51,f58",
      "klt": "101", "fqt": "1", "beg": "20250901", "end": "20250910", "lmt": "3"})
print("\n完成。")
