import os, requests, json, sys

APIKEY = os.environ.get("MX_KEY", "").strip()
if not APIKEY:
    print("NO_KEY"); sys.exit(1)

# 直连 datacenter-web（已知本机可达），不走代理
os.environ["NO_PROXY"] = "*"; os.environ["no_proxy"] = "*"
for k in ("HTTP_PROXY","HTTPS_PROXY","http_proxy","https_proxy"):
    os.environ.pop(k, None)

S = requests.Session()
S.headers.update({"User-Agent":"Mozilla/5.0","Referer":"https://data.eastmoney.com/"})

def probe(label, url, params):
    try:
        r = S.get(url, params=params, timeout=20)
        txt = r.text
        try:
            j = r.json()
            res = j.get("result")
            if res and isinstance(res, dict) and res.get("data"):
                print(f"[{label}] HIT HTTP {r.status_code} rows={len(res['data'])} :: {str(res['data'][0])[:120]}")
            else:
                print(f"[{label}] HTTP {r.status_code} success={j.get('success')} msg={j.get('message')} code={j.get('code')}")
        except Exception:
            print(f"[{label}] HTTP {r.status_code} len={len(txt)} :: {txt[:90]}")
    except Exception as e:
        print(f"[{label}] ERR {type(e).__name__}: {str(e)[:80]}")

BASE = "https://datacenter-web.eastmoney.com/api/data/v1/get"
# 带 token 重试此前失败的报表名（无 token 报配置不存在）
for rep in ["RPT_INDEX_KLINE","RPT_INDEX_DAILYPRICE","RPT_IDX_HISTORY","EM_UD_Indices_Transaction","RPT_INDEX_QUOTE"]:
    probe(rep, BASE, {
        "reportName": rep, "columns":"ALL",
        "filter": "(index_code=\"932000\")",
        "pageSize":"5", "sortColumns":"TRADE_DATE","sortTypes":"-1",
        "source":"WEB","client":"PC","token": APIKEY
    })

# 备选：mktquery16 历史行情带 token
probe("mktquery16-kline", "https://mktquery16.eastmoney.com/api/qt/stock/kline/get", {
    "token": APIKEY, "secid":"1.932000","fields1":"f1,f2,f3","fields2":"f51,f53,f56,f57,f58",
    "klt":"101","fqt":"1","beg":"20260918","end":"20260924","lmt":"5"
})
