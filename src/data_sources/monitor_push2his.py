"""
push2his.eastmoney.com 封锁状态监测脚本

用途: 自动探测东财 push2his API 是否恢复正常，恢复后自动回填主力资金历史数据。

封锁特征:
  - SSL 握手成功，但 HTTP 响应阶段被远端关闭连接
  - 异常: RemoteDisconnected
  - curl: return code 56 + schannel renegotiation

正常特征:
  - HTTP 200, JSON 包含 {"rc":0, "data": {"klines": [...]}}

使用方式:
  python monitor_push2his.py            # 单次探测
  python monitor_push2his.py --loop     # 每小时探测，恢复后自动回填
  python monitor_push2his.py --days 90  # 回填最近90天(默认120)
  python monitor_push2his.py --backfill-only  # 跳过探测直接回填
"""

import urllib.request, json, time, sys, os, logging, argparse
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger(__name__)

API_URL = "https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get?lmt=3&klt=101&secid=1.000001&fields1=f1,f2,f3,f7&fields2=f51,f52,f53,f54,f55"
STATUS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "data", "push2his_status.txt")


def _urllib_probe():
    """用 urllib（无代理）探测 push2his 连通性。"""
    proxy_handler = urllib.request.ProxyHandler({})
    opener = urllib.request.build_opener(proxy_handler)
    start = time.time()
    try:
        url = API_URL + f"&_={int(time.time()*1000)}"
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Connection": "close",
        })
        resp = opener.open(req, timeout=20)
        elapsed = round(time.time() - start, 2)
        body = resp.read().decode("utf-8")
        data = json.loads(body)
        klines = data.get("data", {}).get("klines", [])
        return {
            "ok": True, "elapsed": elapsed,
            "klines_count": len(klines),
            "sample": klines[0] if klines else None,
            "date_range": f"{klines[0].split(',')[0]} ~ {klines[-1].split(',')[0]}" if klines else "",
        }
    except Exception as e:
        elapsed = round(time.time() - start, 2)
        return {"ok": False, "elapsed": elapsed, "error": f"{type(e).__name__}: {e}"}


def _save_status(status):
    """持久化探测结果到 data/push2his_status.txt。"""
    os.makedirs(os.path.dirname(os.path.abspath(STATUS_FILE)), exist_ok=True)
    with open(STATUS_FILE, "a", encoding="utf-8") as f:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if status["ok"]:
            line = f"{ts} OK elapsed={status['elapsed']}s klines={status['klines_count']} range={status['date_range']}\n"
        else:
            line = f"{ts} FAIL elapsed={status['elapsed']}s error={status['error']}\n"
        f.write(line)


def _backfill(days):
    """封锁解除后，回填主力资金历史数据。"""
    logger.info(f"开始回填主力资金历史数据 (最近 {days} 天)...")
    for mod in list(sys.modules.keys()):
        if "fund_flow" in mod or mod.startswith("requests") or mod.startswith("urllib3"):
            del sys.modules[mod]
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sys.path.insert(0, project_root)
    try:
        from src.data_sources.fund_flow import fetch_main_fund_flow, save_fund_flows
        from config.settings import DATABASE_PATH
        import sqlite3
        conn = sqlite3.connect(str(DATABASE_PATH))
        try:
            df = fetch_main_fund_flow(days=days)
            if not df.empty:
                n = save_fund_flows(conn, df)
                logger.info(f"回填完成: 写入 {n} 条记录")
            else:
                logger.warning("回填失败: fetch_main_fund_flow 返回空数据")
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"回填出错: {e}")


def run_once():
    """单次探测并输出结果。"""
    logger.info("探测 push2his.eastmoney.com ...")
    status = _urllib_probe()
    _save_status(status)
    if status["ok"]:
        logger.info(f"连通正常 | 耗时 {status['elapsed']}s | klines {status['klines_count']} 条 | {status['date_range']}")
    else:
        logger.warning(f"仍然封锁 | 耗时 {status['elapsed']}s | {status['error']}")
    return status["ok"]


def run_loop(interval_hours=1, backfill_days=120):
    """循环探测，检测到恢复后自动回填。"""
    logger.info(f"启动循环监测 (间隔 {interval_hours}h, 回填 {backfill_days} 天)")
    attempt = 0
    while True:
        attempt += 1
        logger.info(f"--- 第 {attempt} 次探测 ---")
        ok = run_once()
        if ok:
            logger.info("检测到 push2his 恢复，执行回填...")
            _backfill(backfill_days)
            logger.info("回填完成，退出循环监测")
            break
        logger.info(f"下次探测: {interval_hours} 小时后")
        time.sleep(interval_hours * 3600)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="push2his 封锁状态监测")
    parser.add_argument("--loop", action="store_true", help="循环探测直到恢复")
    parser.add_argument("--interval", type=int, default=1, help="循环间隔(小时, 默认1)")
    parser.add_argument("--days", type=int, default=120, help="回填天数(默认120)")
    parser.add_argument("--backfill-only", action="store_true", help="跳过探测直接回填")
    args = parser.parse_args()
    if args.backfill_only:
        _backfill(args.days)
    elif args.loop:
        run_loop(interval_hours=args.interval, backfill_days=args.days)
    else:
        run_once()
