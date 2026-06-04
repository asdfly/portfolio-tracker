#!/usr/bin/env python3
"""
统一数据回填入口
用法:
    python scripts/run_backfill.py all          # 执行所有回填
    python scripts/run_backfill.py history       # 历史K线
    python scripts/run_backfill.py indicators     # 技术指标
    python scripts/run_backfill.py macro         # 宏观经济
    python scripts/run_backfill.py news         # 新闻数据
    python scripts/run_backfill.py sector       # 行业资金流
    python scripts/run_backfill.py full         # 完整历史回填
"""
import sys
import os
import time
import logging
import logging.handlers
from pathlib import Path

PROJECT_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_DIR))

from config.settings import DATABASE_PATH

BACKFILL_MODULES = {
    "history": {
        "module": "scripts.backfill.backfill_history",
        "desc": "历史K线数据回填",
    },
    "indicators": {
        "module": "scripts.backfill.backfill_indicators",
        "desc": "技术指标回填",
    },
    "macro": {
        "module": "scripts.backfill.backfill_macro",
        "desc": "宏观经济数据回填",
    },
    "news": {
        "module": "scripts.backfill.backfill_news",
        "desc": "新闻数据回填",
    },
    "sector": {
        "module": "scripts.backfill.backfill_sector_enhanced",
        "desc": "行业资金流回填",
    },
    "full": {
        "module": "scripts.backfill.backfill_full_history",
        "desc": "完整历史回填（耗时较长）",
    },
}


def setup_logging():
    log_dir = PROJECT_DIR / "logs"
    log_dir.mkdir(exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.handlers.RotatingFileHandler(log_dir / "backfill.log", maxBytes=10*1024*1024, backupCount=5, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )
    return logging.getLogger(__name__)


def run_single(target: str, logger):
    if target not in BACKFILL_MODULES:
        logger.error(f"未知回填目标: {target}")
        logger.info(f"可选: {', '.join(BACKFILL_MODULES.keys())}")
        return False
    info = BACKFILL_MODULES[target]
    logger.info(f"{'='*50}")
    logger.info(f"开始: {info['desc']} ({target})")
    logger.info(f"{'='*50}")
    t0 = time.time()
    try:
        mod = __import__(info["module"], fromlist=["main"])
        if hasattr(mod, "main"):
            mod.main()
        else:
            logger.warning(f"{info['module']} 没有main()函数，跳过")
        elapsed = time.time() - t0
        logger.info(f"完成: {info['desc']} ({elapsed:.1f}s)")
        return True
    except Exception as e:
        logger.error(f"失败: {info['desc']} - {e}")
        return False


def main():
    logger = setup_logging()
    targets = sys.argv[1:] if len(sys.argv) > 1 else ["all"]

    if "all" in targets:
        targets = list(BACKFILL_MODULES.keys())

    logger.info(f"回填任务: {', '.join(targets)}")
    results = {}
    for t in targets:
        results[t] = run_single(t, logger)

    logger.info(f"{'='*50}")
    logger.info(f"回填结果汇总:")
    for t, ok in results.items():
        status = "OK" if ok else "FAIL"
        desc = BACKFILL_MODULES.get(t, {}).get("desc", t)
        logger.info(f"  [{status}] {desc}")
    logger.info(f"{'='*50}")

    if not all(results.values()):
        sys.exit(1)


if __name__ == "__main__":
    main()
