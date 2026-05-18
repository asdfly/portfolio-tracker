"""黄金模块数据预加载器

在 tab11_gold.py 入口处调用 preload_gold_data()，
并发获取所有共享数据源（带缓存），避免各子 tab 重复串行等待。
"""
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import streamlit as st

logger = logging.getLogger(__name__)

# 预加载键名
SESSION_KEY = "gold_preload"
PRELOAD_VERSION = 3  # 数据源变更时递增，强制刷新


def preload_gold_data():
    """并发预加载黄金模块共享数据源。

    调用时机：tab11 render 函数的顶部（st.tabs 之前）。
    利用 @st.cache_data 的进程级缓存 + ThreadPoolExecutor 的 I/O 并发，
    将首次加载的总耗时从串行 ~50s 降至并发 ~22s。
    """
    state = st.session_state.get(SESSION_KEY)
    if state is not None and state.get("version") == PRELOAD_VERSION:
        return state

    from tabs.gold_components.gold_utils import (
        fetch_sge_benchmark,
        fetch_global_etf_holdings,
        fetch_comex_inventory,
    )

    # 定义预加载任务：name -> callable（不带参数的 lambda）
    tasks = {
        "sge_benchmark": fetch_sge_benchmark,
        "global_etf_holdings": lambda: fetch_global_etf_holdings(years=2),
        "comex_inventory": fetch_comex_inventory,
    }

    results = {}
    t0 = time.time()

    # 对于已有缓存的数据，直接在主线程调用（极快，~0s）
    # 对于未缓存的数据，使用线程池并发 I/O
    # Streamlit 的 @st.cache_data 在子线程中也可命中缓存
    with ThreadPoolExecutor(max_workers=len(tasks)) as pool:
        future_map = {name: pool.submit(fn) for name, fn in tasks.items()}
        for name, future in future_map.items():
            try:
                results[name] = future.result(timeout=60)
            except Exception as e:
                logger.warning("[gold_preloader] %s failed: %s", name, e)
                results[name] = None

    elapsed = time.time() - t0
    logger.info("[gold_preloader] preload completed in %.1fs", elapsed)
    results["version"] = PRELOAD_VERSION
    results["elapsed"] = elapsed
    st.session_state[SESSION_KEY] = results
    return results


def get_preloaded(key, default=None):
    """获取预加载的数据，未预加载则返回 default。"""
    state = st.session_state.get(SESSION_KEY)
    if state is None:
        return default
    return state.get(key, default)
