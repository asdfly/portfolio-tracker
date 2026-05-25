"""
Tab5: 高级分析
"""

import streamlit as st
import base64
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from datetime import datetime
from config.settings import ETF_CATEGORIES, SECTOR_COLORS, PROJECT_ROOT
from tabs._helpers import _generate_oneclick_report
from src.utils.database import get_db_connection
from src.utils.chart_utils import _cleanse_daily_returns


def compute_rebalance_suggestion(target_weights=None, threshold=0.05):
    """计算再平衡建议：基于目标权重与实际权重的偏离，生成调仓方案

    Args:
        target_weights: dict {sector: target_pct}，None则使用等权重
        threshold: 最小偏离阈值（百分比），低于此值不触发调仓

    Returns:
        dict or None
    """
    if target_weights is None:
        target_weights = {
            "医药": 0.15,
            "金融": 0.10,
            "军工": 0.10,
            "新能源": 0.15,
            "科技": 0.15,
            "宽基": 0.20,
            "红利": 0.10,
            "债券": 0.05,
        }

    conn = get_db_connection()
    query = """
        SELECT code, name, market_value, current_price, quantity, cost_price
        FROM portfolio_snapshots 
        WHERE date = (SELECT MAX(date) FROM portfolio_snapshots)
        AND market_value > 0
    """
    df = pd.read_sql(query, conn)
    if df.empty:
        return None

    total_mv = df["market_value"].sum()

    def get_sector(code):
        clean = code.replace("sh", "").replace("sz", "")
        cat = ETF_CATEGORIES.get(clean, {})
        return cat.get("sector", "其他")

    df["sector"] = df["code"].apply(get_sector)

    # 当前行业权重
    current_weights = {}
    sector_etfs = {}
    for sector, grp in df.groupby("sector"):
        current_weights[sector] = float(grp["market_value"].sum() / total_mv)
        sector_etfs[sector] = grp

    # 计算偏离
    suggestions = []
    all_sectors = set(list(target_weights.keys()) + list(current_weights.keys()))

    for sector in all_sectors:
        target = target_weights.get(sector, 0)
        current = current_weights.get(sector, 0)
        diff = current - target  # 正值=超配，负值=低配

        if abs(diff) < threshold:
            continue

        # 调仓金额
        trade_value = -diff * total_mv  # 负diff(低配) => 正trade(买入)

        etfs = sector_etfs.get(sector)
        if etfs is None or etfs.empty:
            continue

        # 等权分配到该行业的各ETF
        n_etfs = len(etfs)
        per_etf_value = trade_value / n_etfs

        for _, etf in etfs.iterrows():
            if abs(per_etf_value) < 100:  # 忽略小额
                continue
            shares = int(per_etf_value / etf["current_price"]) if etf["current_price"] > 0 else 0
            if shares == 0:
                continue
            suggestions.append(
                {
                    "sector": sector,
                    "code": etf["code"],
                    "name": etf["name"],
                    "current_weight": current,
                    "target_weight": target,
                    "diff": diff,
                    "trade_value": per_etf_value,
                    "shares": shares,
                    "direction": "买入" if per_etf_value > 0 else "卖出",
                    "price": etf["current_price"],
                }
            )

    return {
        "current_weights": current_weights,
        "target_weights": target_weights,
        "suggestions": suggestions,
        "total_value": total_mv,
        "threshold": threshold,
    }



def run_monte_carlo(days=252, n_simulations=500, end_date=None):
    """蒙特卡洛模拟：基于历史日收益率分布，生成未来N日组合净值路径

    数据清洗：
    1. 移除 |daily_return| > 15% 的异常值（历史脏数据/数据迁移错误）
    2. 默认仅使用近2年数据采样，避免早期高波动数据污染预测
    3. 近期数据指数加权，更贴近当前市场状态

    Args:
        days: 模拟未来交易日天数
        n_simulations: 模拟路径数量
        end_date: 截止日期

    Returns:
        dict: {
            'paths': np.ndarray (n_simulations, days+1),
            'percentiles': DataFrame (date, p5, p25, p50, p75, p95),
            'last_value': float,
            'mean_return': float,
            'daily_std': float,
            'sample_count': int,      # 采样池大小
            'filtered_count': int,    # 过滤掉的异常值数
            'sample_start': str,      # 采样起始日期
        }
    """
    conn = get_db_connection()
    query = "SELECT date, daily_return, total_value FROM portfolio_summary ORDER BY date"
    df = pd.read_sql(query, conn)
    if df.empty or len(df) < 30:
        return None

    if end_date:
        df = df[df["date"] <= end_date]

    # 获取最新市值
    conn2 = get_db_connection()
    query2 = "SELECT total_value FROM portfolio_summary WHERE date <= ? ORDER BY date DESC LIMIT 1"
    last_row = pd.read_sql(query2, conn2, params=(str(df["date"].max()),))
    conn2.close()

    if last_row.empty:
        return None

    last_value = float(last_row["total_value"].iloc[0])
    # daily_return 在数据库中以百分比形式存储，改用 total_value.pct_change()
    df["daily_return"] = df["total_value"].pct_change()
    returns = df["daily_return"].dropna()

    # ===== 数据清洗（统一使用 _cleanse_daily_returns）=====
    df_clean, clean_stats = _cleanse_daily_returns(
        df[["date", "daily_return"]], return_col="daily_return", threshold=5.0, max_tail=500
    )
    returns = df_clean["daily_return"]
    filtered_count = clean_stats["filtered"]

    sample_start = str(df_clean["date"].iloc[0])

    mean_ret = float(returns.mean())
    std_ret = float(returns.std())

    if std_ret <= 0:
        std_ret = 1e-8

    # ===== Bootstrap 采样（指数加权，近期数据权重更高） =====
    np.random.seed(42)
    hist_returns = returns.values

    # 指数加权：最近的数据权重最大，半年前的权重约为0.5
    n_hist = len(hist_returns)
    half_life = 126  # 半衰期约6个月(126个交易日)
    weights = np.array([2 ** (-i / half_life) for i in range(n_hist)])
    weights = weights[::-1]  # 最近的在末尾，权重最大
    weights /= weights.sum()  # 归一化

    paths = np.zeros((n_simulations, days + 1))
    paths[:, 0] = last_value

    for t in range(1, days + 1):
        # 加权 Bootstrap 采样
        indices = np.random.choice(n_hist, size=n_simulations, replace=True, p=weights)
        samples = hist_returns[indices]
        paths[:, t] = paths[:, t - 1] * (1 + samples)

    # 计算百分位
    percentiles_data = {"day": list(range(days + 1))}
    for p in [5, 25, 50, 75, 95]:
        percentiles_data[f"p{p}"] = np.percentile(paths, p, axis=0)
    percentiles_df = pd.DataFrame(percentiles_data)

    return {
        "paths": paths,
        "percentiles": percentiles_df,
        "last_value": last_value,
        "mean_return": mean_ret,
        "daily_std": std_ret,
        "sample_count": len(returns),
        "filtered_count": filtered_count,
        "sample_start": sample_start,
    }



def capture_dashboard_screenshot(port=8501):
    """截取 Dashboard 页面截图（PNG）

    通过 Selenium headless Chrome + webdriver_manager 自动管理 ChromeDriver。
    智能等待 Plotly 图表渲染完成后全页截图。

    Args:
        port: Streamlit 端口号

    Returns:
        str: PNG 文件路径，失败返回 None
    """
    try:
        import time

        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        from selenium.webdriver.common.by import By
        from webdriver_manager.chrome import ChromeDriverManager
    except ImportError:
        print("截图失败: 缺少 selenium 或 webdriver-manager，请执行 pip install selenium webdriver-manager")
        return None

    output_dir = PROJECT_ROOT / "output"
    output_dir.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    png_path = str(output_dir / f"dashboard_{timestamp}.png")

    try:
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--window-size=1920,3000")

        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.get(f"http://localhost:{port}")

        # Step 1: 等待 Streamlit App 容器就绪
        for i in range(30):
            try:
                el = driver.find_element(By.CSS_SELECTOR, "[data-testid='stApp']")
                if el.is_displayed():
                    break
            except Exception:
                pass
            time.sleep(1)

        # Step 2: 等待 Plotly 图表渲染（至少2个SVG出现）
        for i in range(45):
            try:
                charts = driver.find_elements(By.CSS_SELECTOR, ".js-plotly-plot .main-svg")
                if len(charts) >= 2:
                    time.sleep(2)  # 等待剩余图表
                    break
            except Exception:
                pass
            time.sleep(1)

        # Step 3: 滚动到底部触发懒加载，再滚回顶部
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(1)

        # Step 4: 截取完整页面
        driver.save_screenshot(png_path)
        driver.quit()
        return png_path
    except Exception as e:
        print(f"截图失败: {e}")
        return None



def export_dashboard_pdf(port=8501):
    """导出 Dashboard 为 PDF

    通过 Selenium headless Chrome + CDP Page.printToPDF 实现，A3 宽幅输出。
    智能等待 Plotly 图表渲染完成后导出。

    Args:
        port: Streamlit 端口号

    Returns:
        str: PDF 文件路径，失败返回 None
    """
    try:
        import base64
        import time

        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        from selenium.webdriver.common.by import By
        from webdriver_manager.chrome import ChromeDriverManager
    except ImportError:
        print("PDF导出失败: 缺少 selenium 或 webdriver-manager，请执行 pip install selenium webdriver-manager")
        return None

    output_dir = PROJECT_ROOT / "output"
    output_dir.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    pdf_path = str(output_dir / f"dashboard_{timestamp}.pdf")

    try:
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--window-size=1920,3000")

        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.get(f"http://localhost:{port}")

        # Step 1: 等待 Streamlit App 容器就绪
        for i in range(30):
            try:
                el = driver.find_element(By.CSS_SELECTOR, "[data-testid='stApp']")
                if el.is_displayed():
                    break
            except Exception:
                pass
            time.sleep(1)

        # Step 2: 等待 Plotly 图表渲染
        for i in range(45):
            try:
                charts = driver.find_elements(By.CSS_SELECTOR, ".js-plotly-plot .main-svg")
                if len(charts) >= 2:
                    time.sleep(2)
                    break
            except Exception:
                pass
            time.sleep(1)

        # Step 3: 滚动触发懒加载
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(1)

        # Step 4: CDP printToPDF (A3 宽幅)
        pdf_result = driver.execute_cdp_cmd(
            "Page.printToPDF",
            {
                "landscape": False,
                "displayHeaderFooter": False,
                "printBackground": True,
                "paperWidth": 13.0,
                "paperHeight": 19.0,
                "marginTop": 0.4,
                "marginBottom": 0.4,
                "marginLeft": 0.4,
                "marginRight": 0.4,
            },
        )

        pdf_bytes = base64.b64decode(pdf_result["data"])
        with open(pdf_path, "wb") as f:
            f.write(pdf_bytes)
        driver.quit()
        return pdf_path
    except Exception as e:
        print(f"PDF导出失败: {e}")
        return None



def export_positions_csv(positions_df, filename="持仓数据"):
    """导出持仓数据为CSV"""

    csv = positions_df.to_csv(index=False, encoding="utf-8-sig")
    b64 = base64.b64encode(csv.encode("utf-8-sig")).decode()
    href = f"data:text/csv;charset=utf-8-sig;base64,{b64}"
    return href, f"{filename}.csv"



def export_summary_csv(summary_df, filename="收益数据"):
    """导出收益数据为CSV"""
    csv = summary_df.to_csv(index=False, encoding="utf-8-sig")
    b64 = base64.b64encode(csv.encode("utf-8-sig")).decode()
    href = f"data:text/csv;charset=utf-8-sig;base64,{b64}"
    return href, f"{filename}.csv"



def format_value(val, prefix="", suffix="", decimals=2):
    """格式化数值"""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return "N/A"
    if isinstance(val, (int, float)):
        return f"{prefix}{val:,.{decimals}f}{suffix}"
    return str(val)




def render_tab5(positions, summary, index_quotes, selected_date, selected_benchmark, **kwargs):
    # 从kwargs获取额外的变量
    technical = kwargs.get('technical', pd.DataFrame())
    volatility = kwargs.get('volatility', None)
    max_dd = kwargs.get('max_dd', None)
    sharpe = kwargs.get('sharpe', None)
    cal_data = kwargs.get('cal_data', pd.DataFrame())
    tech_signals = kwargs.get('tech_signals', pd.DataFrame())

    """渲染Tab5: 高级分析"""
    
    st.markdown(
        '<div class="tip-title" style="">高级分析工具<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">包含Monte Carlo模拟（基于历史收益率随机采样预测未来收益区间）和再平衡建议（基于目标权重偏离度生成调仓方案）两种高级分析工具。</span></div>',
        unsafe_allow_html=True,
    )

    # ----- Monte Carlo 模拟 -----
    st.markdown(
        '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">Monte Carlo 模拟（未来收益预测）<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">基于历史日收益率分布进行Bootstrap随机采样，生成大量模拟路径，统计未来市值的概率分布区间（P5/P50/P95）。</span></div>',
        unsafe_allow_html=True,
    )
    

    mc_col1, mc_col2 = st.columns([2, 1])
    with mc_col1:
        mc_days = st.slider("模拟天数", 30, 500, 252, step=30, key="mc_days")
    with mc_col2:
        mc_sims = st.selectbox("模拟路径数", [200, 500, 1000], index=1, key="mc_sims")

    mc_result = run_monte_carlo(days=mc_days, n_simulations=mc_sims, end_date=selected_date)

    if mc_result is not None:
        perc_df = mc_result["percentiles"]

        # 扇形区域图
        fig_mc = go.Figure()

        # 扇形填充区域（从外到内）
        fig_mc.add_trace(
            go.Scatter(
                x=perc_df["day"],
                y=perc_df["p95"],
                mode="lines",
                name="P95",
                line=dict(width=0),
                showlegend=False,
                hoverinfo="skip",
            )
        )
        fig_mc.add_trace(
            go.Scatter(
                x=perc_df["day"],
                y=perc_df["p75"],
                mode="lines",
                name="P75",
                fill="tonexty",
                fillcolor="rgba(88,166,255,0.08)",
                line=dict(width=0),
                showlegend=False,
                hoverinfo="skip",
            )
        )
        fig_mc.add_trace(
            go.Scatter(
                x=perc_df["day"],
                y=perc_df["p25"],
                mode="lines",
                name="P25",
                fill="tonexty",
                fillcolor="rgba(88,166,255,0.12)",
                line=dict(width=0),
                showlegend=False,
                hoverinfo="skip",
            )
        )
        fig_mc.add_trace(
            go.Scatter(
                x=perc_df["day"],
                y=perc_df["p5"],
                mode="lines",
                name="P5",
                fill="tonexty",
                fillcolor="rgba(88,166,255,0.08)",
                line=dict(width=0),
                showlegend=False,
                hoverinfo="skip",
            )
        )

        # 中位数线
        fig_mc.add_trace(
            go.Scatter(
                x=perc_df["day"],
                y=perc_df["p50"],
                mode="lines",
                name="中位数 (P50)",
                line=dict(color="#58a6ff", width=2),
                hovertemplate="第 %{x} 天<br>中位数: ¥%{y:,.0f}<extra></extra>",
            )
        )

        # 起始值水平线
        fig_mc.add_hline(
            y=mc_result["last_value"],
            line_dash="dash",
            line_color="#f59e0b",
            annotation_text=f"当前 ¥{mc_result['last_value']:,.0f}",
            annotation_position="top right",
            annotation_font=dict(size=10, color="#f59e0b"),
        )

        fig_mc.update_layout(
            height=350,
            plot_bgcolor="#0d1117",
            paper_bgcolor="#0d1117",
            font=dict(color="#c9d1d9", size=11),
            margin=dict(l=60, r=20, t=10, b=40),
            xaxis=dict(title="交易日", showgrid=False),
            yaxis=dict(title="组合市值 (¥)", showgrid=True, gridcolor="#21262d"),
            hovermode="x unified",
            legend=dict(
                orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(size=10, color="#8b949e")
            ),
        )
        st.plotly_chart(fig_mc, width="stretch")

        # 模拟摘要
        mc_sum1, mc_sum2, mc_sum3, mc_sum4 = st.columns(4)
        with mc_sum1:
            st.metric("当前市值", f"¥{mc_result['last_value']:,.0f}")
        with mc_sum2:
            final_p50 = perc_df["p50"].iloc[-1]
            chg = (final_p50 / mc_result["last_value"] - 1) * 100 if mc_result["last_value"] > 0 else 0
            st.metric(f"P50 ({mc_days}日后)", f"¥{final_p50:,.0f}", delta=f"{chg:+.1f}%")
        with mc_sum3:
            final_p5 = perc_df["p5"].iloc[-1]
            loss = (final_p5 / mc_result["last_value"] - 1) * 100 if mc_result["last_value"] > 0 else 0
            st.metric("P5 (悲观)", f"¥{final_p5:,.0f}", delta=f"{loss:+.1f}%")
        with mc_sum4:
            final_p95 = perc_df["p95"].iloc[-1]
            gain = (final_p95 / mc_result["last_value"] - 1) * 100 if mc_result["last_value"] > 0 else 0
            st.metric("P95 (乐观)", f"¥{final_p95:,.0f}", delta=f"{gain:+.1f}%")

        # VaR 估计
        with st.expander("查看风险价值 (VaR) 估计", expanded=False):
            var_95 = mc_result["last_value"] - perc_df["p5"].iloc[-1]
            cvar_95 = mc_result["last_value"] - np.percentile(mc_result["paths"][:, -1], 5)
            st.markdown(
                f"**95% VaR（{mc_days}日）:** ¥{var_95:,.0f}\n\n"
                f"**95% CVaR（条件VaR）:** ¥{cvar_95:,.0f}\n\n"
                f"*VaR 表示在 95% 置信度下，{mc_days} 个交易日内的最大可能损失。"
                f"CVaR 是超出 VaR 时的平均损失（尾部风险）。*"
            )

            # VaR终值分布直方图（Phase 7A新增）
            final_values = mc_result["paths"][:, -1]
            fig_var_hist = go.Figure()
            fig_var_hist.add_trace(go.Histogram(
                x=final_values, nbinsx=50,
                marker_color="#58a6ff", marker_line_color="#0d1117",
                marker_line_width=1, opacity=0.85,
                hovertemplate="市值区间: %{x}<br>频次: %{y}<extra></extra>",
            ))
            fig_var_hist.add_vline(
                x=perc_df["p5"].iloc[-1], line_dash="dash", line_color="#ef4444",
                annotation_text=f"VaR 95%: ¥{perc_df['p5'].iloc[-1]:,.0f}",
                annotation_position="top left", annotation_font=dict(size=10, color="#ef4444"),
            )
            fig_var_hist.add_vline(
                x=mc_result["last_value"], line_dash="dot", line_color="#f59e0b",
                annotation_text=f"当前: ¥{mc_result['last_value']:,.0f}",
                annotation_position="top right", annotation_font=dict(size=10, color="#f59e0b"),
            )
            fig_var_hist.add_vline(
                x=perc_df["p50"].iloc[-1], line_dash="dash", line_color="#22c55e",
                annotation_text=f"P50: ¥{perc_df['p50'].iloc[-1]:,.0f}",
                annotation_position="top left", annotation_font=dict(size=10, color="#22c55e"),
            )
            fig_var_hist.update_layout(
                height=250, plot_bgcolor="#0d1117", paper_bgcolor="#0d1117",
                font=dict(color="#c9d1d9", size=11),
                margin=dict(l=60, r=20, t=30, b=30),
                xaxis=dict(title="终值 (¥)", showgrid=True, gridcolor="#21262d"),
                yaxis=dict(title="频次", showgrid=True, gridcolor="#21262d"),
                showlegend=False, bargap=0.02,
            )
            st.plotly_chart(fig_var_hist, width="stretch")
    else:
        st.info("历史数据不足（需要至少30个交易日），暂无法进行 Monte Carlo 模拟")

    # --- 蒙特卡洛收敛诊断（Phase 5C新增）---
    if mc_result is not None:
        with st.expander("模拟收敛诊断", expanded=False):
            # Run short convergence test with increasing simulation counts
            n_runs = [100, 200, 500, 1000, 2000]
            p50_finals = []
            p5_finals = []
            for nr in n_runs:
                conv = run_monte_carlo(days=min(mc_days, 126), n_simulations=nr, end_date=selected_date)
                if conv is not None:
                    p50_finals.append(conv["percentiles"]["p50"].iloc[-1])
                    p5_finals.append(conv["percentiles"]["p5"].iloc[-1])
                else:
                    p50_finals.append(None)
                    p5_finals.append(None)

            fig_conv = go.Figure()
            fig_conv.add_trace(go.Scatter(
                x=[str(n) for n in n_runs],
                y=p50_finals, mode="lines+markers", name="P50 终值",
                line=dict(color="#58a6ff", width=2), marker=dict(size=8),
            ))
            fig_conv.add_trace(go.Scatter(
                x=[str(n) for n in n_runs],
                y=p5_finals, mode="lines+markers", name="P5 终值",
                line=dict(color="#ef4444", width=2), marker=dict(size=8),
            ))
            fig_conv.update_layout(
                height=220,
                plot_bgcolor="#0d1117", paper_bgcolor="#0d1117",
                font=dict(color="#c9d1d9", size=11),
                margin=dict(l=40, r=20, t=10, b=30),
                xaxis=dict(title="模拟路径数", showgrid=True, gridcolor="#21262d"),
                yaxis=dict(title="终值 (¥)", showgrid=True, gridcolor="#21262d"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1,
                            font=dict(size=10, color="#8b949e")),
            )
            st.plotly_chart(fig_conv, width="stretch")
            st.caption(f"*采样池: {mc_result['sample_count']} 条，过滤异常值: {mc_result['filtered_count']} 条，起始: {mc_result['sample_start']}")

    # --- 风险归因分析（Phase 5C新增）---
    st.markdown("---")
    st.markdown(
        '<div class="tip-title" style="font-size:14px;border-bottom:none;padding:5px 0;">'
        '风险归因分析'
        '<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span>'
        '<span class="tip-text" style="left: 4px; top: calc(100% + 10px);">'
        '基于近60日数据，分解各持仓对组合总风险的贡献度。'
        '</span></div>',
        unsafe_allow_html=True,
    )

    if not positions.empty:
        conn_ra = get_db_connection()
        try:
            pos_risk = []
            for _, pos in positions.iterrows():
                pc = str(pos["code"])
                # Get recent daily returns for this position from snapshots
                snap_ret = pd.read_sql_query(
                    f"SELECT date, market_value FROM portfolio_snapshots "
                    f"WHERE code = ? AND date >= date('now', '-90 days') ORDER BY date",
                    conn_ra, params=(pc,)
                )
                if len(snap_ret) >= 20:
                    daily_ret = snap_ret["market_value"].pct_change().dropna()
                    vol = float(daily_ret.std()) * np.sqrt(252) * 100  # annualized vol %
                    weight = pos["market_value"] / positions["market_value"].sum() * 100 if positions["market_value"].sum() > 0 else 0
                    risk_contrib = weight * vol / 100  # marginal risk contribution
                    pos_risk.append({
                        "name": pos["name"], "code": pc,
                        "weight": weight, "volatility": vol,
                        "risk_contrib": risk_contrib * 100,
                    })
            if pos_risk:
                risk_df = pd.DataFrame(pos_risk).sort_values("risk_contrib", ascending=True)
                total_rc = risk_df["risk_contrib"].sum()

                fig_risk = go.Figure()
                bar_colors = risk_df["risk_contrib"].apply(
                    lambda x: "#ef4444" if x > 0 else "#22c55e"
                )
                fig_risk.add_trace(go.Bar(
                    x=risk_df["risk_contrib"],
                    y=risk_df["name"],
                    orientation="h",
                    marker_color=bar_colors,
                    text=[f"{v:.1f}%" for v in risk_df["risk_contrib"]],
                    textposition="auto",
                ))
                fig_risk.update_layout(
                    height=max(200, len(risk_df) * 35),
                    plot_bgcolor="#0d1117", paper_bgcolor="#0d1117",
                    font=dict(color="#c9d1d9", size=11),
                    margin=dict(l=120, r=40, t=10, b=30),
                    xaxis=dict(title="风险贡献度 (%)", showgrid=True, gridcolor="#21262d"),
                    yaxis=dict(autorange="reversed"),
                    showlegend=False,
                )
                st.plotly_chart(fig_risk, width="stretch")

                # Risk attribution summary
                ra_c1, ra_c2, ra_c3 = st.columns(3)
                with ra_c1:
                    top_risk = risk_df.iloc[-1] if len(risk_df) > 0 else None
                    if top_risk is not None:
                        st.metric("最大风险贡献", f"{top_risk['name']}", delta=f"{top_risk['risk_contrib']:.1f}%")
                with ra_c2:
                    avg_vol = risk_df["volatility"].mean()
                    st.metric("平均年化波动率", f"{avg_vol:.1f}%")
                with ra_c3:
                    hhi = (risk_df["risk_contrib"]**2).sum() / (total_rc**2) if total_rc > 0 else 0
                    st.metric("集中度 (HHI)", f"{hhi:.2f}")
        finally:
            conn_ra.close()
    else:
        st.info("暂无持仓数据")

    st.markdown("---")

    # ----- 压力测试 -----
    st.markdown(
        '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">' "持仓压力测试<span class=\"tip-arrow\" style=\"left: 4px; top: calc(100% + 5px);\"></span><span class=\"tip-text\" style=\"left: 4px; top: calc(100% + 10px);\">基于历史波动率和持仓权重，模拟温和下跌、大幅下跌、极端暴跌等多种情景下的组合市值变化。</span></div>",
        unsafe_allow_html=True,
    )
    

    if not positions.empty and not summary.empty:
        total_mv = positions["market_value"].sum()
        current_weights = {}
        for _, pos in positions.iterrows():
            code = str(pos["code"])
            sector = ETF_CATEGORIES.get(code, {}).get("sector", "未知")
            # 行业默认beta：基于各板块相对于沪深300的历史波动特征
            _sector_beta = {
                "宽基": 1.00, "科技": 1.25, "新能源": 1.20, "医药": 0.95,
                "军工": 1.10, "金融": 0.90, "红利": 0.85, "债券": 0.15,
            }
            current_weights[code] = {
                "weight": pos["market_value"] / total_mv if total_mv > 0 else 0,
                "name": pos["name"],
                "beta": _sector_beta.get(sector, 1.0),
                "sector": sector,
                "mv": pos["market_value"],
                "pnl_rate": pos.get("pnl_rate", 0),
            }

        scenarios = {
            "温和下跌": {
                "market": -0.05,
                "label": "基准跌5%",
                "color": "#f59e0b",
                "desc": "市场温和回调，宽基ETF领跌",
            },
            "大幅下跌": {
                "market": -0.15,
                "label": "基准跌15%",
                "color": "#ef4444",
                "desc": "市场大幅下跌，成长板块承压",
            },
            "极端暴跌": {
                "market": -0.30,
                "label": "基准跌30%",
                "color": "#dc2626",
                "desc": "类似股灾级别的系统性风险",
            },
            "震荡盘整": {
                "market": -0.02,
                "label": "基准±2%",
                "color": "#8b949e",
                "desc": "市场窄幅震荡，行业轮动加快",
            },
            "结构牛市": {
                "market": 0.15,
                "label": "基准涨15%",
                "color": "#22c55e",
                "desc": "市场结构性上涨，科技成长领涨",
            },
        }
        st_cols = st.columns(len(scenarios))
        stress_results = []
        for idx, (sname, sdata) in enumerate(scenarios.items()):
            market_shock = sdata["market"]
            total_impact = 0
            sector_impacts = {}
            for code, wdata in current_weights.items():
                beta = wdata["beta"] if wdata["beta"] and not np.isnan(wdata["beta"]) else 1.0
                sector = wdata["sector"]
                if market_shock < -0.1:
                    sector_adj = {"医药": 0.85, "债券": 0.6, "红利": 0.8, "军工": 0.9}.get(sector, 1.0)
                elif market_shock > 0.1:
                    sector_adj = {"科技": 1.2, "新能源": 1.15, "军工": 1.1}.get(sector, 1.0)
                else:
                    sector_adj = 1.0
                adj_shock = market_shock * beta * sector_adj
                total_impact += wdata["weight"] * adj_shock
                if sector not in sector_impacts:
                    sector_impacts[sector] = 0
                sector_impacts[sector] += wdata["weight"] * adj_shock
            est_loss = total_mv * total_impact
            stress_results.append(
                {
                    "scenario": sname,
                    "market": sdata["label"],
                    "est_loss": est_loss,
                    "est_value": total_mv + est_loss,
                    "impact_pct": total_impact * 100,
                    "color": sdata["color"],
                    "desc": sdata["desc"],
                    "sector_impacts": sector_impacts,
                }
            )
            with st_cols[idx]:
                loss_c = "#22c55e" if est_loss >= 0 else "#ef4444"
                st.markdown(
                    f'<div style="padding:8px;border-radius:6px;background:#161b22;'
                    f'border-left:3px solid {sdata["color"]};text-align:center;">'
                    f'<div style="font-size:10px;color:#8b949e;">{sname}</div>'
                    f'<div style="font-size:10px;color:#484f58;">{sdata["label"]}</div>'
                    f'<div style="font-size:14px;font-weight:bold;color:{loss_c};margin:4px 0;">'
                    f'{"+" if est_loss >= 0 else ""}\u00a5{est_loss:,.0f}</div>'
                    f'<div style="font-size:11px;color:{loss_c};">{total_impact*100:+.1f}%</div></div>',
                    unsafe_allow_html=True,
                )

        # 压力测试行业影响雷达图（Phase 7A新增）
        if stress_results:
            all_sectors_stress = set()
            for sr in stress_results:
                all_sectors_stress.update(sr.get("sector_impacts", {}).keys())
            sector_list_radar = sorted(all_sectors_stress)
            if len(sector_list_radar) >= 3:
                fig_radar = go.Figure()
                radar_colors_map = {
                    "温和下跌": "#f59e0b", "大幅下跌": "#ef4444",
                    "极端暴跌": "#dc2626", "震荡盘整": "#8b949e",
                    "结构牛市": "#22c55e",
                }
                for sr in stress_results:
                    impacts = sr.get("sector_impacts", {})
                    values = [impacts.get(s, 0) * 100 for s in sector_list_radar]
                    fig_radar.add_trace(go.Scatterpolar(
                        r=values, theta=sector_list_radar, fill="toself",
                        name=sr["scenario"],
                        line_color=radar_colors_map.get(sr["scenario"], "#58a6ff"),
                        opacity=0.5,
                    ))
                fig_radar.update_layout(
                    height=320,
                    polar=dict(
                        radialaxis=dict(visible=True, gridcolor="#21262d", color="#8b949e"),
                        angularaxis=dict(gridcolor="#21262d", color="#8b949e", tickfont=dict(size=10)),
                        bgcolor="#0d1117",
                    ),
                    plot_bgcolor="#0d1117", paper_bgcolor="#0d1117",
                    font=dict(color="#c9d1d9", size=11),
                    margin=dict(l=40, r=40, t=10, b=30),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02,
                                xanchor="center", x=0.5, font=dict(size=10, color="#8b949e")),
                )
                st.plotly_chart(fig_radar, width="stretch")

        with st.expander("查看压力测试详情", expanded=False):
            for sr in stress_results:
                loss_c = "#22c55e" if sr["est_loss"] >= 0 else "#ef4444"
                st.markdown(
                    f'<div style="margin:8px 0;padding:10px 12px;border-radius:6px;background:#161b22;'
                    f'border-left:3px solid {sr["color"]};">'
                    f'<div style="display:flex;justify-content:space-between;align-items:center;">'
                    f'<span style="font-size:14px;font-weight:bold;color:#c9d1d9;">{sr["scenario"]} '
                    f'<span style="font-size:11px;color:#484f58;">({sr["market"]})</span></span>'
                    f'<span style="font-size:16px;font-weight:bold;color:{loss_c};">'
                    f'{sr["impact_pct"]:+.1f}% ({"+" if sr["est_loss"] >= 0 else ""}\u00a5{sr["est_loss"]:,.0f})</span></div>'
                    f'<div style="font-size:11px;color:#8b949e;margin-top:4px;">{sr["desc"]}</div>'
                    f'<div style="font-size:11px;color:#c9d1d9;margin-top:6px;">'
                    f'预估市值: <b>\u00a5{sr["est_value"]:,.0f}</b> (当前 \u00a5{total_mv:,.0f})</div></div>',
                    unsafe_allow_html=True,
                )
                if sr["sector_impacts"]:
                    si_cols = st.columns(min(len(sr["sector_impacts"]), 4))
                    for si_idx, (sec_name, sec_impact) in enumerate(
                        sorted(sr["sector_impacts"].items(), key=lambda x: abs(x[1]), reverse=True)
                    ):
                        si_c = "#22c55e" if sec_impact >= 0 else "#ef4444"
                        sec_color = SECTOR_COLORS.get(sec_name, "#8b949e")
                        with si_cols[si_idx % len(si_cols)]:
                            st.markdown(
                                f'<div style="text-align:center;padding:4px 0;">'
                                f'<div style="font-size:10px;color:{sec_color};">{sec_name}</div>'
                                f'<div style="font-size:12px;font-weight:bold;color:{si_c};">{sec_impact*100:+.1f}%</div></div>',
                                unsafe_allow_html=True,
                            )
                st.markdown(
                    '<div style="height:1px;background:#21262d;margin:6px 0;"></div>', unsafe_allow_html=True
                )
            worst = min(stress_results, key=lambda x: x["est_value"])
            st.markdown(
                f'<div style="padding:8px 12px;border-radius:6px;background:#2d1215;'
                f'border:1px solid #ef4444;font-size:12px;color:#c9d1d9;">'
                f'<b>极端情景预警:</b> 在「{worst["scenario"]}」({worst["market"]})情景下，'
                f'组合预估损失 <b style="color:#ef4444;">\u00a5{worst["est_loss"]:,.0f} ({worst["impact_pct"]:+.1f}%)</b>，'
                f'预估市值 <b>\u00a5{worst["est_value"]:,.0f}</b>。</div>',
                unsafe_allow_html=True,
            )
    else:
        st.info("暂无持仓数据，无法执行压力测试")

    st.markdown("---")

    # ----- 再平衡建议 -----
    st.markdown(
        '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">再平衡建议<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">基于各行业目标权重与实际权重的偏离度，自动生成调仓方案。超过偏离阈值的行业将给出买入/卖出建议和估算股数。</span></div>',
        unsafe_allow_html=True,
    )
    

    rb_col1, rb_col2 = st.columns([2, 1])
    with rb_col1:
        st.markdown("*默认目标权重*")
    with rb_col2:
        show_rb = st.toggle("显示再平衡方案", value=True, key="rb_toggle")

    # 目标权重展示
    default_targets = {
        "医药": 0.15,
        "金融": 0.10,
        "军工": 0.10,
        "新能源": 0.15,
        "科技": 0.15,
        "宽基": 0.20,
        "红利": 0.10,
        "债券": 0.05,
    }

    if show_rb:
        rb_result = compute_rebalance_suggestion(threshold=0.03)

        if rb_result is not None:
            rw = rb_result["current_weights"]
            tw = rb_result["target_weights"]
            all_sectors = sorted(set(list(rw.keys()) + list(tw.keys())))

            # 权重对比柱状图
            fig_rb = go.Figure()
            x_labels = all_sectors
            fig_rb.add_trace(
                go.Bar(
                    name="当前权重",
                    x=x_labels,
                    y=[rw.get(s, 0) * 100 for s in all_sectors],
                    marker_color="#58a6ff",
                    opacity=0.85,
                    hovertemplate="%{x}<br>当前: %{y:.1f}%<extra></extra>",
                )
            )
            fig_rb.add_trace(
                go.Bar(
                    name="目标权重",
                    x=x_labels,
                    y=[tw.get(s, 0) * 100 for s in all_sectors],
                    marker_color="#f59e0b",
                    opacity=0.6,
                    hovertemplate="%{x}<br>目标: %{y:.1f}%<extra></extra>",
                )
            )

            # 偏离线
            deviations = [(rw.get(s, 0) - tw.get(s, 0)) * 100 for s in all_sectors]
            fig_rb.add_trace(
                go.Scatter(
                    name="偏离",
                    x=x_labels,
                    y=deviations,
                    mode="lines+markers",
                    marker_color="#ef4444",
                    marker_size=6,
                    line=dict(color="#ef4444", width=1.5, dash="dot"),
                    yaxis="y2",
                    hovertemplate="%{x}<br>偏离: %{y:+.1f}%<extra></extra>",
                )
            )

            fig_rb.update_layout(
                height=300,
                barmode="group",
                plot_bgcolor="#0d1117",
                paper_bgcolor="#0d1117",
                font=dict(color="#c9d1d9", size=11),
                margin=dict(l=40, r=40, t=10, b=40),
                xaxis=dict(showgrid=False, tickfont=dict(size=10)),
                yaxis=dict(title="权重 (%)", showgrid=True, gridcolor="#21262d"),
                yaxis2=dict(title="偏离 (%)", overlaying="y", side="right", showgrid=False, range=[-20, 20]),
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="right",
                    x=1,
                    font=dict(size=10, color="#8b949e"),
                ),
            )
            st.plotly_chart(fig_rb, width="stretch")

            # 摘要指标
            rb_s1, rb_s2, rb_s3 = st.columns(3)
            with rb_s1:
                n_suggestions = len(rb_result["suggestions"])
                st.metric("需调仓行业", f"{n_suggestions} 个")
            with rb_s2:
                max_dev = max(abs(rw.get(s, 0) - tw.get(s, 0)) * 100 for s in all_sectors)
                max_sector = max(all_sectors, key=lambda s: abs(rw.get(s, 0) - tw.get(s, 0)))
                st.metric("最大偏离", f"{max_dev:.1f}%", delta=max_sector)
            with rb_s3:
                st.metric("组合总市值", f"¥{rb_result['total_value']:,.0f}")

            # 调仓前后净值模拟对比图（Phase 7A新增）
            conn_sim = get_db_connection()
            try:
                recent_ret = pd.read_sql_query(
                    "SELECT daily_return FROM portfolio_summary "
                    "WHERE daily_return IS NOT NULL ORDER BY date DESC LIMIT 252",
                    conn_sim,
                )
                conn_sim.close()
            except Exception:
                conn_sim.close()
                recent_ret = pd.DataFrame()

            if not recent_ret.empty and len(recent_ret) >= 60:
                ret_arr = recent_ret["daily_return"].dropna().values
                np.random.seed(42)
                n_sim_days = min(60, len(ret_arr))
                sim_curr = [1.0]
                sim_reb = [1.0]
                for t in range(n_sim_days):
                    r = ret_arr[t % len(ret_arr)]
                    sim_curr.append(sim_curr[-1] * (1 + r))
                    rebal_adj = 1.0 - 0.02 * abs(max_dev / 100) if max_dev > 0 else 1.0
                    sim_reb.append(sim_reb[-1] * (1 + r * rebal_adj))

                fig_rebal_sim = go.Figure()
                fig_rebal_sim.add_trace(go.Scatter(
                    x=list(range(n_sim_days + 1)), y=[v * 100 for v in sim_curr],
                    mode="lines", name="当前组合",
                    line=dict(color="#58a6ff", width=2),
                ))
                fig_rebal_sim.add_trace(go.Scatter(
                    x=list(range(n_sim_days + 1)), y=[v * 100 for v in sim_reb],
                    mode="lines", name="调仓后模拟",
                    line=dict(color="#22c55e", width=2, dash="dot"),
                ))
                fig_rebal_sim.update_layout(
                    height=220, plot_bgcolor="#0d1117", paper_bgcolor="#0d1117",
                    font=dict(color="#c9d1d9", size=11),
                    margin=dict(l=40, r=20, t=10, b=30),
                    xaxis=dict(title="交易日", showgrid=True, gridcolor="#21262d"),
                    yaxis=dict(title="净值 (基准100)", showgrid=True, gridcolor="#21262d"),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02,
                                xanchor="right", x=1, font=dict(size=10, color="#8b949e")),
                )
                st.plotly_chart(fig_rebal_sim, width="stretch")
                st.caption("*调仓后模拟基于近期实际收益率，假设调仓降低最大行业偏离后波动率相应下降。仅供参考。*")

            # 调仓明细
            if rb_result["suggestions"]:
                with st.expander("查看调仓明细", expanded=False):
                    rb_rows = []
                    for s in rb_result["suggestions"]:
                        rb_rows.append(
                            {
                                "行业": s["sector"],
                                "ETF": f"{s['name']}（{s['code']}）",
                                "方向": s["direction"],
                                "当前权重": f"{s['current_weight']*100:.1f}%",
                                "目标权重": f"{s['target_weight']*100:.1f}%",
                                "偏离": f"{s['diff']*100:+.1f}%",
                                "调仓金额": f"¥{s['trade_value']:+,.0f}",
                                "预估股数": f"{s['shares']:+,}",
                                "现价": f"¥{s['price']:.3f}",
                            }
                        )
                    st.markdown(pd.DataFrame(rb_rows).to_html(index=False, escape=False), unsafe_allow_html=True)
                    st.caption(
                        f"*调仓阈值为 {rb_result['threshold']*100:.0f}%，低于此偏离的行业不触发调仓。股数按整数估算，实际以交易为准。*"
                    )
            else:
                st.success("当前行业权重分布合理，无需调仓")
        else:
            st.info("暂无持仓数据，无法生成再平衡建议")
    else:
        # 显示目标权重表格
        target_df = pd.DataFrame([{"行业": k, "目标权重": f"{v*100:.0f}%"} for k, v in default_targets.items()])
        st.markdown(target_df.to_html(index=False, escape=False), unsafe_allow_html=True)

    # ========== 技术指标（增强版：点击持仓行查看详情） ==========
    st.markdown(
        '<div class="tip-title" style="margin-top:20px;">🔍 技术指标信号<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">展示各ETF的技术指标信号概览，包括RSI超买超卖、MA均线信号和综合趋势判断(看多/看空/中性)。点击持仓表格中的ETF行可查看完整技术分析面板。</span></div>',
        unsafe_allow_html=True,
    )
    if not technical.empty:
        st.info(
            "💡 点击上方持仓表格中的任意ETF行，即可查看完整的技术分析详情面板（价格走势、RSI/MACD/KDJ指标、收益率分布等）。"
        )
        # 全览信号卡片（精简版）
        trend_map = {
            "bullish": ("看多", "#22c55e"),
            "bearish": ("看空", "#ef4444"),
            "neutral": ("中性", "#f59e0b"),
            None: ("--", "#888"),
        }
        tech_cols = st.columns(min(len(technical), 6))
        for idx, (_, row) in enumerate(technical.iterrows()):
            if idx >= 12:
                break
            with tech_cols[idx % len(tech_cols)]:
                trend_label, trend_color = trend_map.get(row.get("trend"), ("--", "#888"))
                st.markdown(
                    f'<div style="padding:8px;border-radius:6px;background:#161b22;'
                    f'border-left:3px solid {trend_color};margin-bottom:4px;">'
                    f'<div style="font-size:11px;color:#c9d1d9;font-weight:bold;white-space:nowrap;'
                    f'overflow:hidden;text-overflow:ellipsis;">{row.get("name", row.get("code", "未知"))}</div>'
                    f'<div style="font-size:11px;color:{trend_color};">{trend_label}</div>'
                    f'<div style="font-size:10px;color:#8b949e;">RSI: {row.get("rsi_value", 0):.1f} | MA: {row.get("ma_signal", "--")}</div>'
                    f"</div>",
                    unsafe_allow_html=True,
                )

    # ========== 智能建议 ==========
    report_dir = PROJECT_ROOT / "data" / "reports"
    if report_dir.exists():
        report_files = sorted(report_dir.glob("smart_report_*.md"), reverse=True)
        if report_files:
            with st.expander("💡 智能分析建议（最新报告）", expanded=False):
                with open(report_files[0], "r", encoding="utf-8") as f:
                    report_text = f.read()
                st.markdown(report_text[:3000] + ("..." if len(report_text) > 3000 else ""))

    # ========== 数据导出 ==========
    st.markdown("---")
    with st.expander("📥 数据导出", expanded=False):
        col_exp1, col_exp2, col_exp3 = st.columns(3)
        with col_exp1:
            if not positions.empty:
                href_pos, fname_pos = export_positions_csv(positions, f"持仓数据_{selected_date}")
                st.markdown(
                    f'<a href="{href_pos}" download="{fname_pos}" '
                    f'style="display:inline-block;padding:8px 16px;background:#21262d;color:#c9d1d9;'
                    f'border-radius:6px;text-decoration:none;font-size:13px;border:1px solid #30363d;">'
                    f"📋 导出持仓数据 (CSV)</a>",
                    unsafe_allow_html=True,
                )
        with col_exp2:
            if not summary.empty:
                href_sum, fname_sum = export_summary_csv(summary, f"收益数据_{selected_date}")
                st.markdown(
                    f'<a href="{href_sum}" download="{fname_sum}" '
                    f'style="display:inline-block;padding:8px 16px;background:#21262d;color:#c9d1d9;'
                    f'border-radius:6px;text-decoration:none;font-size:13px;border:1px solid #30363d;">'
                    f"📈 导出收益数据 (CSV)</a>",
                    unsafe_allow_html=True,
                )
        with col_exp3:
            if st.button("📸 导出 Dashboard 截图 (PNG)", key="screenshot_btn"):
                with st.spinner("正在截图，请稍候..."):
                    screenshot_path = capture_dashboard_screenshot(port=8501)
                if screenshot_path:
                    st.success(f"截图已保存: {screenshot_path}")
                    # 提供下载链接
                    with open(screenshot_path, "rb") as f:
                        img_bytes = f.read()
                    st.download_button(
                        label="📥 下载截图",
                        data=img_bytes,
                        file_name=f"dashboard_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png",
                        mime="image/png",
                        key="download_screenshot",
                    )
                else:
                    st.error("截图失败，请确认 Dashboard 正在运行")

        # PDF 导出按钮（第4列，新行）
        st.markdown("<br>", unsafe_allow_html=True)
        col_exp4 = st.columns([1, 1, 1])[1]
        with col_exp4:
            if st.button("📄 导出 Dashboard 报告 (PDF)", key="pdf_btn"):
                with st.spinner("正在生成 PDF，请稍候..."):
                    pdf_path = export_dashboard_pdf(port=8501)
                if pdf_path:
                    st.success(f"PDF 已生成: {pdf_path}")
                    with open(pdf_path, "rb") as f:
                        pdf_bytes = f.read()
                    st.download_button(
                        label="📥 下载 PDF 报告",
                        data=pdf_bytes,
                        file_name=f"dashboard_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf",
                        mime="application/pdf",
                        key="download_pdf",
                    )
                else:
                    st.error("PDF 导出失败，请确认 Dashboard 正在运行")

        # 一键综合分析报告
        st.markdown("<br>", unsafe_allow_html=True)
        col_exp5 = st.columns([1, 1, 1])[1]
        with col_exp5:
            if st.button("📋 一键导出综合分析报告 (HTML)", key="report_btn"):
                with st.spinner("正在生成报告..."):
                    report_html = _generate_oneclick_report(
                        positions, summary, technical, selected_date, selected_benchmark
                    )
                if report_html:
                    st.success("报告已生成！")
                    st.download_button(
                        label="📥 下载综合报告",
                        data=report_html.encode("utf-8"),
                        file_name=f"投资组合分析报告_{selected_date}.html",
                        mime="text/html",
                        key="download_report",
                    )
                else:
                    st.error("报告生成失败，数据不足")

    # ========== Tab9: 自定义指标工作台 ==========
    


