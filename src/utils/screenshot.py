"""浏览器截图与 PDF 导出工具

基于 Playwright headless Chromium，提供 Streamlit Dashboard 的
截图（PNG）和 PDF 导出能力。依赖 playwright，缺失时优雅降级。

用法::

    from src.utils.screenshot import capture_screenshot, export_pdf

    png_path = capture_screenshot(port=8501)
    pdf_path = export_pdf(port=8501)
"""

from pathlib import Path
from datetime import datetime


def _launch_headless_chrome(port=8501):
    """启动 headless Chromium 并导航到 Streamlit 应用。

    Returns:
        playwright Browser 实例的 (browser, page) 元组，失败返回 None。
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("浏览器驱动缺失: 请执行 pip install playwright && python -m playwright install chromium")
        return None

    try:
        pw = sync_playwright().start()
        browser = pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-gpu"],
        )
        page = browser.new_page(viewport={"width": 1920, "height": 3000})
        page.goto(f"http://localhost:{port}", wait_until="networkidle", timeout=60000)

        # Step 1: 等待 Streamlit App 容器就绪
        page.wait_for_selector("[data-testid='stApp']", state="visible", timeout=30000)

        # Step 2: 等待 Plotly 图表渲染（至少 2 个 SVG 出现）
        page.wait_for_function(
            "() => document.querySelectorAll('.js-plotly-plot .main-svg').length >= 2",
            timeout=45000,
        )
        page.wait_for_timeout(2000)

        # Step 3: 滚动到底部触发懒加载，再滚回顶部
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(2000)
        page.evaluate("window.scrollTo(0, 0)")
        page.wait_for_timeout(1000)

        return pw, browser, page
    except (OSError, RuntimeError, ValueError) as e:
        print(f"浏览器启动失败: {e}")
        try:
            browser.close()
        except (OSError, RuntimeError):
            pass
        try:
            pw.stop()
        except (OSError, RuntimeError):
            pass
        return None


def capture_screenshot(port=8501, output_dir=None, filename_prefix="dashboard"):
    """截取 Dashboard 页面截图（PNG）。

    Args:
        port: Streamlit 端口号。
        output_dir: 输出目录，默认为 PROJECT_ROOT/output。
        filename_prefix: 文件名前缀。

    Returns:
        str: PNG 文件路径，失败返回 None。
    """
    if output_dir is None:
        from config.settings import PROJECT_ROOT
        output_dir = PROJECT_ROOT / "output"
    output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    png_path = str(output_dir / f"{filename_prefix}_{timestamp}.png")

    result = _launch_headless_chrome(port)
    if result is None:
        return None

    pw, browser, page = result
    try:
        page.screenshot(path=png_path, full_page=True)
        return png_path
    except (OSError, RuntimeError, ValueError) as e:
        print(f"截图失败: {e}")
        return None
    finally:
        browser.close()
        pw.stop()


def export_pdf(port=8501, output_dir=None, filename_prefix="dashboard"):
    """导出 Dashboard 页面为 PDF（A3 宽幅）。

    Args:
        port: Streamlit 端口号。
        output_dir: 输出目录，默认为 PROJECT_ROOT/output。
        filename_prefix: 文件名前缀。

    Returns:
        str: PDF 文件路径，失败返回 None。
    """
    if output_dir is None:
        from config.settings import PROJECT_ROOT
        output_dir = PROJECT_ROOT / "output"
    output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    pdf_path = str(output_dir / f"{filename_prefix}_{timestamp}.pdf")

    result = _launch_headless_chrome(port)
    if result is None:
        return None

    pw, browser, page = result
    try:
        page.pdf(
            path=pdf_path,
            landscape=False,
            print_background=True,
            format="A3",
            margin={"top": "0.4in", "bottom": "0.4in", "left": "0.4in", "right": "0.4in"},
        )
        return pdf_path
    except (OSError, RuntimeError, ValueError) as e:
        print(f"PDF导出失败: {e}")
        return None
    finally:
        browser.close()
        pw.stop()
