"""浏览器截图与 PDF 导出工具

基于 Selenium headless Chrome，提供 Streamlit Dashboard 的
截图（PNG）和 PDF 导出能力。依赖 selenium + webdriver_manager，
缺失时优雅降级。

用法::

    from src.utils.screenshot import capture_screenshot, export_pdf

    png_path = capture_screenshot(port=8501)
    pdf_path = export_pdf(port=8501)
"""

from pathlib import Path
from datetime import datetime


def _launch_headless_chrome(port=8501):
    """启动 headless Chrome 并导航到 Streamlit 应用。

    Returns:
        webdriver.Chrome 实例，失败返回 None。
    """
    try:
        import time
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        from selenium.webdriver.common.by import By
        from webdriver_manager.chrome import ChromeDriverManager
    except ImportError:
        print("浏览器驱动缺失: 请执行 pip install selenium webdriver-manager")
        return None

    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1920,3000")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.get(f"http://localhost:{port}")

    # Step 1: 等待 Streamlit App 容器就绪
    from selenium.common.exceptions import WebDriverException
    for _ in range(30):
        try:
            el = driver.find_element(By.CSS_SELECTOR, "[data-testid='stApp']")
            if el.is_displayed():
                break
        except WebDriverException:
            pass
        time.sleep(1)

    # Step 2: 等待 Plotly 图表渲染（至少 2 个 SVG 出现）
    for _ in range(45):
        try:
            charts = driver.find_elements(By.CSS_SELECTOR, ".js-plotly-plot .main-svg")
            if len(charts) >= 2:
                time.sleep(2)
                break
        except WebDriverException:
            pass
        time.sleep(1)

    # Step 3: 滚动到底部触发懒加载，再滚回顶部
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(2)
    driver.execute_script("window.scrollTo(0, 0);")
    time.sleep(1)

    return driver


def capture_screenshot(port=8501, output_dir=None, filename_prefix="dashboard"):
    """截取 Dashboard 页面截图（PNG）。

    Args:
        port: Streamlit 端口号。
        output_dir: 输出目录，默认为 PROJECT_ROOT/output。
        filename_prefix: 文件名前缀。

    Returns:
        str: PNG 文件路径，失败返回 None。
    """
    from selenium.common.exceptions import WebDriverException

    if output_dir is None:
        from config.settings import PROJECT_ROOT
        output_dir = PROJECT_ROOT / "output"
    output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    png_path = str(output_dir / f"{filename_prefix}_{timestamp}.png")

    driver = _launch_headless_chrome(port)
    if driver is None:
        return None

    try:
        driver.save_screenshot(png_path)
        return png_path
    except WebDriverException as e:
        print(f"截图失败: {e}")
        return None
    finally:
        driver.quit()


def export_pdf(port=8501, output_dir=None, filename_prefix="dashboard"):
    """导出 Dashboard 页面为 PDF（A3 宽幅）。

    Args:
        port: Streamlit 端口号。
        output_dir: 输出目录，默认为 PROJECT_ROOT/output。
        filename_prefix: 文件名前缀。

    Returns:
        str: PDF 文件路径，失败返回 None。
    """
    import base64
    from selenium.common.exceptions import WebDriverException

    if output_dir is None:
        from config.settings import PROJECT_ROOT
        output_dir = PROJECT_ROOT / "output"
    output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    pdf_path = str(output_dir / f"{filename_prefix}_{timestamp}.pdf")

    driver = _launch_headless_chrome(port)
    if driver is None:
        return None

    try:
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
        return pdf_path
    except WebDriverException as e:
        print(f"PDF导出失败: {e}")
        return None
    finally:
        driver.quit()
