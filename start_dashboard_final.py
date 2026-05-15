import subprocess
import sys
import os
import time

project_dir = r"C:\Users\HUAWEI\Documents\lingxi-claw\portfolio_tracker"
venv_dir = os.path.join(project_dir, "venv")
streamlit_exe = os.path.join(venv_dir, "Scripts", "streamlit.exe")
script_path = os.path.join(project_dir, "dashboard_main.py")

print("启动投资组合跟踪系统...")
print(f"端口: 8501")
cmd = [streamlit_exe, "run", script_path, "--server.port", "8501", "--server.headless", "true"]
proc = subprocess.Popen(cmd, cwd=project_dir)
print(f"进程已启动，PID: {proc.pid}")
print(f"请访问 http://localhost:8501")
print("按 Ctrl+C 停止。")
try:
    # 保持脚本运行，直到用户中断
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("\n停止应用...")
    proc.terminate()
