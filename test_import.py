# -*- coding: utf-8 -*-
import sys
sys.path.insert(0, r"C:\Users\HUAWEI\Documents\lingxi-claw\portfolio_tracker")
try:
    from config.settings import DATABASE_PATH, INDEX_CODES
    print("OK - settings imports work")
except Exception as e:
    print(f"FAIL settings: {e}")

try:
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    print("OK - plotly imports work")
except Exception as e:
    print(f"FAIL plotly: {e}")

try:
    import streamlit
    print(f"OK - streamlit {streamlit.__version__}")
except Exception as e:
    print(f"FAIL streamlit: {e}")
