"""labels.py 未来函数修正自检（合成序列，手算可复核）。

验证两件事：
  A. fwd_vol_n[t] 的窗口恰好是 log_ret[t+1..t+n]，不含 t 及之前；
  B. is_up_n 在末段无未来数据时为 pd.NA，而不是 0。

构造：让过去与未来的收益量级差距极大（过去日收益恒为 +10%，未来恒为 -0.01%），
若窗口混入过去值，fwd_vol 会被过去的高波动污染成很大；真实未来窗口波动≈0。
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.analysis.predictor.labels import compute_forward_returns, compute_forward_volatility

N = 20
# 前 10 天：日收益 +10%（高波动区间之后）；后 10 天：日收益 -0.01%（几乎无波动）
r_past, r_future = 0.10, -0.0001
log_r = np.array([r_past] * 10 + [r_future] * 10)
close = pd.Series(100.0 * np.exp(np.concatenate([[0.0], np.cumsum(log_r)])),
                  index=pd.bdate_range("2024-01-01", periods=N + 1))
close = close.iloc[1:]  # 丢弃构造用的起点，保留 N 行

lr = np.log(close / close.shift(1))
print("log_ret =", np.round(lr.values, 5))

for n in (5, 20, 60):
    vol = compute_forward_volatility(close, windows=(n,))
    vol = compute_forward_volatility(close, windows=(n,))[f"fwd_vol_{n}"]
    ok = True
    for t in range(len(close) - n):
        expect = lr.values[t + 1:t + 1 + n].std(ddof=1)
        got = vol.iloc[t]
        if not np.isclose(got, expect, atol=1e-12):
            ok = False
            print(f"  [FAIL] n={n} t={t}: got={got} expect={expect}")
    # 未来窗口应全部为 NaN（不足 n 个未来值）
    tail_nan = bool(vol.iloc[len(close) - n:].isna().all())
    print(f"n={n}: 窗口[t+1..t+n]逐点复核 {'PASS' if ok else 'FAIL'}；"
          f"末 {n} 行全 NaN: {tail_nan}")
    assert ok and tail_nan, f"fwd_vol_{n} 窗口不正确"

# 关键判别：t=0 处未来 5 日全在"低波动未来区"，fwd_vol_5 应 ≈ 0（过去高波动被排除）
v5 = compute_forward_volatility(close, windows=(5,))["fwd_vol_5"]
print(f"fwd_vol_5[0] = {v5.iloc[0]:.8f} （过去区日收益 10%，若含过去应 >> 0）")
assert v5.iloc[0] < 1e-3, "fwd_vol_5 仍混入过去收益"
# 反例校验：旧写法把 t+1 的未来值和 t-3..t 的过去值混在一起
old = lr.shift(-1).rolling(5).std()
t_ref = 9  # 旧写法窗口 = log_ret[6..10]（4 个过去 10% + 1 个未来）；新写法 = log_ret[10..14]（纯未来）
print(f"（对照）t={t_ref}: 旧写法 = {old.iloc[t_ref]:.6f}（含过去 10% 收益，虚高）；"
      f"新写法 = {v5.iloc[t_ref]:.8f}（纯未来窗口）")

# ---- B. is_up 缺失值 ----
ret = compute_forward_returns(close, windows=(5, 20, 60))
for n in (5, 20, 60):
    col = f"is_up_{n}"
    fwd = ret[f"fwd_ret_{n}"]
    tail = ret.loc[fwd.isna(), col]
    print(f"{col}: 缺失标签 {int(fwd.isna().sum())} 行，其中 is_up 为 pd.NA 的"
          f" {int(tail.isna().sum())} 行，被误写 0 的 {int((tail == 0).sum())} 行")
    assert tail.isna().all(), f"{col} 末段存在被误写为 0 的伪标签"
    assert ret.loc[fwd.notna(), col].notna().all()

print("\n全部自检通过：fwd_vol 窗口干净为 [t+1..t+n]，is_up 末段保持 pd.NA。")
