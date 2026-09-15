"""改动验证：Tab16 / risk_report 改为纯历史统计后的冒烟测试。

1) risk_report.get_risk_outlook 能产出 ok=True 的纯历史统计（不读 etf_predictions）。
2) 生成的 HTML 不含"预测/LightGBM/回测"字样，且明确写"不含模型预测"。
3) Tab16 的 _build_window_df 直接吃 etf_features 产出分档 + 趋势 + 分位。
4) 确认 etf_predictions 里 model='risk_lgb' 的 44 行历史数据仍在（未删）。
"""
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config.settings import DATABASE_PATH
from src.utils.risk_report import get_risk_outlook, build_risk_outlook_html


def main():
    conn = sqlite3.connect(str(DATABASE_PATH))

    out = get_risk_outlook(conn, str(DATABASE_PATH))
    assert out["ok"], f"get_risk_outlook 失败: {out.get('note')}"
    print("stat_date:", out["stat_date"])
    for w in (20, 60):
        recs = out["windows"].get(w, [])
        print(f"  窗口 {w}: {len(recs)} 只, 高/中/低 = "
              f"{out['high_count' if w == 20 else 'high_count']}/"
              f"{out['mid_count']}/{out['low_count']}")
        for r in recs[:3]:
            print("    ", r)
    print("port_vol:", out["port_vol"], "hi_w:", out["hi_w"],
          "high_list:", out["high_list"])
    print("hist_dd 只数:", len(out["hist_dd"]))

    html = build_risk_outlook_html(out, theme="dark")
    for bad in ("LightGBM", "回测(样本外)", "AUC"):
        assert bad not in html, f"HTML 仍含已下线的模型字样: {bad}"
    assert "不含模型预测" in html, "HTML 缺少'不含模型预测'声明"
    print(f"\nHTML 长度 {len(html)}；已确认无 LightGBM/回测/AUC 字样，且含'不含模型预测'声明。")

    light = build_risk_outlook_html(out, theme="light")
    assert "不含模型预测" in light

    # Tab16 数据构造（不启动 streamlit，直接调纯函数）
    sys.path.insert(0, str(ROOT / "tabs"))
    from tabs.tab16_risk_outlook import _build_window_df  # noqa: E402
    latest_date = conn.execute("SELECT MAX(date) FROM etf_features").fetchone()[0]
    import pandas as pd
    hist_latest = pd.read_sql_query(
        "SELECT date, code, vol_20d, vol_60d, vol_5d FROM etf_features WHERE date=?",
        conn, params=[latest_date])
    nm = {c: (n or c) for c, n in conn.execute(
        "SELECT DISTINCT code, name FROM portfolio_snapshots WHERE date=(SELECT MAX(date) "
        "FROM portfolio_snapshots)").fetchall()}
    for w in (20, 60):
        df = _build_window_df(hist_latest, w, nm)
        assert not df.empty and {"vol_ann", "ref_vol_ann", "trend", "cls",
                                 "pct_rank"}.issubset(df.columns)
        print(f"Tab16 w={w}: {len(df)} 行；分档={df['cls'].value_counts().to_dict()}；"
              f"趋势={df['trend'].value_counts().to_dict()}")
        print("   样例:", df[["name", "vol_ann", "ref_vol_ann", "trend", "pct_rank", "cls"]]
              .head(3).to_dict("records"))

    n = conn.execute(
        "SELECT COUNT(*) FROM etf_predictions WHERE model='risk_lgb'").fetchone()[0]
    print(f"\n留档校验: etf_predictions model='risk_lgb' 仍有 {n} 行（未删）")
    assert n == 44, f"risk_lgb 历史行数异常: {n}"
    conn.close()
    print("\n全部验证通过。")


if __name__ == "__main__":
    main()
