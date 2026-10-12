# westock_mcp 限频下的改源评估（2026-10-09）

## 背景
维护 9 只行业 ETF 的 `westock_mcp` 日频 OHLCV 兜底下，本会话 `westock-mcp` 已连通（与历史 14 次失败不同），但取数触发硬性配额限频（「服务限频」）。步骤 1 仅 3/9 成功（159267 / 159770 / 159796，最新 2026-10-09）；其余 6 只（159819 / 159949 / 159992 / 515010 / 515120 / 561910）被限频。

本评估考察已连接的 **neodata** 与 **tdx-connector** 能否等价构造这 6 只的 `_latest` 缓存，作为 westock 配额未恢复时的备用方案。

## 实测（以 159819 人工智能ETF易方达 为例，交易日 2026-10-09）

| 源 | 返回结构 | 关键字段 | 成交量单位 | 收盘字段名 |
|----|---------|---------|-----------|-----------|
| westock `data_kline` | `data.nodes[]` | date / open / **last** / high / low / volume / amount | 系统按 `etf_price_history.volume` 的「手」处理 | `last` |
| neodata `quote_and_kline` | `kline_list[]` | trade_date / open / high / low / **close** / volume / amount | **股 (shares)** | `close` |
| tdx `tdx_kline` | `Rows[]` | Data / Open / High / Low / **Close** / Volume / Amount | **手 (lots)**（Unit=100，Rows.Volume 已除权） | `Close` |

数值对照（2026-10-09）：
- OHLC 三者完全一致：open 1.588 / high 1.611 / low 1.533 / close 1.605。
- 成交量互证：neodata `volume=237105300`（股）；tdx `Volume=2371052.64`（手，RawVolume=237105264 ÷ 100）；两者 ×100 一致。

## 结论
1. **数据可得性 ✅**：neodata 与 tdx 均能返回这 6 只的日频 OHLCV；字段映射直接（date / open / close→last / high / low / volume / amount）。
2. **成交量单位（数据完整性铁律关注点）**：
   - **tdx 返回「手」，与 `etf_price_history.volume` 列单位直接一致 → 无需换算，最干净。**
   - neodata 返回「股」，需 ÷100 才能对齐「手」单位；若漏换算，量级错 100 倍（静默失真）。
3. **source 标识（改源唯一实质性障碍）**：回填脚本 `backfill_etf_price_history_westock.py` 将行写死为 `source='westock_mcp'`；`price_history.py` 兜底只查 `source='westock_mcp'`。若用他源数据填 `_latest`：
   - 方案 (a) 仍标 `westock_mcp`：数据来源 provenance 失真，但兜底无需改代码即可生效 → **不推荐**（造假）。
   - 方案 (b) 标新源（如 `tdx` / `neodata`）并改 `price_history.py` 兜底同时查新源 → 正确，但需动代码 + 重新验证契约。
4. **建议**：优先等 westock 配额重置后走原路径（source 标识正确、零代码改动）；**tdx 已验证为体积单位最匹配的备用源**，若 westock 长期不可用，采用方案 (b) 并同步改 `price_history.py`。

## 风险提示
- 改源触碰数据完整性铁律（volume 单位绝不可混淆）：用 neodata 必须 ÷100，用 tdx 直接可用。
- 勿覆盖 `etf_westock_<代码>.json` 种子；`_latest` 为实时缓存区。
- 当前主分析链（东财 em + 新浪 tx）健康，`westock_mcp` 兜底处于休眠、低风险态；这 6 只缺口仅在 em/tx 再次 RST 时暴露为陈旧 OHLCV。

## 当前缓存状态
- 已落盘 `_latest`（最新 2026-10-09）：159267、159770、159796。
- 待补齐（被限频）：159819、159949、159992、515010、515120、561910。
