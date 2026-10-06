# 已知问题与根因追踪

> 归档于 WorkBuddy 资料库「portfolio_tracker 项目知识库」

## 1. 中证2000（sh932000）行情缺失
- **背景**：9/22 新增为第 12 只基准指数（`INDEX_CODES`）
- **现象**：`index_quotes` 0 行（PE 已由 NeoData 回填 10 行）
- **根因（多源实测）**：新浪不覆盖 / 腾讯不跟踪 932000 / 东财 `push2his` 对本机 host 级 RST（经代理 503） / 网易 `chddata` 502 / NeoData 截断 + 窗口不确定（不可靠时序，曾误插 7 月陈旧数据已回滚）
- **修复（b4e625e）**：`_fetch_index_quotes` 捕获由 `except OSError` 改为 `except (DataSourceError, OSError)`，单指数失败仅告警跳过，整轮降级（degraded）而非 failed
- **落地条件**：需「对 eastmoney 放行的出网代理 / VPN」或腾讯云 Lighthouse 异地实例重跑 `backfill_single_index.py`

## 2. 沙箱出网 SMTP RST（WinError 10054）
- **根因**：沙箱出向 SMTP 被 RST（QQ 出口 IP 风控），系统性影响所有邮件任务
- **修复**：失败队列 + 每小时消费者补发（`send_generic_email.py --queue-on-fail`）；6 个邮件任务已铺开免疫
- **根治（长期）**：改走 API / 代理通道或绕开沙箱出网

## 3. 单指数取数失败拖垮整轮（已修复）
- **现象**：09-24 `run_status=failed`，`basic/risk/monitor/dq_check` 全缺，`critical / pipeline_incomplete`
- **根因**：`_fetch_index_quotes` `except OSError` 漏捕 `DataSourceError`（故障转移层包装），`sh932000` 四源失败上抛中断整轮
- **修复**：`b4e625e`，捕获 `(DataSourceError, OSError)`；新增回归测试 `test_fetch_index_quotes_isolation.py`
- **注意**：`dq_score=null` 是 run incomplete 导致，与 9/17 suppressed 抑制值语义不同；`critical` 分级逻辑未动

## 4. B1 代理透传（代码侧就绪）
- `base.py::resolve_env_proxies` 显式注入 env 代理 + 可观测日志
- **验证**：sina 取 510300 经 10808 代理成功（price=4.515），证明透传出网生效
- **限制**：本环境 10808 真实出网但对金融域（eastmoney K线 / 网易）上游策略限制，故 932000 本地仍缺
- **解锁路径**：真机生产环境配可用出网代理，或建腾讯云 Lighthouse 异地实例跑取数

## 5. 根因诊断纪律（本项目沉淀）
- 先排查根因再给修复，不盲目重试
- 单指数 / 单源失败必须优雅隔离，不得中断整轮
- 不可靠源（窗口不确定的搜索服务）不得作为时序回填源，误插数据必须立即回滚并留备份
