# chain-ingestion-gateway区块链数据摄入网关
核心业务：
## 1. 多链异构与高可用摄入引擎 (Multi-Chain & HA Ingestion)
多链并发：不再只写死一个以太坊 URL。通过 application.yml 的 List 配置，通过 Spring Boot 动态拉起多个线程池，同时监听 Ethereum, BSC, Polygon。

RPC 节点灾备 (Failover)：配置主备节点（如 Infura 挂了自动切 Alchemy），确保底层 WebSocket 连接的绝对高可用。

## 2. 全局高水位线状态机 (HWM State Machine)
状态持久化：基于 PostgreSQL 的断点续传机制，确保重启绝对不漏数。

状态管理 API：暴露 Spring Boot REST API（如 GET /api/v1/sync-state 或 POST /api/v1/sync-state/reset），允许你在排查线上问题时，通过接口手动回拨或重置某条链的抓取高度。

## 3. 可观测性与健康监控 (Observability & Metrics)
指标暴露：集成 Prometheus + Micrometer (spring-boot-starter-actuator)。

核心监控项：向外部暴露出极其关键的业务指标，例如：

web3_rpc_call_latency (节点响应延迟)

kafka_push_throughput (每秒写入 Kafka 的 TPS)

chain_sync_lag (当前抓取高度与链上真实最新高度的差值，Lag 越大说明系统卡顿越严重)。

## 4. 旁路数据对账调度 (Reconciliation Scheduler)
数据完整性巡检：引入定时任务框架。每天凌晨触发一次对账逻辑，对比 PostgreSQL 里记录的理论区块数，与下游最终落盘的数据量是否一致。如果发现 Gap，主动生成告警日志。