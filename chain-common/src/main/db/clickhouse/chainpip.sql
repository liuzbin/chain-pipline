-- 1. 创建 Database (等同于 Schema)
-- ON CLUSTER 命令会自动在两个节点上同时建库
CREATE DATABASE IF NOT EXISTS chainpip ON CLUSTER web3_cluster;

-- ==========================================
-- 2. Blocks 相关表 (区块表)
-- ==========================================
-- 2.1 本地物理表 (Flink 直接写入这个表)
CREATE TABLE IF NOT EXISTS chainpip.raw_blocks_local ON CLUSTER web3_cluster (
    chain_name String,
    block_number UInt64,
    block_hash String,
    parent_hash String,
    timestamp UInt64,
    miner String,
    base_fee_per_gas UInt64,
    ingestion_timestamp UInt64
) ENGINE = MergeTree()
PARTITION BY chain_name  -- 既然加了多链字段，按链分区是最佳实践
ORDER BY (block_number);

-- 2.2 全局分布式表 (前端和 BI 查询用这个表)
CREATE TABLE IF NOT EXISTS chainpip.raw_blocks_all ON CLUSTER web3_cluster
AS chainpip.raw_blocks_local
ENGINE = Distributed(web3_cluster, chainpip, raw_blocks_local, block_number);


-- ==========================================
-- 3. Transactions 相关表 (交易表)
-- ==========================================
-- 3.1 本地物理表
CREATE TABLE IF NOT EXISTS chainpip.raw_transactions_local ON CLUSTER web3_cluster (
    chain_name String,
    tx_hash String,
    block_number UInt64,
    block_hash String,
    from_address String,
    to_address String,
    value String,           -- 已经按你的最新代码改成了 String 防止精度丢失
    timestamp UInt64,
    ingestion_timestamp UInt64
) ENGINE = MergeTree()
PARTITION BY chain_name
ORDER BY (block_number, tx_hash);

-- 3.2 全局分布式表
CREATE TABLE IF NOT EXISTS chainpip.raw_transactions_all ON CLUSTER web3_cluster
AS chainpip.raw_transactions_local
ENGINE = Distributed(web3_cluster, chainpip, raw_transactions_local, block_number);


-- ==========================================
-- 4. Logs 相关表 (事件日志表)
-- ==========================================
-- 4.1 本地物理表
CREATE TABLE IF NOT EXISTS chainpip.raw_logs_local ON CLUSTER web3_cluster (
    chain_name String,
    log_index UInt64,       -- 已经修改为 log_index
    block_number UInt64,
    block_hash String,
    tx_hash String,
    address String,
    topic0 String,
    topic1 String,
    topic2 String,
    topic3 String,
    data String,
    timestamp UInt64,
    ingestion_timestamp UInt64
) ENGINE = MergeTree()
PARTITION BY chain_name
ORDER BY (block_number, tx_hash, log_index);

-- 4.2 全局分布式表
CREATE TABLE IF NOT EXISTS chainpip.raw_logs_all ON CLUSTER web3_cluster
AS chainpip.raw_logs_local
ENGINE = Distributed(web3_cluster, chainpip, raw_logs_local, block_number);