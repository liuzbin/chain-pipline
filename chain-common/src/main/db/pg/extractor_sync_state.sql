-- 1. 创建专门存放抽取器元数据的 Schema (如果不存在则创建)
CREATE SCHEMA IF NOT EXISTS chain_extractor;

-- 2. 在指定 Schema 下创建水位线表
CREATE TABLE chain_extractor.extractor_sync_state (
    task_name VARCHAR(50) PRIMARY KEY,
    last_processed_block BIGINT NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. 初始化以太坊和 BSC 的水位线 (放入新 Schema)
-- 注意：这里我顺手把 bsc 也加上了，配合我们刚才写的 Java 定时任务
INSERT INTO chain_extractor.extractor_sync_state (task_name, last_processed_block)
VALUES
    ('ethereum', 10000),
    ('bsc', 10000);