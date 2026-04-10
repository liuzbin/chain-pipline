CREATE DATABASE chain_pipeline;

-- 1. 创建独立的 Schema (逻辑隔离区)
CREATE SCHEMA IF NOT EXISTS gateway;

-- 2. 在 gateway schema 下创建高水位线状态表
CREATE TABLE IF NOT EXISTS gateway.extractor_sync_state (
    task_name VARCHAR(50) PRIMARY KEY,
    last_processed_block BIGINT NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. 创建自动更新时间的触发器函数 (绑定到 gateway 命名空间)
CREATE OR REPLACE FUNCTION gateway.update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- 4. 挂载触发器
CREATE TRIGGER update_extractor_sync_state_modtime
BEFORE UPDATE ON gateway.extractor_sync_state
FOR EACH ROW
EXECUTE FUNCTION gateway.update_modified_column();

-- 5. 注入初始任务配置 (请把 22000000 替换为你当前想抓取的最新高度减去 10)
INSERT INTO gateway.extractor_sync_state (task_name, last_processed_block)
VALUES ('ethereum_mainnet', 22000000);
INSERT INTO gateway.extractor_sync_state (task_name, last_processed_block)
VALUES
('bsc_mainnet', 37000000),
('polygon_mainnet', 54000000);