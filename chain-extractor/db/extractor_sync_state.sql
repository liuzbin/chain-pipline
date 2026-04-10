-- Create the synchronization state table
CREATE TABLE extractor_sync_state (
    task_name VARCHAR(50) PRIMARY KEY,
    last_processed_block BIGINT NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Initialize the High Watermark for the Ethereum mainnet task
-- Note: Replace 0 with the actual block number you want to start syncing from (e.g., 19000000)
INSERT INTO extractor_sync_state (task_name, last_processed_block)
VALUES ('ethereum_mainnet', 0);