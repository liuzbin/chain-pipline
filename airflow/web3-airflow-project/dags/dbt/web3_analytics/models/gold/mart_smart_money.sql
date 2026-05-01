{{ config(
    materialized='table'
) }}

-- ⚠️ 注意这里的 ref 魔法！我们不再写具体的底层表名了
WITH transactions AS (
    SELECT * FROM {{ ref('stg_transactions') }}
)

SELECT
    from_address AS whale_wallet,
    COUNT(*) AS total_tx_count,
    -- 简化的聚合逻辑
    MAX(partition_date) AS last_active_date
FROM transactions
GROUP BY from_address
HAVING COUNT(*) > 5  -- 比如我们定义交易大于5次的为巨鲸
ORDER BY total_tx_count DESC