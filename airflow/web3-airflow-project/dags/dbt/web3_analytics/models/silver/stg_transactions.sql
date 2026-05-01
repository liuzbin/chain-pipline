-- 1. 这是 dbt 的魔法配置 (Jinja 语法)
-- 我们告诉 dbt：请把这段 SQL 的结果，在 Snowflake 里物理落地成一张表(Table)
{{ config(
    materialized='table'
) }}

-- 2. 这就是你之前在 Snowflake 里写的查询语句
-- dbt 会负责把它包裹成 CREATE TABLE ... AS 语句发送给引擎
WITH raw_stage_data AS (
    SELECT
        $1:chainName::STRING AS chain_name,
        $1:blockNumber::NUMBER AS block_number,
        $1:from::STRING AS from_address,
        $1:to::STRING AS to_address,
        $1:value::STRING AS tx_value,
        $1:dt::STRING AS partition_date,
        METADATA$FILENAME AS s3_file_path
    FROM @WEB3_LAKEHOUSE.PUBLIC.bronze_s3_stage/transactions/
    (FILE_FORMAT => 'my_parquet_format', PATTERN => '.*.parquet')
)

SELECT * FROM raw_stage_data
-- 这里还可以加上各种清洗逻辑，比如过滤掉没有 to_address 的异常交易
WHERE to_address IS NOT NULL