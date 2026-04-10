package com.web3.chainflinkprocessor.config;

public class AppConfig {
    // ==========================================
    // Kafka 配置
    // ==========================================
    public static final String KAFKA_BROKERS = "localhost:9092";
    public static final String KAFKA_GROUP_ID = "flink_web3_ingestion_group";

    public static final String TOPIC_BLOCKS = "topic_raw_blocks";
    public static final String TOPIC_TRANSACTIONS = "topic_raw_transactions";
    public static final String TOPIC_LOGS = "topic_raw_logs";

    // ==========================================
    // ClickHouse 集群配置
    // ==========================================
    // 用于拉取分片元数据的协调节点 (随便选集群里的一台节点即可)
    public static final String CH_COORDINATOR_URL = "jdbc:clickhouse://localhost:8123/default";
    // 你的 ClickHouse 真实集群名称 (在 system.clusters 表中配置的名字)
    public static final String CH_CLUSTER_NAME = "web3_cluster";

    public static final String CH_USER = "default";
    public static final String CH_PASSWORD = "";
    public static final String CH_DRIVER = "com.clickhouse.jdbc.ClickHouseDriver";
}