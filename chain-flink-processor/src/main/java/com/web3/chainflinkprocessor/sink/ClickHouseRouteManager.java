package com.web3.chainflinkprocessor.sink;

import com.web3.chainflinkprocessor.config.AppConfig;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * 负责与 ClickHouse 集群通信，动态获取分片拓扑，并维护所有 Shard 的物理连接
 */
@Slf4j
public class ClickHouseRouteManager implements Serializable {

    private final String insertSql;

    // 这些属于运行时的物理对象，用 transient 标记，防止 Flink 序列化报错
    private transient Connection[] connections;
    private transient PreparedStatement[] statements;
    private transient int[] batchCounts;
    private transient int shardCount;

    public ClickHouseRouteManager(String insertSql) {
        this.insertSql = insertSql;
    }

    /**
     * 在 Flink TaskManager 的 open() 阶段被触发调用
     */
    public void init() throws Exception {
        Class.forName(AppConfig.CH_DRIVER);
        log.info("🔍 RouteManager: 开始向协调节点拉取 [{}] 拓扑...", AppConfig.CH_CLUSTER_NAME);

        List<String> shardUrls = new ArrayList<>();

        // 1. 动态感知：获取集群分片信息
        try (Connection coordConn = DriverManager.getConnection(AppConfig.CH_COORDINATOR_URL, AppConfig.CH_USER, AppConfig.CH_PASSWORD);
             PreparedStatement coordStmt = coordConn.prepareStatement(
                     "SELECT host_address, port FROM system.clusters WHERE cluster = ? ORDER BY shard_num ASC")) {

            coordStmt.setString(1, AppConfig.CH_CLUSTER_NAME);
            try (ResultSet rs = coordStmt.executeQuery()) {
                while (rs.next()) {
                    String shardUrl = String.format("jdbc:clickhouse://%s:%d/web3_ods",
                            rs.getString("host_address"), rs.getInt("port"));
                    shardUrls.add(shardUrl);
                }
            }
        }

        if (shardUrls.isEmpty()) {
            log.warn("⚠️ RouteManager: 未查到集群拓扑，退化为本地节点模式。");
            shardUrls.add("jdbc:clickhouse://localhost:8123/web3_ods");
        }

        this.shardCount = shardUrls.size();
        this.connections = new Connection[shardCount];
        this.statements = new PreparedStatement[shardCount];
        this.batchCounts = new int[shardCount];

        // 2. 建立所有物理连接
        for (int i = 0; i < shardCount; i++) {
            connections[i] = DriverManager.getConnection(shardUrls.get(i), AppConfig.CH_USER, AppConfig.CH_PASSWORD);
            statements[i] = connections[i].prepareStatement(insertSql);
            batchCounts[i] = 0;
            log.info("✅ RouteManager: 专线连通 Shard [{}] -> {}", i, shardUrls.get(i));
        }
    }

    /**
     * 根据 Sharding Key 获取对应的 Statement 并累加批次
     */
    public PreparedStatement getStatementAndIncrement(long shardingKey) {
        int shardIndex = (int) (Math.abs(shardingKey) % shardCount);
        batchCounts[shardIndex]++;
        return statements[shardIndex];
    }

    /**
     * 检查某个 Shard 的批次是否已满，若满则执行写入
     */
    public void checkAndFlushIfFull(long shardingKey, int batchSize) throws Exception {
        int shardIndex = (int) (Math.abs(shardingKey) % shardCount);
        if (batchCounts[shardIndex] >= batchSize) {
            statements[shardIndex].executeBatch();
            batchCounts[shardIndex] = 0;
            log.debug("🚀 RouteManager: Shard [{}] 触发 Batch 写入。", shardIndex);
        }
    }

    /**
     * 供 Checkpoint 触发时的全局强制刷盘
     */
    public void flushAll() throws Exception {
        if (shardCount == 0 || statements == null) return;
        for (int i = 0; i < shardCount; i++) {
            if (batchCounts[i] > 0) {
                statements[i].executeBatch();
                batchCounts[i] = 0;
            }
        }
    }

    public void closeAll() throws Exception {
        if (shardCount == 0 || statements == null) return;
        for (int i = 0; i < shardCount; i++) {
            if (statements[i] != null) statements[i].close();
            if (connections[i] != null) connections[i].close();
        }
    }
}