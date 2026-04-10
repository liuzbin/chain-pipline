package com.web3.chainflinkprocessor.sink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.PreparedStatement;

public class ClickHouseShardingSink<T> extends RichSinkFunction<T> implements CheckpointedFunction {

    private final ClickHouseRouteManager routeManager;
    private final ShardingKeyExtractor<T> shardingKeyExtractor;
    private final StatementBinder<T> statementBinder;
    private final int batchSize;

    public ClickHouseShardingSink(ClickHouseRouteManager routeManager, int batchSize,
                                  ShardingKeyExtractor<T> shardingKeyExtractor,
                                  StatementBinder<T> statementBinder) {
        this.routeManager = routeManager;
        this.batchSize = batchSize;
        this.shardingKeyExtractor = shardingKeyExtractor;
        this.statementBinder = statementBinder;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 核心：在真正运行的节点上，唤醒管理器去初始化连接
        routeManager.init();
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        long shardingKey = shardingKeyExtractor.getShardingKey(value);

        // 向管理器索要对应分片的 Statement
        PreparedStatement stmt = routeManager.getStatementAndIncrement(shardingKey);

        // 绑定字段
        statementBinder.bind(stmt, value);
        stmt.addBatch();

        // 检查批次是否满载
        routeManager.checkAndFlushIfFull(shardingKey, batchSize);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // Checkpoint 时强行刷出所有暂存数据，保证 Exactly-Once
        routeManager.flushAll();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {}

    @Override
    public void close() throws Exception {
        routeManager.closeAll();
    }

    public interface ShardingKeyExtractor<T> extends java.io.Serializable {
        long getShardingKey(T element);
    }

    public interface StatementBinder<T> extends java.io.Serializable {
        void bind(PreparedStatement statement, T element) throws Exception;
    }
}