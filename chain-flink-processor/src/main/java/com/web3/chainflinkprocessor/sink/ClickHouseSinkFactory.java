package com.web3.chainflinkprocessor.sink;

import com.web3.chainflinkprocessor.entity.RawBlock;
import com.web3.chainflinkprocessor.entity.RawLog;
import com.web3.chainflinkprocessor.entity.RawTransaction;

public class ClickHouseSinkFactory {

    // 生产环境建议设为 10000 甚至更大
    private static final int BATCH_SIZE = 1000;

    public static ClickHouseShardingSink<RawBlock> buildBlockSink() {
        // 注意：这里写入的是底层带有 ON CLUSTER 的本地表 raw_blocks_local
        String sql = "INSERT INTO web3_ods.raw_blocks_local (block_number, block_hash, parent_hash, timestamp, miner, base_fee_per_gas) VALUES (?, ?, ?, ?, ?, ?)";
        ClickHouseRouteManager routeManager = new ClickHouseRouteManager(sql);
        return new ClickHouseShardingSink<>(routeManager, BATCH_SIZE,
                RawBlock::getBlockNumber, // 极其核心：以 blockNumber 为 Hash 分片键
                (stmt, block) -> {
                    stmt.setLong(1, block.getBlockNumber());
                    stmt.setString(2, block.getBlockHash());
                    stmt.setString(3, block.getParentHash());
                    stmt.setLong(4, block.getTimestamp());
                    stmt.setString(5, block.getMiner());
                    stmt.setLong(6, block.getBaseFeePerGas());
                });
    }

    public static ClickHouseShardingSink<RawTransaction> buildTransactionSink() {
        String sql = "INSERT INTO web3_ods.raw_transactions_local (tx_hash, block_number, block_hash, from_address, to_address, value, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)";
        ClickHouseRouteManager routeManager = new ClickHouseRouteManager(sql);
        return new ClickHouseShardingSink<>(routeManager, BATCH_SIZE,
                RawTransaction::getBlockNumber, // 子表同样以 blockNumber 分片，保证 Join 的数据在同一台物理机上
                (stmt, tx) -> {
                    stmt.setString(1, tx.getTxHash());
                    stmt.setLong(2, tx.getBlockNumber());
                    stmt.setString(3, tx.getBlockHash());
                    stmt.setString(4, tx.getFrom());
                    stmt.setString(5, tx.getTo());
                    stmt.setString(6, tx.getValue() != null ? tx.getValue().toPlainString() : "0");
                    stmt.setLong(7, tx.getTimestamp());
                });
    }

    public static ClickHouseShardingSink<RawLog> buildLogSink() {
        String sql = "INSERT INTO web3_ods.raw_logs_local (log_id, block_number, block_hash, tx_hash, address, topic0, topic1, topic2, topic3, data, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        ClickHouseRouteManager routeManager = new ClickHouseRouteManager(sql);
        return new ClickHouseShardingSink<>(routeManager, BATCH_SIZE,
                RawLog::getBlockNumber, // 同理，锁定物理节点
                (stmt, log) -> {
                    stmt.setString(1, log.getLogId());
                    stmt.setLong(2, log.getBlockNumber());
                    stmt.setString(3, log.getBlockHash());
                    stmt.setString(4, log.getTxHash());
                    stmt.setString(5, log.getAddress());
                    stmt.setString(6, log.getTopic0());
                    stmt.setString(7, log.getTopic1());
                    stmt.setString(8, log.getTopic2());
                    stmt.setString(9, log.getTopic3());
                    stmt.setString(10, log.getData());
                    stmt.setLong(11, log.getTimestamp());
                });
    }
}