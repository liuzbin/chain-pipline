package com.web3.chainflinkprocessor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.web3.chainflinkprocessor.config.AppConfig;
import com.web3.chainflinkprocessor.entity.RawBlock;
import com.web3.chainflinkprocessor.entity.RawLog;
import com.web3.chainflinkprocessor.entity.RawTransaction;
import com.web3.chainflinkprocessor.sink.ClickHouseSinkFactory;
import com.web3.chainflinkprocessor.source.KafkaSourceBuilder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkDataPipelineJob {
    private static final Logger log = LoggerFactory.getLogger(FlinkDataPipelineJob.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 核心容错：每 10 秒强制触发一次 Checkpoint，底层 ShardingSink 会在此时将不满批次的数据强制 Flush 入库
        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);

        // 我们写了高度并发安全的组件，这里你可以大胆地把并行度开到 4 甚至更高
        env.setParallelism(4);

        ObjectMapper mapper = new ObjectMapper();

        // 1. Blocks 管道
        env.fromSource(KafkaSourceBuilder.build(AppConfig.TOPIC_BLOCKS), WatermarkStrategy.noWatermarks(), "Kafka-Blocks")
                .map(json -> mapper.readValue(json, RawBlock.class)).name("Parse-Blocks")
                .addSink(ClickHouseSinkFactory.buildBlockSink()).name("CH-Sharding-Sink-Blocks");

        // 2. Transactions 管道
        env.fromSource(KafkaSourceBuilder.build(AppConfig.TOPIC_TRANSACTIONS), WatermarkStrategy.noWatermarks(), "Kafka-Txs")
                .map(json -> mapper.readValue(json, RawTransaction.class)).name("Parse-Txs")
                .addSink(ClickHouseSinkFactory.buildTransactionSink()).name("CH-Sharding-Sink-Txs");

        // 3. Logs 管道
        env.fromSource(KafkaSourceBuilder.build(AppConfig.TOPIC_LOGS), WatermarkStrategy.noWatermarks(), "Kafka-Logs")
                .map(json -> mapper.readValue(json, RawLog.class)).name("Parse-Logs")
                .addSink(ClickHouseSinkFactory.buildLogSink()).name("CH-Sharding-Sink-Logs");

        log.info("🚀 Web3 分布式直写引擎准备就绪，点火发射...");
        env.execute("Web3 Chain Pipeline - Client Side Routing");
    }
}