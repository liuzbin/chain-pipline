package com.web3.spark;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

public class MdsIngestionJob {

    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        // 1. 初始化 Spark Session 并强制关闭 UI 避免依赖冲突
        SparkSession spark = SparkSession.builder()
                .appName("Kafka-To-Delta-MultiStream-Ingestion")
                .master("local[*]") // 使用所有可用核心，因为我们要跑 3 个并发流
                .config("spark.ui.enabled", "false")
                // 启用 Delta Lake 扩展
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                // 配置 MinIO S3
                .config("spark.hadoop.fs.s3a.access.key", "admin")
                .config("spark.hadoop.fs.s3a.secret.key", "password123")
                .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9005")
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .getOrCreate();

        // ==========================================
        // 2. 定义三张核心表的 JSON Schema
        // ==========================================
        StructType blockSchema = new StructType()
                .add("chainName", DataTypes.StringType)
                .add("blockNumber", DataTypes.LongType)
                .add("blockHash", DataTypes.StringType)
                .add("parentHash", DataTypes.StringType)
                .add("timestamp", DataTypes.LongType)
                .add("miner", DataTypes.StringType)
                .add("baseFeePerGas", DataTypes.LongType)
                .add("ingestionTimestamp", DataTypes.LongType);

        StructType txSchema = new StructType()
                .add("chainName", DataTypes.StringType)
                .add("txHash", DataTypes.StringType)
                .add("blockNumber", DataTypes.LongType)
                .add("from", DataTypes.StringType)
                .add("to", DataTypes.StringType)
                .add("value", DataTypes.StringType) // 保持 String 防精度丢失
                .add("timestamp", DataTypes.LongType)
                .add("ingestionTimestamp", DataTypes.LongType);

        StructType logSchema = new StructType()
                .add("chainName", DataTypes.StringType)
                .add("logIndex", DataTypes.LongType)
                .add("blockNumber", DataTypes.LongType)
                .add("blockHash", DataTypes.StringType)
                .add("txHash", DataTypes.StringType)
                .add("address", DataTypes.StringType)
                .add("topic0", DataTypes.StringType)
                .add("topic1", DataTypes.StringType)
                .add("topic2", DataTypes.StringType)
                .add("topic3", DataTypes.StringType)
                .add("data", DataTypes.StringType)
                .add("timestamp", DataTypes.LongType)
                .add("ingestionTimestamp", DataTypes.LongType);

        // ==========================================
        // 3. 并发启动三个数据湖摄入流
        // ==========================================
        System.out.println("🚀 正在启动 Blocks 数据流...");
        startIngestionStream(spark, "topic_raw_blocks", blockSchema, "blocks");

        System.out.println("🚀 正在启动 Transactions 数据流...");
        startIngestionStream(spark, "topic_raw_transactions", txSchema, "transactions");

        System.out.println("🚀 正在启动 Logs 数据流...");
        startIngestionStream(spark, "topic_raw_logs", logSchema, "logs");

        // 4. 阻塞主线程，等待任意一个流被终止（保持程序持续运行）
        // 注意：多流场景下不能对单个流 awaitTermination，必须用全局的 awaitAnyTermination
        System.out.println("✅ 所有数据流启动完毕，正在持续监控并写入 MinIO...");
        spark.streams().awaitAnyTermination();
    }

    /**
     * 核心写入逻辑抽象：将重复的 Kafka 读取、JSON 解析、Delta 写入过程封装
     */
    private static void startIngestionStream(SparkSession spark, String topic, StructType schema, String tableName) throws TimeoutException {
        spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", topic)
                .option("startingOffsets", "earliest")
                .load()
                // 1. 解析 Kafka 的 value 字节数组为 String
                .selectExpr("CAST(value AS STRING) as json_str")
                // 2. 根据传入的 Schema 解析 JSON
                .select(from_json(col("json_str"), schema).alias("data"))
                .select("data.*")
                // 3. 增加动态分区字段 dt (Date)
                .withColumn("dt", from_unixtime(col("timestamp"), "yyyy-MM-dd"))
                // 4. 写入 Delta 湖床
                .writeStream()
                .format("delta")
                .outputMode("append")
                .partitionBy("chainName", "dt")
                // ⚠️ 极其关键：不同流的 Checkpoint 目录必须严格物理隔离
                .option("checkpointLocation", "s3a://web3-datalake/checkpoints/" + tableName)
                .option("path", "s3a://web3-datalake/bronze/" + tableName)
                .trigger(Trigger.ProcessingTime("10 seconds"))
                .start(); // start() 是非阻塞的，会立刻返回
    }
}