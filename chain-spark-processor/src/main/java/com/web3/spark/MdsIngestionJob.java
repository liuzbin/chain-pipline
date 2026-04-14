package com.web3.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

public class MdsIngestionJob {

    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        // 1. 初始化 Spark Session 并配置 Delta & MinIO(S3)
        SparkSession spark = SparkSession.builder()
                .appName("Kafka-To-Delta-Ingestion")
                .master("local[2]") // 本地双核运行
                // 启用 Delta Lake 扩展
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                // 配置 MinIO S3 凭据与端点
                .config("spark.hadoop.fs.s3a.access.key", "admin")
                .config("spark.hadoop.fs.s3a.secret.key", "password123")
                .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9005")
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .getOrCreate();

        // 2. 定义从 Kafka 读出的 JSON Schema (以 Transaction 为例)
        StructType txSchema = new StructType()
                .add("chainName", DataTypes.StringType)
                .add("txHash", DataTypes.StringType)
                .add("blockNumber", DataTypes.LongType)
                .add("from", DataTypes.StringType)
                .add("to", DataTypes.StringType)
                .add("value", DataTypes.StringType) // 保持 String 防精度丢失
                .add("timestamp", DataTypes.LongType)
                .add("ingestionTimestamp", DataTypes.LongType);

        // 3. 读取 Kafka 流
        Dataset<Row> kafkaStream = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "topic_raw_transactions")
                .option("startingOffsets", "earliest") // 从头开始消费
                .load();

        // 4. 解析 JSON 并提取核心字段
        Dataset<Row> parsedStream = kafkaStream
                .selectExpr("CAST(value AS STRING) as json_str")
                .select(from_json(col("json_str"), txSchema).alias("data"))
                .select("data.*")
                // 增加分区字段，按天存储
                .withColumn("dt", from_unixtime(col("timestamp"), "yyyy-MM-dd"));

        // 5. 写入 MinIO (Delta 格式)
        parsedStream.writeStream()
                .format("delta")
                .outputMode("append")
                // 按照链名和日期进行物理分区，极大提升后续 OLAP 查询效率
                .partitionBy("chainName", "dt")
                .option("checkpointLocation", "s3a://web3-datalake/checkpoints/transactions")
                .option("path", "s3a://web3-datalake/bronze/transactions")
                // 模拟微批处理，每 10 秒提交一次，避免产生过多碎文件
                .trigger(Trigger.ProcessingTime("10 seconds"))
                .start()
                .awaitTermination();
    }
}