package com.web3.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class MdsQueryJob {

    public static void main(String[] args) {
        // 1. 初始化 Spark Session (和写入时的配置保持一致，但不需要流处理配置)
        SparkSession spark = SparkSession.builder()
                .appName("Delta-Lake-Query")
                .master("local[*]")
                .config("spark.ui.enabled", "false") // 本地查询依然关掉 UI 避免干扰
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

        // 将日志级别调高，避免 Spark 底层的 INFO 日志淹没我们的查询结果
        spark.sparkContext().setLogLevel("WARN");

        System.out.println("🚀 正在连接 MinIO 加载 Delta 湖床数据...");

        // ==========================================
        // 2. 将 S3 目录加载为 Spark DataFrame 并注册为临时表
        // ==========================================
        // 加载 Transactions
        Dataset<Row> txDf = spark.read().format("delta").load("s3a://web3-datalake/bronze/transactions");
        txDf.createOrReplaceTempView("transactions");

        // 加载 Blocks
        Dataset<Row> blockDf = spark.read().format("delta").load("s3a://web3-datalake/bronze/blocks");
        blockDf.createOrReplaceTempView("blocks");

        // ==========================================
        // 3. 尽情使用 Spark SQL 进行 OLAP 查询
        // ==========================================

        System.out.println("\n========== 📊 场景 1: 查询最新的 5 条交易记录 ==========");
        spark.sql("SELECT chainName, blockNumber, txHash, `from`, `to`, value " +
                "FROM transactions " +
                "ORDER BY timestamp DESC LIMIT 5").show(false); // false 表示不截断字符串

        System.out.println("\n========== 📊 场景 2: 统计每条链、每天的交易笔数 ==========");
        spark.sql("SELECT chainName, dt, COUNT(*) as tx_count " +
                "FROM transactions " +
                "GROUP BY chainName, dt " +
                "ORDER BY dt DESC, tx_count DESC").show();

        System.out.println("\n========== 📊 场景 3: 查询当前已经同步的最高区块 ==========");
        spark.sql("SELECT chainName, MAX(blockNumber) as latest_block " +
                "FROM blocks " +
                "GROUP BY chainName").show();

        System.out.println("\n========== 📊 场景 4: 关联查询 (找出区块高度为某值的交易) ==========");
        // 注意：这里由于是测试，可能会没有恰好关联上的数据，如果在生产环境中，这就是极其强大的 Join 能力
        spark.sql("SELECT b.blockNumber, b.miner, COUNT(t.txHash) as tx_count " +
                "FROM blocks b " +
                "LEFT JOIN transactions t ON b.blockNumber = t.blockNumber AND b.chainName = t.chainName " +
                "GROUP BY b.blockNumber, b.miner " +
                "ORDER BY b.blockNumber DESC LIMIT 5").show(false);

        // 结束任务
        spark.stop();
        System.out.println("✅ 数据查询完毕！");
    }
}