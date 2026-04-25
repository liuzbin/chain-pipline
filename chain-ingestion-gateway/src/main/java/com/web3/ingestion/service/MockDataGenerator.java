package com.web3.ingestion.service;

import com.web3.common.core.entity.RawBlock;
import com.web3.common.core.entity.RawLog;
import com.web3.common.core.entity.RawTransaction;
import com.web3.ingestion.dto.MockBlockBundle;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

@Component
public class MockDataGenerator {

    private final Random random = new Random();

    public MockBlockBundle generateBlockData(String chainName, long blockNumber) {
        long ingestionTime = System.currentTimeMillis();
        long blockTimestamp = ingestionTime / 1000;
        String blockHash = "0x" + UUID.randomUUID().toString().replace("-", "");

        // 1. 生成 Block
        RawBlock block = new RawBlock();
        block.setChainName(chainName);
        block.setBlockNumber(blockNumber);
        block.setBlockHash(blockHash);
        block.setParentHash("0x" + UUID.randomUUID().toString().replace("-", ""));
        block.setTimestamp(blockTimestamp);
        block.setMiner(generateFakeAddress());
        block.setBaseFeePerGas(20000000000L + random.nextInt(1000000000));
        block.setIngestionTimestamp(ingestionTime);

        // 2. 生成 Transactions 和 Logs
        List<RawTransaction> transactions = new ArrayList<>();
        List<RawLog> logs = new ArrayList<>();

        int txCount = 100 + random.nextInt(200);
        long logGlobalIndex = 0;

        for (int i = 0; i < txCount; i++) {
            String txHash = "0x" + UUID.randomUUID().toString().replace("-", "");

            RawTransaction tx = new RawTransaction();
            tx.setChainName(chainName);
            tx.setTxHash(txHash);
            tx.setBlockNumber(blockNumber);
            tx.setBlockHash(blockHash);
            tx.setFrom(generateFakeAddress());
            tx.setTo(generateFakeAddress());
            tx.setValue(String.valueOf(Math.abs(random.nextLong())) + "0000000");
            tx.setTimestamp(blockTimestamp);
            tx.setIngestionTimestamp(ingestionTime);
            transactions.add(tx);

            int logCount = 1 + random.nextInt(4);
            for (int j = 0; j < logCount; j++) {
                RawLog logEntity = new RawLog();
                logEntity.setChainName(chainName);
                logEntity.setLogIndex(logGlobalIndex++);
                logEntity.setBlockNumber(blockNumber);
                logEntity.setBlockHash(blockHash);
                logEntity.setTxHash(txHash);
                logEntity.setAddress(generateFakeAddress());
                logEntity.setTopic0("0x" + UUID.randomUUID().toString().replace("-", ""));
                logEntity.setData("0x000000000000000000000000000000000000000000000000000000000000000" + random.nextInt(9));
                logEntity.setTimestamp(blockTimestamp);
                logEntity.setIngestionTimestamp(ingestionTime);
                logs.add(logEntity);
            }
        }

        return new MockBlockBundle(block, transactions, logs);
    }

    private String generateFakeAddress() {
        StringBuilder sb = new StringBuilder("0x");
        for (int i = 0; i < 40; i++) {
            sb.append(Integer.toHexString(random.nextInt(16)));
        }
        return sb.toString();
    }

    public static void main(String[] args) throws Exception {
        MockDataGenerator generator = new MockDataGenerator();

        // 模拟生成以太坊第 10001 块的数据
        MockBlockBundle bundle = generator.generateBlockData("ethereum", 10001L);

        // 使用 Jackson 打印成漂亮的 JSON 格式
        com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        mapper.enable(com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT);

        System.out.println("====== 生成的 Block 数据 ======");
        System.out.println(mapper.writeValueAsString(bundle.getBlock()));

        System.out.println("\n====== 生成的 Transactions 数量: " + bundle.getTransactions().size() + " ======");
        System.out.println("--> 随机抽查第一笔 Tx:");
        System.out.println(mapper.writeValueAsString(bundle.getTransactions().get(0)));

        System.out.println("\n====== 生成的 Logs 数量: " + bundle.getLogs().size() + " ======");
        System.out.println("--> 随机抽查第一条 Log:");
        System.out.println(mapper.writeValueAsString(bundle.getLogs().get(0)));
    }
}