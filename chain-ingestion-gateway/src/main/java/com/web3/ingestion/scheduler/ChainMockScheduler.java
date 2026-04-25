package com.web3.ingestion.scheduler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.web3.common.core.entity.RawLog;
import com.web3.common.core.entity.RawTransaction;
import com.web3.ingestion.dto.MockBlockBundle;
import com.web3.ingestion.repository.SyncStateMapper;
import com.web3.ingestion.service.MockDataGenerator;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class ChainMockScheduler {

    private final SyncStateMapper syncStateMapper;
    private final MockDataGenerator dataGenerator;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @PostConstruct
    public void init() {
        // 项目启动时，确保数据库里有这两条链的初始水位线记录
        syncStateMapper.initTaskIfNotExists("ethereum", 10000L);
        syncStateMapper.initTaskIfNotExists("bsc", 10000L);
        log.info("⏰ 多链模拟调度器已启动...");
    }

    // 每 2000 毫秒 (2秒) 触发一次以太坊出块
    @Scheduled(fixedDelay = 2000)
    public void scheduleEthereumMock() {
        processNextBlock("ethereum");
    }

    // 每 3000 毫秒 (3秒) 触发一次 BSC 出块
    @Scheduled(fixedDelay = 3000)
    public void scheduleBscMock() {
        processNextBlock("bsc");
    }

    /**
     * 核心流程：读取水位 -> 生成数据 -> 发送Kafka -> 更新水位
     */
    private void processNextBlock(String chainName) {
        try {
            Long currentHwm = syncStateMapper.getHighWatermark(chainName);
            long nextBlockToProcess = (currentHwm != null ? currentHwm : 10000L) + 1;

            long startMs = System.currentTimeMillis();

            // 1. 调用生成器生成一整个块的捆绑数据
            MockBlockBundle bundle = dataGenerator.generateBlockData(chainName, nextBlockToProcess);

            // 2. 发送到 Kafka
            sendToKafka("topic_raw_blocks", bundle.getBlock().getBlockHash(), bundle.getBlock());

            for (RawTransaction tx : bundle.getTransactions()) {
                sendToKafka("topic_raw_transactions", tx.getTxHash(), tx);
            }

            for (RawLog rawLog : bundle.getLogs()) {
                sendToKafka("topic_raw_logs", rawLog.getAddress(), rawLog);
            }

            // 3. 所有数据发送成功后，回写水位线
            syncStateMapper.updateHighWatermark(chainName, nextBlockToProcess);

            log.info("🌊 [{}] Block {} 处理完成. 包含 {} Tx, {} Logs. 耗时: {}ms",
                    chainName, nextBlockToProcess, bundle.getTransactions().size(), bundle.getLogs().size(), (System.currentTimeMillis() - startMs));

        } catch (Exception e) {
            log.error("❌ 处理链 {} 的模拟数据时发生异常", chainName, e);
        }
    }

    private void sendToKafka(String topic, String key, Object payload) throws Exception {
        String jsonMessage = objectMapper.writeValueAsString(payload);
        kafkaTemplate.send(topic, key, jsonMessage);
    }
}