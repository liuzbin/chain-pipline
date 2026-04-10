package com.web3.extractor.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.web3.datacommon.core.entity.RawBlock;
import com.web3.datacommon.core.entity.RawLog;
import com.web3.datacommon.core.entity.RawTransaction;
import com.web3.extractor.repository.SyncStateRepository;   // You will need to create this interface
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.DefaultBlockParameter;
import org.web3j.protocol.core.methods.request.EthFilter;
import org.web3j.protocol.core.methods.response.EthBlock;
import org.web3j.protocol.core.methods.response.Log;
import org.web3j.protocol.websocket.WebSocketService;
import org.web3j.protocol.websocket.events.NewHead;
import org.web3j.protocol.websocket.events.NewHeadsNotification;
import org.web3j.utils.Numeric;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.ConnectException;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class Web3ExtractorService {

    @Value("${web3.node-url}")
    private String nodeUrl;

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper(); // Used for JSON serialization

    // Inject the PostgreSQL repository for HWM persistence
    private final SyncStateRepository syncStateRepository;

    private WebSocketService webSocketService;
    private Web3j web3j;

    // Cache HWM in memory to avoid excessive DB reads during real-time processing
    private volatile long inMemoryHwm = 0L;

    @PostConstruct
    public void startExtractor() {
        try {
            log.info("Establishing WebSocket connection with Ethereum node: {}", nodeUrl);
            webSocketService = new WebSocketService(nodeUrl, true);
            webSocketService.connect();
            web3j = Web3j.build(webSocketService);

            // 1. Read the HWM from PostgreSQL before connecting to WebSocket
            inMemoryHwm = syncStateRepository.getHighWatermark();
            log.info("System startup. Current High Watermark (HWM) in DB: {}", inMemoryHwm);

            // 2. Perform a catch-up poll immediately in case blocks were missed while offline
            catchUpToLatest();

            // 3. Subscribe to new block headers (New Heads)
            web3j.newHeadsNotifications().subscribe(
                    (NewHeadsNotification notification) -> {
                        NewHead newHead = notification.getParams().getResult();
                        String blockHash = newHead.getHash();
                        BigInteger blockNumber = Numeric.decodeQuantity(newHead.getNumber());

                        log.info("🔥 New block generated! Height: {}, Hash: {}", blockNumber, blockHash);

                        // Trigger core pulling logic with gap detection
                        handleIncomingBlock(blockNumber.longValue());
                    },
                    (Throwable error) -> log.error("WebSocket subscription error, reconnection might be needed", error)
            );

        } catch (ConnectException e) {
            log.error("WebSocket connection failed", e);
        }
    }

    /**
     * Core logic to handle incoming blocks, detecting gaps and handling duplicates.
     */
    private synchronized void handleIncomingBlock(long currentBlockNumber) {
        // Scenario 1: Gap detected (e.g., HWM is 99, current is 101)
        if (currentBlockNumber > inMemoryHwm + 1) {
            log.warn("Gap detected! HWM is {}, but received {}. Initiating backfill.", inMemoryHwm, currentBlockNumber);
            // Synchronously fetch missing blocks before processing the current one
            for (long missingBlock = inMemoryHwm + 1; missingBlock < currentBlockNumber; missingBlock++) {
                fetchAndPushToKafka(missingBlock);
                updateHwm(missingBlock);
            }
        }

        // Process the current block
        fetchAndPushToKafka(currentBlockNumber);

        // Scenario 2 & Normal Flow: Update HWM only if it moves forward.
        // If currentBlockNumber <= inMemoryHwm (Re-org or duplicate push), we ALREADY pushed it to Kafka above,
        // so downstream Flink can process the Re-org, but we DO NOT regress our HWM.
        if (currentBlockNumber > inMemoryHwm) {
            updateHwm(currentBlockNumber);
        }
    }

    /**
     * Catch up from DB HWM to the current network latest block via HTTP RPC polling.
     */
    private void catchUpToLatest() {
        try {
            long networkLatest = web3j.ethBlockNumber().send().getBlockNumber().longValue();
            if (networkLatest > inMemoryHwm) {
                log.info("Catching up from block {} to {}", inMemoryHwm + 1, networkLatest);
                for (long i = inMemoryHwm + 1; i <= networkLatest; i++) {
                    fetchAndPushToKafka(i);
                    updateHwm(i);
                }
            }
        } catch (Exception e) {
            log.error("Failed during catch-up polling", e);
        }
    }

    /**
     * Updates both the in-memory cache and the PostgreSQL database.
     */
    private void updateHwm(long blockNumber) {
        syncStateRepository.updateHighWatermark(blockNumber);
        inMemoryHwm = blockNumber;
    }

    /**
     * Extremely restrained RPC call strategy (only requires 2 requests per block)
     */
    private void fetchAndPushToKafka(long blockNumber) {
        try {
            DefaultBlockParameter blockParam = DefaultBlockParameter.valueOf(BigInteger.valueOf(blockNumber));

            // [1st RPC]: Fetch complete block data (including all internal Transaction objects)
            EthBlock.Block block = web3j.ethGetBlockByNumber(blockParam, true).send().getBlock();
            if (block == null) return;

            long timestamp = block.getTimestamp().longValue();
            String parentHash = block.getParentHash();
            String currentBlockHash = block.getHash();

            // 1. Assemble and send RawBlock
            RawBlock rawBlock = new RawBlock(
                    blockNumber, currentBlockHash, parentHash, timestamp,
                    block.getMiner(),
                    block.getBaseFeePerGas() != null ? block.getBaseFeePerGas().longValue() : 0L,
                    1, timestamp // sign = 1, version = timestamp
            );
            sendToKafka("topic_raw_blocks", rawBlock.getBlockHash(), rawBlock);

            // 2. Iterate and send RawTransaction
            List<EthBlock.TransactionResult> txResults = block.getTransactions();
            for (EthBlock.TransactionResult txResult : txResults) {
                EthBlock.TransactionObject tx = (EthBlock.TransactionObject) txResult.get();

                RawTransaction rawTx = new RawTransaction(
                        tx.getHash(), blockNumber, currentBlockHash, parentHash, timestamp,
                        tx.getFrom(), tx.getTo(),
                        new BigDecimal(tx.getValue()), // Convert to BigDecimal
                        0L, // GasUsed needs to be fetched via Receipt, often skipped in streaming to save performance
                        1, 1, timestamp // status defaults to 1, sign = 1, version = timestamp
                );
                sendToKafka("topic_raw_transactions", rawTx.getTxHash(), rawTx);
            }

            // [2nd RPC]: Fetch all Logs within the block in one go
            EthFilter filter = new EthFilter(blockParam, blockParam, (String) null);
            List<org.web3j.protocol.core.methods.response.EthLog.LogResult> logResults = web3j.ethGetLogs(filter).send().getLogs();

            for (org.web3j.protocol.core.methods.response.EthLog.LogResult logResult : logResults) {
                Log ethLog = (Log) logResult.get();
                List<String> topics = ethLog.getTopics();

                String logId = blockNumber + "_" + ethLog.getLogIndex().toString();

                RawLog rawLog = new RawLog(
                        logId, blockNumber, currentBlockHash, parentHash,
                        ethLog.getTransactionHash(), ethLog.getLogIndex().intValue(), timestamp,
                        ethLog.getAddress(),
                        topics.size() > 0 ? topics.get(0) : null,
                        topics.size() > 1 ? topics.get(1) : null,
                        topics.size() > 2 ? topics.get(2) : null,
                        topics.size() > 3 ? topics.get(3) : null,
                        ethLog.getData(),
                        1, timestamp // sign = 1, version = timestamp
                );

                // Use Contract Address as Kafka Partition Key to ensure logs of the same contract are consumed in order by Flink
                sendToKafka("topic_raw_logs", rawLog.getAddress(), rawLog);
            }

            log.info("✅ Block {} processed. Contains {} transactions, {} logs.", blockNumber, txResults.size(), logResults.size());

        } catch (Exception e) {
            log.error("Fatal error occurred while processing block {}", blockNumber, e);
        }
    }

    private void sendToKafka(String topic, String key, Object payload) {
        try {
            String jsonMessage = objectMapper.writeValueAsString(payload);
            // key ensures data from the same hash or contract address enters the same Kafka Partition
            kafkaTemplate.send(topic, key, jsonMessage);
        } catch (Exception e) {
            log.error("Failed to send Kafka message, Topic: {}", topic, e);
        }
    }

    @PreDestroy
    public void stopExtractor() {
        if (webSocketService != null) {
            webSocketService.close();
        }
    }
}