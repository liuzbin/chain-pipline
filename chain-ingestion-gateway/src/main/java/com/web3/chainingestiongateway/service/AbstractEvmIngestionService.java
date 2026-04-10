package com.web3.chainingestiongateway.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.web3.chainingestiongateway.entity.RawBlock;
import com.web3.chainingestiongateway.entity.RawLog;
import com.web3.chainingestiongateway.entity.RawTransaction;
import com.web3.chainingestiongateway.entity.SyncTaskState;
import com.web3.chainingestiongateway.mapper.SyncStateMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
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
import java.util.List;

@Slf4j
public abstract class AbstractEvmIngestionService {

    protected final String taskName;
    protected final String nodeUrl;
    protected final SyncStateMapper syncStateMapper;
    protected final KafkaTemplate<String, String> kafkaTemplate;
    protected final ObjectMapper objectMapper = new ObjectMapper();

    protected WebSocketService webSocketService;
    protected Web3j web3j;
    protected volatile long inMemoryHwm = 0L;

    public AbstractEvmIngestionService(String taskName, String nodeUrl,
                                       SyncStateMapper syncStateMapper,
                                       KafkaTemplate<String, String> kafkaTemplate) {
        this.taskName = taskName;
        this.nodeUrl = nodeUrl;
        this.syncStateMapper = syncStateMapper;
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * 核心生命周期：子类在 @PostConstruct 中调用此方法启动引擎
     */
    public void start() {
        try {
            log.info("[{}] 初始化 WebSocket 链接: {}", taskName, nodeUrl);
            webSocketService = new WebSocketService(nodeUrl, true);
            webSocketService.connect();
            web3j = Web3j.build(webSocketService);

            loadWatermark();
            catchUpToLatest();
            subscribeRealTime();

        } catch (Exception e) {
            log.error("[{}] 引擎启动遭遇致命错误", taskName, e);
        }
    }

    private void loadWatermark() {
        SyncTaskState state = syncStateMapper.getTaskState(taskName);
        if (state == null) {
            log.warn("[{}] 数据库无水位线记录，初始化为 0。请手动在 DB 中设定起始高度!", taskName);
            syncStateMapper.insertInitialState(new SyncTaskState(taskName, 0L, null));
            inMemoryHwm = 0L;
        } else {
            inMemoryHwm = state.getLastProcessedBlock();
            log.info("[{}] 成功加载历史高水位线 (HWM): {}", taskName, inMemoryHwm);
        }
    }

    private void catchUpToLatest() {
        try {
            long networkLatest = web3j.ethBlockNumber().send().getBlockNumber().longValue();
            if (networkLatest > inMemoryHwm) {
                log.info("[{}] 触发断点追赶机制: 从 {} 追赶至最新高度 {}", taskName, inMemoryHwm + 1, networkLatest);
                for (long i = inMemoryHwm + 1; i <= networkLatest; i++) {
                    fetchAndPushToKafka(i);
                    updateHwm(i);
                }
                log.info("[{}] 断点追赶完成，准备切换至实时监听。", taskName);
            }
        } catch (Exception e) {
            log.error("[{}] 断点追赶执行失败", taskName, e);
        }
    }

    private void subscribeRealTime() {
        web3j.newHeadsNotifications().subscribe(
                (NewHeadsNotification notification) -> {
                    NewHead newHead = notification.getParams().getResult();
                    long currentBlock = Numeric.decodeQuantity(newHead.getNumber()).longValue();
                    handleIncomingBlock(currentBlock);
                },
                (Throwable error) -> log.error("[{}] WebSocket 监听异常断开", taskName, error)
        );
    }

    /**
     * 加锁保证漏块填补与实时推送的并发安全
     */
    private synchronized void handleIncomingBlock(long currentBlockNumber) {
        // 1. 发现 Gap 漏块，立即强行补齐
        if (currentBlockNumber > inMemoryHwm + 1) {
            log.warn("[{}] 发现网络漏块! 期望高度: {}, 实际接收: {}. 启动同步补录.", taskName, inMemoryHwm + 1, currentBlockNumber);
            for (long missingBlock = inMemoryHwm + 1; missingBlock < currentBlockNumber; missingBlock++) {
                fetchAndPushToKafka(missingBlock);
                updateHwm(missingBlock);
            }
        }

        // 2. 处理当前收到的区块
        fetchAndPushToKafka(currentBlockNumber);

        // 3. 只有高度超越当前 HWM 时，才更新水位（防止 Re-org 导致水位倒退）
        if (currentBlockNumber > inMemoryHwm) {
            updateHwm(currentBlockNumber);
        }
    }

    private void updateHwm(long blockNumber) {
        syncStateMapper.updateWatermark(taskName, blockNumber);
        inMemoryHwm = blockNumber;
    }

    /**
     * 极简 RPC 抓取与 Kafka 投递逻辑 (两次网络请求)
     */
    protected void fetchAndPushToKafka(long blockNumber) {
        try {
            DefaultBlockParameter blockParam = DefaultBlockParameter.valueOf(BigInteger.valueOf(blockNumber));

            // [请求 1]: 抓取区块头与内部所有交易
            EthBlock.Block block = web3j.ethGetBlockByNumber(blockParam, true).send().getBlock();
            if (block == null) return;

            long timestamp = block.getTimestamp().longValue();
            String currentBlockHash = block.getHash();

            // 构建并发送 Block
            RawBlock rawBlock = new RawBlock(blockNumber, currentBlockHash, block.getParentHash(), timestamp,
                    block.getMiner(), block.getBaseFeePerGas() != null ? block.getBaseFeePerGas().longValue() : 0L, 1, timestamp);
            sendToKafka("topic_raw_blocks", rawBlock.getBlockHash(), rawBlock);

            // 构建并发送 Transactions
            for (EthBlock.TransactionResult txResult : block.getTransactions()) {
                EthBlock.TransactionObject tx = (EthBlock.TransactionObject) txResult.get();
                RawTransaction rawTx = new RawTransaction(tx.getHash(), blockNumber, currentBlockHash, block.getParentHash(), timestamp,
                        tx.getFrom(), tx.getTo(), new BigDecimal(tx.getValue()), 0L, 1, 1, timestamp);
                sendToKafka("topic_raw_transactions", rawTx.getTxHash(), rawTx);
            }

            // [请求 2]: 抓取区块内所有 Logs
            EthFilter filter = new EthFilter(blockParam, blockParam, (String) null);
            List<org.web3j.protocol.core.methods.response.EthLog.LogResult> logResults = web3j.ethGetLogs(filter).send().getLogs();

            for (org.web3j.protocol.core.methods.response.EthLog.LogResult logResult : logResults) {
                Log ethLog = (Log) logResult.get();
                List<String> topics = ethLog.getTopics();
                String logId = blockNumber + "_" + ethLog.getLogIndex().toString();

                RawLog rawLog = new RawLog(logId, blockNumber, currentBlockHash, block.getParentHash(),
                        ethLog.getTransactionHash(), ethLog.getLogIndex().intValue(), timestamp, ethLog.getAddress(),
                        topics.size() > 0 ? topics.get(0) : null, topics.size() > 1 ? topics.get(1) : null,
                        topics.size() > 2 ? topics.get(2) : null, topics.size() > 3 ? topics.get(3) : null,
                        ethLog.getData(), 1, timestamp);
                sendToKafka("topic_raw_logs", rawLog.getAddress(), rawLog);
            }

            log.info("[{}] 成功抽取并投递区块 {}。Tx: {}, Logs: {}", taskName, blockNumber, block.getTransactions().size(), logResults.size());

        } catch (Exception e) {
            log.error("[{}] 抓取区块 {} 失败，将被跳过并在未来依赖重试策略", taskName, blockNumber, e);
        }
    }

    private void sendToKafka(String topic, String key, Object payload) {
        try {
            kafkaTemplate.send(topic, key, objectMapper.writeValueAsString(payload));
        } catch (Exception e) {
            log.error("[{}] 投递 Kafka 异常 (Topic: {})", taskName, topic, e);
        }
    }

    public String getTaskName() {
        return this.taskName;
    }

    public long getCurrentHwm() {
        return this.inMemoryHwm;
    }

    /**
     * 获取链上真实的最新区块高度
     */
    public long getNetworkLatestBlock() {
        try {
            if (web3j != null) {
                return web3j.ethBlockNumber().send().getBlockNumber().longValue();
            }
        } catch (Exception e) {
            log.error("[{}] 获取链上最新高度失败", taskName, e);
        }
        return -1L; // 返回 -1 表示节点 RPC 异常
    }
}