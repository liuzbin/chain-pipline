package com.web3.chainingestiongateway.service.impl;

import com.web3.chainingestiongateway.mapper.SyncStateMapper;
import com.web3.chainingestiongateway.service.AbstractEvmIngestionService;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class EthereumIngestionService extends AbstractEvmIngestionService {

    public EthereumIngestionService(
            // 从 application.yml 读取以太坊节点的 URL
            @Value("${web3.chains.ethereum}") String nodeUrl,
            SyncStateMapper syncStateMapper,
            KafkaTemplate<String, String> kafkaTemplate) {

        // 调用父类，注入以太坊专属的 taskName 和节点配置
        super("ethereum_mainnet", nodeUrl, syncStateMapper, kafkaTemplate);
    }

    @PostConstruct
    public void init() {
        log.info(">>> 正在启动以太坊主网摄入网关...");
        // 触发父类的生命周期核心逻辑
        super.start();
    }
}