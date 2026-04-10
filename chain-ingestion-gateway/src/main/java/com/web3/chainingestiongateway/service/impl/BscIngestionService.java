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
public class BscIngestionService extends AbstractEvmIngestionService {

    public BscIngestionService(
            // 从 application.yml 读取 BSC 节点的 URL
            @Value("${web3.chains.bsc}") String nodeUrl,
            SyncStateMapper syncStateMapper,
            KafkaTemplate<String, String> kafkaTemplate) {

        // 注入 BSC 专属的 taskName
        super("bsc_mainnet", nodeUrl, syncStateMapper, kafkaTemplate);
    }

    @PostConstruct
    public void init() {
        log.info(">>> 正在启动 BSC (币安智能链) 摄入网关...");
        super.start();
    }
}