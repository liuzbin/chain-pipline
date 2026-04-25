package com.web3.common.core.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class RawLog {
    // 多链路由字段
    private String chainName;

    // 核心修改：使用 logIndex 替代不存在的全局 logId
    private Long logIndex;

    private Long blockNumber;
    private String blockHash;
    private String txHash;
    private String address;
    private String topic0;
    private String topic1;
    private String topic2;
    private String topic3;
    private String data;
    private Long timestamp;

    // 引擎摄入时间
    private Long ingestionTimestamp;
}