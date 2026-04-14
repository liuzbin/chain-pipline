package com.web3.datacommon.core.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class RawBlock {
    // 多链路由字段
    private String chainName;

    private Long blockNumber;
    private String blockHash;
    private String parentHash;
    private Long timestamp;
    private String miner;
    private Long baseFeePerGas;

    // 引擎摄入时间（系统处理时间）
    private Long ingestionTimestamp;
}