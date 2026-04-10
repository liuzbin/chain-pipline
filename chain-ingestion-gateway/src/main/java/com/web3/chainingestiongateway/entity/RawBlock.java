package com.web3.chainingestiongateway.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class RawBlock {
    private Long blockNumber;
    private String blockHash;
    private String parentHash;
    private Long timestamp;
    private String miner;
    private Long baseFeePerGas; // EIP-1559 基础 Gas 费

    // 管道控制字段
    private Integer sign;
    private Long version;
}