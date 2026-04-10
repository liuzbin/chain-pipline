package com.web3.chainingestiongateway.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class RawTransaction {
    private String txHash;
    private Long blockNumber;
    private String blockHash;
    private String parentHash;
    private Long blockTimestamp;

    private String fromAddress;
    private String toAddress;
    private BigDecimal value;   // 转移的原生 ETH 数量
    private Long gasUsed;       // 实际消耗的 Gas
    private Integer status;     // 1: 成功, 0: Reverted (失败)

    // 管道控制字段
    private Integer sign;
    private Long version;
}