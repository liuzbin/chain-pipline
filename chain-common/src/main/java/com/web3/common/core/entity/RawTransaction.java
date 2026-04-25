package com.web3.common.core.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class RawTransaction {
    // 多链路由字段
    private String chainName;

    private String txHash;
    private Long blockNumber;
    private String blockHash;
    private String from;
    private String to;

    // 核心修改：使用 String 接收 Wei 单位的超大数值，防止精度丢失
    private String value;

    private Long timestamp;

    // 引擎摄入时间
    private Long ingestionTimestamp;
}