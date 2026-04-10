package com.web3.chainflinkprocessor.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import java.math.BigDecimal;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class RawTransaction {
    private String txHash;
    private Long blockNumber;
    private String blockHash;
    private String from;   // 对应 JSON 里的 from (插入 CH 时映射为 from_address)
    private String to;     // 对应 JSON 里的 to (插入 CH 时映射为 to_address)
    private BigDecimal value; // 原始数值极大，保留 BigDecimal 接收
    private Long timestamp;
}