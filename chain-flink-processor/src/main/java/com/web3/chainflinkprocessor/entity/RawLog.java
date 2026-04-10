package com.web3.chainflinkprocessor.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class RawLog {
    private String logId;
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
}