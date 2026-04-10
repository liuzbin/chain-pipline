package com.web3.chainflinkprocessor.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class RawBlock {
    private Long blockNumber;
    private String blockHash;
    private String parentHash;
    private Long timestamp;
    private String miner;
    private Long baseFeePerGas;
}