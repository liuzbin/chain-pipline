package com.web3.chainingestiongateway.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

/**
 * 映射 PostgreSQL 中的 extractor_sync_state 表
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SyncTaskState {
    private String taskName;           // 例如: "ethereum_mainnet", "bsc_mainnet"
    private Long lastProcessedBlock;   // 当前水位线
    private Date updatedAt;            // 最后更新时间
}