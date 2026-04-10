package com.web3.datacommon.core.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 原始智能合约事件日志。
 * 包含了极其严密的物理定位与重组防范控制字段。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class RawLog {

    // ==========================================
    // 1. 物理定位与去重主键 (Physical Locators)
    // ==========================================
    /**
     * 全局唯一主键：通常由 "blockNumber_logIndex" 拼接而成。
     * 用于 ClickHouse 底层的 Replacing/Collapsing 引擎进行精确匹配。
     */
    private String logId;
    private Long blockNumber;
    private String blockHash;
    /**
     * 【核心防线】：父区块哈希。
     * Flink 将通过比对 current.parentHash == previous.blockHash 来维系哈希树。
     * 一旦等式不成立，Flink 将立刻触发 Re-org 分叉告警流程。
     */
    private String parentHash;
    private String txHash;
    private Integer logIndex;
    private Long blockTimestamp;

    // ==========================================
    // 2. 原始业务负载 (Raw Payload)
    // ==========================================
    private String address; // 触发事件的合约地址 (如 USDT 合约地址)
    private String topic0;  // 事件签名的 Keccak-256 Hash (如 Transfer 事件的签名)
    private String topic1;  // Indexed 参数 1 (如 from 地址)
    private String topic2;  // Indexed 参数 2 (如 to 地址)
    private String topic3;  // Indexed 参数 3 (少用)
    private String data;    // 未被 Index 的十六进制长字符串 (如转账金额，需 Flink 动态 ABI 解析)

    // ==========================================
    // 3. 管道控制字段 (Pipeline Control)
    // ==========================================
    /**
     * ClickHouse VersionedCollapsingMergeTree 折叠凭证。
     * 1 : 正常抓取的正向数据。
     * -1: Flink 发现区块分叉后，主动生成的抵消（回滚）数据。
     */
    private Integer sign;

    /**
     * 数据版本号。
     * ClickHouse 依赖此字段判断数据的绝对先后顺序。通常直接使用 blockTimestamp。
     */
    private Long version;
}