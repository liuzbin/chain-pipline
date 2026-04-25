package com.web3.ingestion.repository;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface SyncStateMapper {

    // 获取当前链的水位线
    Long getHighWatermark(@Param("taskName") String taskName);

    // 更新水位线
    void updateHighWatermark(@Param("taskName") String taskName, @Param("blockNumber") Long blockNumber);

    // 如果数据库没数据，初始化一条（容错机制）
    void initTaskIfNotExists(@Param("taskName") String taskName, @Param("blockNumber") Long blockNumber);
}