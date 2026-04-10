package com.web3.chainingestiongateway.mapper;

import com.web3.chainingestiongateway.entity.SyncTaskState;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface SyncStateMapper {
    SyncTaskState getTaskState(@Param("taskName") String taskName);
    int updateWatermark(@Param("taskName") String taskName, @Param("blockNumber") long blockNumber);
    int insertInitialState(SyncTaskState state);
}