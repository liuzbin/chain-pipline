package com.web3.ingestion.dto;

import com.web3.common.core.entity.RawBlock;
import com.web3.common.core.entity.RawLog;
import com.web3.common.core.entity.RawTransaction;
import lombok.AllArgsConstructor;
import lombok.Data;
import java.util.List;

@Data
@AllArgsConstructor
public class MockBlockBundle {
    private RawBlock block;
    private List<RawTransaction> transactions;
    private List<RawLog> logs;
}