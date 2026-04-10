package com.web3.chainingestiongateway.scheduler;

import com.web3.chainingestiongateway.service.AbstractEvmIngestionService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class DataReconciliationJob {

    // 极其优雅的架构设计：Spring 会自动把 Ethereum, BSC, Polygon 的 Service 实例全部注入到这个 List 中
    private final List<AbstractEvmIngestionService> ingestionServices;

    // 告警阈值：如果落后超过 50 个区块，视为系统出现严重卡顿或故障
    private static final long ALARM_THRESHOLD_BLOCKS = 50L;

    /**
     * 生产环境通常设为每天凌晨对账一次，或者每 5 分钟巡检一次。
     * 这里使用 fixedRate = 300000 (每 5 分钟执行一次) 进行高频健康检查。
     */
    @Scheduled(fixedRate = 300000)
    public void executeReconciliation() {
        log.info("==================================================");
        log.info("🔍 [对账调度器] 开始执行多链摄入水位线巡检...");

        if (ingestionServices == null || ingestionServices.isEmpty()) {
            log.warn("未检测到任何正在运行的摄入网关实例。");
            return;
        }

        for (AbstractEvmIngestionService service : ingestionServices) {
            String taskName = service.getTaskName();
            long currentHwm = service.getCurrentHwm();
            long networkLatest = service.getNetworkLatestBlock();

            // 1. 检查 RPC 节点健康度
            if (networkLatest == -1L) {
                log.error("❌ [告警] 任务 {} 无法连接底层 Web3 节点，RPC 可能已宕机！", taskName);
                continue;
            }

            // 2. 计算摄入延迟 (Lag)
            long lag = networkLatest - currentHwm;

            // 3. 判定健康状态
            if (lag > ALARM_THRESHOLD_BLOCKS) {
                log.error("⚠️ [严重告警] 任务 {} 出现严重摄入延迟！", taskName);
                log.error("   当前水位 HWM: {}, 链上最新: {}, 积压区块数 (Lag): {}", currentHwm, networkLatest, lag);
                // 工业级扩展：在这里可以调用企业微信、钉钉或 Slack 的 Webhook API 发送报警通知
            } else if (lag < 0) {
                log.error("💥 [数据异常] 任务 {} 水位线超前于链上高度！HWM: {}, 链上: {}。可能发生了链上深度重组或数据库脏写。", taskName, currentHwm, networkLatest);
            } else {
                log.info("✅ 任务 {} 状态健康. 延迟 Lag: {} 区块 (HWM: {} / Network: {})", taskName, lag, currentHwm, networkLatest);
            }
        }

        log.info("🔍 [对账调度器] 多链巡检执行完毕。");
        log.info("==================================================");
    }
}