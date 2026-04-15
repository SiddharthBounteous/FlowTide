package com.bounteous.FlowTide.server.retention;

import com.bounteous.FlowTide.server.log.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Periodically evicts events that violate each topic's retention policy.
 *
 * <p>Runs every 60 seconds. Each {@link com.bounteous.FlowTide.server.log.PartitionLog}
 * handles its own eviction logic; this scheduler simply triggers it.
 */
@Component
public class RetentionScheduler {

    private static final Logger log = LoggerFactory.getLogger(RetentionScheduler.class);

    private final LogManager logManager;

    public RetentionScheduler(LogManager logManager) {
        this.logManager = logManager;
    }

    @Scheduled(fixedDelayString = "${kafka.retention.check-interval-ms:60000}")
    public void runEviction() {
        long before = logManager.totalEventsStored();
        logManager.applyRetentionToAll();
        long after = logManager.totalEventsStored();
        long evicted = before - after;
        if (evicted > 0) {
            log.info("Retention eviction complete: removed {} events. Total remaining: {}", evicted, after);
        }
    }
}
