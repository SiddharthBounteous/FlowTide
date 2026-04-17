package com.bounteous.flowtide.controller.scheduler;

import com.bounteous.flowtide.controller.model.BrokerInfo;
import com.bounteous.flowtide.controller.service.BrokerRegistryService;
import com.bounteous.flowtide.controller.service.FailoverService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Periodically scans all registered brokers for heartbeat timeouts.
 *
 * <p>If a broker has not sent a heartbeat within node-timeout-ms,
 * it is declared dead and FailoverService reassigns its partitions.
 */
@Component
public class HealthCheckScheduler {

    private static final Logger log = LoggerFactory.getLogger(HealthCheckScheduler.class);

    private final BrokerRegistryService brokerRegistry;
    private final FailoverService       failoverService;

    @Value("${kafka.cluster.node-timeout-ms:15000}")
    private long nodeTimeoutMs;

    public HealthCheckScheduler(BrokerRegistryService brokerRegistry,
                                FailoverService failoverService) {
        this.brokerRegistry  = brokerRegistry;
        this.failoverService = failoverService;
    }

    @Scheduled(fixedDelayString = "${kafka.cluster.heartbeat-interval-ms:5000}")
    public void checkBrokerHealth() {
        List<BrokerInfo> deadBrokers = brokerRegistry.detectDeadBrokers(nodeTimeoutMs);

        if (deadBrokers.isEmpty()) {
            log.trace("Health check passed — all {} brokers alive",
                    brokerRegistry.getActiveBrokerCount());
            return;
        }

        for (BrokerInfo dead : deadBrokers) {
            log.warn("Dead broker detected: {} — initiating failover", dead.getId());
            failoverService.handleDeadBroker(dead);
        }
    }
}
