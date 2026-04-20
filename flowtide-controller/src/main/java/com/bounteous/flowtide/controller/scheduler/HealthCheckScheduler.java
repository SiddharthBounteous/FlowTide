package com.bounteous.flowtide.controller.scheduler;

import com.bounteous.flowtide.controller.model.BrokerInfo;
import com.bounteous.flowtide.controller.service.BrokerRegistryService;
import com.bounteous.flowtide.controller.service.ConsumerGroupService;
import com.bounteous.flowtide.controller.service.FailoverService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Periodically checks health of both brokers and consumer members.
 *
 * <h3>Broker health</h3>
 * Brokers that miss heartbeats for {@code node-timeout-ms} are declared dead
 * and their partitions are failed over via {@link FailoverService}.
 *
 * <h3>Consumer member health</h3>
 * Consumer members that miss heartbeats for {@code consumer.member-timeout-ms}
 * are evicted from their groups and their partitions are rebalanced to
 * surviving members.
 */
@Component
public class HealthCheckScheduler {

    private static final Logger log = LoggerFactory.getLogger(HealthCheckScheduler.class);

    private final BrokerRegistryService brokerRegistry;
    private final FailoverService       failoverService;
    private final ConsumerGroupService  consumerGroupService;

    @Value("${kafka.cluster.node-timeout-ms:15000}")
    private long nodeTimeoutMs;

    @Value("${kafka.consumer.member-timeout-ms:30000}")
    private long memberTimeoutMs;

    @Value("${kafka.topic.default-partitions:3}")
    private int defaultPartitionCount;

    public HealthCheckScheduler(BrokerRegistryService brokerRegistry,
                                FailoverService failoverService,
                                ConsumerGroupService consumerGroupService) {
        this.brokerRegistry      = brokerRegistry;
        this.failoverService     = failoverService;
        this.consumerGroupService = consumerGroupService;
    }

    @Scheduled(fixedDelayString = "${kafka.cluster.heartbeat-interval-ms:5000}")
    public void checkHealth() {
        checkBrokerHealth();
        checkConsumerHealth();
    }

    // ─────────────────────────────────────────────────────────────────────────

    private void checkBrokerHealth() {
        List<BrokerInfo> deadBrokers = brokerRegistry.detectDeadBrokers(nodeTimeoutMs);

        if (deadBrokers.isEmpty()) {
            log.trace("Broker health OK — {} active broker(s)", brokerRegistry.getActiveBrokerCount());
            return;
        }

        for (BrokerInfo dead : deadBrokers) {
            log.warn("Dead broker detected: {} — initiating failover", dead.getId());
            failoverService.handleDeadBroker(dead);
        }
    }

    private void checkConsumerHealth() {
        consumerGroupService.evictDeadMembers(memberTimeoutMs, defaultPartitionCount);
    }
}
