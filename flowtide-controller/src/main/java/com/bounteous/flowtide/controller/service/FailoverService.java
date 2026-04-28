package com.bounteous.flowtide.controller.service;

import com.bounteous.flowtide.controller.model.BrokerInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Orchestrates failover when a broker dies.
 *
 * <p>Called by HealthCheckScheduler when a dead broker is detected.
 * Delegates the partition reassignment to PartitionAssignmentService
 * and logs the full failover event.
 */
@Service
public class FailoverService {

    private static final Logger log = LoggerFactory.getLogger(FailoverService.class);

    private final BrokerRegistryService      brokerRegistry;
    private final PartitionAssignmentService partitionAssignment;

    public FailoverService(BrokerRegistryService brokerRegistry,
                           PartitionAssignmentService partitionAssignment) {
        this.brokerRegistry      = brokerRegistry;
        this.partitionAssignment = partitionAssignment;
    }

    /**
     * Handles a dead broker:
     * 1. Gets remaining active brokers
     * 2. Promotes followers to leaders for all affected partitions
     * 3. Logs the full failover summary
     */
    public void handleDeadBroker(BrokerInfo deadBroker) {
        String deadId = deadBroker.getId();
        log.warn("Starting failover for dead broker: {}", deadId);

        List<BrokerInfo> activeBrokers = brokerRegistry.getActiveBrokers();
        if (activeBrokers.isEmpty()) {
            log.error("Failover failed — no active brokers remaining! Cluster is DOWN.");
            return;
        }
        List<String> affectedTopics = partitionAssignment.handleBrokerFailure(deadId, activeBrokers);

        if (affectedTopics.isEmpty()) {
            log.info("Failover complete — broker {} had no partition leadership", deadId);
        } else {
            log.warn("Failover complete — broker {} was leader for topics: {} — partitions reassigned to alive brokers",
                    deadId, affectedTopics);
        }
    }
}
