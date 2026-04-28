package com.bounteous.FlowTide.server;

import com.bounteous.FlowTide.client.ControllerClient;
import com.bounteous.FlowTide.client.model.HeartbeatRequest;
import com.bounteous.FlowTide.client.model.PartitionAssignment;
import com.bounteous.FlowTide.partition.PartitionOwnershipService;
import com.bounteous.FlowTide.server.log.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Set;

/**
 * Sends a heartbeat to flowtide-controller every {@code heartbeat-interval-ms}
 * and applies the returned partition assignment + topic list.
 *
 * <h3>What the heartbeat response does</h3>
 * <ul>
 *   <li><b>assignments</b> — updated partition roles (leader/follower).
 *       Applied to {@link PartitionOwnershipService} so this broker
 *       immediately reflects any rebalance or failover.
 *   <li><b>knownTopics</b> — the full set of topics the controller knows.
 *       Any local topic NOT in this set was deleted on another broker →
 *       delete it locally so all brokers stay in sync automatically.
 * </ul>
 *
 * <h3>Self-healing registration</h3>
 * The controller auto-registers unknown brokers on heartbeat receipt, so
 * a broker that missed its startup registration window recovers silently
 * within one heartbeat interval.
 */
@Component
public class HeartbeatScheduler {

    private static final Logger log = LoggerFactory.getLogger(HeartbeatScheduler.class);

    private final ControllerClient          controllerClient;
    private final PartitionOwnershipService ownershipService;
    private final LogManager                logManager;
    private final ServerPortProvider        portProvider;

    @Value("${server.host:localhost}")
    private String brokerHost;

    public HeartbeatScheduler(ControllerClient controllerClient,
                              PartitionOwnershipService ownershipService,
                              LogManager logManager,
                              ServerPortProvider portProvider) {
        this.controllerClient = controllerClient;
        this.ownershipService = ownershipService;
        this.logManager       = logManager;
        this.portProvider     = portProvider;
    }

    @Scheduled(fixedDelayString = "${kafka.cluster.heartbeat-interval-ms:5000}")
    public void sendHeartbeat() {
        int  port        = portProvider.getPort();
        long totalEvents = logManager.totalEventsStored();

        try {
            HeartbeatRequest    request    = new HeartbeatRequest(brokerHost, port,
                                                System.currentTimeMillis(), totalEvents);
            PartitionAssignment assignment = controllerClient.heartbeat(request);

            // 1. Apply partition role changes (rebalance / failover)
            ownershipService.applyAssignment(assignment);

            // 2. Sync topic deletions — remove any local topic the controller no longer knows
            syncTopics(assignment.getKnownTopics());

            log.trace("Heartbeat OK — leading={} following={} port={}",
                    ownershipService.getLeaderPartitionCount(),
                    ownershipService.getFollowerPartitionCount(),
                    port);

        } catch (Exception e) {
            log.warn("Heartbeat failed: {} — will retry next interval.", e.getMessage());
        }
    }

    /**
     * Removes local topics that are no longer known to the controller.
     *
     * <p>This is the mechanism by which topic deletions propagate to all brokers:
     * when broker A deletes a topic it notifies the controller, which removes it
     * from {@code knownTopics}. On the next heartbeat every other broker sees the
     * topic missing from the set and deletes its local partition logs here.
     */
    private void syncTopics(Set<String> controllerTopics) {
        if (controllerTopics == null || controllerTopics.isEmpty()) {
            return; // controller returned no topics — skip to avoid wiping on startup race
        }

        Set<String> localTopics = logManager.getAllTopics();
        for (String local : localTopics) {
            if (!controllerTopics.contains(local)) {
                logManager.deleteTopic(local);
                log.info("Topic '{}' removed locally — no longer known to controller (deleted cluster-wide)",
                        local);
            }
        }
    }
}
