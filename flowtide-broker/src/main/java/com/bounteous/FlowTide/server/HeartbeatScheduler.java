package com.bounteous.FlowTide.server;

import com.bounteous.FlowTide.client.ControllerClient;
import com.bounteous.FlowTide.client.model.HeartbeatRequest;
import com.bounteous.FlowTide.client.model.PartitionAssignment;
import com.bounteous.FlowTide.server.registry.LocalBrokerRegistry;
import com.bounteous.FlowTide.partition.PartitionOwnershipService;
import com.bounteous.FlowTide.server.log.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Periodically sends a heartbeat to the flowtide-controller and applies
 * any updated partition assignment the controller returns.
 *
 * <h3>Why the heartbeat response matters</h3>
 * The controller may change this broker's roles at any time — after a
 * rebalance (new broker joined) or after a failover (another broker died).
 * The heartbeat response always carries the broker's current assignment,
 * so applying it here keeps {@link PartitionOwnershipService} in sync
 * without requiring a separate push mechanism.
 */
@Component
public class HeartbeatScheduler {

    private static final Logger log = LoggerFactory.getLogger(HeartbeatScheduler.class);

    private final ControllerClient       controllerClient;
    private final LocalBrokerRegistry    localRegistry;
    private final PartitionOwnershipService ownershipService;
    private final LogManager             logManager;
    private final ServerPortProvider     portProvider;

    @Value("${server.host:localhost}")
    private String brokerHost;

    public HeartbeatScheduler(ControllerClient controllerClient,
                              LocalBrokerRegistry localRegistry,
                              PartitionOwnershipService ownershipService,
                              LogManager logManager,
                              ServerPortProvider portProvider) {
        this.controllerClient = controllerClient;
        this.localRegistry    = localRegistry;
        this.ownershipService = ownershipService;
        this.logManager       = logManager;
        this.portProvider     = portProvider;
    }

    @Scheduled(fixedDelayString = "${kafka.cluster.heartbeat-interval-ms:5000}")
    public void sendHeartbeat() {
        int  brokerPort  = portProvider.getPort();
        long totalEvents = logManager.totalEventsStored();

        // 1. Send heartbeat to controller — get back current assignment
        try {
            HeartbeatRequest    request    = new HeartbeatRequest(
                    brokerHost, brokerPort, System.currentTimeMillis(), totalEvents);
            PartitionAssignment assignment = controllerClient.heartbeat(request);

            // Apply assignment — detects rebalance or failover changes automatically
            ownershipService.applyAssignment(assignment);

            log.trace("Heartbeat OK — leading={} following={} port={}",
                    ownershipService.getLeaderPartitionCount(),
                    ownershipService.getFollowerPartitionCount(),
                    brokerPort);

        } catch (Exception e) {
            log.warn("Heartbeat to controller failed: {} — will retry next interval.", e.getMessage());
        }

        // 2. Keep local registry fresh for the admin API (same JVM, no network)
        localRegistry.updateHeartbeat(brokerHost, brokerPort);
    }
}
