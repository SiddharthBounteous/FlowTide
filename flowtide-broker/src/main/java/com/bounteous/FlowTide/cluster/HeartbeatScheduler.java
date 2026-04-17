package com.bounteous.FlowTide.cluster;

import com.bounteous.FlowTide.client.ControllerClient;
import com.bounteous.FlowTide.client.model.HeartbeatRequest;
import com.bounteous.FlowTide.cluster.metadata.MetadataController;
import com.bounteous.FlowTide.server.log.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Periodically sends a heartbeat to the flowtide-controller service so it
 * knows this broker is still alive.
 *
 * <p>Also keeps the local MetadataController up-to-date for the admin API
 * (same-JVM call, no network overhead).
 */
@Component
public class HeartbeatScheduler {

    private static final Logger log = LoggerFactory.getLogger(HeartbeatScheduler.class);

    private final ControllerClient   controllerClient;
    private final MetadataController metadataController;
    private final LogManager         logManager;

    @Value("${server.host:localhost}")
    private String brokerHost;

    /** Injected after Tomcat binds — gives the actual port even when server.port=0. */
    @LocalServerPort
    private int brokerPort;

    public HeartbeatScheduler(ControllerClient controllerClient,
                              MetadataController metadataController,
                              LogManager logManager) {
        this.controllerClient   = controllerClient;
        this.metadataController = metadataController;
        this.logManager         = logManager;
    }

    @Scheduled(fixedDelayString = "${kafka.cluster.heartbeat-interval-ms:5000}")
    public void sendHeartbeat() {
        long totalEvents = logManager.totalEventsStored();

        // 1. Notify flowtide-controller (cross-service, used for failover detection)
        try {
            HeartbeatRequest request = new HeartbeatRequest(
                    brokerHost, brokerPort, System.currentTimeMillis(), totalEvents);
            controllerClient.heartbeat(request);
            log.trace("Heartbeat sent to controller: host={} port={} totalEvents={}",
                    brokerHost, brokerPort, totalEvents);
        } catch (Exception e) {
            log.warn("Failed to send heartbeat to controller: {} — will retry next interval.",
                    e.getMessage());
        }

        // 2. Update local MetadataController for the admin API (same JVM, always succeeds)
        metadataController.updateHeartbeat(brokerHost, brokerPort);
    }
}
