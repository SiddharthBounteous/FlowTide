package com.bounteous.FlowTide.cluster;

import com.bounteous.FlowTide.cluster.metadata.MetadataController;
import com.bounteous.FlowTide.server.log.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Periodically refreshes this broker's heartbeat in MetadataController.
 *
 * <p>No TCP needed — broker and MetadataController are in the same JVM.
 * ClusterManager reads lastHeartbeat to detect dead nodes.
 */
@Component
public class HeartbeatScheduler {

    private static final Logger log = LoggerFactory.getLogger(HeartbeatScheduler.class);

    private final MetadataController metadataController;
    private final LogManager logManager;

    @Value("${server.host:localhost}")
    private String brokerHost;

    @Value("${server.port:8083}")
    private int brokerPort;

    public HeartbeatScheduler(MetadataController metadataController, LogManager logManager) {
        this.metadataController = metadataController;
        this.logManager = logManager;
    }

    @Scheduled(fixedDelayString = "${kafka.cluster.heartbeat-interval-ms:5000}")
    public void sendHeartbeat() {
        metadataController.updateHeartbeat(brokerHost, brokerPort);
        log.trace("Heartbeat updated: host={} port={} totalEvents={}",
                brokerHost, brokerPort, logManager.totalEventsStored());
    }
}
