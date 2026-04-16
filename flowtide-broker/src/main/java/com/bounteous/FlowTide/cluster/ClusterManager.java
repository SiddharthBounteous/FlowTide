package com.bounteous.FlowTide.cluster;

import com.bounteous.FlowTide.audit.AuditLogger;
import com.bounteous.FlowTide.cluster.metadata.BrokerNode;
import com.bounteous.FlowTide.cluster.metadata.MetadataController;
import com.bounteous.FlowTide.server.registry.BrokerInfo;
import com.bounteous.FlowTide.server.registry.PartitionMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Orchestrates cluster health and failover decisions.
 *
 * <p>This class no longer stores any data of its own. All broker lists,
 * heartbeat timestamps, and partition maps live in {@link MetadataController}.
 * {@code ClusterManager} reads from it, makes decisions, and writes results back.
 *
 * <p>Responsibilities:
 * <ul>
 *   <li>Accept broker registrations and heartbeats → forward to MetadataController
 *   <li>Periodically scan for timed-out brokers → trigger failover via MetadataController
 *   <li>Expose cluster state to the admin REST API
 * </ul>
 */
@Service
public class ClusterManager {

    private static final Logger log = LoggerFactory.getLogger(ClusterManager.class);

    private final MetadataController metadataController;
    private final AuditLogger auditLogger;

    @Value("${kafka.cluster.node-timeout-ms:15000}")
    private long nodeTimeoutMs;

    public ClusterManager(MetadataController metadataController, AuditLogger auditLogger) {
        this.metadataController = metadataController;
        this.auditLogger = auditLogger;
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Node join / heartbeat
    // ─────────────────────────────────────────────────────────────────────────

    public void registerNode(BrokerInfo broker) {
        metadataController.registerBroker(broker);
        auditLogger.log("NODE_JOINED", "broker=" + broker);
    }

    public void receiveHeartbeat(String host, int port) {
        metadataController.updateHeartbeat(host, port);
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Health check — called periodically by HeartbeatScheduler
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Scans all known brokers. Any broker whose last heartbeat is older than
     * {@code kafka.cluster.node-timeout-ms} is declared dead; MetadataController
     * handles the failover.
     */
    public void checkNodeHealth() {
        long now = System.currentTimeMillis();
        for (BrokerNode node : metadataController.getAllBrokerNodes()) {
            if (node.isActive() && now - node.getLastHeartbeat() > nodeTimeoutMs) {
                log.warn("Node timed out ({}ms without heartbeat): {}", nodeTimeoutMs, node.key());
                auditLogger.log("NODE_FAILED", "broker=" + node.key());
                metadataController.handleBrokerFailure(node.getBrokerInfo());
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Partition metadata
    // ─────────────────────────────────────────────────────────────────────────

    public void updatePartitionMetadata(String topic, List<PartitionMetadata> partitionMetadata) {
        metadataController.updatePartitions(topic, partitionMetadata);
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Admin API support
    // ─────────────────────────────────────────────────────────────────────────

    public List<BrokerInfo> getActiveNodes() {
        return metadataController.getActiveBrokers();
    }

    public int getActiveNodeCount() {
        return metadataController.getActiveBrokerCount();
    }
}
