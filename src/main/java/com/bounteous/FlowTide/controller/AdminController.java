package com.bounteous.FlowTide.controller;

import com.bounteous.FlowTide.cluster.ClusterManager;
import com.bounteous.FlowTide.consumer.ConsumerGroupInfo;
import com.bounteous.FlowTide.consumer.ConsumerGroupManager;
import com.bounteous.FlowTide.metrics.MetricsSnapshot;
import com.bounteous.FlowTide.metrics.MetricsService;
import com.bounteous.FlowTide.server.log.LogManager;
import com.bounteous.FlowTide.server.registry.BrokerInfo;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Administrative API for cluster operators.
 *
 * <p>Base path: {@code /api/admin}
 *
 * <p>In production this should be secured behind admin-only authentication.
 */
@RestController
@RequestMapping("/api/admin")
public class AdminController {

    private final ClusterManager clusterManager;
    private final ConsumerGroupManager consumerGroupManager;
    private final MetricsService metricsService;
    private final LogManager logManager;

    public AdminController(ClusterManager clusterManager,
                           ConsumerGroupManager consumerGroupManager,
                           MetricsService metricsService,
                           LogManager logManager) {
        this.clusterManager = clusterManager;
        this.consumerGroupManager = consumerGroupManager;
        this.metricsService = metricsService;
        this.logManager = logManager;
    }

    // ─── Cluster ─────────────────────────────────────────────────────────────

    /** List all nodes currently active in the cluster. */
    @GetMapping("/nodes")
    public List<BrokerInfo> getActiveNodes() {
        return clusterManager.getActiveNodes();
    }

    /** Trigger a manual node health check (normally runs on schedule). */
    @PostMapping("/nodes/health-check")
    public ResponseEntity<String> triggerHealthCheck() {
        clusterManager.checkNodeHealth();
        return ResponseEntity.ok("Health check complete. Active nodes: " + clusterManager.getActiveNodeCount());
    }

    // ─── Consumer Groups ─────────────────────────────────────────────────────

    /** List all consumer groups and their partition assignments. */
    @GetMapping("/groups")
    public Collection<ConsumerGroupInfo> getAllGroups() {
        return consumerGroupManager.getAllGroups();
    }

    /** Get consumer lag for a specific group and topic. */
    @GetMapping("/groups/{groupId}/lag")
    public ResponseEntity<Map<String, Object>> getConsumerLag(
            @PathVariable String groupId,
            @RequestParam String topic) {
        long lag = consumerGroupManager.computeLag(groupId, topic);
        metricsService.updateConsumerLag(topic, groupId, lag);
        return ResponseEntity.ok(Map.of(
                "groupId", groupId,
                "topic", topic,
                "lag", lag
        ));
    }

    /** Force a rebalance for a consumer group. */
    @PostMapping("/groups/{groupId}/rebalance")
    public ResponseEntity<String> triggerRebalance(@PathVariable String groupId, @RequestParam String topic) {
        consumerGroupManager.getGroup(groupId, topic).ifPresent(
                group -> consumerGroupManager.rebalance(group, topic));
        return ResponseEntity.ok("Rebalance triggered for group=" + groupId + " topic=" + topic);
    }

    // ─── Storage ─────────────────────────────────────────────────────────────

    /** Overall platform metrics snapshot. */
    @GetMapping("/metrics")
    public MetricsSnapshot getMetrics() {
        return metricsService.snapshot();
    }

    /** Forcefully clears all stored events (use with caution!). */
    @DeleteMapping("/storage/clear")
    public ResponseEntity<String> clearAll() {
        logManager.clearAll();
        return ResponseEntity.ok("All events cleared from all partitions");
    }
}
