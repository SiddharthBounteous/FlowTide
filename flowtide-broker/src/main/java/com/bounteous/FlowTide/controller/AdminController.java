package com.bounteous.FlowTide.controller;

import com.bounteous.FlowTide.client.ControllerClient;
import com.bounteous.FlowTide.client.model.BrokerInfo;
import com.bounteous.FlowTide.metrics.MetricsSnapshot;
import com.bounteous.FlowTide.metrics.MetricsService;
import com.bounteous.FlowTide.server.log.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Administrative API for broker operators.
 *
 * <p>Base path: {@code /api/admin}
 *
 * <p>Covers broker-specific concerns only:
 * <ul>
 *   <li>Cluster node visibility (local view of registered brokers)
 *   <li>Metrics snapshot
 *   <li>Storage management
 * </ul>
 *
 * <p>Consumer group management belongs to flowtide-consumer + flowtide-controller.
 * Topic management belongs to /api/topics (TopicController).
 */
@RestController
@RequestMapping("/api/admin")
public class AdminController {

    private static final Logger log = LoggerFactory.getLogger(AdminController.class);

    private final ControllerClient controllerClient;
    private final MetricsService   metricsService;
    private final LogManager       logManager;

    public AdminController(ControllerClient controllerClient,
                           MetricsService metricsService,
                           LogManager logManager) {
        this.controllerClient = controllerClient;
        this.metricsService   = metricsService;
        this.logManager       = logManager;
    }

    // ─── Cluster ─────────────────────────────────────────────────────────────

    /**
     * List all active broker nodes across the entire cluster.
     * Queries flowtide-controller — the single source of truth for broker membership.
     * Falls back to empty list when the controller is unreachable.
     */
    @GetMapping("/nodes")
    public List<BrokerInfo> getActiveNodes() {
        try {
            return controllerClient.getActiveBrokers();
        } catch (Exception e) {
            log.warn("Cannot reach controller for broker list: {} — returning empty", e.getMessage());
            return Collections.emptyList();
        }
    }

    /** Count of active broker nodes across the entire cluster. */
    @GetMapping("/nodes/count")
    public ResponseEntity<Map<String, Integer>> getActiveNodeCount() {
        try {
            int count = controllerClient.getActiveBrokers().size();
            return ResponseEntity.ok(Map.of("activeNodes", count));
        } catch (Exception e) {
            log.warn("Cannot reach controller for broker count: {} — returning 0", e.getMessage());
            return ResponseEntity.ok(Map.of("activeNodes", 0));
        }
    }

    // ─── Metrics ─────────────────────────────────────────────────────────────

    /** Full metrics snapshot — event counts, throughput, partition sizes. */
    @GetMapping("/metrics")
    public MetricsSnapshot getMetrics() {
        return metricsService.snapshot();
    }

    // ─── Storage ─────────────────────────────────────────────────────────────

    /** Forcefully clears all stored events from all partitions (use with caution). */
    @DeleteMapping("/storage/clear")
    public ResponseEntity<String> clearAll() {
        logManager.clearAll();
        return ResponseEntity.ok("All events cleared from all partitions");
    }
}
