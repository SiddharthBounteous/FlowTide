package com.bounteous.FlowTide.client;

import com.bounteous.FlowTide.client.model.*;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * Feign client for the flowtide-controller service.
 *
 * <p>Used by each broker to:
 * <ul>
 *   <li>Register itself on startup — receives initial partition assignment
 *   <li>Send periodic heartbeats — receives current assignment (detects rebalance / failover)
 *   <li>Look up partition leader for routing decisions
 * </ul>
 */
@FeignClient(name = "flowtide-controller")
public interface ControllerClient {

    /** Register this broker with the controller on startup. */
    @PostMapping("/controller/brokers/register")
    PartitionAssignment register(@RequestBody BrokerRegistration registration);

    /**
     * Get all active brokers known to the controller — global cluster view.
     * Used by {@code AdminController} so GET /api/admin/nodes always returns
     * the full cluster, not just this single broker's local view.
     */
    @GetMapping("/controller/brokers/active")
    List<BrokerInfo> getActiveBrokers();

    /**
     * Send heartbeat to the controller.
     * Returns the broker's current partition assignment — if the controller
     * has rebalanced or run failover, the returned assignment will differ
     * from the local view and must be applied to PartitionOwnershipService.
     */
    @PostMapping("/controller/brokers/heartbeat")
    PartitionAssignment heartbeat(@RequestBody HeartbeatRequest request);

    /** Register a new topic with the controller so it assigns partitions to all brokers. */
    @PostMapping("/controller/topics")
    TopicMetadata createTopic(@RequestBody TopicCreateRequest request);

    /**
     * Get all topics known to the controller — global cluster view.
     * Used by {@code TopicManager.listTopics()} so the broker returns the
     * full topic list regardless of which broker instance the gateway hits.
     */
    @GetMapping("/controller/topics")
    Map<String, TopicMetadata> getAllTopics();

    /** Get full topic metadata including leader map. */
    @GetMapping("/controller/topics/{topic}")
    TopicMetadata getTopicMetadata(@PathVariable("topic") String topic);

    /** Get the leader broker id for a specific partition. */
    @GetMapping("/controller/topics/{topic}/{partition}/leader")
    String getLeader(@PathVariable("topic") String topic,
                     @PathVariable("partition") int partition);

    /**
     * Report ISR change to controller after a follower joins or leaves ISR.
     * @param followers current in-sync follower IDs (leader not included)
     */
    @PostMapping("/controller/topics/{topic}/{partition}/isr")
    String updateISR(@PathVariable("topic") String topic,
                     @PathVariable("partition") int partition,
                     @RequestParam("leaderId") String leaderId,
                     @RequestBody List<String> followers);

    /**
     * Gracefully removes a broker from the cluster.
     * Triggers failover of all its partitions before removal.
     */
    @DeleteMapping("/controller/brokers/{brokerId}")
    String removeBroker(@PathVariable("brokerId") String brokerId);
}
