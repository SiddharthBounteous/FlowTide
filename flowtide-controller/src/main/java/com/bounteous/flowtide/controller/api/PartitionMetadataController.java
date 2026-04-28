package com.bounteous.flowtide.controller.api;

import com.bounteous.flowtide.controller.model.TopicCreateRequest;
import com.bounteous.flowtide.controller.model.TopicMetadata;
import com.bounteous.flowtide.controller.service.BrokerRegistryService;
import com.bounteous.flowtide.controller.service.ISRService;
import com.bounteous.flowtide.controller.service.PartitionAssignmentService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * REST API for topic partition metadata.
 *
 * <p>Endpoints:
 * <ul>
 *   <li>POST /controller/topics              — create topic + assign partitions to brokers
 *   <li>GET  /controller/topics/{topic}      — get leader map for a topic
 *   <li>GET  /controller/topics              — get all topic metadata
 *   <li>GET  /controller/topics/{topic}/{p}  — get leader for a specific partition
 * </ul>
 */
@RestController
@RequestMapping("/controller/topics")
public class PartitionMetadataController {

    private static final Logger log = LoggerFactory.getLogger(PartitionMetadataController.class);

    private final PartitionAssignmentService partitionAssignment;
    private final BrokerRegistryService      brokerRegistry;
    private final ISRService                 isrService;

    public PartitionMetadataController(PartitionAssignmentService partitionAssignment,
                                       BrokerRegistryService brokerRegistry,
                                       ISRService isrService) {
        this.partitionAssignment = partitionAssignment;
        this.brokerRegistry      = brokerRegistry;
        this.isrService          = isrService;
    }

    /**
     * Creates a topic and assigns its partitions across active brokers.
     * If the topic already exists, returns existing assignment (idempotent).
     *
     * <p>Returns 503 when no brokers are currently registered so that the calling
     * broker can log a warning and fall back to local-only operation, rather than
     * crashing with a 500 that looks like a server bug.
     */
    @PostMapping
    public ResponseEntity<TopicMetadata> createTopic(@RequestBody TopicCreateRequest request) {
        log.info("Create topic request: topic={} partitions={} rf={}",
                request.getTopic(), request.getPartitions(), request.getReplicationFactor());

        if (brokerRegistry.getActiveBrokerCount() == 0) {
            log.warn("Cannot assign topic '{}' — no active brokers registered yet. " +
                     "The broker will retry automatically on its next heartbeat cycle.",
                    request.getTopic());
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).build();
        }

        TopicMetadata metadata = partitionAssignment.assignTopic(
                request.getTopic(),
                request.getPartitions(),
                request.getReplicationFactor(),
                brokerRegistry.getActiveBrokers());

        return ResponseEntity.ok(metadata);
    }

    /**
     * Returns full metadata for a topic, or 404 if the topic does not exist.
     */
    @GetMapping("/{topic}")
    public ResponseEntity<TopicMetadata> getTopicMetadata(@PathVariable String topic) {
        if (!partitionAssignment.topicExists(topic)) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(partitionAssignment.getTopicMetadata(topic));
    }

    /**
     * Returns all topic metadata.
     */
    @GetMapping
    public Map<String, TopicMetadata> getAllTopics() {
        return partitionAssignment.getAllTopicMetadata();
    }

    /**
     * Deletes a topic from the controller's assignment maps.
     *
     * <p>All brokers will detect the deletion on their next heartbeat cycle
     * (within {@code heartbeat-interval-ms}) via the {@code knownTopics} field
     * in the heartbeat response, and will delete their local partition logs.
     */
    @DeleteMapping("/{topic}")
    public ResponseEntity<String> deleteTopic(@PathVariable String topic) {
        if (!partitionAssignment.topicExists(topic)) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body("Topic not found: " + topic);
        }
        partitionAssignment.deleteTopic(topic);
        log.info("Topic '{}' deleted from controller", topic);
        return ResponseEntity.ok("Topic '" + topic + "' deleted from cluster");
    }

    /**
     * Returns the leader broker ID for a specific topic-partition.
     * Used by brokers to check routing before handling a request.
     */
    @GetMapping("/{topic}/{partition}/leader")
    public ResponseEntity<String> getLeader(@PathVariable String topic,
                                            @PathVariable int partition) {
        return partitionAssignment.getLeader(topic, partition)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  ISR
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Called by the leader broker when its ISR changes (follower added or removed).
     * Body: list of current in-sync follower broker IDs (leader not included).
     */
    @PostMapping("/{topic}/{partition}/isr")
    public ResponseEntity<String> updateISR(
            @PathVariable String topic,
            @PathVariable int    partition,
            @RequestParam String leaderId,
            @RequestBody  List<String> followers) {
        isrService.updateISR(topic, partition, leaderId, followers);
        return ResponseEntity.ok("ISR updated");
    }

    /** Returns current ISR for a specific partition. */
    @GetMapping("/{topic}/{partition}/isr")
    public ResponseEntity<Set<String>> getISR(
            @PathVariable String topic,
            @PathVariable int    partition) {
        return ResponseEntity.ok(isrService.getISR(topic, partition));
    }

    /** Returns full ISR map for all topics (admin view). */
    @GetMapping("/isr")
    public Map<String, Map<Integer, Set<String>>> getAllISR() {
        return isrService.getAllISR();
    }
}
