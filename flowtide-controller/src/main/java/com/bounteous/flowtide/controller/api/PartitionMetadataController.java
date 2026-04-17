package com.bounteous.flowtide.controller.api;

import com.bounteous.flowtide.controller.model.TopicCreateRequest;
import com.bounteous.flowtide.controller.model.TopicMetadata;
import com.bounteous.flowtide.controller.service.BrokerRegistryService;
import com.bounteous.flowtide.controller.service.PartitionAssignmentService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

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

    public PartitionMetadataController(PartitionAssignmentService partitionAssignment,
                                       BrokerRegistryService brokerRegistry) {
        this.partitionAssignment = partitionAssignment;
        this.brokerRegistry      = brokerRegistry;
    }

    /**
     * Creates a topic and assigns its partitions across active brokers.
     * If the topic already exists, returns existing assignment (idempotent).
     */
    @PostMapping
    public ResponseEntity<TopicMetadata> createTopic(@RequestBody TopicCreateRequest request) {
        log.info("Create topic request: topic={} partitions={} rf={}",
                request.getTopic(), request.getPartitions(), request.getReplicationFactor());

        TopicMetadata metadata = partitionAssignment.assignTopic(
                request.getTopic(),
                request.getPartitions(),
                request.getReplicationFactor(),
                brokerRegistry.getActiveBrokers());

        return ResponseEntity.ok(metadata);
    }

    /**
     * Returns full metadata for a topic.
     * If the topic is not known to the controller, auto-assigns it.
     */
    @GetMapping("/{topic}")
    public ResponseEntity<TopicMetadata> getTopicMetadata(@PathVariable String topic) {
        if (!partitionAssignment.topicExists(topic)) {
            log.info("Topic '{}' not found — auto-assigning with defaults", topic);
            TopicMetadata metadata = partitionAssignment.autoAssignTopic(
                    topic, brokerRegistry.getActiveBrokers());
            return ResponseEntity.ok(metadata);
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
     * Returns the leader broker ID for a specific topic-partition.
     * Used by brokers to check routing before handling a request.
     */
    @GetMapping("/{topic}/{partition}/leader")
    public ResponseEntity<String> getLeader(@PathVariable String topic,
                                            @PathVariable int partition) {
        // Auto-assign topic if unknown
        if (!partitionAssignment.topicExists(topic)) {
            partitionAssignment.autoAssignTopic(topic, brokerRegistry.getActiveBrokers());
        }

        return partitionAssignment.getLeader(topic, partition)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }
}
