package com.bounteous.FlowTide.controller;

import com.bounteous.FlowTide.client.BrokerClient;
import com.bounteous.FlowTide.consumer.ConsumerGroupManager;
import com.bounteous.FlowTide.consumer.OffsetManager;
import com.bounteous.FlowTide.consumer.OffsetStrategy;
import com.bounteous.FlowTide.model.Event;
import com.bounteous.FlowTide.model.OffsetCommitRequest;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

/**
 * Handles consumer group polling via HTTP.
 *
 * <p>Flow: REST → ConsumerController → ConsumerGroupManager (partition assignment)
 *                                    → BrokerClient (Feign) → GET /internal/events
 */
@RestController
@RequestMapping("/api/consumer")
public class ConsumerController {

    private static final Logger log = LoggerFactory.getLogger(ConsumerController.class);

    private final ConsumerGroupManager groupManager;
    private final OffsetManager offsetManager;
    private final BrokerClient brokerClient;

    public ConsumerController(ConsumerGroupManager groupManager,
                              OffsetManager offsetManager,
                              BrokerClient brokerClient) {
        this.groupManager = groupManager;
        this.offsetManager = offsetManager;
        this.brokerClient = brokerClient;
    }

    @GetMapping("/groups/{groupId}/poll")
    public ResponseEntity<List<Event>> poll(
            @PathVariable String groupId,
            @RequestParam String topic,
            @RequestParam String memberId,
            @RequestParam(defaultValue = "COMMITTED") OffsetStrategy strategy,
            @RequestParam(defaultValue = "100") int limit,
            @RequestParam(defaultValue = "true") boolean autoCommit) {

        // 1. Ask broker how many partitions this topic has
        Map<String, Integer> meta = brokerClient.getMetadata(topic);
        int partitionCount = meta.getOrDefault("partitionCount", 3);

        // 2. Join group — assigns partitions round-robin
        List<Integer> assignedPartitions = groupManager.join(groupId, memberId, topic, partitionCount);

        if (assignedPartitions.isEmpty()) {
            return ResponseEntity.ok(Collections.emptyList());
        }

        List<Event> results = new ArrayList<>();
        int perPartitionLimit = Math.max(1, limit / assignedPartitions.size());

        for (int partition : assignedPartitions) {

            // 3. Resolve start offset
            long startOffset = resolveOffset(strategy, groupId, topic, partition);

            // 4. Fetch from broker via Feign
            List<Event> events = brokerClient.fetchEvents(topic, partition, startOffset, perPartitionLimit);
            results.addAll(events);

            log.debug("Polled {} events: topic={} partition={} offset={}", events.size(), topic, partition, startOffset);

            // 5. Auto-commit
            if (autoCommit && !events.isEmpty()) {
                long next = events.get(events.size() - 1).getOffset() + 1;
                offsetManager.commit(groupId, topic, partition, next);
            }
        }

        return ResponseEntity.ok(results);
    }

    @DeleteMapping("/groups/{groupId}/members/{memberId}")
    public ResponseEntity<String> leaveGroup(@PathVariable String groupId,
                                             @PathVariable String memberId,
                                             @RequestParam String topic) {
        Map<String, Integer> meta = brokerClient.getMetadata(topic);
        int partitionCount = meta.getOrDefault("partitionCount", 3);
        groupManager.leave(groupId, memberId, topic, partitionCount);
        return ResponseEntity.ok("Member " + memberId + " left group " + groupId);
    }

    @PostMapping("/offsets/commit")
    public ResponseEntity<String> commitOffset(@RequestBody @Valid OffsetCommitRequest request) {
        offsetManager.commit(request.getGroupId(), request.getTopic(), request.getPartition(), request.getOffset());
        return ResponseEntity.ok("Offset committed");
    }

    @GetMapping("/groups/{groupId}/offsets")
    public Map<String, Long> getOffsets(@PathVariable String groupId) {
        return offsetManager.getGroupOffsets(groupId);
    }

    private long resolveOffset(OffsetStrategy strategy, String groupId, String topic, int partition) {
        return switch (strategy) {
            case EARLIEST  -> 0L;
            case LATEST    -> 0L;
            case COMMITTED -> {
                long c = offsetManager.getCommittedOffset(groupId, topic, partition);
                yield c < 0 ? 0L : c;
            }
        };
    }
}
