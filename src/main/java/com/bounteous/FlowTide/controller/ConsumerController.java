package com.bounteous.FlowTide.controller;

import com.bounteous.FlowTide.clients.consumer.Consumer;
import com.bounteous.FlowTide.consumer.ConsumerGroupManager;
import com.bounteous.FlowTide.consumer.OffsetManager;
import com.bounteous.FlowTide.consumer.OffsetStrategy;
import com.bounteous.FlowTide.metrics.MetricsService;
import com.bounteous.FlowTide.model.Event;
import com.bounteous.FlowTide.model.OffsetCommitRequest;
import com.bounteous.FlowTide.server.broker.MetadataCache;
import com.bounteous.FlowTide.server.log.LogManager;
import com.bounteous.FlowTide.server.log.PartitionLog;
import com.bounteous.FlowTide.server.registry.BrokerInfo;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

/**
 * REST API for consuming events and managing consumer groups.
 *
 * <p>Two reading paths:
 * <ul>
 *   <li><b>Direct reads</b> ({@code /topics/.../messages}) — reads from the local
 *       {@link LogManager}. Good for admin/debugging on a single-node deployment.
 *
 *   <li><b>Group polls</b> ({@code /groups/.../poll}) — the correct multi-broker path:
 *       <ol>
 *         <li>Resolve partition assignment for this member.
 *         <li>Ask {@link MetadataCache} which broker is the <em>leader</em> for each partition.
 *         <li>Open a TCP {@link Consumer} to that leader.
 *         <li>Fetch {@link Event} objects directly from the broker's {@link PartitionLog}.
 *         <li>Auto-commit (or let the client call {@code POST /offsets/commit}).
 *       </ol>
 *       This works correctly even when partitions are spread across different broker JVMs.
 * </ul>
 */
@RestController
@RequestMapping("/api/consumer")
public class ConsumerController {

    private static final Logger log = LoggerFactory.getLogger(ConsumerController.class);

    private final LogManager logManager;
    private final ConsumerGroupManager groupManager;
    private final OffsetManager offsetManager;
    private final MetricsService metricsService;

    /** Fallback broker used when metadata cache has no leader info yet. */
    @Value("${kafka.broker.host:localhost}")
    private String defaultBrokerHost;

    @Value("${kafka.broker.port:9092}")
    private int defaultBrokerPort;

    public ConsumerController(LogManager logManager,
                              ConsumerGroupManager groupManager,
                              OffsetManager offsetManager,
                              MetricsService metricsService) {
        this.logManager = logManager;
        this.groupManager = groupManager;
        this.offsetManager = offsetManager;
        this.metricsService = metricsService;
    }

    // ─── Direct reads (local LogManager — admin/debug) ───────────────────────

    @GetMapping("/topics/{topic}/partition/{partition}/messages")
    public List<Event> getMessages(
            @PathVariable String topic,
            @PathVariable int partition,
            @RequestParam(defaultValue = "0") long offset,
            @RequestParam(defaultValue = "100") int limit) {

        return logManager.getLog(topic, partition).readFrom(offset, limit);
    }

    @GetMapping("/topics/{topic}/messages")
    public List<Event> getTopicMessages(
            @PathVariable String topic,
            @RequestParam(defaultValue = "100") int limit) {

        List<Event> all = new ArrayList<>();
        for (PartitionLog pl : logManager.getTopicLogs(topic).values()) {
            for (Event e : pl.readFrom(0, limit)) {
                if (all.size() >= limit) return all;
                all.add(e);
            }
        }
        return all;
    }

    @GetMapping("/topics")
    public Set<String> getTopics() {
        return logManager.getAllTopics();
    }

    @GetMapping("/topics/{topic}/partitions")
    public List<Integer> getPartitions(@PathVariable String topic) {
        List<Integer> partitions = new ArrayList<>();
        for (String key : logManager.getTopicLogs(topic).keySet()) {
            partitions.add(Integer.parseInt(key.substring(key.lastIndexOf('-') + 1)));
        }
        return partitions;
    }

    // ─── Consumer group poll (TCP → correct broker) ───────────────────────────

    /**
     * Poll for new events as a named member of a consumer group.
     *
     * <p>For each assigned partition, this method:
     * <ol>
     *   <li>Asks {@link MetadataCache} which broker JVM is the <em>leader</em>.
     *   <li>Opens a TCP {@link Consumer} to that broker.
     *   <li>Fetches events via {@code RequestHandler → PartitionLog} on the remote JVM.
     *   <li>Auto-commits the offset so the next poll resumes where this one left off.
     * </ol>
     *
     * <p>In a three-broker cluster where each broker owns different partitions,
     * three separate TCP connections are opened — one per broker — and the results
     * are merged into one response.
     */
    @GetMapping("/groups/{groupId}/poll")
    public ResponseEntity<List<Event>> poll(
            @PathVariable String groupId,
            @RequestParam String topic,
            @RequestParam String memberId,
            @RequestParam(defaultValue = "COMMITTED") OffsetStrategy strategy,
            @RequestParam(defaultValue = "100") int limit,
            @RequestParam(defaultValue = "true") boolean autoCommit) {

        // 1. Join group — idempotent; triggers rebalance only if member is new
        List<Integer> assignedPartitions = groupManager.join(groupId, memberId, topic);

        if (assignedPartitions.isEmpty()) {
            return ResponseEntity.ok(Collections.emptyList());
        }

        List<Event> results = new ArrayList<>();
        int perPartitionLimit = Math.max(1, limit / assignedPartitions.size());

        for (int partition : assignedPartitions) {

            // 2. Find which broker is leader for this partition
            BrokerInfo leader = MetadataCache.getLeader(topic, partition)
                    .orElseGet(() -> {
                        log.warn("No leader found for topic={} partition={}, falling back to default broker", topic, partition);
                        return new BrokerInfo(defaultBrokerHost, defaultBrokerPort);
                    });

            // 3. Resolve start offset based on strategy
            long startOffset = resolveStartOffset(strategy, groupId, topic, partition);

            // 4. Fetch from the correct broker via TCP
            Consumer consumer = new Consumer(leader.getHost(), leader.getPort());
            List<Event> events = consumer.poll(topic, partition, startOffset, perPartitionLimit);
            results.addAll(events);

            log.debug("Polled {} events from broker={} topic={} partition={} offset={}",
                    events.size(), leader, topic, partition, startOffset);

            // 5. Auto-commit: next poll starts after the last event we just read
            if (autoCommit && !events.isEmpty()) {
                long nextOffset = events.get(events.size() - 1).getOffset() + 1;
                offsetManager.commit(groupId, topic, partition, nextOffset);
            }
        }

        // 6. Record metrics
        metricsService.recordConsume(topic, groupId, results.size());
        long lag = groupManager.computeLag(groupId, topic);
        metricsService.updateConsumerLag(topic, groupId, lag);

        return ResponseEntity.ok(results);
    }

    @DeleteMapping("/groups/{groupId}/members/{memberId}")
    public ResponseEntity<String> leaveGroup(
            @PathVariable String groupId,
            @PathVariable String memberId,
            @RequestParam String topic) {

        groupManager.leave(groupId, memberId, topic);
        return ResponseEntity.ok("Member " + memberId + " left group " + groupId);
    }

    // ─── Offset management ────────────────────────────────────────────────────

    @PostMapping("/offsets/commit")
    public ResponseEntity<String> commitOffset(@RequestBody @Valid OffsetCommitRequest request) {
        offsetManager.commit(request.getGroupId(), request.getTopic(),
                request.getPartition(), request.getOffset());
        return ResponseEntity.ok("Offset committed");
    }

    @GetMapping("/groups/{groupId}/offsets")
    public Map<String, Long> getGroupOffsets(@PathVariable String groupId) {
        return offsetManager.getGroupOffsets(groupId);
    }

    // ─── Cleanup ─────────────────────────────────────────────────────────────

    @DeleteMapping("/topics/{topic}")
    public ResponseEntity<String> clearTopic(@PathVariable String topic) {
        logManager.getTopicLogs(topic).values().forEach(PartitionLog::clear);
        return ResponseEntity.ok("Topic '" + topic + "' cleared");
    }

    // ─── Internal ────────────────────────────────────────────────────────────

    private long resolveStartOffset(OffsetStrategy strategy, String groupId,
                                    String topic, int partition) {
        return switch (strategy) {
            case EARLIEST -> 0L;
            case LATEST   -> logManager.getLog(topic, partition).latestOffset();
            case COMMITTED -> {
                long committed = offsetManager.getCommittedOffset(groupId, topic, partition);
                yield committed < 0 ? 0L : committed;
            }
        };
    }
}
