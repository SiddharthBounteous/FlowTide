package com.bounteous.FlowTide.controller;

import com.bounteous.FlowTide.client.BrokerClient;
import com.bounteous.FlowTide.client.CoordinatorClient;
import com.bounteous.FlowTide.client.DirectBrokerClient;
import com.bounteous.FlowTide.consumer.LeaderMetadataCache;
import com.bounteous.FlowTide.consumer.OffsetManager;
import com.bounteous.FlowTide.consumer.OffsetStrategy;
import com.bounteous.FlowTide.model.ConsumerRecord;
import com.bounteous.FlowTide.model.Event;
import com.bounteous.FlowTide.model.JoinRequest;
import com.bounteous.FlowTide.model.JoinResponse;
import com.bounteous.FlowTide.model.OffsetCommitRequest;
import com.bounteous.FlowTide.model.PollResponse;
import feign.FeignException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.HttpClientErrorException;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Consumer group API.
 *
 * <h3>Usage flow</h3>
 * <pre>
 * 1. POST /api/consumer/groups/{groupId}/join?topic=orders
 *    ← { memberId, assignedPartitions, ... }
 *       Store memberId — needed for every poll and leave call.
 *
 * 2. GET /api/consumer/groups/{groupId}/poll
 *       ?memberId=...&topic=orders&strategy=COMMITTED&limit=100
 *    ← PollResponse with records, lag, committedOffsets
 *
 * 3. DELETE /api/consumer/groups/{groupId}/members/{memberId}?topic=orders
 * </pre>
 *
 * <h3>Direct leader routing</h3>
 * At join time this service fetches the partition→leader map from the controller
 * and caches it in {@link LeaderMetadataCache}.  Every subsequent poll uses
 * {@link DirectBrokerClient} to call the leader broker directly, skipping Eureka
 * load balancing entirely — zero wrong-broker hops under normal operation.
 *
 * <p>If a broker returns HTTP 409 (it lost leadership after a failover), the cache
 * is refreshed from the controller and the fetch is retried once against the new leader.
 */
@RestController
@RequestMapping("/api/consumer")
public class ConsumerController {

    private static final Logger log = LoggerFactory.getLogger(ConsumerController.class);

    private final CoordinatorClient  coordinatorClient;
    private final BrokerClient       brokerClient;       // Feign/LB — used only for metadata
    private final DirectBrokerClient directBrokerClient; // plain RestTemplate → leader directly
    private final LeaderMetadataCache leaderCache;
    private final OffsetManager      offsetManager;

    /**
     * Local cache: "groupId:topic" → current memberId.
     * Used by auto-rejoin when the coordinator returns 404.
     */
    private final ConcurrentHashMap<String, String> memberIdCache = new ConcurrentHashMap<>();

    public ConsumerController(CoordinatorClient coordinatorClient,
                              BrokerClient brokerClient,
                              DirectBrokerClient directBrokerClient,
                              LeaderMetadataCache leaderCache,
                              OffsetManager offsetManager) {
        this.coordinatorClient  = coordinatorClient;
        this.brokerClient       = brokerClient;
        this.directBrokerClient = directBrokerClient;
        this.leaderCache        = leaderCache;
        this.offsetManager      = offsetManager;
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  JOIN
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Join a consumer group and warm the leader metadata cache for this topic.
     *
     * <p>After joining, the leader map is fetched from the controller so that
     * every subsequent poll can go directly to the correct leader broker.
     */
    @PostMapping("/groups/{groupId}/join")
    public ResponseEntity<JoinResponse> join(
            @PathVariable String groupId,
            @RequestParam String topic) {

        Map<String, Integer> meta          = brokerClient.getMetadata(topic);
        int                  partitionCount = meta.getOrDefault("partitionCount", 3);

        JoinResponse response = coordinatorClient.join(groupId, new JoinRequest(topic, partitionCount));

        // Cache memberId for auto-rejoin
        memberIdCache.put(groupId + ":" + topic, response.getMemberId());

        // Warm the leader cache — from this point polls go directly to leaders
        leaderCache.refresh(topic);

        log.info("Joined: group={} member={} topic={} partitions={}",
                groupId, response.getMemberId(), topic, response.getAssignedPartitions());

        return ResponseEntity.ok(response);
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  POLL
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Poll events for this member's assigned partitions.
     *
     * <p>Flow:
     * <ol>
     *   <li>Ask coordinator for current partition assignment (also acts as heartbeat).
     *   <li>For each assigned partition, resolve the start offset.
     *   <li>Call the leader broker <em>directly</em> via {@link DirectBrokerClient}.
     *       If the leader has changed (HTTP 409), refresh the cache and retry once.
     *       If the leader URL is unknown (cache miss), fall back to Feign load balancer.
     *   <li>Auto-commit offsets if {@code autoCommit=true}.
     *   <li>Calculate lag per partition.
     * </ol>
     *
     * @param groupId    consumer group name
     * @param memberId   coordinator-assigned memberId from /join
     * @param topic      topic to consume
     * @param strategy   EARLIEST | LATEST | COMMITTED (default)
     * @param limit      max total events to return across all partitions
     * @param autoCommit commit offsets automatically after fetch
     */
    @GetMapping("/groups/{groupId}/poll")
    public ResponseEntity<PollResponse> poll(
            @PathVariable String groupId,
            @RequestParam String memberId,
            @RequestParam String topic,
            @RequestParam(defaultValue = "COMMITTED") OffsetStrategy strategy,
            @RequestParam(defaultValue = "100") int limit,
            @RequestParam(defaultValue = "true") boolean autoCommit) {

        String  cacheKey       = groupId + ":" + topic;
        String  activeMemberId = memberIdCache.getOrDefault(cacheKey, memberId);
        boolean rebalanced     = false;

        // ── 1. Get assignment from coordinator (also acts as heartbeat) ────────
        List<Integer> assignedPartitions;
        try {
            assignedPartitions = coordinatorClient.getAssignment(groupId, activeMemberId, topic);
        } catch (FeignException.NotFound e) {
            log.warn("Member '{}' not found in group '{}' — auto-rejoining", activeMemberId, groupId);
            activeMemberId     = rejoin(groupId, topic, cacheKey);
            assignedPartitions = coordinatorClient.getAssignment(groupId, activeMemberId, topic);
            rebalanced         = true;
        }

        if (assignedPartitions == null || assignedPartitions.isEmpty()) {
            return ResponseEntity.ok(emptyPollResponse(activeMemberId, groupId, topic, rebalanced));
        }

        // ── 2. Ensure leader cache is populated ───────────────────────────────
        leaderCache.refreshIfAbsent(topic);

        // ── 3. Fetch events per partition ─────────────────────────────────────
        List<ConsumerRecord> records          = new ArrayList<>();
        Map<String, Long>    committedOffsets = new LinkedHashMap<>();
        Map<String, Long>    lag             = new LinkedHashMap<>();
        int perPartitionLimit                 = Math.max(1, limit / assignedPartitions.size());

        for (int partition : assignedPartitions) {
            long        startOffset = resolveOffset(strategy, groupId, topic, partition);
            List<Event> events      = fetchFromLeader(topic, partition, startOffset, perPartitionLimit);

            for (Event event : events) {
                records.add(ConsumerRecord.builder()
                        .partition(partition)
                        .offset(event.getOffset())
                        .nextOffset(event.getOffset() + 1)
                        .event(event)
                        .build());
            }

            log.debug("Polled {} events: topic={} partition={} startOffset={}",
                    events.size(), topic, partition, startOffset);

            // ── 4. Auto-commit ─────────────────────────────────────────────
            long nextCommit = startOffset;
            if (autoCommit && !events.isEmpty()) {
                nextCommit = events.get(events.size() - 1).getOffset() + 1;
                offsetManager.commit(groupId, topic, partition, nextCommit);
            }
            committedOffsets.put(String.valueOf(partition), nextCommit);

            // ── 5. Lag ─────────────────────────────────────────────────────
            lag.put(String.valueOf(partition), calculateLag(topic, partition, groupId));
        }

        return ResponseEntity.ok(PollResponse.builder()
                .memberId(activeMemberId)
                .groupId(groupId)
                .topic(topic)
                .assignedPartitions(assignedPartitions)
                .recordCount(records.size())
                .records(records)
                .committedOffsets(committedOffsets)
                .lagPerPartition(lag)
                .rebalanced(rebalanced)
                .pollTimestamp(System.currentTimeMillis())
                .build());
    }

    // ────────────────────────────────────`─────────────────────────────────────
    //  LEAVE
    // ─────────────────────────────────────────────────────────────────────────

    @DeleteMapping("/groups/{groupId}/members/{memberId}")
    public ResponseEntity<String> leaveGroup(
            @PathVariable String groupId,
            @PathVariable String memberId,
            @RequestParam String topic) {

        Map<String, Integer> meta          = brokerClient.getMetadata(topic);
        int                  partitionCount = meta.getOrDefault("partitionCount", 3);

        coordinatorClient.leave(groupId, memberId, topic, partitionCount);
        memberIdCache.remove(groupId + ":" + topic);
        leaderCache.evict(topic);

        return ResponseEntity.ok("Member " + memberId + " left group " + groupId);
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  OFFSETS
    // ─────────────────────────────────────────────────────────────────────────

    @PostMapping("/offsets/commit")
    public ResponseEntity<String> commitOffset(@RequestBody OffsetCommitRequest request) {
        offsetManager.commit(request.getGroupId(), request.getTopic(),
                request.getPartition(), request.getOffset());
        return ResponseEntity.ok("Offset committed");
    }

    @GetMapping("/groups/{groupId}/offsets")
    public Map<String, Long> getOffsets(@PathVariable String groupId) {
        return offsetManager.getGroupOffsets(groupId);
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Private helpers
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Fetch events from the partition leader directly.
     *
     * <ol>
     *   <li>Look up the leader URL from the cache.
     *   <li>Call {@link DirectBrokerClient} → zero extra hops.
     *   <li>On HTTP 409 (leader changed after failover) → refresh cache → retry once.
     *   <li>If leader URL is unknown (cache miss or refresh failed) → fall back to
     *       Feign load balancer ({@link BrokerClient}) — the broker-side forwarding
     *       logic handles the rest.
     * </ol>
     */
    private List<Event> fetchFromLeader(String topic, int partition, long offset, int limit) {
        String leaderUrl = leaderCache.getLeaderUrl(topic, partition);

        if (leaderUrl == null) {
            // Cache miss — fall back to load-balanced Feign call
            log.debug("No cached leader for {}-{} — using load balancer", topic, partition);
            return brokerClient.fetchEvents(topic, partition, offset, limit);
        }

        try {
            return directBrokerClient.fetchEvents(leaderUrl, topic, partition, offset, limit);
        } catch (HttpClientErrorException e) {
            if (e.getStatusCode() == HttpStatus.CONFLICT) {
                // 409 — this broker is no longer the leader (failover happened)
                log.warn("409 from leader {} for {}-{} — refreshing leader cache and retrying",
                        leaderUrl, topic, partition);
                leaderCache.refresh(topic);
                String newLeaderUrl = leaderCache.getLeaderUrl(topic, partition);
                if (newLeaderUrl != null && !newLeaderUrl.equals(leaderUrl)) {
                    return directBrokerClient.fetchEvents(newLeaderUrl, topic, partition, offset, limit);
                }
            }
            // Other error or same URL after refresh — fall back to load balancer
            log.warn("Direct fetch failed for {}-{} ({}), falling back to load balancer",
                    topic, partition, e.getMessage());
            return brokerClient.fetchEvents(topic, partition, offset, limit);
        } catch (Exception e) {
            log.warn("Direct fetch error for {}-{}: {} — falling back to load balancer",
                    topic, partition, e.getMessage());
            return brokerClient.fetchEvents(topic, partition, offset, limit);
        }
    }

    /**
     * Calculates consumer lag for a partition: latestOffset − committedOffset.
     * Uses a direct call to the leader for an accurate high-water mark.
     */
    private long calculateLag(String topic, int partition, String groupId) {
        try {
            long latestOffset;
            String leaderUrl = leaderCache.getLeaderUrl(topic, partition);
            if (leaderUrl != null) {
                latestOffset = directBrokerClient.getLatestOffset(leaderUrl, topic, partition);
            } else {
                latestOffset = brokerClient.getLatestOffset(topic, partition);
            }
            long committed = offsetManager.getCommittedOffset(groupId, topic, partition);
            return Math.max(0, latestOffset - (committed < 0 ? 0 : committed));
        } catch (Exception e) {
            return -1L;
        }
    }

    private long resolveOffset(OffsetStrategy strategy, String groupId, String topic, int partition) {
        return switch (strategy) {
            case EARLIEST -> 0L;
            case LATEST   -> {
                try {
                    String leaderUrl = leaderCache.getLeaderUrl(topic, partition);
                    yield leaderUrl != null
                            ? directBrokerClient.getLatestOffset(leaderUrl, topic, partition)
                            : brokerClient.getLatestOffset(topic, partition);
                } catch (Exception e) {
                    log.warn("Could not get latest offset for {}-{}: {}", topic, partition, e.getMessage());
                    yield 0L;
                }
            }
            case COMMITTED -> {
                long c = offsetManager.getCommittedOffset(groupId, topic, partition);
                yield c < 0 ? 0L : c;
            }
        };
    }

    private String rejoin(String groupId, String topic, String cacheKey) {
        Map<String, Integer> meta          = brokerClient.getMetadata(topic);
        int                  partitionCount = meta.getOrDefault("partitionCount", 3);
        JoinResponse         response       = coordinatorClient.join(groupId,
                                                 new JoinRequest(topic, partitionCount));
        String newMemberId = response.getMemberId();
        memberIdCache.put(cacheKey, newMemberId);
        leaderCache.refresh(topic);
        log.info("Auto-rejoin: group={} newMemberId={} topic={} partitions={}",
                groupId, newMemberId, topic, response.getAssignedPartitions());
        return newMemberId;
    }

    private PollResponse emptyPollResponse(String memberId, String groupId,
                                           String topic, boolean rebalanced) {
        return PollResponse.builder()
                .memberId(memberId)
                .groupId(groupId)
                .topic(topic)
                .assignedPartitions(List.of())
                .recordCount(0)
                .records(List.of())
                .committedOffsets(Map.of())
                .lagPerPartition(Map.of())
                .rebalanced(rebalanced)
                .pollTimestamp(System.currentTimeMillis())
                .build();
    }
}
