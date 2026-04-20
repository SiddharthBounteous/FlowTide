package com.bounteous.FlowTide.controller;

import com.bounteous.FlowTide.client.BrokerClient;
import com.bounteous.FlowTide.client.CoordinatorClient;
import com.bounteous.FlowTide.consumer.OffsetManager;
import com.bounteous.FlowTide.consumer.OffsetStrategy;
import com.bounteous.FlowTide.model.Event;
import com.bounteous.FlowTide.model.JoinRequest;
import com.bounteous.FlowTide.model.JoinResponse;
import com.bounteous.FlowTide.model.OffsetCommitRequest;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

/**
 * Consumer group API.
 *
 * <h3>Usage flow</h3>
 * <pre>
 * 1. POST /api/consumer/groups/{groupId}/join?topic=orders
 *    ← { memberId: "orders-grp-a3f9d2", assignedPartitions: [0, 1] }
 *       Store this memberId — use it for every poll and leave call.
 *
 * 2. GET /api/consumer/groups/{groupId}/poll
 *       ?memberId=orders-grp-a3f9d2&topic=orders
 *    ← [ { event... }, ... ]
 *
 * 3. DELETE /api/consumer/groups/{groupId}/members/{memberId}?topic=orders
 *    ← "Member ... left group ..."
 * </pre>
 *
 * <p>Group membership and partition assignment are managed by the
 * flowtide-controller coordinator — not locally.  This means multiple
 * instances of flowtide-consumer can run without conflicting assignments.
 */
@RestController
@RequestMapping("/api/consumer")
public class ConsumerController {

    private static final Logger log = LoggerFactory.getLogger(ConsumerController.class);

    private final CoordinatorClient coordinatorClient;
    private final BrokerClient      brokerClient;
    private final OffsetManager     offsetManager;

    public ConsumerController(CoordinatorClient coordinatorClient,
                              BrokerClient brokerClient,
                              OffsetManager offsetManager) {
        this.coordinatorClient = coordinatorClient;
        this.brokerClient      = brokerClient;
        this.offsetManager     = offsetManager;
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  JOIN
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Join a consumer group.
     *
     * <p>Delegates to the controller coordinator which generates a unique
     * memberId and assigns partitions.  The caller must store the returned
     * memberId and use it for all subsequent poll and leave calls.
     */
    @PostMapping("/groups/{groupId}/join")
    public ResponseEntity<JoinResponse> join(
            @PathVariable String groupId,
            @RequestParam String topic) {

        Map<String, Integer> meta           = brokerClient.getMetadata(topic);
        int                  partitionCount = meta.getOrDefault("partitionCount", 3);

        JoinResponse response = coordinatorClient.join(groupId, new JoinRequest(topic, partitionCount));

        log.info("Joined: group={} member={} topic={} partitions={}",
                groupId, response.getMemberId(), topic, response.getAssignedPartitions());

        return ResponseEntity.ok(response);
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  POLL
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Poll events for the partitions assigned to this member.
     *
     * <p>Gets the current partition assignment from the coordinator
     * (this also refreshes the member's heartbeat), then fetches events
     * from the broker for each assigned partition.
     *
     * @param groupId    consumer group name
     * @param memberId   coordinator-assigned memberId from /join
     * @param topic      topic to consume
     * @param strategy   EARLIEST | LATEST | COMMITTED (default)
     * @param limit      max total events to return
     * @param autoCommit commit offsets automatically after fetch
     */
    @GetMapping("/groups/{groupId}/poll")
    public ResponseEntity<List<Event>> poll(
            @PathVariable String groupId,
            @RequestParam String memberId,
            @RequestParam String topic,
            @RequestParam(defaultValue = "COMMITTED") OffsetStrategy strategy,
            @RequestParam(defaultValue = "100") int limit,
            @RequestParam(defaultValue = "true") boolean autoCommit) {

        // Fetch assignment from coordinator (also acts as heartbeat)
        List<Integer> assignedPartitions =
                coordinatorClient.getAssignment(groupId, memberId, topic);

        if (assignedPartitions == null || assignedPartitions.isEmpty()) {
            return ResponseEntity.ok(Collections.emptyList());
        }

        List<Event> results           = new ArrayList<>();
        int         perPartitionLimit = Math.max(1, limit / assignedPartitions.size());

        for (int partition : assignedPartitions) {
            long startOffset = resolveOffset(strategy, groupId, topic, partition);

            List<Event> events = brokerClient.fetchEvents(topic, partition, startOffset, perPartitionLimit);
            results.addAll(events);

            log.debug("Polled {} events: topic={} partition={} offset={}",
                    events.size(), topic, partition, startOffset);

            if (autoCommit && !events.isEmpty()) {
                long next = events.get(events.size() - 1).getOffset() + 1;
                offsetManager.commit(groupId, topic, partition, next);
            }
        }

        return ResponseEntity.ok(results);
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  LEAVE
    // ─────────────────────────────────────────────────────────────────────────

    @DeleteMapping("/groups/{groupId}/members/{memberId}")
    public ResponseEntity<String> leaveGroup(
            @PathVariable String groupId,
            @PathVariable String memberId,
            @RequestParam String topic) {

        Map<String, Integer> meta           = brokerClient.getMetadata(topic);
        int                  partitionCount = meta.getOrDefault("partitionCount", 3);

        coordinatorClient.leave(groupId, memberId, topic, partitionCount);
        return ResponseEntity.ok("Member " + memberId + " left group " + groupId);
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  OFFSETS
    // ─────────────────────────────────────────────────────────────────────────

    @PostMapping("/offsets/commit")
    public ResponseEntity<String> commitOffset(@RequestBody @Valid OffsetCommitRequest request) {
        offsetManager.commit(request.getGroupId(), request.getTopic(),
                request.getPartition(), request.getOffset());
        return ResponseEntity.ok("Offset committed");
    }

    @GetMapping("/groups/{groupId}/offsets")
    public Map<String, Long> getOffsets(@PathVariable String groupId) {
        return offsetManager.getGroupOffsets(groupId);
    }

    // ─────────────────────────────────────────────────────────────────────────

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
