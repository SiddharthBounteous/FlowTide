package com.bounteous.FlowTide.controller;

import com.bounteous.FlowTide.model.Event;
import com.bounteous.FlowTide.model.PublishRequest;
import com.bounteous.FlowTide.model.PublishResponse;
import com.bounteous.FlowTide.partition.BrokerForwardingService;
import com.bounteous.FlowTide.partition.PartitionOwnershipService;
import com.bounteous.FlowTide.partition.PartitionRoutingException;
import com.bounteous.FlowTide.producer.ProducerService;
import com.bounteous.FlowTide.server.log.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * Internal HTTP API used exclusively by flowtide-producer and flowtide-consumer.
 *
 * <p>The gateway does NOT expose /internal/** to the outside world.
 *
 * <h3>Partition routing</h3>
 * <ul>
 *   <li>If this broker is the <b>leader</b> for the target partition → handle locally.
 *   <li>If this broker is <b>not</b> the leader → ask flowtide-controller for the leader's
 *       address and transparently forward the request there via
 *       {@link BrokerForwardingService}.
 *   <li>If ownership is unknown (controller unreachable at startup) → handle locally
 *       as a graceful degradation so the broker still works in single-node mode.
 * </ul>
 *
 * <ul>
 *   <li>POST /internal/events              — publish an event
 *   <li>GET  /internal/events/{topic}/{p}  — fetch events from a partition
 *   <li>GET  /internal/metadata/{topic}    — partition count for a topic
 * </ul>
 */
@RestController
@RequestMapping("/internal")
public class InternalBrokerController {

    private static final Logger log = LoggerFactory.getLogger(InternalBrokerController.class);

    private final ProducerService          producerService;
    private final LogManager               logManager;
    private final PartitionOwnershipService ownershipService;
    private final BrokerForwardingService  forwardingService;

    public InternalBrokerController(ProducerService producerService,
                                    LogManager logManager,
                                    PartitionOwnershipService ownershipService,
                                    BrokerForwardingService forwardingService) {
        this.producerService  = producerService;
        this.logManager       = logManager;
        this.ownershipService = ownershipService;
        this.forwardingService = forwardingService;
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Produce
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Publish an event.
     *
     * <p>The target partition is resolved by {@link ProducerService} (key-hash or
     * round-robin), so we first compute it via a dry-run helper, then decide
     * whether to handle locally or forward.
     */
    @PostMapping("/events")
    public ResponseEntity<PublishResponse> storeEvent(@RequestBody PublishRequest request) {
        // Resolve which partition this event will land on
        int partitionCount = resolvePartitionCount(request.getTopic());
        int partition      = selectPartition(request.getKey(), partitionCount);

        // If we're not the leader and ownership is known → forward
        if (ownershipService.getLeaderPartitionCount() > 0
                && !ownershipService.isLeader(request.getTopic(), partition)) {
            log.debug("Not leader for {}-{} — forwarding publish to leader broker.",
                    request.getTopic(), partition);
            try {
                PublishResponse forwarded = forwardingService.forwardPublish(
                        request.getTopic(), partition, request);
                return ResponseEntity.ok(forwarded);
            } catch (PartitionRoutingException ex) {
                log.error("Forward failed: {} — falling back to local handling.", ex.getMessage());
                // Fall through to local handling as last resort
            }
        }
        return ResponseEntity.ok(producerService.publish(request));
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Fetch
    // ─────────────────────────────────────────────────────────────────────────

    @GetMapping("/events/{topic}/{partition}")
    public ResponseEntity<List<Event>> fetchEvents(
            @PathVariable String topic,
            @PathVariable int    partition,
            @RequestParam(defaultValue = "0")   long offset,
            @RequestParam(defaultValue = "100") int  limit) {

        // Reject fetches for topics that have been deleted.
        // Without this guard, logManager.getLog() would silently recreate a
        // PartitionLog for the deleted topic, making it appear alive again.
        if (!logManager.topicExists(topic)) {
            return ResponseEntity.notFound().build();
        }

        // If we're not the leader and ownership is known → forward to leader
        if (ownershipService.getLeaderPartitionCount() > 0
                && !ownershipService.isLeader(topic, partition)) {
            log.debug("Not leader for {}-{} — forwarding fetch to leader broker.", topic, partition);
            try {
                List<Event> forwarded = forwardingService.forwardFetch(topic, partition, offset, limit);
                return ResponseEntity.ok(forwarded);
            } catch (PartitionRoutingException ex) {
                log.error("Forward failed: {} — falling back to local log.", ex.getMessage());
            }
        }

        List<Event> events = logManager.getLog(topic, partition).readFrom(offset, limit);
        log.debug("Fetched {} events: topic={} partition={} offset={}", events.size(), topic, partition, offset);
        return ResponseEntity.ok(events);
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Replication  (GET = follower catch-up pull,  POST = ISR push receive)
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * GET — Follower catch-up pull.
     * Returns events starting at {@code fromOffset}.  Called by
     * {@link com.bounteous.FlowTide.replication.ReplicationPoller} when a follower
     * is behind and needs to catch up before joining the ISR.
     */
    @GetMapping("/replicate/{topic}/{partition}")
    public ResponseEntity<List<Event>> replicateFetch(
            @PathVariable String topic,
            @PathVariable int    partition,
            @RequestParam(defaultValue = "0")   long fromOffset,
            @RequestParam(defaultValue = "500") int  limit) {

        if (!ownershipService.isLeader(topic, partition)
                && ownershipService.getLeaderPartitionCount() > 0) {
            log.warn("Catch-up fetch for {}-{} arrived at non-leader broker.", topic, partition);
            return ResponseEntity.status(HttpStatus.CONFLICT).build();
        }

        List<Event> events = logManager.getLog(topic, partition).readFrom(fromOffset, limit);
        log.trace("Catch-up fetch: topic={} partition={} fromOffset={} returned={}",
                topic, partition, fromOffset, events.size());
        return ResponseEntity.ok(events);
    }

    /**
     * GET offset — Returns this broker's current high-water mark for a partition.
     * Used by followers to detect when they are fully caught up with the leader.
     */
    @GetMapping("/replicate/{topic}/{partition}/offset")
    public ResponseEntity<Long> getReplicationOffset(
            @PathVariable String topic,
            @PathVariable int    partition) {
        long offset = logManager.getLog(topic, partition).latestOffset();
        return ResponseEntity.ok(offset);
    }

    /**
     * POST — ISR push receive.
     * Called by the leader to synchronously push new events to this follower
     * as part of ISR replication.  Returns "ACK" immediately after appending.
     */
    @PostMapping("/replicate/{topic}/{partition}")
    public ResponseEntity<String> receiveISRPush(
            @PathVariable String topic,
            @PathVariable int    partition,
            @RequestBody List<Event> events) {

        for (Event event : events) {
            logManager.getLog(topic, partition).replicateAppend(event);
        }
        log.trace("ISR push received: topic={} partition={} count={}", topic, partition, events.size());
        return ResponseEntity.ok("ACK");
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Metadata
    // ─────────────────────────────────────────────────────────────────────────

    @GetMapping("/metadata/{topic}")
    public ResponseEntity<Map<String, Integer>> getMetadata(@PathVariable String topic) {
        // Only serve metadata for topics that are explicitly registered.
        // Do NOT auto-create via getLog() here — that would silently resurrect
        // deleted topics, making topicExists() return true again after a delete.
        if (!logManager.topicExists(topic)) {
            return ResponseEntity.notFound().build();
        }
        int count = logManager.getTopicLogs(topic).size();
        return ResponseEntity.ok(Map.of("partitionCount", count == 0 ? 3 : count));
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Exception handler
    // ─────────────────────────────────────────────────────────────────────────

    @ExceptionHandler(PartitionRoutingException.class)
    public ResponseEntity<String> handleRoutingError(PartitionRoutingException ex) {
        log.error("Partition routing error: {}", ex.getMessage());
        return ResponseEntity.status(HttpStatus.BAD_GATEWAY)
                .body("Partition routing failed: " + ex.getMessage());
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Helpers  (mirrors ProducerService logic so we can peek at partition
    //            before committing to local vs forward path)
    // ─────────────────────────────────────────────────────────────────────────

    private int resolvePartitionCount(String topic) {
        int count = logManager.getTopicLogs(topic).size();
        return count > 0 ? count : 3;   // default matches ProducerService
    }

    private int selectPartition(String key, int partitionCount) {
        if (key == null || key.isBlank()) {
            return 0;   // round-robin starts at 0 for routing purposes; ProducerService handles true RR
        }
        int hash = key.hashCode();
        return Math.abs(hash % partitionCount);
    }
}
