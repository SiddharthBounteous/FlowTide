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

        // If we're not the leader and ownership is known → forward to leader
        if (ownershipService.getLeaderPartitionCount() > 0
                && !ownershipService.isLeader(topic, partition)) {
            log.debug("Not leader for {}-{} — forwarding fetch to leader broker.", topic, partition);
            try {
                List<Event> forwarded = forwardingService.forwardFetch(topic, partition, offset, limit);
                return ResponseEntity.ok(forwarded);
            } catch (PartitionRoutingException ex) {
                log.error("Forward failed: {} — falling back to local log.", ex.getMessage());
                // Fall through to local read
            }
        }

        List<Event> events = logManager.getLog(topic, partition).readFrom(offset, limit);
        log.debug("Fetched {} events: topic={} partition={} offset={}", events.size(), topic, partition, offset);
        return ResponseEntity.ok(events);
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Replication  (leader serves this; followers poll it)
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Returns up to {@code limit} events starting at {@code fromOffset} for the
     * given topic-partition.
     *
     * <p>This endpoint is called exclusively by follower brokers running
     * {@link com.bounteous.FlowTide.replication.ReplicationPoller}.  It is
     * functionally identical to the consumer fetch but lives under a separate
     * path so it's easy to apply different rate-limits or auth in the future.
     *
     * <p>The gateway does NOT expose /internal/** — replication traffic stays
     * inside the cluster.
     */
    @GetMapping("/replicate/{topic}/{partition}")
    public ResponseEntity<List<Event>> replicateFetch(
            @PathVariable String topic,
            @PathVariable int    partition,
            @RequestParam(defaultValue = "0")   long fromOffset,
            @RequestParam(defaultValue = "500") int  limit) {

        if (!ownershipService.isLeader(topic, partition)
                && ownershipService.getLeaderPartitionCount() > 0) {
            // Safety guard: only the leader should serve replication reads.
            // Return 409 so the follower knows to re-query the controller for the real leader.
            log.warn("Replication request for {}-{} arrived at non-leader broker.", topic, partition);
            return ResponseEntity.status(HttpStatus.CONFLICT).build();
        }

        List<Event> events = logManager.getLog(topic, partition).readFrom(fromOffset, limit);
        log.trace("Replication fetch: topic={} partition={} fromOffset={} returned={}",
                topic, partition, fromOffset, events.size());
        return ResponseEntity.ok(events);
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Metadata
    // ─────────────────────────────────────────────────────────────────────────

    @GetMapping("/metadata/{topic}")
    public Map<String, Integer> getMetadata(@PathVariable String topic) {
        int count = logManager.getTopicLogs(topic).size();
        if (count == 0) {
            // Auto-create default partitions on first metadata request
            int defaultPartitions = 3;
            for (int i = 0; i < defaultPartitions; i++) logManager.getLog(topic, i);
            count = defaultPartitions;
        }
        return Map.of("partitionCount", count);
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
