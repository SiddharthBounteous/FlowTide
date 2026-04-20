package com.bounteous.FlowTide.replication;

import com.bounteous.FlowTide.client.ControllerClient;
import com.bounteous.FlowTide.model.Event;
import com.bounteous.FlowTide.partition.PartitionOwnershipService;
import com.bounteous.FlowTide.server.ServerPortProvider;
import com.bounteous.FlowTide.server.log.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-Sync Replica (ISR) manager — runs on the LEADER broker only.
 *
 * <h3>What ISR means</h3>
 * The ISR is the set of replicas (leader + followers) that are fully caught up
 * with the leader's log.  Only ISR members count toward the write acknowledgement.
 *
 * <h3>Two-phase replication model</h3>
 * <pre>
 * Phase 1 — Catch-up (follower is behind):
 *   ReplicationPoller pulls events from leader via GET /internal/replicate
 *   Once follower reaches leader offset → reports catch-up → joins ISR
 *
 * Phase 2 — In-sync (follower is in ISR):
 *   Every new event is PUSHED synchronously to all ISR followers
 *   Leader waits for ACK before returning success to producer
 *   If follower doesn't ACK within timeout → removed from ISR → falls back to Phase 1
 * </pre>
 *
 * <h3>Durability guarantee</h3>
 * With {@code kafka.replication.min-isr=2}, a write is only acknowledged when
 * at least 2 replicas (leader + 1 follower) have the event in memory.
 * If only 1 replica is available, writes are rejected (unless min-isr=1).
 */
@Component
public class ISRManager {

    private static final Logger log = LoggerFactory.getLogger(ISRManager.class);

    /**
     * ISR per partition key ("topic-partition").
     * Leader is always implicitly in the ISR — only follower IDs are stored here.
     * e.g. "orders-1" → {"localhost:8084", "localhost:8085"}
     */
    private final ConcurrentHashMap<String, Set<String>> isrFollowers = new ConcurrentHashMap<>();

    private final ISRReplicationClient      replicationClient;
    private final ControllerClient          controllerClient;
    private final PartitionOwnershipService ownershipService;
    private final LogManager                logManager;
    private final ServerPortProvider        portProvider;

    @Value("${kafka.replication.min-isr:1}")
    private int minISR;

    @Value("${kafka.replication.ack-timeout-ms:2000}")
    private long ackTimeoutMs;

    @Value("${server.host:localhost}")
    private String selfHost;

    public ISRManager(ISRReplicationClient replicationClient,
                      ControllerClient controllerClient,
                      PartitionOwnershipService ownershipService,
                      LogManager logManager,
                      ServerPortProvider portProvider) {
        this.replicationClient = replicationClient;
        this.controllerClient  = controllerClient;
        this.ownershipService  = ownershipService;
        this.logManager        = logManager;
        this.portProvider      = portProvider;
    }

    // -------------------------------------------------------------------------
    //  Called by ProducerService after every local append
    // -------------------------------------------------------------------------

    /**
     * Pushes a newly appended event to all in-sync followers synchronously.
     *
     * <p>Called by the leader immediately after appending to its local log.
     * Waits for ACK from each ISR follower.  Followers that don't respond
     * within {@code ack-timeout-ms} are removed from ISR.
     *
     * <p>If the number of remaining ISR members (including leader itself)
     * drops below {@code min-isr}, a {@link ISRException} is thrown and
     * the write should be rejected.
     *
     * @param topic     topic name
     * @param partition partition index
     * @param event     the event just appended by the leader
     * @throws ISRException if ISR size < min-isr after failed ACKs
     */
    public void replicateToISR(String topic, int partition, Event event) {
        String key = partitionKey(topic, partition);

        Set<String> followers = isrFollowers.getOrDefault(key, Collections.emptySet());
        if (followers.isEmpty()) {
            // No followers in ISR — single broker mode, nothing to push
            return;
        }

        List<String> toRemove = new ArrayList<>();

        for (String followerId : new ArrayList<>(followers)) {
            try {
                replicationClient.push(followerId, topic, partition,
                        List.of(event), ackTimeoutMs);
                log.trace("ISR ACK: follower={} topic={} partition={} offset={}",
                        followerId, topic, partition, event.getOffset());
            } catch (Exception e) {
                log.warn("ISR ACK FAILED: follower={} topic={} partition={} — removing from ISR: {}",
                        followerId, topic, partition, e.getMessage());
                toRemove.add(followerId);
            }
        }

        // Remove unresponsive followers from ISR
        if (!toRemove.isEmpty()) {
            toRemove.forEach(id -> {
                followers.remove(id);
                reportISRChange(topic, partition);
            });

            // Check min-isr: +1 for the leader itself
            int isrSize = followers.size() + 1;
            if (isrSize < minISR) {
                throw new ISRException(String.format(
                        "ISR size %d < min-isr %d for %s-%d — write rejected for durability",
                        isrSize, minISR, topic, partition));
            }
        }
    }

    // -------------------------------------------------------------------------
    //  Called by ReplicationPoller when follower catches up
    // -------------------------------------------------------------------------

    /**
     * Called by the follower's {@link ReplicationPoller} when the follower's
     * local log offset matches the leader's offset — meaning it is fully caught up.
     *
     * <p>The follower is added to the ISR and will receive synchronous pushes
     * for all future events.
     *
     * @param followerId  the follower's "host:port"
     * @param topic       topic name
     * @param partition   partition index
     */
    public void markInSync(String followerId, String topic, int partition) {
        if (!ownershipService.isLeader(topic, partition)) return;  // only leader manages ISR

        String key = partitionKey(topic, partition);
        isrFollowers.computeIfAbsent(key, k -> ConcurrentHashMap.newKeySet()).add(followerId);
        reportISRChange(topic, partition);
        log.info("ISR JOIN: follower={} joined ISR for {}-{}", followerId, topic, partition);
    }

    /**
     * Removes a follower from ISR (e.g. when it is declared dead by failover).
     */
    public void removeFromISR(String followerId, String topic, int partition) {
        String key = partitionKey(topic, partition);
        Set<String> followers = isrFollowers.get(key);
        if (followers != null) {
            followers.remove(followerId);
            reportISRChange(topic, partition);
            log.info("ISR LEAVE: follower={} removed from ISR for {}-{}", followerId, topic, partition);
        }
    }

    // -------------------------------------------------------------------------
    //  Query
    // -------------------------------------------------------------------------

    /**
     * Returns current ISR follower IDs for a partition (leader not included).
     */
    public Set<String> getISRFollowers(String topic, int partition) {
        return Collections.unmodifiableSet(
                isrFollowers.getOrDefault(partitionKey(topic, partition), Collections.emptySet()));
    }

    /** Returns ISR size including the leader itself (+1). */
    public int getISRSize(String topic, int partition) {
        return getISRFollowers(topic, partition).size() + 1;
    }

    // -------------------------------------------------------------------------
    //  Private
    // -------------------------------------------------------------------------

    private void reportISRChange(String topic, int partition) {
        try {
            Set<String> current  = isrFollowers.getOrDefault(partitionKey(topic, partition), Set.of());
            String      selfId   = selfHost + ":" + portProvider.getPort();
            controllerClient.updateISR(topic, partition, selfId, new ArrayList<>(current));
        } catch (Exception e) {
            log.warn("Failed to report ISR change to controller for {}-{}: {}", topic, partition, e.getMessage());
        }
    }

    private static String partitionKey(String topic, int partition) {
        return topic + "-" + partition;
    }
}
