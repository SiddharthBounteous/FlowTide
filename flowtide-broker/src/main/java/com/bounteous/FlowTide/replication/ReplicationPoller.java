package com.bounteous.FlowTide.replication;

import com.bounteous.FlowTide.client.ControllerClient;
import com.bounteous.FlowTide.model.Event;
import com.bounteous.FlowTide.partition.PartitionOwnershipService;
import com.bounteous.FlowTide.server.ServerPortProvider;
import com.bounteous.FlowTide.server.log.LogManager;
import com.bounteous.FlowTide.server.log.PartitionLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Follower catch-up replication loop.
 *
 * <h3>Role in the two-phase replication model</h3>
 * <pre>
 * Phase 1 — Catch-up (this poller):
 *   Follower is behind → pulls missing events from leader via GET /internal/replicate
 *   Once follower reaches leader offset → calls ISRManager.markInSync()
 *   → Follower is added to ISR → receives synchronous pushes from now on
 *
 * Phase 2 — In-sync (ISRManager push):
 *   Leader pushes every new event to ISR followers directly
 *   This poller becomes a no-op for caught-up partitions
 *   (It still runs but finds no new events to fetch)
 * </pre>
 *
 * <h3>Re-catch-up after ISR eviction</h3>
 * If a follower is removed from ISR (missed an ISR push), it falls back to
 * Phase 1 automatically — this poller will detect the lag and pull to catch up.
 */
@Component
public class ReplicationPoller {

    private static final Logger log = LoggerFactory.getLogger(ReplicationPoller.class);

    private final PartitionOwnershipService ownershipService;
    private final ControllerClient          controllerClient;
    private final LeaderFetchClient         leaderFetchClient;
    private final LogManager                logManager;
    private final ISRManager                isrManager;
    private final ServerPortProvider        portProvider;

    @Value("${server.host:localhost}")
    private String selfHost;

    public ReplicationPoller(PartitionOwnershipService ownershipService,
                             ControllerClient controllerClient,
                             LeaderFetchClient leaderFetchClient,
                             LogManager logManager,
                             ISRManager isrManager,
                             ServerPortProvider portProvider) {
        this.ownershipService  = ownershipService;
        this.controllerClient  = controllerClient;
        this.leaderFetchClient = leaderFetchClient;
        this.logManager        = logManager;
        this.isrManager        = isrManager;
        this.portProvider      = portProvider;
    }

    @Scheduled(fixedDelayString = "${kafka.replication.poll-interval-ms:1000}")
    public void poll() {
        for (String key : ownershipService.getFollowerKeys()) {
            try {
                pollPartition(key);
            } catch (Exception e) {
                log.error("Replication error [{}]: {}", key, e.getMessage());
            }
        }
    }

    // -------------------------------------------------------------------------

    private void pollPartition(String partitionKey) {
        int lastDash = partitionKey.lastIndexOf('-');
        if (lastDash < 0) return;

        String topic    = partitionKey.substring(0, lastDash);
        int    partition;
        try {
            partition = Integer.parseInt(partitionKey.substring(lastDash + 1));
        } catch (NumberFormatException e) {
            log.warn("Cannot parse partition from key: {}", partitionKey);
            return;
        }

        PartitionLog localLog   = logManager.getLog(topic, partition);
        long         myOffset   = localLog.latestOffset();

        // Resolve leader
        String leaderId;
        try {
            leaderId = controllerClient.getLeader(topic, partition);
        } catch (Exception e) {
            log.warn("Cannot resolve leader for {}-{}: {}", topic, partition, e.getMessage());
            return;
        }
        if (leaderId == null || leaderId.isBlank()) return;

        // Pull any events the leader has that we don't
        List<Event> newEvents = leaderFetchClient.fetch(leaderId, topic, partition, myOffset);

        if (newEvents.isEmpty()) {
            // Already caught up — report in-sync to ISRManager so we join ISR
            reportCaughtUp(leaderId, topic, partition, localLog);
            return;
        }

        // Append missed events
        for (Event event : newEvents) {
            localLog.replicateAppend(event);
        }

        log.debug("Catch-up replicated {} event(s) to {}-{} (offsets {}-{})",
                newEvents.size(), topic, partition,
                newEvents.get(0).getOffset(),
                newEvents.get(newEvents.size() - 1).getOffset());

        // Check again after appending — if now equal, we're caught up
        reportCaughtUp(leaderId, topic, partition, localLog);
    }

    /**
     * Asks the leader for its current high-water mark and compares with ours.
     * If equal, calls {@link ISRManager#markInSync} to join the ISR.
     */
    private void reportCaughtUp(String leaderId, String topic, int partition, PartitionLog localLog) {
        try {
            long leaderOffset = leaderFetchClient.getLatestOffset(leaderId, topic, partition);
            if (localLog.latestOffset() >= leaderOffset) {
                String selfId = selfHost + ":" + portProvider.getPort();
                isrManager.markInSync(selfId, topic, partition);
                log.trace("Caught up {}-{} offset={} → joined ISR", topic, partition, localLog.latestOffset());
            }
        } catch (Exception e) {
            log.warn("Could not verify catch-up for {}-{}: {}", topic, partition, e.getMessage());
        }
    }
}
