package com.bounteous.FlowTide.replication;

import com.bounteous.FlowTide.client.ControllerClient;
import com.bounteous.FlowTide.model.Event;
import com.bounteous.FlowTide.partition.PartitionOwnershipService;
import com.bounteous.FlowTide.server.log.LogManager;
import com.bounteous.FlowTide.server.log.PartitionLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Follower-side replication loop.
 *
 * <h3>What it does</h3>
 * <ol>
 *   <li>Every {@code kafka.replication.poll-interval-ms} milliseconds (default 1 s),
 *       iterates over all partitions this broker <em>follows</em>.
 *   <li>For each follower partition it asks flowtide-controller for the current leader.
 *   <li>Calls the leader's {@code GET /internal/replicate/{topic}/{partition}?fromOffset=…}
 *       endpoint via {@link LeaderFetchClient}.
 *   <li>Appends received events to the local {@link PartitionLog} using
 *       {@link PartitionLog#replicateAppend} so the leader-assigned offsets are preserved.
 * </ol>
 *
 * <h3>When the poller is idle</h3>
 * If this broker has no follower partitions (e.g. it's the only broker, or it
 * owns only leader partitions), the scheduled method is a no-op.
 *
 * <h3>Error handling</h3>
 * Any failure for one partition is logged and skipped — the next poll will
 * retry.  Failures never block replication of other partitions.
 */
@Component
public class ReplicationPoller {

    private static final Logger log = LoggerFactory.getLogger(ReplicationPoller.class);

    private final PartitionOwnershipService ownershipService;
    private final ControllerClient          controllerClient;
    private final LeaderFetchClient         leaderFetchClient;
    private final LogManager                logManager;

    public ReplicationPoller(PartitionOwnershipService ownershipService,
                             ControllerClient controllerClient,
                             LeaderFetchClient leaderFetchClient,
                             LogManager logManager) {
        this.ownershipService  = ownershipService;
        this.controllerClient  = controllerClient;
        this.leaderFetchClient = leaderFetchClient;
        this.logManager        = logManager;
    }

    @Scheduled(fixedDelayString = "${kafka.replication.poll-interval-ms:1000}")
    public void poll() {
        // followerKeys are strings like "orders-2", "payments-0"
        for (String key : ownershipService.getFollowerKeys()) {
            try {
                pollPartition(key);
            } catch (Exception e) {
                log.error("Unexpected error replicating partition [{}]: {}", key, e.getMessage());
            }
        }
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    private void pollPartition(String partitionKey) {
        // Parse "topic-partition" — topic names can contain hyphens, so split from the right
        int lastDash = partitionKey.lastIndexOf('-');
        if (lastDash < 0) {
            log.warn("Unrecognisable partition key: {}", partitionKey);
            return;
        }
        String topic    = partitionKey.substring(0, lastDash);
        int    partition;
        try {
            partition = Integer.parseInt(partitionKey.substring(lastDash + 1));
        } catch (NumberFormatException e) {
            log.warn("Could not parse partition index from key: {}", partitionKey);
            return;
        }

        // Current high-water mark on this follower
        PartitionLog localLog  = logManager.getLog(topic, partition);
        long         fromOffset = localLog.latestOffset();   // next expected offset

        // Resolve the current leader from the controller
        String leaderId;
        try {
            leaderId = controllerClient.getLeader(topic, partition);
        } catch (Exception e) {
            log.warn("Cannot resolve leader for {}-{}: {}", topic, partition, e.getMessage());
            return;
        }

        if (leaderId == null || leaderId.isBlank()) {
            log.warn("Controller returned no leader for {}-{} — skipping.", topic, partition);
            return;
        }

        // Fetch new events from leader
        List<Event> newEvents = leaderFetchClient.fetch(leaderId, topic, partition, fromOffset);
        if (newEvents.isEmpty()) {
            log.trace("No new events for {}-{} (fromOffset={})", topic, partition, fromOffset);
            return;
        }

        // Append with leader-assigned offsets preserved
        int appended = 0;
        for (Event event : newEvents) {
            localLog.replicateAppend(event);
            appended++;
        }

        log.debug("Replicated {} event(s) to {}-{} (offsets {}-{})",
                appended, topic, partition,
                newEvents.get(0).getOffset(),
                newEvents.get(newEvents.size() - 1).getOffset());
    }
}
