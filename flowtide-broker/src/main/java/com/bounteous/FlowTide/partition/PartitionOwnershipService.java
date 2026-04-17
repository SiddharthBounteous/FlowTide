package com.bounteous.FlowTide.partition;

import com.bounteous.FlowTide.client.model.PartitionAssignment;
import com.bounteous.FlowTide.client.model.PartitionAssignment.PartitionRole;
import com.bounteous.FlowTide.client.model.PartitionAssignment.Role;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks which partitions this broker currently leads or follows.
 *
 * <p>Populated at startup via {@link #applyAssignment(PartitionAssignment)}
 * when the broker registers with flowtide-controller.  The controller may
 * push a new assignment at any time (e.g. after a failover) and this service
 * replaces its view atomically.
 *
 * <p>Key used for every map/set: {@code "topic-partition"}  (e.g. "orders-0").
 */
@Service
public class PartitionOwnershipService {

    private static final Logger log = LoggerFactory.getLogger(PartitionOwnershipService.class);

    /** Partitions for which this broker is the LEADER. */
    private volatile Set<String> leaderKeys = Collections.emptySet();

    /** Partitions for which this broker is a FOLLOWER. */
    private volatile Set<String> followerKeys = Collections.emptySet();

    /** Full assignment snapshot from the last controller response. */
    private volatile PartitionAssignment currentAssignment;

    // -----------------------------------------------------------------------
    // Assignment
    // -----------------------------------------------------------------------

    /**
     * Replaces the current ownership view with the one returned by the controller.
     *
     * @param assignment the assignment received from flowtide-controller
     */
    public void applyAssignment(PartitionAssignment assignment) {
        if (assignment == null || assignment.getAssignments() == null) {
            log.warn("Received null or empty assignment — ownership unchanged.");
            return;
        }

        Set<String> newLeaders   = ConcurrentHashMap.newKeySet();
        Set<String> newFollowers = ConcurrentHashMap.newKeySet();

        for (PartitionRole role : assignment.getAssignments()) {
            String key = partitionKey(role.getTopic(), role.getPartition());
            if (role.getRole() == Role.LEADER) {
                newLeaders.add(key);
            } else {
                newFollowers.add(key);
            }
        }

        this.leaderKeys        = newLeaders;
        this.followerKeys      = newFollowers;
        this.currentAssignment = assignment;

        log.info("Ownership updated — leading {} partition(s), following {} partition(s).",
                newLeaders.size(), newFollowers.size());
    }

    // -----------------------------------------------------------------------
    // Queries
    // -----------------------------------------------------------------------

    /**
     * Returns {@code true} if this broker is the current leader for the given
     * topic-partition.
     */
    public boolean isLeader(String topic, int partition) {
        return leaderKeys.contains(partitionKey(topic, partition));
    }

    /**
     * Returns {@code true} if this broker is a follower for the given
     * topic-partition.
     */
    public boolean isFollower(String topic, int partition) {
        return followerKeys.contains(partitionKey(topic, partition));
    }

    /**
     * Returns {@code true} if this broker has any role (leader or follower)
     * for the given topic-partition.
     */
    public boolean owns(String topic, int partition) {
        return isLeader(topic, partition) || isFollower(topic, partition);
    }

    /** Number of partitions this broker currently leads. */
    public int getLeaderPartitionCount() {
        return leaderKeys.size();
    }

    /** Number of partitions this broker currently follows. */
    public int getFollowerPartitionCount() {
        return followerKeys.size();
    }

    /** Returns an unmodifiable view of leader partition keys ("topic-partition"). */
    public Set<String> getLeaderKeys() {
        return Collections.unmodifiableSet(leaderKeys);
    }

    /** Returns the raw assignment last applied (may be {@code null} if not yet registered). */
    public PartitionAssignment getCurrentAssignment() {
        return currentAssignment;
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static String partitionKey(String topic, int partition) {
        return topic + "-" + partition;
    }
}
