package com.bounteous.flowtide.controller.service;

import com.bounteous.flowtide.controller.model.ConsumerGroupState;
import com.bounteous.flowtide.controller.model.ConsumerMember;
import com.bounteous.flowtide.controller.model.JoinResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Consumer group coordinator — the single source of truth for group membership
 * and partition assignment across all consumer service instances.
 *
 * <h3>Why this lives in the controller</h3>
 * If ConsumerGroupManager lived in flowtide-consumer, each consumer service
 * instance would have its own isolated in-memory state.  Two instances serving
 * the same group would assign overlapping partitions, causing duplicate reads.
 * By moving coordination here, all consumer service instances talk to one place.
 *
 * <h3>Lifecycle</h3>
 * <pre>
 * Consumer calls JOIN   → coordinator generates memberId, assigns partitions
 * Consumer calls POLL   → consumer reads assignment from coordinator
 * Consumer calls HB     → coordinator resets member timeout timer
 * HealthCheckScheduler  → evicts members that miss heartbeats → rebalance
 * Consumer calls LEAVE  → member removed, group rebalanced
 * </pre>
 */
@Service
public class ConsumerGroupService {

    private static final Logger log = LoggerFactory.getLogger(ConsumerGroupService.class);

    /** "groupId:topic" → ConsumerGroupState */
    private final ConcurrentHashMap<String, ConsumerGroupState> groups = new ConcurrentHashMap<>();

    // -------------------------------------------------------------------------
    //  Join
    // -------------------------------------------------------------------------

    /**
     * Registers a new consumer member in a group.
     *
     * <p>Generates a unique memberId, adds the member, rebalances partitions
     * across all active members, and returns the assignment.
     *
     * @param groupId        consumer group name
     * @param topic          topic to consume
     * @param partitionCount total partitions for the topic
     * @return JoinResponse with coordinator-generated memberId and assigned partitions
     */
    public JoinResponse join(String groupId, String topic, int partitionCount) {
        String memberId = generateMemberId(groupId);
        String key      = groupKey(groupId, topic);

        ConsumerGroupState group  = groups.computeIfAbsent(key, k -> new ConsumerGroupState(groupId, topic));
        ConsumerMember     member = new ConsumerMember(memberId, groupId, topic);

        group.addMember(member);
        rebalance(group, partitionCount);

        log.info("JOIN: group={} member={} topic={} assignedPartitions={}",
                groupId, memberId, topic, member.getAssignedPartitions());

        return new JoinResponse(memberId, groupId, topic, member.getAssignedPartitions());
    }

    // -------------------------------------------------------------------------
    //  Heartbeat
    // -------------------------------------------------------------------------

    /**
     * Refreshes a member's heartbeat timestamp.
     * Called by the consumer service on every poll or explicit heartbeat request.
     *
     * @throws IllegalArgumentException if memberId is unknown
     */
    public void heartbeat(String groupId, String memberId, String topic) {
        ConsumerMember member = getMemberOrThrow(groupId, memberId, topic);
        member.updateHeartbeat();
        log.trace("HB: group={} member={}", groupId, memberId);
    }

    // -------------------------------------------------------------------------
    //  Get assignment
    // -------------------------------------------------------------------------

    /**
     * Returns the partitions assigned to a member.
     * Also refreshes the member's heartbeat (every poll counts as alive).
     *
     * @throws IllegalArgumentException if memberId is unknown
     */
    public List<Integer> getAssignment(String groupId, String memberId, String topic) {
        ConsumerMember member = getMemberOrThrow(groupId, memberId, topic);
        member.updateHeartbeat();
        return member.getAssignedPartitions();
    }

    // -------------------------------------------------------------------------
    //  Leave
    // -------------------------------------------------------------------------

    /**
     * Removes a member from a group and rebalances remaining members.
     */
    public void leave(String groupId, String memberId, String topic, int partitionCount) {
        String             key   = groupKey(groupId, topic);
        ConsumerGroupState group = groups.get(key);
        if (group == null) return;

        group.removeMember(memberId);
        log.info("LEAVE: group={} member={} topic={}", groupId, memberId, topic);

        if (group.isEmpty()) {
            groups.remove(key);
            log.info("Group '{}' for topic '{}' is now empty — removed.", groupId, topic);
        } else {
            rebalance(group, partitionCount);
        }
    }

    // -------------------------------------------------------------------------
    //  Dead member eviction (called by HealthCheckScheduler)
    // -------------------------------------------------------------------------

    /**
     * Scans all groups for members whose last heartbeat is older than
     * {@code timeoutMs}. Evicts dead members and rebalances affected groups.
     *
     * @param timeoutMs heartbeat timeout in milliseconds
     * @param defaultPartitionCount fallback if partition count is unknown
     */
    public void evictDeadMembers(long timeoutMs, int defaultPartitionCount) {
        long now = System.currentTimeMillis();

        for (ConsumerGroupState group : groups.values()) {
            List<String> dead = new ArrayList<>();

            for (ConsumerMember member : group.getAllMembers().values()) {
                if (member.isActive() && now - member.getLastHeartbeat() > timeoutMs) {
                    member.markDead();
                    dead.add(member.getMemberId());
                    log.warn("EVICT: group={} member={} topic={} — no heartbeat for {}ms",
                            group.getGroupId(), member.getMemberId(), group.getTopic(), timeoutMs);
                }
            }

            if (!dead.isEmpty()) {
                dead.forEach(id -> group.removeMember(id));
                rebalance(group, defaultPartitionCount);
            }
        }
    }

    // -------------------------------------------------------------------------
    //  Query
    // -------------------------------------------------------------------------

    /** Returns all group states (for admin/monitoring). */
    public Map<String, ConsumerGroupState> getAllGroups() {
        return Collections.unmodifiableMap(groups);
    }

    /** Returns the group state for a specific group+topic, or empty. */
    public Optional<ConsumerGroupState> getGroup(String groupId, String topic) {
        return Optional.ofNullable(groups.get(groupKey(groupId, topic)));
    }

    // -------------------------------------------------------------------------
    //  Rebalance — round-robin partition assignment across all active members
    // -------------------------------------------------------------------------

    private synchronized void rebalance(ConsumerGroupState group, int partitionCount) {
        if (partitionCount == 0 || group.isEmpty()) return;

        List<String> sortedMembers = new ArrayList<>(group.getMemberIds());
        Collections.sort(sortedMembers);   // deterministic order

        // Clear existing assignments
        sortedMembers.forEach(id -> {
            ConsumerMember m = group.getMember(id);
            if (m != null) m.setAssignedPartitions(new ArrayList<>());
        });

        // Distribute partitions round-robin
        for (int p = 0; p < partitionCount; p++) {
            String        memberId = sortedMembers.get(p % sortedMembers.size());
            ConsumerMember member  = group.getMember(memberId);
            if (member != null) {
                List<Integer> partitions = new ArrayList<>(member.getAssignedPartitions());
                partitions.add(p);
                member.setAssignedPartitions(partitions);
            }
        }

        group.setLastRebalanceAt(System.currentTimeMillis());

        log.info("REBALANCE: group={} topic={} partitions={} members={}",
                group.getGroupId(), group.getTopic(), partitionCount, sortedMembers.size());
        sortedMembers.forEach(id -> {
            ConsumerMember m = group.getMember(id);
            if (m != null) log.info("  {} → partitions {}", id, m.getAssignedPartitions());
        });

    }

    // -------------------------------------------------------------------------
    //  Helpers
    // -------------------------------------------------------------------------

    private ConsumerMember getMemberOrThrow(String groupId, String memberId, String topic) {
        ConsumerGroupState group = groups.get(groupKey(groupId, topic));
        if (group == null || !group.hasMember(memberId)) {
            throw new IllegalArgumentException(
                    "Unknown memberId '" + memberId + "' in group '" + groupId +
                    "' for topic '" + topic + "'. Call /join first.");
        }
        return group.getMember(memberId);
    }

    private String generateMemberId(String groupId) {
        // Format: "groupId-xxxxxxxx" (8 hex chars from UUID)
        return groupId + "-" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    }

    private String groupKey(String groupId, String topic) {
        return groupId + ":" + topic;
    }
}
