package com.bounteous.FlowTide.consumer;

import com.bounteous.FlowTide.audit.AuditLogger;
import com.bounteous.FlowTide.server.log.LogManager;
import com.bounteous.FlowTide.server.log.PartitionLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages consumer group membership and partition assignment (rebalancing).
 *
 * <p>Each topic can have multiple groups. Within a group, each partition is
 * assigned to exactly one member — this is the fundamental consumer group contract.
 *
 * <p>Assignment algorithm: Round-robin across sorted member IDs.
 * (Real Kafka uses a configurable assignor; this is a simple but correct implementation.)
 */
@Service
public class ConsumerGroupManager {

    private static final Logger log = LoggerFactory.getLogger(ConsumerGroupManager.class);

    /** "groupId:topic" → ConsumerGroupInfo */
    private final ConcurrentHashMap<String, ConsumerGroupInfo> groups = new ConcurrentHashMap<>();

    private final LogManager logManager;
    private final OffsetManager offsetManager;
    private final AuditLogger auditLogger;

    public ConsumerGroupManager(LogManager logManager, OffsetManager offsetManager, AuditLogger auditLogger) {
        this.logManager = logManager;
        this.offsetManager = offsetManager;
        this.auditLogger = auditLogger;
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Join / Leave
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Registers a consumer as a member of a group for a given topic,
     * then triggers a rebalance to assign partitions.
     *
     * @return the partitions assigned to this member after rebalance
     */
    public List<Integer> join(String groupId, String memberId, String topic) {
        String key = groupKey(groupId, topic);
        ConsumerGroupInfo group = groups.computeIfAbsent(key,
                k -> new ConsumerGroupInfo(groupId, topic));

        if (!group.hasMember(memberId)) {
            group.addMember(memberId);
            auditLogger.log("CONSUMER_JOINED", "group=" + groupId + " member=" + memberId + " topic=" + topic);
            rebalance(group, topic);
        }

        group.updateHeartbeat(memberId);
        return group.getAssignedPartitions(memberId);
    }

    /**
     * Removes a member from a group and triggers a rebalance so their
     * partitions are redistributed to remaining members.
     */
    public void leave(String groupId, String memberId, String topic) {
        String key = groupKey(groupId, topic);
        ConsumerGroupInfo group = groups.get(key);
        if (group == null) return;

        group.removeMember(memberId);
        auditLogger.log("CONSUMER_LEFT", "group=" + groupId + " member=" + memberId + " topic=" + topic);

        if (group.getMembers().isEmpty()) {
            groups.remove(key);
            log.info("Consumer group '{}' for topic '{}' is now empty and removed", groupId, topic);
        } else {
            rebalance(group, topic);
        }
    }

    public void heartbeat(String groupId, String memberId, String topic) {
        ConsumerGroupInfo group = groups.get(groupKey(groupId, topic));
        if (group != null) group.updateHeartbeat(memberId);
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Rebalance
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Round-robin partition assignment across all active members.
     * Called whenever a member joins or leaves.
     */
    public synchronized void rebalance(ConsumerGroupInfo group, String topic) {
        Map<String, PartitionLog> topicLogs = logManager.getTopicLogs(topic);
        int partitionCount = topicLogs.size();

        if (partitionCount == 0 || group.getMembers().isEmpty()) return;

        // Sort members for deterministic, stable assignment
        List<String> sortedMembers = new ArrayList<>(group.getMembers());
        Collections.sort(sortedMembers);

        // Clear existing assignments
        sortedMembers.forEach(m -> group.getAssignments().put(m, new ArrayList<>()));

        // Round-robin assign
        for (int p = 0; p < partitionCount; p++) {
            String assignedMember = sortedMembers.get(p % sortedMembers.size());
            group.getAssignments().get(assignedMember).add(p);
        }

        group.setLastRebalanceAt(System.currentTimeMillis());
        log.info("Rebalanced group='{}' topic='{}': {} partitions across {} members",
                group.getGroupId(), topic, partitionCount, sortedMembers.size());
        auditLogger.log("REBALANCE", "group=" + group.getGroupId() + " topic=" + topic +
                " members=" + sortedMembers.size() + " partitions=" + partitionCount);
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Query
    // ─────────────────────────────────────────────────────────────────────────

    public Optional<ConsumerGroupInfo> getGroup(String groupId, String topic) {
        return Optional.ofNullable(groups.get(groupKey(groupId, topic)));
    }

    public List<Integer> getAssignment(String groupId, String memberId, String topic) {
        return getGroup(groupId, topic)
                .map(g -> g.getAssignedPartitions(memberId))
                .orElse(Collections.emptyList());
    }

    /** Returns all consumer group infos across all topics. */
    public Collection<ConsumerGroupInfo> getAllGroups() {
        return Collections.unmodifiableCollection(groups.values());
    }

    /**
     * Computes consumer lag: total events not yet consumed by this group.
     * lag = sum over all assigned partitions of (latestOffset - committedOffset)
     */
    public long computeLag(String groupId, String topic) {
        Map<String, PartitionLog> topicLogs = logManager.getTopicLogs(topic);
        long totalLag = 0;
        for (Map.Entry<String, PartitionLog> entry : topicLogs.entrySet()) {
            String partitionKey = entry.getKey(); // "topic-partition"
            int partition = Integer.parseInt(partitionKey.substring(partitionKey.lastIndexOf('-') + 1));
            long committed = offsetManager.getCommittedOffset(groupId, topic, partition);
            long latest = entry.getValue().latestOffset();
            long effectiveCommitted = committed < 0 ? 0 : committed;
            totalLag += Math.max(0, latest - effectiveCommitted);
        }
        return totalLag;
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Internal
    // ─────────────────────────────────────────────────────────────────────────

    private String groupKey(String groupId, String topic) {
        return groupId + ":" + topic;
    }
}
