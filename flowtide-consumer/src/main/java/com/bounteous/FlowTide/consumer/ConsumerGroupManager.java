package com.bounteous.FlowTide.consumer;

import com.bounteous.FlowTide.audit.AuditLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages consumer group membership and partition assignment for the
 * consumer service.
 *
 * <p>Unlike the broker-side version, this does not depend on LogManager.
 * Partition count is passed in from the ConsumerController, which resolves
 * it from the Registry via {@link com.bounteous.FlowTide.registry.RegistryClient}.
 */
@Service
public class ConsumerGroupManager {

    private static final Logger log = LoggerFactory.getLogger(ConsumerGroupManager.class);

    /** "groupId:topic" → ConsumerGroupInfo */
    private final ConcurrentHashMap<String, ConsumerGroupInfo> groups = new ConcurrentHashMap<>();

    private final AuditLogger auditLogger;

    public ConsumerGroupManager(AuditLogger auditLogger) {
        this.auditLogger = auditLogger;
    }

    /**
     * Registers a member in a consumer group and assigns partitions.
     *
     * @param partitionCount total partitions for the topic (resolved from registry)
     * @return partitions assigned to this member
     */
    public List<Integer> join(String groupId, String memberId, String topic, int partitionCount) {
        String key = groupKey(groupId, topic);
        ConsumerGroupInfo group = groups.computeIfAbsent(key, k -> new ConsumerGroupInfo(groupId, topic));

        if (!group.hasMember(memberId)) {
            group.addMember(memberId);
            auditLogger.log("CONSUMER_JOINED", "group=" + groupId + " member=" + memberId + " topic=" + topic);
            rebalance(group, partitionCount);
        }

        group.updateHeartbeat(memberId);
        return group.getAssignedPartitions(memberId);
    }

    public void leave(String groupId, String memberId, String topic, int partitionCount) {
        String key = groupKey(groupId, topic);
        ConsumerGroupInfo group = groups.get(key);
        if (group == null) return;

        group.removeMember(memberId);
        auditLogger.log("CONSUMER_LEFT", "group=" + groupId + " member=" + memberId + " topic=" + topic);

        if (group.getMembers().isEmpty()) {
            groups.remove(key);
        } else {
            rebalance(group, partitionCount);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────

    private synchronized void rebalance(ConsumerGroupInfo group, int partitionCount) {
        if (partitionCount == 0 || group.getMembers().isEmpty()) return;

        List<String> sortedMembers = new ArrayList<>(group.getMembers());
        Collections.sort(sortedMembers);
        sortedMembers.forEach(m -> group.getAssignments().put(m, new ArrayList<>()));

        for (int p = 0; p < partitionCount; p++) {
            String member = sortedMembers.get(p % sortedMembers.size());
            group.getAssignments().get(member).add(p);
        }

        group.setLastRebalanceAt(System.currentTimeMillis());
        log.info("Rebalanced group='{}' topic='{}': {} partitions across {} members",
                group.getGroupId(), group.getTopic(), partitionCount, sortedMembers.size());
    }

    private String groupKey(String groupId, String topic) {
        return groupId + ":" + topic;
    }
}
