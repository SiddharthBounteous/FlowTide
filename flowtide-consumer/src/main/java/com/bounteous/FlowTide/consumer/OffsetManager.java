package com.bounteous.FlowTide.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks committed consumer offsets per group and topic-partition — purely in-memory.
 *
 * <p>Structure: groupId → "topic-partition" → committed offset
 *
 * <p>Offsets are held in memory for the lifetime of the consumer process.
 * On restart offsets reset, consistent with FlowTide's in-memory design.
 */
@Service
public class OffsetManager {

    private static final Logger log = LoggerFactory.getLogger(OffsetManager.class);

    /** groupId → partitionKey ("topic-partition") → offset */
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, Long>> offsets =
            new ConcurrentHashMap<>();

    // ─────────────────────────────────────────────────────────────────────────
    //  Commit
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Commits the offset for a group+topic+partition.
     * The committed offset is the next offset to consume (i.e., lastConsumed + 1).
     */
    public void commit(String groupId, String topic, int partition, long offset) {
        offsets.computeIfAbsent(groupId, g -> new ConcurrentHashMap<>())
               .put(partitionKey(topic, partition), offset);
        log.debug("Committed offset: group={} topic={} partition={} offset={}", groupId, topic, partition, offset);
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Read
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Returns the last committed offset for this group + partition.
     * Returns {@code -1} if no offset has been committed (first-time consumer).
     */
    public long getCommittedOffset(String groupId, String topic, int partition) {
        Map<String, Long> groupOffsets = offsets.get(groupId);
        if (groupOffsets == null) return -1L;
        return groupOffsets.getOrDefault(partitionKey(topic, partition), -1L);
    }

    /** Returns all committed offsets for a group, keyed by "topic-partition". */
    public Map<String, Long> getGroupOffsets(String groupId) {
        return offsets.getOrDefault(groupId, new ConcurrentHashMap<>());
    }

    /** Returns all group IDs that have committed offsets. */
    public Set<String> getAllGroups() {
        return offsets.keySet();
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Internal
    // ─────────────────────────────────────────────────────────────────────────

    private String partitionKey(String topic, int partition) {
        return topic + "-" + partition;
    }
}
