package com.bounteous.FlowTide.consumer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks committed consumer offsets per group and topic-partition.
 *
 * <p>Structure: groupId → "topic-partition" → committed offset
 *
 * <p>Offsets are persisted to a JSON file on shutdown and reloaded on startup,
 * so consumers can resume from where they left off after a restart.
 */
@Service
public class OffsetManager {

    private static final Logger log = LoggerFactory.getLogger(OffsetManager.class);

    /** groupId → partitionKey ("topic-partition") → offset */
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, Long>> offsets =
            new ConcurrentHashMap<>();

    @Value("${kafka.offsets.persist-path:data/offsets.json}")
    private String persistPath;

    private final ObjectMapper mapper = new ObjectMapper();

    // ─────────────────────────────────────────────────────────────────────────
    //  Startup / Shutdown
    // ─────────────────────────────────────────────────────────────────────────

    @PostConstruct
    public void loadFromDisk() {
        File file = new File(persistPath);
        if (!file.exists()) return;

        try {
            Map<String, Map<String, Long>> loaded = mapper.readValue(file,
                    new TypeReference<>() {});
            loaded.forEach((group, partitionMap) -> {
                ConcurrentHashMap<String, Long> concurrent = new ConcurrentHashMap<>(partitionMap);
                offsets.put(group, concurrent);
            });
            log.info("Loaded consumer offsets from {}", persistPath);
        } catch (Exception e) {
            log.warn("Could not load offsets from {}: {}", persistPath, e.getMessage());
        }
    }

    @PreDestroy
    public void persistToDisk() {
        try {
            File file = new File(persistPath);
            file.getParentFile().mkdirs();
            mapper.writerWithDefaultPrettyPrinter().writeValue(file, offsets);
            log.info("Persisted consumer offsets to {}", persistPath);
        } catch (Exception e) {
            log.error("Failed to persist offsets to {}", persistPath, e);
        }
    }

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
    public java.util.Set<String> getAllGroups() {
        return offsets.keySet();
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Internal
    // ─────────────────────────────────────────────────────────────────────────

    private String partitionKey(String topic, int partition) {
        return topic + "-" + partition;
    }
}
