package com.bounteous.FlowTide.server.log;

import com.bounteous.FlowTide.topic.RetentionPolicy;
import com.bounteous.FlowTide.topic.TopicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Spring singleton that owns all {@link PartitionLog} instances.
 *
 * <p>Every component that needs to read or write events goes through this class.
 * Being a Spring {@code @Service} ensures there is exactly ONE log store in the
 * whole application — the critical guarantee that lets the broker and the REST
 * consumer API see the same data.
 */
@Service
public class LogManager {

    private static final Logger log = LoggerFactory.getLogger(LogManager.class);

    /** "topic-partition" → PartitionLog */
    private final ConcurrentHashMap<String, PartitionLog> logMap = new ConcurrentHashMap<>();

    /** topic name → TopicConfig */
    private final ConcurrentHashMap<String, TopicConfig> topicConfigs = new ConcurrentHashMap<>();

    // ─────────────────────────────────────────────────────────────────────────
    //  Topic registration
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Registers a topic with its full config, pre-creating all partition logs.
     * Idempotent — calling again with the same name is a no-op.
     */
    public void registerTopic(TopicConfig config) {
        if (topicConfigs.putIfAbsent(config.getName(), config) != null) {
            return; // already registered
        }
        for (int i = 0; i < config.getPartitions(); i++) {
            String key = partitionKey(config.getName(), i);
            logMap.putIfAbsent(key, new PartitionLog(config.getName(), i, config.getRetentionPolicy()));
        }
        log.info("Registered topic '{}' with {} partitions", config.getName(), config.getPartitions());
    }

    public void deleteTopic(String topic) {
        topicConfigs.remove(topic);
        logMap.keySet().removeIf(key -> key.startsWith(topic + "-"));
        log.info("Deleted topic '{}'", topic);
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Partition log access
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Returns the {@link PartitionLog} for the given topic-partition, creating
     * it on-the-fly with default retention if the topic hasn't been explicitly
     * registered (auto-create behaviour).
     */
    public PartitionLog getLog(String topic, int partition) {
        String key = partitionKey(topic, partition);
        return logMap.computeIfAbsent(key, k -> {
            RetentionPolicy retention = Optional.ofNullable(topicConfigs.get(topic))
                    .map(TopicConfig::getRetentionPolicy)
                    .orElseGet(() -> RetentionPolicy.builder().build());
            return new PartitionLog(topic, partition, retention);
        });
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Topic discovery
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Returns only topics that were explicitly registered via {@link #registerTopic}.
     *
     * <p>Intentionally excludes topics that exist only in {@code logMap} via
     * {@link #getLog} auto-creation (replication, internal fetches).
     * Those are internal partition log entries, not user-visible topics.
     */
    public Set<String> getAllTopics() {
        return Collections.unmodifiableSet(topicConfigs.keySet());
    }

    public Map<String, PartitionLog> getTopicLogs(String topic) {
        String prefix = topic + "-";
        return logMap.entrySet().stream()
                .filter(e -> e.getKey().startsWith(prefix))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public Optional<TopicConfig> getTopicConfig(String topic) {
        return Optional.ofNullable(topicConfigs.get(topic));
    }

    public Map<String, TopicConfig> getAllTopicConfigs() {
        return Collections.unmodifiableMap(topicConfigs);
    }

    /**
     * Returns true only if the topic was explicitly registered via {@link #registerTopic}.
     *
     * <p>Does NOT check {@code logMap} — partition logs can be auto-created by
     * internal calls ({@link #getLog}, replication) even for topics that were never
     * registered or have since been deleted. Using logMap here caused deleted topics
     * to appear alive again after any fetch or replication call recreated their log entries.
     */
    public boolean topicExists(String topic) {
        return topicConfigs.containsKey(topic);
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Retention
    // ─────────────────────────────────────────────────────────────────────────

    /** Called by {@link com.bounteous.FlowTide.server.retention.RetentionScheduler}. */
    public void applyRetentionToAll() {
        logMap.values().forEach(PartitionLog::applyRetention);
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Metrics helpers
    // ─────────────────────────────────────────────────────────────────────────

    public long totalEventsStored() {
        return logMap.values().stream().mapToLong(PartitionLog::size).sum();
    }

    public long totalBytesEstimated() {
        return logMap.values().stream().mapToLong(PartitionLog::estimatedBytes).sum();
    }

    public void clearAll() {
        logMap.values().forEach(PartitionLog::clear);
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Internal
    // ─────────────────────────────────────────────────────────────────────────

    private String partitionKey(String topic, int partition) {
        return topic + "-" + partition;
    }
}
