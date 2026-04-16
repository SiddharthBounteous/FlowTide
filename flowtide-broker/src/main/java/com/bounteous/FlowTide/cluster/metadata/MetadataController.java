package com.bounteous.FlowTide.cluster.metadata;

import com.bounteous.FlowTide.server.registry.BrokerInfo;
import com.bounteous.FlowTide.server.registry.PartitionMetadata;
import com.bounteous.FlowTide.topic.TopicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Single source of truth for all cluster metadata.
 *
 *
 * <p>Stores four types of metadata:
 * <ol>
 *   <li><b>Topics</b> — name, partitions, replication factor, retention policy
 *   <li><b>Brokers</b> — host, port, last heartbeat, alive/dead status
 *   <li><b>Partitions</b> — leader broker and replica list per topic-partition
 *   <li><b>Cluster state</b> — controller epoch, controller broker key
 * </ol>
 *
 * <p>All other components ({@code ClusterManager}, {@code BrokerSelfRegistration})
 * read and write through this class only — no component maintains its own copy.
 */
@Service
public class MetadataController {

    private static final Logger log = LoggerFactory.getLogger(MetadataController.class);

    // ── Topics ───────────────────────────────────────────────────────────────
    /** topic name → TopicConfig */
    private final ConcurrentHashMap<String, TopicConfig> topics = new ConcurrentHashMap<>();

    // ── Brokers ──────────────────────────────────────────────────────────────
    /** "host:port" → BrokerNode  (contains heartbeat timestamp + alive/dead status) */
    private final ConcurrentHashMap<String, BrokerNode> brokers = new ConcurrentHashMap<>();

    // ── Partitions ───────────────────────────────────────────────────────────
    /** topic → ordered list of PartitionMetadata (list index == partition id) */
    private final ConcurrentHashMap<String, List<PartitionMetadata>> partitions = new ConcurrentHashMap<>();

    // ── Cluster state ────────────────────────────────────────────────────────
    /** Incremented on every leader election or topology change. */
    private final AtomicInteger epoch = new AtomicInteger(0);

    /** Key ("host:port") of the broker currently acting as cluster controller. */
    private volatile String controllerKey;

    // ════════════════════════════════════════════════════════════════════════
    //  TOPIC operations
    // ════════════════════════════════════════════════════════════════════════

    /** Idempotent — calling with the same name twice is a no-op. */
    public void registerTopic(TopicConfig config) {
        if (topics.putIfAbsent(config.getName(), config) == null) {
            log.info("[Metadata] Topic registered: name={} partitions={} rf={}",
                    config.getName(), config.getPartitions(), config.getReplicationFactor());
        }
    }

    public void deleteTopic(String topic) {
        topics.remove(topic);
        partitions.remove(topic);
        log.info("[Metadata] Topic deleted: {}", topic);
    }

    public Optional<TopicConfig> getTopicConfig(String topic) {
        return Optional.ofNullable(topics.get(topic));
    }

    public boolean topicExists(String topic) {
        return topics.containsKey(topic);
    }

    public Set<String> getAllTopics() {
        return Collections.unmodifiableSet(topics.keySet());
    }

    public Map<String, TopicConfig> getAllTopicConfigs() {
        return Collections.unmodifiableMap(topics);
    }

    // ════════════════════════════════════════════════════════════════════════
    //  BROKER operations
    // ════════════════════════════════════════════════════════════════════════

    /**
     * Registers a broker or refreshes its heartbeat if already known.
     * Elects the first registered broker as controller if no controller exists yet.
     */
    public void registerBroker(BrokerInfo brokerInfo) {
        String key = brokerKey(brokerInfo);
        BrokerNode existing = brokers.get(key);
        if (existing != null) {
            existing.updateHeartbeat();
        } else {
            brokers.put(key, new BrokerNode(brokerInfo));
            log.info("[Metadata] Broker registered: {}", key);
        }
        if (controllerKey == null) {
            controllerKey = key;
            log.info("[Metadata] Controller elected: {}", key);
        }
    }

    /** Updates the heartbeat timestamp for a known broker. */
    public void updateHeartbeat(String host, int port) {
        BrokerNode node = brokers.get(host + ":" + port);
        if (node != null) {
            node.updateHeartbeat();
            log.trace("[Metadata] Heartbeat: {}:{}", host, port);
        }
    }

    /** Returns only brokers with ACTIVE status. */
    public List<BrokerInfo> getActiveBrokers() {
        return brokers.values().stream()
                .filter(BrokerNode::isActive)
                .map(BrokerNode::getBrokerInfo)
                .collect(Collectors.toList());
    }

    /** Returns all broker nodes regardless of status (for health-check inspection). */
    public List<BrokerNode> getAllBrokerNodes() {
        return new ArrayList<>(brokers.values());
    }

    public int getActiveBrokerCount() {
        return (int) brokers.values().stream().filter(BrokerNode::isActive).count();
    }

    /**
     * Marks the broker dead, removes it from the active set, triggers partition
     * failover, and elects a new controller if needed.
     *
     * <p>Called by {@link com.bounteous.FlowTide.cluster.ClusterManager}
     * when a heartbeat timeout is detected.
     */
    public void handleBrokerFailure(BrokerInfo dead) {
        String key = brokerKey(dead);
        BrokerNode node = brokers.remove(key);
        if (node != null) {
            node.markDead();
        }
        if (key.equals(controllerKey)) {
            electNewController();
        }
        failoverPartitions(dead);
        epoch.incrementAndGet();
        log.warn("[Metadata] Broker failure handled: {} — cluster epoch={}", key, epoch.get());
    }

    // ════════════════════════════════════════════════════════════════════════
    //  PARTITION operations
    // ════════════════════════════════════════════════════════════════════════

    /** Replaces the full partition list for a topic. Thread-safe via CopyOnWriteArrayList. */
    public void updatePartitions(String topic, List<PartitionMetadata> metas) {
        partitions.put(topic, new CopyOnWriteArrayList<>(metas));
        log.debug("[Metadata] Partitions updated: topic='{}' count={}", topic, metas.size());
    }

    public Optional<BrokerInfo> getLeader(String topic, int partition) {
        List<PartitionMetadata> metas = partitions.get(topic);
        if (metas == null || partition < 0 || partition >= metas.size()) return Optional.empty();
        return Optional.ofNullable(metas.get(partition).getLeader());
    }

    public List<BrokerInfo> getFollowers(String topic, int partition) {
        List<PartitionMetadata> metas = partitions.get(topic);
        if (metas == null || partition < 0 || partition >= metas.size()) return Collections.emptyList();
        List<BrokerInfo> replicas = metas.get(partition).getReplicas();
        return replicas != null ? replicas : Collections.emptyList();
    }

    public List<PartitionMetadata> getPartitionMetadata(String topic) {
        return partitions.getOrDefault(topic, Collections.emptyList());
    }

    /**
     * Returns ALL topic → partition metadata.
     * Returns all topic partition metadata.
     */
    public Map<String, List<PartitionMetadata>> getAllPartitionMetadata() {
        return Collections.unmodifiableMap(partitions);
    }

    // ════════════════════════════════════════════════════════════════════════
    //  CLUSTER STATE
    // ════════════════════════════════════════════════════════════════════════

    public int getEpoch() { return epoch.get(); }

    public String getControllerKey() { return controllerKey; }

    public boolean isController(String host, int port) {
        return (host + ":" + port).equals(controllerKey);
    }

    // ════════════════════════════════════════════════════════════════════════
    //  INTERNAL helpers
    // ════════════════════════════════════════════════════════════════════════

    private void failoverPartitions(BrokerInfo deadBroker) {
        List<BrokerInfo> alive = getActiveBrokers();
        for (Map.Entry<String, List<PartitionMetadata>> entry : partitions.entrySet()) {
            String topic = entry.getKey();
            List<PartitionMetadata> metas = entry.getValue();
            for (int i = 0; i < metas.size(); i++) {
                PartitionMetadata meta = metas.get(i);
                if (deadBroker.equals(meta.getLeader())) {
                    promoteLeader(topic, i, meta, deadBroker, alive);
                }
            }
        }
    }

    private void promoteLeader(String topic, int partition, PartitionMetadata meta,
                                BrokerInfo deadLeader, List<BrokerInfo> alive) {
        Optional<BrokerInfo> newLeader = meta.getReplicas().stream()
                .filter(r -> !r.equals(deadLeader))
                .filter(alive::contains)
                .findFirst();

        if (newLeader.isEmpty()) {
            log.error("[Metadata] No replica for topic={} partition={} — PARTITION UNAVAILABLE", topic, partition);
            return;
        }

        List<BrokerInfo> newReplicas = new ArrayList<>(meta.getReplicas());
        newReplicas.remove(newLeader.get());

        PartitionMetadata updated = new PartitionMetadata(topic, partition, newLeader.get(), newReplicas);
        partitions.get(topic).set(partition, updated);

        log.info("[Metadata] Leader failover: topic={} partition={} dead={} new={}",
                topic, partition, deadLeader, newLeader.get());
    }

    private void electNewController() {
        List<BrokerNode> active = brokers.values().stream().filter(BrokerNode::isActive).toList();
        if (active.isEmpty()) {
            controllerKey = null;
            log.error("[Metadata] No active brokers — cluster has no controller!");
        } else {
            controllerKey = active.get(0).key();
            log.info("[Metadata] New controller elected: {}", controllerKey);
        }
    }

    private String brokerKey(BrokerInfo broker) {
        return broker.getHost() + ":" + broker.getPort();
    }
}
