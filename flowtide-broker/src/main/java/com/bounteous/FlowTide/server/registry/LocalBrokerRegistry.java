package com.bounteous.FlowTide.server.registry;

import com.bounteous.FlowTide.topic.TopicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Broker-local registry for topics and broker heartbeat state.
 *
 * <h3>Scope — this broker only</h3>
 * This class holds only what this single broker needs to serve its
 * admin API and topic lifecycle operations. It does NOT make any
 * cluster-wide decisions — those belong to flowtide-controller.
 *
 * <ul>
 *   <li><b>Topics</b> — config registered when a topic is created on this broker.
 *   <li><b>Brokers</b> — heartbeat timestamps for brokers that have contacted
 *       this instance (used only by GET /api/admin/nodes).
 * </ul>
 *
 * <h3>What lives in flowtide-controller instead</h3>
 * Partition leadership, failover, leader election, and cluster epoch are all
 * managed by flowtide-controller via {@code PartitionAssignmentService}
 * and {@code FailoverService}. This class has no knowledge of those concerns.
 */
@Service
public class LocalBrokerRegistry {

    private static final Logger log = LoggerFactory.getLogger(LocalBrokerRegistry.class);

    // ── Topics ───────────────────────────────────────────────────────────────
    /** topic name → TopicConfig */
    private final ConcurrentHashMap<String, TopicConfig> topics = new ConcurrentHashMap<>();

    // ── Brokers (local heartbeat view only) ──────────────────────────────────
    /** "host:port" → BrokerNode */
    private final ConcurrentHashMap<String, BrokerNode> brokers = new ConcurrentHashMap<>();

    // ════════════════════════════════════════════════════════════════════════
    //  TOPIC operations
    // ════════════════════════════════════════════════════════════════════════

    /** Idempotent — calling with the same name twice is a no-op. */
    public void registerTopic(TopicConfig config) {
        if (topics.putIfAbsent(config.getName(), config) == null) {
            log.info("Topic registered locally: name={} partitions={} rf={}",
                    config.getName(), config.getPartitions(), config.getReplicationFactor());
        }
    }

    public void deleteTopic(String topic) {
        topics.remove(topic);
        log.info("Topic deleted locally: {}", topic);
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
    //  BROKER operations (local heartbeat tracking for admin API only)
    // ════════════════════════════════════════════════════════════════════════

    /**
     * Registers a broker in the local view.
     * Called by {@link com.bounteous.FlowTide.server.BrokerStartupRegistration}
     * so this broker appears in GET /api/admin/nodes.
     */
    public void registerBroker(BrokerInfo brokerInfo) {
        String key = brokerKey(brokerInfo);
        brokers.computeIfAbsent(key, k -> {
            log.info("Broker registered in local view: {}", key);
            return new BrokerNode(brokerInfo);
        });
    }

    /** Updates the local heartbeat timestamp for a known broker. */
    public void updateHeartbeat(String host, int port) {
        BrokerNode node = brokers.get(host + ":" + port);
        if (node != null) {
            node.updateHeartbeat();
            log.trace("Heartbeat updated locally: {}:{}", host, port);
        }
    }

    /** Returns only brokers with ACTIVE status. */
    public List<BrokerInfo> getActiveBrokers() {
        return brokers.values().stream()
                .filter(BrokerNode::isActive)
                .map(BrokerNode::getBrokerInfo)
                .collect(Collectors.toList());
    }

    public int getActiveBrokerCount() {
        return (int) brokers.values().stream().filter(BrokerNode::isActive).count();
    }

    /** Returns all broker nodes (for health inspection). */
    public List<BrokerNode> getAllBrokerNodes() {
        return new ArrayList<>(brokers.values());
    }

    // ─────────────────────────────────────────────────────────────────────────

    private String brokerKey(BrokerInfo broker) {
        return broker.getHost() + ":" + broker.getPort();
    }
}
