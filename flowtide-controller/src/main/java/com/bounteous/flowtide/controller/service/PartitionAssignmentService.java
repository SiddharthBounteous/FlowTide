package com.bounteous.flowtide.controller.service;

import com.bounteous.flowtide.controller.model.BrokerInfo;
import com.bounteous.flowtide.controller.model.PartitionAssignment;
import com.bounteous.flowtide.controller.model.PartitionAssignment.PartitionRole;
import com.bounteous.flowtide.controller.model.PartitionAssignment.Role;
import com.bounteous.flowtide.controller.model.TopicMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Assigns partitions to brokers and tracks the full leader map.
 *
 * <p>Assignment strategy: round-robin across sorted active brokers.
 *
 * <p>Example — 3 brokers, topic "orders" with 3 partitions:
 * <pre>
 * brokers (sorted): [broker1:8083, broker2:8084, broker3:8085]
 *
 * partition 0 % 3 = 0 → broker1:8083 is LEADER
 * partition 1 % 3 = 1 → broker2:8084 is LEADER
 * partition 2 % 3 = 2 → broker3:8085 is LEADER
 * </pre>
 *
 * <p>Replication (if replicationFactor > 1):
 * <pre>
 * partition 0 leader=broker1, follower=broker2
 * partition 1 leader=broker2, follower=broker3
 * partition 2 leader=broker3, follower=broker1
 * </pre>
 */
@Service
public class PartitionAssignmentService {

    private static final Logger log = LoggerFactory.getLogger(PartitionAssignmentService.class);

    @Value("${kafka.topic.default-partitions:3}")
    private int defaultPartitions;

    @Value("${kafka.topic.default-replication-factor:2}")
    private int defaultReplicationFactor;

    /**
     * topic → (partition → leaderId)
     * The single source of truth for partition ownership.
     */
    private final ConcurrentHashMap<String, Map<Integer, String>> leaderMap = new ConcurrentHashMap<>();

    /**
     * topic → (partition → list of follower broker ids)
     */
    private final ConcurrentHashMap<String, Map<Integer, List<String>>> followerMap = new ConcurrentHashMap<>();

    // ─────────────────────────────────────────────────────────────────────────
    //  Topic creation / assignment
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Creates partition assignment for a topic across active brokers.
     * Idempotent — if topic already exists, returns existing assignment.
     */
    public TopicMetadata assignTopic(String topic, int partitionCount,
                                     int replicationFactor, List<BrokerInfo> activeBrokers) {
        if (leaderMap.containsKey(topic)) {
            log.info("Topic '{}' already assigned — returning existing metadata", topic);
            return buildTopicMetadata(topic);
        }

        if (activeBrokers.isEmpty()) {
            throw new IllegalStateException("No active brokers available to assign topic: " + topic);
        }

        int effectiveRF = Math.min(replicationFactor, activeBrokers.size());
        Map<Integer, String>       leaders   = new LinkedHashMap<>();
        Map<Integer, List<String>> followers = new LinkedHashMap<>();

        for (int p = 0; p < partitionCount; p++) {
            // Leader: round-robin
            BrokerInfo leader = activeBrokers.get(p % activeBrokers.size());
            leaders.put(p, leader.getId());

            // Followers: next N-1 brokers in the ring
            List<String> replicas = new ArrayList<>();
            for (int r = 1; r < effectiveRF; r++) {
                BrokerInfo follower = activeBrokers.get((p + r) % activeBrokers.size());
                replicas.add(follower.getId());
            }
            followers.put(p, replicas);
        }

        leaderMap.put(topic, leaders);
        followerMap.put(topic, followers);

        log.info("Assigned topic '{}': {} partitions, RF={} across {} brokers",
                topic, partitionCount, effectiveRF, activeBrokers.size());

        return buildTopicMetadata(topic);
    }

    /**
     * Auto-assigns a topic with default partition count and replication factor.
     */
    public TopicMetadata autoAssignTopic(String topic, List<BrokerInfo> activeBrokers) {
        return assignTopic(topic, defaultPartitions, defaultReplicationFactor, activeBrokers);
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Failover
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Handles a dead broker — promotes followers to leaders for all
     * partitions where the dead broker was the leader.
     */
    public List<String> handleBrokerFailure(String deadBrokerId, List<BrokerInfo> activeBrokers) {
        List<String> affectedTopics = new ArrayList<>();

        for (String topic : leaderMap.keySet()) {
            Map<Integer, String>       leaders   = leaderMap.get(topic);
            Map<Integer, List<String>> followers = followerMap.get(topic);
            boolean topicAffected = false;

            for (Map.Entry<Integer, String> entry : leaders.entrySet()) {
                int    partition  = entry.getKey();
                String currentLeader = entry.getValue();

                if (deadBrokerId.equals(currentLeader)) {
                    // Promote first alive follower
                    List<String> replicas = followers.getOrDefault(partition, new ArrayList<>());
                    Optional<String> newLeader = replicas.stream()
                            .filter(r -> activeBrokers.stream().anyMatch(b -> b.getId().equals(r)))
                            .findFirst();

                    if (newLeader.isPresent()) {
                        leaders.put(partition, newLeader.get());
                        replicas.remove(newLeader.get());
                        log.info("Failover: topic={} partition={} dead={} newLeader={}",
                                topic, partition, deadBrokerId, newLeader.get());
                    } else {
                        // No follower available — reassign to any alive broker
                        if (!activeBrokers.isEmpty()) {
                            String fallback = activeBrokers.get(partition % activeBrokers.size()).getId();
                            leaders.put(partition, fallback);
                            log.warn("No follower for topic={} partition={} — assigned to fallback={}",
                                    topic, partition, fallback);
                        } else {
                            log.error("No brokers available for topic={} partition={} — PARTITION UNAVAILABLE",
                                    topic, partition);
                        }
                    }
                    topicAffected = true;
                }

                // Remove dead broker from follower list too
                followers.getOrDefault(partition, new ArrayList<>()).remove(deadBrokerId);
            }

            if (topicAffected) affectedTopics.add(topic);
        }

        return affectedTopics;
    }

    /**
     * Full reassignment after a new broker joins — rebalances all topics.
     */
    public void rebalanceAll(List<BrokerInfo> activeBrokers) {
        Set<String> topics = new HashSet<>(leaderMap.keySet());
        for (String topic : topics) {
            int partitionCount = leaderMap.get(topic).size();
            // Clear and reassign
            leaderMap.remove(topic);
            followerMap.remove(topic);
            assignTopic(topic, partitionCount, defaultReplicationFactor, activeBrokers);
        }
        log.info("Full rebalance complete: {} topics reassigned across {} brokers",
                topics.size(), activeBrokers.size());
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Query
    // ─────────────────────────────────────────────────────────────────────────

    public Optional<String> getLeader(String topic, int partition) {
        Map<Integer, String> leaders = leaderMap.get(topic);
        if (leaders == null) return Optional.empty();
        return Optional.ofNullable(leaders.get(partition));
    }

    public TopicMetadata getTopicMetadata(String topic) {
        return buildTopicMetadata(topic);
    }

    public Map<String, TopicMetadata> getAllTopicMetadata() {
        Map<String, TopicMetadata> all = new LinkedHashMap<>();
        for (String topic : leaderMap.keySet()) {
            all.put(topic, buildTopicMetadata(topic));
        }
        return all;
    }

    /**
     * Returns all partition assignments for a specific broker.
     * Used by brokers on startup to know which partitions they own.
     */
    public PartitionAssignment getAssignmentForBroker(String brokerId) {
        List<PartitionRole> roles = new ArrayList<>();

        for (Map.Entry<String, Map<Integer, String>> topicEntry : leaderMap.entrySet()) {
            String topic = topicEntry.getKey();

            // Leader roles
            for (Map.Entry<Integer, String> partEntry : topicEntry.getValue().entrySet()) {
                if (brokerId.equals(partEntry.getValue())) {
                    roles.add(new PartitionRole(topic, partEntry.getKey(), Role.LEADER));
                }
            }

            // Follower roles
            Map<Integer, List<String>> followers = followerMap.get(topic);
            if (followers != null) {
                for (Map.Entry<Integer, List<String>> partEntry : followers.entrySet()) {
                    if (partEntry.getValue().contains(brokerId)) {
                        roles.add(new PartitionRole(topic, partEntry.getKey(), Role.FOLLOWER));
                    }
                }
            }
        }

        return new PartitionAssignment(brokerId, roles);
    }

    public boolean topicExists(String topic) {
        return leaderMap.containsKey(topic);
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Internal
    // ─────────────────────────────────────────────────────────────────────────

    private TopicMetadata buildTopicMetadata(String topic) {
        Map<Integer, String> leaders = leaderMap.getOrDefault(topic, Collections.emptyMap());
        return new TopicMetadata(topic, leaders.size(), new LinkedHashMap<>(leaders));
    }
}
