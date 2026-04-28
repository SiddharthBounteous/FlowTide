package com.bounteous.FlowTide.topic;

import com.bounteous.FlowTide.audit.AuditLogger;
import com.bounteous.FlowTide.client.ControllerClient;
import com.bounteous.FlowTide.client.model.TopicCreateRequest;
import com.bounteous.FlowTide.server.log.LogManager;
import feign.FeignException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;

/**
 * High-level service for topic lifecycle management.
 *
 * <h3>Single source of truth for topic config: LogManager</h3>
 * {@link LogManager} stores both the {@link TopicConfig} and the in-memory
 * partition logs. There is no separate local registry — everything goes through
 * LogManager directly.
 *
 * <h3>Cluster sync on create</h3>
 * After registering locally, the controller is notified so it assigns partitions
 * across all brokers. Other brokers learn their roles on the next heartbeat.
 *
 * <h3>Cluster sync on delete</h3>
 * The controller is notified first. On the next heartbeat cycle every other broker
 * receives an updated {@code knownTopics} set that no longer includes this topic
 * and deletes its local logs automatically.
 */
@Service
public class TopicManager {

    private static final Logger log = LoggerFactory.getLogger(TopicManager.class);

    private final LogManager       logManager;
    private final AuditLogger      auditLogger;
    private final ControllerClient controllerClient;

    public TopicManager(LogManager logManager,
                        AuditLogger auditLogger,
                        ControllerClient controllerClient) {
        this.logManager       = logManager;
        this.auditLogger      = auditLogger;
        this.controllerClient = controllerClient;
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Create
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Creates a topic from an API request.
     * Idempotent — creating an already-existing topic returns the existing config.
     */
    public TopicConfig createTopic(CreateTopicRequest request) {
        validateTopicName(request.getName());

        // Guard: TopicController calls topicExists() before reaching here,
        // but autoCreate() bypasses the controller — check locally as a safety net.
        Optional<TopicConfig> existing = logManager.getTopicConfig(request.getName());
        if (existing.isPresent()) {
            log.debug("Topic '{}' already registered locally — returning existing config", request.getName());
            return existing.get();
        }

        RetentionPolicy retention = RetentionPolicy.builder()
                .maxAge(Duration.ofHours(request.getRetentionHours()))
                .maxEventsPerPartition(request.getMaxEventsPerPartition())
                .build();

        TopicConfig config = TopicConfig.builder()
                .name(request.getName())
                .partitions(request.getPartitions())
                .replicationFactor(request.getReplicationFactor())
                .retentionPolicy(retention)
                .build();

        // Register locally — creates PartitionLog instances so this broker can
        // immediately accept writes/reads for any partition it is assigned.
        logManager.registerTopic(config);

        // Notify controller — assigns partitions to ALL brokers in the cluster.
        // Other brokers learn their leader/follower roles on next heartbeat.
        try {
            controllerClient.createTopic(new TopicCreateRequest(
                    request.getName(),
                    request.getPartitions(),
                    request.getReplicationFactor()));
            log.info("Topic '{}' registered with controller — partitions distributed across cluster",
                    request.getName());
        } catch (Exception e) {
            log.warn("Could not register topic '{}' with controller: {} " +
                     "— topic is local-only until controller is reachable.", request.getName(), e.getMessage());
        }

        auditLogger.log("TOPIC_CREATED", "topic=" + request.getName()
                + " partitions=" + request.getPartitions()
                + " replication=" + request.getReplicationFactor());

        return config;
    }

    /**
     * Auto-creates a topic with default settings on first publish.
     */
    public TopicConfig autoCreate(String topicName, int defaultPartitions, int defaultReplicationFactor) {
        CreateTopicRequest req = new CreateTopicRequest();
        req.setName(topicName);
        req.setPartitions(defaultPartitions);
        req.setReplicationFactor(defaultReplicationFactor);
        return createTopic(req);
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Delete
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Deletes a topic and all its stored events — cluster-wide.
     *
     * <ol>
     *   <li>Validates the topic exists on this broker.
     *   <li>Notifies the controller so it removes the topic from its assignment maps.
     *   <li>Deletes local logs on this broker.
     *   <li>All other brokers detect the deletion on their next heartbeat
     *       via {@code knownTopics} in the heartbeat response and delete locally.
     * </ol>
     */
    public void deleteTopic(String topicName) {
        // Always validate against the controller — it is the single source of truth.
        // Checking local state first causes ghost deletes: another broker may have
        // already deleted the topic from the controller, but this broker's local
        // topicConfigs hasn't synced yet (heartbeat runs every 5s).
        // Result without this fix: click 1 deletes, clicks 2/3/... also "succeed"
        // against stale local state, only failing after all brokers have synced.
        try {
            controllerClient.getTopicMetadata(topicName);
            // 200 OK → topic exists in controller → proceed
        } catch (FeignException.NotFound e) {
            // Controller says topic doesn't exist — clean up any stale local state
            logManager.deleteTopic(topicName);
            throw new IllegalArgumentException("Topic not found: " + topicName);
        } catch (Exception e) {
            // Controller unreachable — fall back to local check
            log.warn("Controller unreachable, falling back to local existence check: {}", e.getMessage());
            if (!logManager.topicExists(topicName)) {
                throw new IllegalArgumentException("Topic not found: " + topicName);
            }
        }

        // Tell controller to remove the topic — all brokers sync on next heartbeat
        try {
            controllerClient.deleteTopic(topicName);
            log.info("Topic '{}' removed from controller", topicName);
        } catch (Exception e) {
            log.warn("Could not remove topic '{}' from controller: {}", topicName, e.getMessage());
        }

        // Remove from this broker's local logs
        logManager.deleteTopic(topicName);
        auditLogger.log("TOPIC_DELETED", "topic=" + topicName);
        log.info("Topic '{}' deleted", topicName);
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Query
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Lists all topics known across the cluster.
     * Queries controller first (authoritative). Falls back to local LogManager.
     */
    public Collection<String> listTopics() {
        try {
            Set<String> controllerTopics = controllerClient.getAllTopics().keySet();
            log.debug("listTopics(): {} topic(s) from controller", controllerTopics.size());
            return controllerTopics;
        } catch (Exception e) {
            log.warn("Cannot reach controller for topic list ({}); falling back to local",
                    e.getMessage());
            return logManager.getAllTopics();
        }
    }

    /**
     * Returns the config for a topic.
     * Checks local LogManager first (fast, no network). Falls back to controller
     * for topics created on other brokers and not yet seen locally.
     */
    public Optional<TopicConfig> getTopic(String topicName) {
        Optional<TopicConfig> local = logManager.getTopicConfig(topicName);
        if (local.isPresent()) return local;

        // Not local — try controller (topic may have been created on another broker)
        try {
            com.bounteous.FlowTide.client.model.TopicMetadata meta =
                    controllerClient.getTopicMetadata(topicName);
            if (meta == null || meta.getTopic() == null) return Optional.empty();

            // Use createdAt from the controller — set once when the topic was
            // first assigned. Without this, @Builder.Default fires Instant.now()
            // every time this broker synthesises the config, producing a different
            // createdAt on every GET when different broker instances handle the request.
            TopicConfig synthesised = TopicConfig.builder()
                    .name(meta.getTopic())
                    .partitions(meta.getPartitionCount())
                    .replicationFactor(meta.getReplicationFactor())
                    .createdAt(meta.getCreatedAt())
                    .retentionPolicy(RetentionPolicy.builder().build())
                    .build();
            logManager.registerTopic(synthesised);  // cache locally — same value on repeat calls
            log.debug("Topic '{}' fetched from controller and cached locally (createdAt={})",
                    topicName, meta.getCreatedAt());
            return Optional.of(synthesised);
        } catch (Exception e) {
            log.debug("Topic '{}' not found in controller: {}", topicName, e.getMessage());
            return Optional.empty();
        }
    }

    public boolean topicExists(String topicName) {
        if (logManager.topicExists(topicName)) return true;
        // Also check controller — topic may exist on another broker
        try {
            com.bounteous.FlowTide.client.model.TopicMetadata meta =
                    controllerClient.getTopicMetadata(topicName);
            return meta != null && meta.getTopic() != null;
        } catch (Exception e) {
            return false;
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Validation
    // ─────────────────────────────────────────────────────────────────────────

    private void validateTopicName(String name) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Topic name must not be blank");
        }
        if (!name.matches("[a-zA-Z0-9._-]+")) {
            throw new IllegalArgumentException(
                    "Topic name '" + name + "' is invalid. Use only letters, digits, dots, underscores, and hyphens.");
        }
        if (name.length() > 249) {
            throw new IllegalArgumentException("Topic name must be 249 characters or fewer");
        }
    }
}
