package com.bounteous.FlowTide.topic;

import com.bounteous.FlowTide.audit.AuditLogger;
import com.bounteous.FlowTide.client.ControllerClient;
import com.bounteous.FlowTide.client.model.TopicCreateRequest;
import com.bounteous.FlowTide.server.registry.LocalBrokerRegistry;
import com.bounteous.FlowTide.server.log.LogManager;
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
 * <h3>What happens when a topic is created</h3>
 * <ol>
 *   <li>Validate the topic name.
 *   <li>Register config in local {@link MetadataController} (admin API, retention).
 *   <li>Pre-create {@link com.bounteous.FlowTide.server.log.PartitionLog} instances
 *       in {@link LogManager} so this broker can immediately accept writes
 *       for any partition it is assigned as leader.
 *   <li>Notify <b>flowtide-controller</b> via {@link ControllerClient#createTopic}
 *       so the controller assigns partitions to <em>all</em> brokers in the cluster.
 *       Other brokers learn their follower/leader roles on the next heartbeat cycle.
 * </ol>
 *
 * <p>Without step 4, topic data would stay on a single broker because the
 * controller would never know the topic exists and would never assign followers.
 */
@Service
public class TopicManager {

    private static final Logger log = LoggerFactory.getLogger(TopicManager.class);

    private final LogManager         logManager;
    private final LocalBrokerRegistry localRegistry;
    private final AuditLogger        auditLogger;
    private final ControllerClient   controllerClient;

    public TopicManager(LogManager logManager,
                        LocalBrokerRegistry localRegistry,
                        AuditLogger auditLogger,
                        ControllerClient controllerClient) {
        this.logManager    = logManager;
        this.localRegistry = localRegistry;
        this.auditLogger   = auditLogger;
        this.controllerClient = controllerClient;
    }

    /**
     * Creates a topic from an API request.
     * Idempotent — creating an already-existing topic returns the existing config.
     *
     * @throws IllegalArgumentException if the topic name is invalid
     */
    public TopicConfig createTopic(CreateTopicRequest request) {
        validateTopicName(request.getName());

        if (localRegistry.topicExists(request.getName())) {
            log.debug("Topic '{}' already exists — returning existing config", request.getName());
            return localRegistry.getTopicConfig(request.getName()).orElseThrow();
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

        // 1. Register locally — this broker can now store events for this topic
        localRegistry.registerTopic(config);
        logManager.registerTopic(config);

        // 2. Notify the controller — assigns partitions across ALL brokers in the cluster
        //    so every broker learns which partitions it leads/follows via heartbeat.
        try {
            TopicCreateRequest controllerRequest = new TopicCreateRequest(
                    request.getName(),
                    request.getPartitions(),
                    request.getReplicationFactor());
            controllerClient.createTopic(controllerRequest);
            log.info("Topic '{}' registered with controller — partitions will be distributed across cluster",
                    request.getName());
        } catch (Exception e) {
            // Controller is unavailable — topic still works locally (single-broker mode)
            log.warn("Could not register topic '{}' with controller: {} " +
                     "— topic is local only until controller is reachable.",
                    request.getName(), e.getMessage());
        }

        auditLogger.log("TOPIC_CREATED", "topic=" + request.getName() +
                " partitions=" + request.getPartitions() +
                " replication=" + request.getReplicationFactor());

        return config;
    }

    /**
     * Deletes a topic and all its stored events.
     *
     * @throws IllegalArgumentException if the topic does not exist
     */
    public void deleteTopic(String topicName) {
        if (!localRegistry.topicExists(topicName)) {
            throw new IllegalArgumentException("Topic not found: " + topicName);
        }
        localRegistry.deleteTopic(topicName);
        logManager.deleteTopic(topicName);
        auditLogger.log("TOPIC_DELETED", "topic=" + topicName);
    }

    /**
     * Lists all topics known across the entire cluster.
     *
     * <p>Queries flowtide-controller first (global, authoritative view).
     * Falls back to the broker-local registry when the controller is unreachable
     * (e.g. single-broker dev mode without the controller running).
     */
    public Collection<String> listTopics() {
        try {
            Set<String> controllerTopics = controllerClient.getAllTopics().keySet();
            log.debug("listTopics(): {} topic(s) from controller", controllerTopics.size());
            return controllerTopics;
        } catch (Exception e) {
            log.warn("Cannot reach controller for topic list ({}); falling back to local registry",
                    e.getMessage());
            return localRegistry.getAllTopics();
        }
    }

    /**
     * Returns the local config for a topic.
     * If not registered locally (e.g. created on another broker), synthesises
     * a minimal {@link TopicConfig} from the controller's partition count.
     */
    public Optional<TopicConfig> getTopic(String topicName) {
        // 1. Local registry — has full config (retention, rf, …)
        Optional<TopicConfig> local = localRegistry.getTopicConfig(topicName);
        if (local.isPresent()) {
            return local;
        }

        // 2. Controller — may know the topic even if this broker never registered it
        try {
            com.bounteous.FlowTide.client.model.TopicMetadata meta =
                    controllerClient.getTopicMetadata(topicName);
            if (meta == null || meta.getTopic() == null) {
                return Optional.empty();
            }
            TopicConfig synthesised = TopicConfig.builder()
                    .name(meta.getTopic())
                    .partitions(meta.getPartitionCount())
                    .replicationFactor(1) // unknown at this point; controller doesn't store rf
                    .retentionPolicy(RetentionPolicy.builder().build())
                    .build();
            return Optional.of(synthesised);
        } catch (Exception e) {
            log.debug("Topic '{}' not found in controller: {}", topicName, e.getMessage());
            return Optional.empty();
        }
    }

    public boolean topicExists(String topicName) {
        return localRegistry.topicExists(topicName);
    }

    /**
     * Auto-creates a topic with default settings when a producer first publishes to it.
     * Also notifies the controller so partitions are distributed.
     */
    public TopicConfig autoCreate(String topicName, int defaultPartitions, int defaultReplicationFactor) {
        CreateTopicRequest req = new CreateTopicRequest();
        req.setName(topicName);
        req.setPartitions(defaultPartitions);
        req.setReplicationFactor(defaultReplicationFactor);
        return createTopic(req);
    }

    // -------------------------------------------------------------------------

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
