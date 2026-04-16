package com.bounteous.FlowTide.topic;

import com.bounteous.FlowTide.audit.AuditLogger;
import com.bounteous.FlowTide.cluster.metadata.MetadataController;
import com.bounteous.FlowTide.server.log.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Collection;
import java.util.Optional;

/**
 * High-level service for topic lifecycle management.
 *
 * <p>Registers topics in BOTH:
 * <ul>
 *   <li>{@link MetadataController} — stores topic config as cluster metadata
 *   <li>{@link LogManager} — pre-creates the {@link com.bounteous.FlowTide.server.log.PartitionLog}
 *       instances that actually hold the events
 * </ul>
 *
 * <p>This keeps metadata and data stores in sync via a single call path.
 */
@Service
public class TopicManager {

    private static final Logger log = LoggerFactory.getLogger(TopicManager.class);

    private final LogManager logManager;
    private final MetadataController metadataController;
    private final AuditLogger auditLogger;

    public TopicManager(LogManager logManager,
                        MetadataController metadataController,
                        AuditLogger auditLogger) {
        this.logManager = logManager;
        this.metadataController = metadataController;
        this.auditLogger = auditLogger;
    }

    /**
     * Creates a topic from an API request.
     * Idempotent — creating an already-existing topic returns the existing config.
     *
     * @throws IllegalArgumentException if the topic name is invalid
     */
    public TopicConfig createTopic(CreateTopicRequest request) {
        validateTopicName(request.getName());

        if (metadataController.topicExists(request.getName())) {
            log.debug("Topic '{}' already exists, returning existing config", request.getName());
            return metadataController.getTopicConfig(request.getName()).orElseThrow();
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

        // Register in MetadataController (cluster metadata)
        metadataController.registerTopic(config);

        // Register in LogManager (creates PartitionLog instances for event storage)
        logManager.registerTopic(config);

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
        if (!metadataController.topicExists(topicName)) {
            throw new IllegalArgumentException("Topic not found: " + topicName);
        }
        metadataController.deleteTopic(topicName);
        logManager.deleteTopic(topicName);
        auditLogger.log("TOPIC_DELETED", "topic=" + topicName);
    }

    public Collection<String> listTopics() {
        return metadataController.getAllTopics();
    }

    public Optional<TopicConfig> getTopic(String topicName) {
        return metadataController.getTopicConfig(topicName);
    }

    public boolean topicExists(String topicName) {
        return metadataController.topicExists(topicName);
    }

    /** Auto-creates a topic with default settings when a producer first publishes to it. */
    public TopicConfig autoCreate(String topicName, int defaultPartitions, int defaultReplicationFactor) {
        CreateTopicRequest req = new CreateTopicRequest();
        req.setName(topicName);
        req.setPartitions(defaultPartitions);
        req.setReplicationFactor(defaultReplicationFactor);
        return createTopic(req);
    }

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
