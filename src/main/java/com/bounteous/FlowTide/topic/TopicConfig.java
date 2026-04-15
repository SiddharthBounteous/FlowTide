package com.bounteous.FlowTide.topic;

import lombok.Builder;
import lombok.Getter;

import java.io.Serializable;
import java.time.Instant;

/**
 * Stores the complete configuration for a topic.
 * Created when a topic is first registered and stored in the registry.
 */
@Getter
@Builder
public class TopicConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String name;

    @Builder.Default
    private int partitions = 3;

    @Builder.Default
    private int replicationFactor = 1;

    @Builder.Default
    private RetentionPolicy retentionPolicy = RetentionPolicy.builder().build();

    @Builder.Default
    private long createdAt = Instant.now().toEpochMilli();

    @Builder.Default
    private boolean autoCreateEnabled = true;
}
