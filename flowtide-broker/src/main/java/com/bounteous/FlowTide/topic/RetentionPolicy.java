package com.bounteous.FlowTide.topic;

import lombok.Builder;
import lombok.Getter;

import java.io.Serializable;
import java.time.Duration;

/**
 * Defines when old events should be evicted from a partition's in-memory log.
 * All three limits are checked; whichever is exceeded first triggers eviction.
 */
@Getter
@Builder
public class RetentionPolicy implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Maximum age of an event before it is evicted.
     * Default: 24 hours.
     */
    @Builder.Default
    private Duration maxAge = Duration.ofHours(24);

    /**
     * Maximum number of events stored per partition.
     * Default: 1,000,000.
     */
    @Builder.Default
    private long maxEventsPerPartition = 1_000_000L;

    /**
     * Maximum memory (in bytes) allowed per partition.
     * Estimated as: event count × average payload size (256 bytes).
     * Default: 256 MB per partition.
     */
    @Builder.Default
    private long maxBytesPerPartition = 256L * 1024 * 1024;

    /** Convenience factory — no limits (for development/testing). */
    public static RetentionPolicy unlimited() {
        return RetentionPolicy.builder()
                .maxAge(Duration.ofDays(365))
                .maxEventsPerPartition(Long.MAX_VALUE)
                .maxBytesPerPartition(Long.MAX_VALUE)
                .build();
    }

    /** Convenience factory — short retention for high-throughput topics. */
    public static RetentionPolicy shortLived() {
        return RetentionPolicy.builder()
                .maxAge(Duration.ofHours(1))
                .maxEventsPerPartition(100_000L)
                .maxBytesPerPartition(64L * 1024 * 1024)
                .build();
    }
}
