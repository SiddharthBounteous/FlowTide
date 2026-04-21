package com.bounteous.FlowTide.metrics;

import lombok.Builder;
import lombok.Getter;

import java.util.Map;

/**
 * Point-in-time snapshot of key platform metrics, returned by GET /api/admin/metrics.
 *
 * <p>Covers requirements:
 * <ul>
 *   <li>Publish throughput (publishedTotal)
 *   <li>Memory usage (totalBytesEstimated)
 *   <li>Partition count (totalPartitions)
 *   <li>Topic count (topicCount)
 *   <li>Node health (captured at call time)
 * </ul>
 */
@Getter
@Builder
public class MetricsSnapshot {
    /** Total events currently in memory across all partitions on this broker. */
    private final long totalEventsStored;

    /** Estimated bytes used by all partition logs on this broker. */
    private final long totalBytesEstimated;

    /** Total number of topics registered on this broker. */
    private final int topicCount;

    /** Total number of partitions across all topics on this broker. */
    private final int totalPartitions;

    /** Per-topic event count. Key = topic name, Value = event count. */
    private final Map<String, Long> eventsPerTopic;

    /** Per-topic estimated bytes. Key = topic name, Value = bytes. */
    private final Map<String, Long> bytesPerTopic;

    /** Approximate publish rate (events published since last snapshot). */
    private final long publishedTotal;

    /** Epoch ms when this snapshot was taken. */
    private final long snapshotTimestamp;
}
