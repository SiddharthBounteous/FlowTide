package com.bounteous.FlowTide.model;

import lombok.Builder;
import lombok.Getter;

import java.util.List;
import java.util.Map;

/**
 * Response returned on every poll call.
 *
 * <p>Mirrors what a Kafka consumer gets from {@code poll()} but as a REST body:
 * <ul>
 *   <li>Which member is active (may have changed if auto-rejoin happened)
 *   <li>Which partitions are assigned to this member right now
 *   <li>The actual records, grouped with their partition + offset context
 *   <li>Committed offsets per partition (after autoCommit)
 *   <li>Consumer lag per partition (how many unconsumed events remain)
 *   <li>Whether a rebalance / auto-rejoin occurred during this poll
 * </ul>
 */
@Getter
@Builder
public class PollResponse {

    /** Active memberId — may differ from the requested memberId after auto-rejoin. */
    private final String memberId;

    private final String groupId;
    private final String topic;

    /** Partitions currently assigned to this member. */
    private final List<Integer> assignedPartitions;

    /** Total records returned in this poll. */
    private final int recordCount;

    /** The actual events with full partition+offset context. */
    private final List<ConsumerRecord> records;

    /**
     * Committed offset per partition after this poll.
     * Key = partition index (as String for JSON compatibility).
     * Only populated when autoCommit=true.
     */
    private final Map<String, Long> committedOffsets;

    /**
     * Consumer lag per partition: latestOffset - committedOffset.
     * Shows how many unconsumed events remain in each partition.
     * Key = partition index (as String for JSON compatibility).
     */
    private final Map<String, Long> lagPerPartition;

    /**
     * True if this member was auto-rejoined during this poll call
     * (e.g. controller restarted). The caller should update their
     * stored memberId to the new value in this response.
     */
    private final boolean rebalanced;

    /** Epoch milliseconds when this poll was served. */
    private final long pollTimestamp;
}
