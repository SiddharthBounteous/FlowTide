package com.bounteous.FlowTide.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * A single record returned in a poll response.
 * Wraps an Event with its partition and offset context.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ConsumerRecord {
    /** Partition this event came from. */
    private int    partition;
    /** Offset of this event in the partition log. */
    private long   offset;
    /** The next offset to consume from this partition (offset + 1). */
    private long   nextOffset;
    /** The actual event payload. */
    private Event  event;
}
