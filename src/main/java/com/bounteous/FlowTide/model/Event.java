package com.bounteous.FlowTide.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * The core unit of data in the event streaming platform.
 * Replaces the old Message class with a richer, industry-standard model.
 */
@Getter
@Setter
@Builder
@AllArgsConstructor
public class Event implements Serializable {

    private static final long serialVersionUID = 2L;

    /** Auto-generated unique ID for deduplication and tracing. */
    @Builder.Default
    private String id = UUID.randomUUID().toString();

    private String topic;
    private int partition;

    /** Optional. If present, events with the same key always go to the same partition. */
    private String key;

    private String payload;

    @Builder.Default
    private long timestamp = System.currentTimeMillis();

    /** Arbitrary key-value metadata (e.g. source, version, correlation-id). */
    @Builder.Default
    private Map<String, String> headers = new HashMap<>();

    /** Assigned by the broker on append. Zero until stored. */
    private long offset;

    /**
     * True when this event is being replicated to a follower.
     * Prevents infinite replication loops.
     */
    @Builder.Default
    private boolean replica = false;

    /** Delivery guarantee requested by the producer. */
    @Builder.Default
    private DeliveryMode deliveryMode = DeliveryMode.AT_LEAST_ONCE;

    public enum DeliveryMode {
        /** Send and forget. No ack from broker. Lowest latency. */
        AT_MOST_ONCE,
        /** Wait for broker ack (offset). Message may duplicate on retry but won't be lost. */
        AT_LEAST_ONCE
    }
}
