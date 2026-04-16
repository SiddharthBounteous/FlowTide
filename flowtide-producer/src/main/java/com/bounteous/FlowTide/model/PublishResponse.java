package com.bounteous.FlowTide.model;

import lombok.AllArgsConstructor;
import lombok.Data;

/** Returned to the producer after a successful event store. */
@Data
@AllArgsConstructor
public class PublishResponse {
    private String eventId;
    private String topic;
    private int partition;
    private long offset;
    private long timestamp;
}
