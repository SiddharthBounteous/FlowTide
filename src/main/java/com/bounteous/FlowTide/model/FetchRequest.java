package com.bounteous.FlowTide.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.Serializable;

/** Sent over TCP from Consumer to Broker to fetch events from a partition. */
@Getter
@AllArgsConstructor
public class FetchRequest implements Serializable {

    private static final long serialVersionUID = 2L;

    private final String topic;
    private final int partition;
    private final long offset;
    private final int maxMessages;

    /** Optional consumer group for offset tracking. */
    private final String consumerGroupId;
}
