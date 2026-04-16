package com.bounteous.FlowTide.metrics;

import lombok.Builder;
import lombok.Getter;

/** A point-in-time snapshot of key platform metrics, returned by the Admin API. */
@Getter
@Builder
public class MetricsSnapshot {
    private final long totalEventsStored;
    private final long totalBytesEstimated;
    private final int topicCount;
}
