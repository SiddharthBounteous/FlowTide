package com.bounteous.FlowTide.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.Serializable;

/** Sent periodically by each broker to the registry to prove it's still alive. */
@Getter
@AllArgsConstructor
public class HeartbeatMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String host;
    private final int port;
    private final long timestamp;
    private final long totalEventsStored;
    private final double cpuUsage;
    private final long memoryUsedBytes;
}
