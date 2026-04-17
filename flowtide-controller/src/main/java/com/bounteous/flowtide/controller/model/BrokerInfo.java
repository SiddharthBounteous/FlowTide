package com.bounteous.flowtide.controller.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * Represents a registered broker with its runtime state.
 * Tracked by BrokerRegistryService.
 *
 * <p>Uses explicit Lombok annotations instead of @Data to avoid a duplicate-
 * constructor conflict: @Data would generate BrokerInfo(String host, int port)
 * from the two final fields, clashing with the manual constructor that also
 * initialises lastHeartbeat and active.
 */
@Getter
@Setter
@ToString
@EqualsAndHashCode(of = {"host", "port"})
public class BrokerInfo {

    private final String host;
    private final int    port;

    private volatile long    lastHeartbeat;
    private volatile boolean active;

    public BrokerInfo(String host, int port) {
        this.host          = host;
        this.port          = port;
        this.lastHeartbeat = System.currentTimeMillis();
        this.active        = true;
    }

    /** Unique broker identifier used as map key everywhere. */
    public String getId() {
        return host + ":" + port;
    }

    /** Called on each heartbeat — marks broker alive and refreshes timestamp. */
    public void updateHeartbeat() {
        this.lastHeartbeat = System.currentTimeMillis();
        this.active        = true;
    }

    /** Called by HealthCheckScheduler when heartbeat times out. */
    public void markDead() {
        this.active = false;
    }
}
