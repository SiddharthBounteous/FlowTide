package com.bounteous.flowtide.controller.model;

import lombok.Data;

/**
 * Represents a registered broker with its runtime state.
 * Tracked by BrokerRegistryService.
 */
@Data
public class BrokerInfo {

    private final String host;
    private final int port;
    private volatile long lastHeartbeat;
    private volatile boolean active;

    public BrokerInfo(String host, int port) {
        this.host          = host;
        this.port          = port;
        this.lastHeartbeat = System.currentTimeMillis();
        this.active        = true;
    }

    public String getId() {
        return host + ":" + port;
    }

    public void updateHeartbeat() {
        this.lastHeartbeat = System.currentTimeMillis();
        this.active        = true;
    }

    public void markDead() {
        this.active = false;
    }
}
