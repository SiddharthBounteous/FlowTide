package com.bounteous.FlowTide.cluster.metadata;

import com.bounteous.FlowTide.server.registry.BrokerInfo;
import lombok.Getter;

/**
 * A broker with its runtime state — heartbeat timestamp and alive/dead status.
 *
 * <p>Stored in {@link MetadataController} as the authoritative record of each broker.
 * Volatile fields allow safe cross-thread reads without locking.
 */
@Getter
public class BrokerNode {

    public enum Status { ACTIVE, DEAD }

    private final BrokerInfo brokerInfo;
    private volatile long lastHeartbeat;
    private volatile Status status;

    public BrokerNode(BrokerInfo brokerInfo) {
        this.brokerInfo = brokerInfo;
        this.lastHeartbeat = System.currentTimeMillis();
        this.status = Status.ACTIVE;
    }

    public void updateHeartbeat() {
        this.lastHeartbeat = System.currentTimeMillis();
        this.status = Status.ACTIVE;
    }

    public void markDead() {
        this.status = Status.DEAD;
    }

    public boolean isActive() {
        return status == Status.ACTIVE;
    }

    public String key() {
        return brokerInfo.getHost() + ":" + brokerInfo.getPort();
    }

    @Override
    public String toString() {
        return "BrokerNode{" + key() + ", status=" + status + ", lastHb=" + lastHeartbeat + "}";
    }
}
