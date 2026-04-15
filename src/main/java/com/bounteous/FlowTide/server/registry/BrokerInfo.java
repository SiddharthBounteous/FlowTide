package com.bounteous.FlowTide.server.registry;

import java.io.Serializable;
import java.util.Objects;

public class BrokerInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String host;
    private final int port;
    private long lastHeartbeat;

    public BrokerInfo(String host, int port) {
        this.host = host;
        this.port = port;
        this.lastHeartbeat = System.currentTimeMillis();
    }

    public String getHost() { return host; }
    public int getPort() { return port; }
    public long getLastHeartbeat() { return lastHeartbeat; }

    public void updateHeartbeat() {
        this.lastHeartbeat = System.currentTimeMillis();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BrokerInfo that)) return false;
        return port == that.port && Objects.equals(host, that.host);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, port);
    }

    @Override
    public String toString() {
        return host + ":" + port;
    }
}
