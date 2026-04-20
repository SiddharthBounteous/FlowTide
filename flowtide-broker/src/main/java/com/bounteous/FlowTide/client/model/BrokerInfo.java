package com.bounteous.FlowTide.client.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

/**
 * Controller's view of a registered broker — deserialized from
 * GET /controller/brokers/active responses.
 *
 * <p>Field names match the controller's {@code BrokerInfo} JSON output
 * so Jackson can deserialize automatically.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class BrokerInfo {
    private String  host;
    private int     port;
    private long    lastHeartbeat;
    private boolean active;

    /** Convenience constructor used when only host/port are needed. */
    public BrokerInfo(String host, int port) {
        this.host   = host;
        this.port   = port;
        this.active = true;
    }

    public String getId() {
        return host + ":" + port;
    }
}
