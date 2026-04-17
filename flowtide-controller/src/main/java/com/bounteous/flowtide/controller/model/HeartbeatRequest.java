package com.bounteous.flowtide.controller.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Sent by each broker every kafka.cluster.heartbeat-interval-ms milliseconds.
 * The controller uses this to confirm the broker is still alive.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class HeartbeatRequest {
    private String host;
    private int    port;
    private long   timestamp;
    private long   totalEventsStored;

    public String getBrokerId() {
        return host + ":" + port;
    }
}
