package com.bounteous.FlowTide.client.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class HeartbeatRequest {
    private String host;
    private int    port;
    private long   timestamp;
    private long   totalEventsStored;
}
