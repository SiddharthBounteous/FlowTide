package com.bounteous.flowtide.controller.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Sent by a broker to the controller when it starts up.
 * The controller uses this to add the broker to the active pool
 * and assign it partitions.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class BrokerRegistration {
    private String host;
    private int port;

    public String getId() {
        return host + ":" + port;
    }
}
