package com.bounteous.FlowTide.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * The core unit of data in the event streaming platform.
 */
@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Event implements Serializable {

    private static final long serialVersionUID = 2L;

    @Builder.Default
    private String id = UUID.randomUUID().toString();

    private String topic;
    private int partition;
    private String key;
    private String payload;

    @Builder.Default
    private long timestamp = System.currentTimeMillis();

    @Builder.Default
    private Map<String, String> headers = new HashMap<>();

    private long offset;

    @Builder.Default
    private boolean replica = false;

    @Builder.Default
    private DeliveryMode deliveryMode = DeliveryMode.AT_LEAST_ONCE;

    public enum DeliveryMode {
        AT_MOST_ONCE,
        AT_LEAST_ONCE
    }
}
