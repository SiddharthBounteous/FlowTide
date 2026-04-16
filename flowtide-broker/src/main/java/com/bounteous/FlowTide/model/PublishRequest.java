package com.bounteous.FlowTide.model;

import jakarta.validation.constraints.NotBlank;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;

/** REST request body for publishing a single event. */
@Data
public class PublishRequest {

    @NotBlank(message = "Topic is required")
    private String topic;

    /** Optional. Drives partition selection via consistent hashing. */
    private String key;

    @NotBlank(message = "Payload is required")
    private String payload;

    private Map<String, String> headers = new HashMap<>();

    private Event.DeliveryMode deliveryMode = Event.DeliveryMode.AT_LEAST_ONCE;
}
