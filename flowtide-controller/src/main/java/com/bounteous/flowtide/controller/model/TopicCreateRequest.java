package com.bounteous.flowtide.controller.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Request body for creating a topic on the controller.
 * The controller assigns partitions to brokers and returns full metadata.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TopicCreateRequest {
    private String topic;
    private int    partitions;
    private int    replicationFactor;
}
