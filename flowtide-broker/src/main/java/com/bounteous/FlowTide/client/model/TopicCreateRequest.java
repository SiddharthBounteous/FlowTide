package com.bounteous.FlowTide.client.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Sent to flowtide-controller when a new topic is created.
 * The controller assigns partitions across all active brokers and returns
 * the full leader map.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TopicCreateRequest {
    private String topic;
    private int    partitions;
    private int    replicationFactor;
}
