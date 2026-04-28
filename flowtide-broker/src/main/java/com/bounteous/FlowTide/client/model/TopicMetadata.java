package com.bounteous.FlowTide.client.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TopicMetadata {
    private String               topic;
    private int                  partitionCount;
    private int                  replicationFactor;
    /** partition index → brokerId ("host:port") of the leader */
    private Map<Integer, String> leaderMap;
    /** Epoch millis set once by the controller when the topic was first created. */
    private long createdAt;
}
