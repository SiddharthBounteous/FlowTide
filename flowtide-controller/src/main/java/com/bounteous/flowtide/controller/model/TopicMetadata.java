package com.bounteous.flowtide.controller.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Full metadata for a topic returned by the controller.
 * Brokers use this to know who is the leader for each partition.
 *
 * Example:
 * {
 *   topic: "orders",
 *   partitionCount: 3,
 *   leaderMap: {
 *     0: "localhost:8083",
 *     1: "localhost:8084",
 *     2: "localhost:8083"
 *   }
 * }
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TopicMetadata {
    private String               topic;
    private int                  partitionCount;
    /** partition index → brokerId ("host:port") of the leader */
    private Map<Integer, String> leaderMap;
}
