package com.bounteous.FlowTide.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Topic metadata returned by flowtide-controller.
 *
 * <p>The leaderMap tells the consumer exactly which broker host:port
 * is the leader for each partition, so it can call that broker directly
 * instead of going through the Eureka load balancer and risking a wrong-broker hit.
 *
 * <p>Example:
 * <pre>
 * {
 *   "topic": "orders",
 *   "partitionCount": 3,
 *   "leaderMap": {
 *     "0": "localhost:8083",
 *     "1": "localhost:8084",
 *     "2": "localhost:8083"
 *   }
 * }
 * </pre>
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TopicMetadata {
    private String               topic;
    private int                  partitionCount;
    /** partition index → "host:port" of the current leader broker */
    private Map<Integer, String> leaderMap;
}
