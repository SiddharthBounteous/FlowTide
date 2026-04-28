package com.bounteous.flowtide.controller.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Set;

/**
 * Returned to a broker after registration or heartbeat.
 *
 * <p>Contains two things:
 * <ol>
 *   <li>{@code assignments} — which partitions this broker leads/follows.
 *   <li>{@code knownTopics} — the full set of topics the controller knows about.
 *       Brokers use this on every heartbeat to detect and remove topics that were
 *       deleted on another broker, keeping all brokers in sync without extra calls.
 * </ol>
 *
 * Example:
 * {
 *   brokerId: "localhost:8083",
 *   assignments: [
 *     { topic: "orders",   partition: 0, role: LEADER   },
 *     { topic: "orders",   partition: 2, role: FOLLOWER }
 *   ],
 *   knownTopics: ["orders", "payments"]
 * }
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PartitionAssignment {

    private String brokerId;
    private List<PartitionRole> assignments;

    /**
     * All topics currently known to the controller.
     * Brokers delete any local topic not in this set on each heartbeat.
     */
    private Set<String> knownTopics;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PartitionRole {
        private String topic;
        private int    partition;
        private Role   role;
    }

    public enum Role {
        LEADER,
        FOLLOWER
    }
}
