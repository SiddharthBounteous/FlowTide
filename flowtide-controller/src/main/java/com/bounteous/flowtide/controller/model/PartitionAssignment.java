package com.bounteous.flowtide.controller.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Returned to a broker after registration.
 * Tells the broker exactly which partitions (per topic) it is the leader for.
 *
 * Example:
 * {
 *   brokerId: "localhost:8083",
 *   assignments: [
 *     { topic: "orders",   partition: 0, role: LEADER   },
 *     { topic: "orders",   partition: 2, role: FOLLOWER },
 *     { topic: "payments", partition: 0, role: LEADER   }
 *   ]
 * }
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PartitionAssignment {

    private String brokerId;
    private List<PartitionRole> assignments;

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
