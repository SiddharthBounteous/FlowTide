package com.bounteous.FlowTide.client.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Set;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PartitionAssignment {

    private String              brokerId;
    private List<PartitionRole> assignments;

    /**
     * All topics currently known to the controller.
     * On each heartbeat the broker deletes any local topic not in this set —
     * that is how topic deletions propagate across the cluster automatically.
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
