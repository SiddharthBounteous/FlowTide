package com.bounteous.FlowTide.client.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PartitionAssignment {

    private String            brokerId;
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
