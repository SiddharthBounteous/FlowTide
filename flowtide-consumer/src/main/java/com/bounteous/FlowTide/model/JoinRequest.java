package com.bounteous.FlowTide.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Sent to the coordinator when joining a consumer group.
 * memberId is NOT included — the coordinator generates it.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class JoinRequest {
    private String topic;
    private int    partitionCount;
}
