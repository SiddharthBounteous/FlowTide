package com.bounteous.flowtide.controller.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Sent by a consumer to join a group.
 * memberId is NOT included — the coordinator generates it.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class JoinRequest {
    private String topic;
    private int    partitionCount;  // resolved by consumer service from broker metadata
}
