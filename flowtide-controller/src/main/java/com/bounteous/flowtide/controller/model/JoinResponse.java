package com.bounteous.flowtide.controller.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Returned to a consumer after successfully joining a group.
 *
 * <p>The consumer MUST use the returned {@code memberId} for all
 * subsequent poll, heartbeat, and leave calls.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class JoinResponse {
    /** Coordinator-generated unique member ID. Never client-chosen. */
    private String        memberId;
    private String        groupId;
    private String        topic;
    /** Partition indices assigned to this member. */
    private List<Integer> assignedPartitions;
}
