package com.bounteous.FlowTide.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Returned to a consumer after joining a group.
 *
 * <p>The consumer MUST use the {@code memberId} returned here for all
 * subsequent poll and leave calls.  The coordinator (consumer service)
 * generates this ID — the client never chooses it.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class JoinResponse {
    /** Coordinator-assigned unique ID for this consumer instance. */
    private String       memberId;
    private String       groupId;
    private String       topic;
    /** Partition indices assigned to this member. */
    private List<Integer> assignedPartitions;
}
