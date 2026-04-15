package com.bounteous.FlowTide.model;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;

/** REST request body for manually committing a consumer group offset. */
@Data
public class OffsetCommitRequest {

    @NotBlank(message = "Group ID is required")
    private String groupId;

    @NotBlank(message = "Topic is required")
    private String topic;

    @Min(0)
    private int partition;

    @Min(0)
    private long offset;
}
