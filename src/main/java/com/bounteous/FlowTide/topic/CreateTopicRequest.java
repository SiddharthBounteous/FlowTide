package com.bounteous.FlowTide.topic;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;

/** REST request body for creating a topic via the Admin/Topic API. */
@Data
public class CreateTopicRequest {

    @NotBlank(message = "Topic name is required")
    private String name;

    @Min(value = 1, message = "Must have at least 1 partition")
    private int partitions = 3;

    @Min(value = 1, message = "Replication factor must be at least 1")
    private int replicationFactor = 1;

    /** Maximum age in hours before events are evicted. Default: 24 hours. */
    private long retentionHours = 24;

    /** Max events per partition. Default: 1,000,000. */
    private long maxEventsPerPartition = 1_000_000L;
}
