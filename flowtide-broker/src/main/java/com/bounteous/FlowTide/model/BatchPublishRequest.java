package com.bounteous.FlowTide.model;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

import java.util.List;

/** REST request body for publishing multiple events atomically. */
@Data
public class BatchPublishRequest {

    @NotNull
    @NotEmpty(message = "Events list must not be empty")
    @Valid
    private List<PublishRequest> events;
}
