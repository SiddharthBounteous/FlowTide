package com.bounteous.FlowTide.server.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.Serializable;

@Getter
@AllArgsConstructor
public class FetchRequest implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String topic;
    private final int partition;
    private final long offset;
}
