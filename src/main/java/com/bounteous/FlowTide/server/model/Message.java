package com.bounteous.FlowTide.server.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
@AllArgsConstructor
public class Message implements Serializable {

    private static final long serialVersionUID = 1L;

    private String topic;
    private int partition;
    private String key;
    private String value;
    /** True when this message is being replicated to a follower, preventing infinite replication loops. */
    private boolean replica;
}
