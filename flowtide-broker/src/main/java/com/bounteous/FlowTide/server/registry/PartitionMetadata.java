package com.bounteous.FlowTide.server.registry;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.Serializable;
import java.util.List;

@Getter
@AllArgsConstructor
public class PartitionMetadata implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String topic;
    private final int partitionId;
    private final BrokerInfo leader;
    private final List<BrokerInfo> replicas;
}
