package com.bounteous.FlowTide.clients.producer;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class RecordMetaData {
    private final String topic;
    private final int partition;
    private final long offset;
}
