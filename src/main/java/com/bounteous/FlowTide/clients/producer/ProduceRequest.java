package com.bounteous.FlowTide.clients.producer;

import lombok.Data;

@Data
public class ProduceRequest {
    private String topic;
    private String key;
    private String value;
}
