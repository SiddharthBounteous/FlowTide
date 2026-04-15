package com.bounteous.FlowTide.clients.producer;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class ProducerRecord<K, V> {
    private final String topic;
    private final K key;
    private final V value;
}
