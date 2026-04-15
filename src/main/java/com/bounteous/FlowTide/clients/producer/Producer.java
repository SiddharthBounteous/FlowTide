package com.bounteous.FlowTide.clients.producer;

import java.util.concurrent.Future;

public interface Producer<K, V> {

    Future<RecordMetaData> send(ProducerRecord<K, V> record);

    void flush();

    void close();
}
