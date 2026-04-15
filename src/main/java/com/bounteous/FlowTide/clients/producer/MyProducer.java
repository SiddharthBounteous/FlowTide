package com.bounteous.FlowTide.clients.producer;

import com.bounteous.FlowTide.model.Event;
import com.bounteous.FlowTide.server.registry.BrokerInfo;
import com.bounteous.FlowTide.server.registry.PartitionMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * TCP producer — the ONLY path events take from REST API to broker storage.
 *
 * <p>Flow for every event:
 * <ol>
 *   <li>Ask the Registry: "Which broker is leader for topic X, partition Y?"
 *   <li>Pick partition (key-hash or round-robin).
 *   <li>Open TCP socket directly to that leader broker.
 *   <li>Send the {@link Event} object.
 *   <li>Read back the real offset the broker assigned.
 * </ol>
 *
 * <p>This means even in a multi-broker cluster, each event always reaches
 * the correct broker — not just whichever JVM the REST API happens to run on.
 */
public class MyProducer<K, V> implements Producer<K, V> {

    private static final Logger log = LoggerFactory.getLogger(MyProducer.class);

    private final String registryHost;
    private final int registryPort;
    private final ExecutorService executor;

    public MyProducer(String registryHost, int registryPort, int threadPoolSize) {
        this.registryHost = registryHost;
        this.registryPort = registryPort;
        this.executor = Executors.newFixedThreadPool(threadPoolSize,
                r -> new Thread(r, "producer-sender-" + System.nanoTime()));
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Primary API — used by ProducerService (rich Event with all metadata)
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Publishes a fully built {@link Event} to its correct broker via TCP.
     * Partition and leader are resolved from the Registry at send time.
     *
     * @return a Future that resolves to the broker-assigned RecordMetaData
     */
    public Future<RecordMetaData> sendEvent(Event event) {
        CompletableFuture<RecordMetaData> result = new CompletableFuture<>();

        executor.submit(() -> {
            try {
                // 1. Fetch live metadata from registry
                Map<String, List<PartitionMetadata>> metadata = fetchMetadata(event.getTopic());
                List<PartitionMetadata> partitions = metadata.get(event.getTopic());

                if (partitions == null || partitions.isEmpty()) {
                    throw new IllegalStateException("No partitions available for topic: " + event.getTopic());
                }

                // 2. Select partition (key-hash for ordering, random for load balance)
                int partition = selectPartition(event.getKey(), partitions.size());
                event.setPartition(partition);

                // 3. Get the leader broker for this partition
                BrokerInfo leader = partitions.get(partition).getLeader();
                log.debug("Routing event to leader {} for topic={} partition={}", leader, event.getTopic(), partition);

                // 4. Send over TCP and read back the real broker-assigned offset
                long offset = sendToLeader(leader, event);

                result.complete(new RecordMetaData(event.getTopic(), partition, offset));

            } catch (Exception e) {
                log.error("Failed to send event to topic {}", event.getTopic(), e);
                result.completeExceptionally(e);
            }
        });

        return result;
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Legacy API — Producer<K,V> interface (wraps sendEvent)
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Future<RecordMetaData> send(ProducerRecord<K, V> record) {
        Event event = Event.builder()
                .topic(record.getTopic())
                .key(record.getKey() != null ? record.getKey().toString() : null)
                .payload(record.getValue() != null ? record.getValue().toString() : "")
                .build();
        return sendEvent(event);
    }

    @Override
    public void flush() {
        // No-op for the singleton producer.
        // Callers can call .get() on the returned Future to wait for individual sends.
    }

    @Override
    public void close() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Internal
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Opens a TCP connection to the leader broker, sends the Event,
     * reads back the broker-assigned offset, then closes the connection.
     */
    private long sendToLeader(BrokerInfo leader, Event event) throws Exception {
        try (Socket socket = new Socket(leader.getHost(), leader.getPort())) {
            // IMPORTANT: write first, then create ObjectInputStream.
            // The broker (RequestHandler) creates ObjectOutputStream lazily AFTER reading
            // the request. If we create ObjectInputStream before writing, both sides block
            // waiting for each other's stream header → deadlock → "socket hang up".
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.writeObject(event);
            out.flush();

            // Broker has now received the Event, created its OOS, written its stream header.
            // Safe to create OIS and read the offset back.
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
            return in.readLong();
        }
    }

    /**
     * Asks the Registry for the full topic-partition-leader mapping.
     * Sends "GET_METADATA:{topic}" and receives a Map deserialized via Java serialization.
     */
    @SuppressWarnings("unchecked")
    private Map<String, List<PartitionMetadata>> fetchMetadata(String topic) throws Exception {
        try (Socket socket = new Socket(registryHost, registryPort)) {
            // Write request first — registry creates OOS lazily after reading the request.
            // Creating OIS before writing would deadlock both sides on stream header exchange.
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.writeObject("GET_METADATA:" + topic);
            out.flush();

            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
            return (Map<String, List<PartitionMetadata>>) in.readObject();
        }
    }

    private int selectPartition(String key, int numPartitions) {
        if (key == null || key.isBlank()) {
            // Round-robin based on thread ID — uniform load, no contention
            return (int) (Thread.currentThread().getId() % numPartitions);
        }
        // Consistent hash — same key always goes to the same partition
        return Math.abs(key.hashCode()) % numPartitions;
    }
}
