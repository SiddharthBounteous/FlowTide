package com.bounteous.FlowTide.clients.consumer;

import com.bounteous.FlowTide.model.Event;
import com.bounteous.FlowTide.model.FetchRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Collections;
import java.util.List;

/**
 * TCP consumer — connects directly to a broker and fetches {@link Event} objects.
 *
 * <p>One instance is created per broker connection. In a multi-broker cluster,
 * the caller is responsible for creating a Consumer pointed at the correct
 * leader broker for each partition (resolved via {@link com.bounteous.FlowTide.server.broker.MetadataCache}).
 *
 * <p>Flow:
 * <pre>
 * ConsumerController
 *   → new Consumer(leaderHost, leaderPort)
 *   → pollAndReturn(topic, partition, offset, maxMessages)
 *   → TCP socket to broker
 *   → sends FetchRequest
 *   → RequestHandler.handleFetch()
 *   → PartitionLog.readFrom()
 *   → returns List&lt;Event&gt; over TCP
 * </pre>
 */
public class Consumer {

    private static final Logger log = LoggerFactory.getLogger(Consumer.class);

    private final String host;
    private final int port;

    public Consumer(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /**
     * Fetches events from a specific partition on this broker.
     * Returns an empty list on error (silent fail — use {@link #pollAndReturn} if you need the exception).
     */
    public List<Event> poll(String topic, int partition, long offset, int maxMessages) {
        try {
            return pollAndReturn(topic, partition, offset, maxMessages);
        } catch (Exception e) {
            log.error("Poll failed: topic={} partition={} offset={} broker={}:{}",
                    topic, partition, offset, host, port, e);
            return Collections.emptyList();
        }
    }

    /**
     * Fetches events and propagates exceptions to the caller.
     *
     * @param topic       the topic to read from
     * @param partition   the partition index
     * @param offset      the starting offset (inclusive)
     * @param maxMessages maximum number of events to return
     * @return list of events from [offset, offset+maxMessages)
     * @throws RuntimeException wrapping the underlying I/O or deserialization error
     */
    @SuppressWarnings("unchecked")
    public List<Event> pollAndReturn(String topic, int partition, long offset, int maxMessages) {
        try (Socket socket = new Socket(host, port)) {
            // Write request first — broker creates OOS lazily after reading the FetchRequest.
            // Creating OIS before writing would deadlock both sides on stream header exchange.
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.writeObject(new FetchRequest(topic, partition, offset, maxMessages, null));
            out.flush();

            // Broker has now read the request, created its OOS, written its stream header.
            // Safe to create OIS and read List<Event> back.
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
            return (List<Event>) in.readObject();

        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("Fetch failed: broker=%s:%d topic=%s partition=%d offset=%d",
                            host, port, topic, partition, offset), e);
        }
    }
}
