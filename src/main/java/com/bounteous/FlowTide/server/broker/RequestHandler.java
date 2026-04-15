package com.bounteous.FlowTide.server.broker;

import com.bounteous.FlowTide.model.Event;
import com.bounteous.FlowTide.model.FetchRequest;
import com.bounteous.FlowTide.server.log.LogManager;
import com.bounteous.FlowTide.server.log.PartitionLog;
import com.bounteous.FlowTide.server.registry.BrokerInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.StreamCorruptedException;
import java.net.Socket;
import java.util.List;

/**
 * Handles a single TCP connection from a producer or consumer client.
 *
 * <p>Protocol (binary, Java serialization):
 * <ul>
 *   <li><b>Produce</b>: client sends {@link Event} → broker appends, replies with {@code long} offset.
 *       For AT_MOST_ONCE events the broker still stores them; the producer just doesn't wait for the reply.
 *   <li><b>Fetch</b>: client sends {@link FetchRequest} → broker replies with {@code List<Event>}.
 * </ul>
 */
public class RequestHandler {

    private static final Logger log = LoggerFactory.getLogger(RequestHandler.class);

    private final LogManager logManager;

    public RequestHandler(LogManager logManager) {
        this.logManager = logManager;
    }

    public void handle(Socket socket) {
        try (socket) {
            // ObjectInputStream is always needed to read the incoming request.
            // ObjectOutputStream is created lazily because clients fall into two categories:
            //
            //  1. Real producer (MyProducer)  — creates OOS then OIS on its side
            //                                   → needs offset written back   → lazy OOS created here
            //  2. Leader replicating to follower — creates OOS only (fire-and-forget)
            //                                   → must NOT write any response  → OOS never created
            //  3. Consumer (fetch)            — creates OOS then OIS on its side
            //                                   → needs List<Event> back       → lazy OOS created here
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
            Object request = in.readObject();

            if (request instanceof Event event) {
                handleProduce(event, socket);
            } else if (request instanceof FetchRequest fetchRequest) {
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                handleFetch(fetchRequest, out);
            } else {
                log.warn("Unknown request type: {}", request == null ? "null" : request.getClass().getName());
            }

        } catch (StreamCorruptedException e) {
            // Someone connected to the broker's TCP port using HTTP/HTTPS instead of
            // the Java serialization protocol. This is harmless — just a wrong-port hit
            // (e.g. browser, curl, or health-check tool targeting :9092 instead of :8080).
            // Header bytes 0x16 0x03 0x03 = TLS 1.2 ClientHello handshake.
            log.warn("Non-Java-serialization connection rejected on broker port from {} " +
                     "(expected Java serialization, got HTTP/TLS — use port 8080 for REST API)",
                     socket.getRemoteSocketAddress());
        } catch (Exception e) {
            log.error("Error handling request from {}", socket.getRemoteSocketAddress(), e);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Produce
    // ─────────────────────────────────────────────────────────────────────────

    private void handleProduce(Event event, Socket socket) throws IOException {
        PartitionLog partitionLog = logManager.getLog(event.getTopic(), event.getPartition());
        long offset = partitionLog.append(event);

        log.info("Stored event: topic={} partition={} offset={} role={} id={}",
                event.getTopic(), event.getPartition(), offset,
                event.isReplica() ? "REPLICA" : "LEADER", event.getId());

        if (event.isReplica()) {
            // Replica event sent by a leader broker — fire-and-forget.
            // The leader never created an ObjectInputStream on its side,
            // so writing anything back here would cause a SocketException.
            return;
        }

        // Real producer connection: write offset back so the producer can confirm.
        ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
        out.writeLong(offset);
        out.flush();

        // Now replicate to all follower brokers (after responding to producer).
        replicateToFollowers(event);
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Fetch
    // ─────────────────────────────────────────────────────────────────────────

    private void handleFetch(FetchRequest request, ObjectOutputStream out) throws IOException {
        PartitionLog partitionLog = logManager.getLog(request.getTopic(), request.getPartition());
        List<Event> events = partitionLog.readFrom(request.getOffset(), request.getMaxMessages());

        log.debug("Fetch: topic={} partition={} offset={} returned={}",
                request.getTopic(), request.getPartition(), request.getOffset(), events.size());

        out.writeObject(events);
        out.flush();
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Replication
    // ─────────────────────────────────────────────────────────────────────────

    private void replicateToFollowers(Event event) {
        List<BrokerInfo> followers = MetadataCache.getFollowers(event.getTopic(), event.getPartition());
        if (followers.isEmpty()) return;

        event.setReplica(true);
        for (BrokerInfo follower : followers) {
            try (
                Socket socket = new Socket(follower.getHost(), follower.getPort());
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())
            ) {
                out.writeObject(event);
                out.flush();
            } catch (Exception e) {
                log.error("Replication failed to follower {}: {}", follower, e.getMessage());
            }
        }
    }
}
