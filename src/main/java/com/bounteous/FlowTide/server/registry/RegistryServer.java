package com.bounteous.FlowTide.server.registry;

import com.bounteous.FlowTide.cluster.metadata.MetadataController;
import com.bounteous.FlowTide.model.HeartbeatMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * TCP registry server that:
 * <ol>
 *   <li>Accepts broker registrations → stored in {@link MetadataController}.
 *   <li>Processes heartbeats → forwarded to {@link MetadataController}.
 *   <li>Serves topic-partition metadata → read from {@link MetadataController}.
 *   <li>Auto-creates topic metadata on first GET_METADATA request.
 * </ol>
 *
 * <p>All state lives in {@link MetadataController} — this class contains zero static fields.
 * Uses a fixed thread pool — no unbounded raw thread creation.
 */
public class RegistryServer {

    private static final Logger log = LoggerFactory.getLogger(RegistryServer.class);

    private final int port;
    private final int defaultPartitions;
    private final int defaultReplicationFactor;
    private final ExecutorService threadPool;
    private final MetadataController metadata;

    public RegistryServer(int port, int defaultPartitions, int defaultReplicationFactor,
                          int threadPoolSize, MetadataController metadata) {
        this.port = port;
        this.defaultPartitions = defaultPartitions;
        this.defaultReplicationFactor = defaultReplicationFactor;
        this.threadPool = Executors.newFixedThreadPool(threadPoolSize,
                r -> new Thread(r, "registry-handler-" + System.nanoTime()));
        this.metadata = metadata;
    }

    public void start() throws Exception {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            log.info("Registry started on port {}", port);
            while (!Thread.currentThread().isInterrupted()) {
                Socket socket = serverSocket.accept();
                threadPool.submit(() -> handle(socket));
            }
        } finally {
            threadPool.shutdown();
        }
    }

    private void handle(Socket socket) {
        try (socket) {
            // ObjectInputStream always needed to read the request.
            // ObjectOutputStream created lazily — only for GET_METADATA which needs a response.
            // Heartbeat and registration clients are fire-and-forget (no OIS on their side),
            // so writing any response would cause a SocketException.
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
            Object request = in.readObject();

            if (request instanceof BrokerInfo broker) {
                handleBrokerRegistration(broker);
            } else if (request instanceof HeartbeatMessage hb) {
                handleHeartbeat(hb);
            } else if (request instanceof String req && req.startsWith("GET_METADATA")) {
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                handleMetadataRequest(req, out);
            } else {
                log.warn("Unknown registry request: {}", request == null ? "null" : request.getClass().getName());
            }

        } catch (Exception e) {
            log.error("Error handling registry request", e);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Broker registration
    // ─────────────────────────────────────────────────────────────────────────

    private void handleBrokerRegistration(BrokerInfo broker) {
        metadata.registerBroker(broker);
        // No response — fire-and-forget from client
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Heartbeat
    // ─────────────────────────────────────────────────────────────────────────

    private void handleHeartbeat(HeartbeatMessage hb) {
        metadata.updateHeartbeat(hb.getHost(), hb.getPort());
        // No response — fire-and-forget from HeartbeatScheduler
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Metadata
    // ─────────────────────────────────────────────────────────────────────────

    private void handleMetadataRequest(String req, ObjectOutputStream out) throws IOException {
        String[] parts = req.split(":", 2);
        String topic = parts.length > 1 ? parts[1].trim() : null;

        // Auto-create topic metadata if this is the first request for it
        if (topic != null && !topic.isEmpty() && metadata.getPartitionMetadata(topic).isEmpty()) {
            List<com.bounteous.FlowTide.server.registry.BrokerInfo> activeBrokers = metadata.getActiveBrokers();
            if (activeBrokers.isEmpty()) {
                log.warn("No brokers registered — cannot auto-create metadata for topic '{}'", topic);
            } else {
                buildMetadata(topic, activeBrokers);
            }
        }

        out.writeObject(metadata.getAllPartitionMetadata());
        out.flush();
    }

    private void buildMetadata(String topic, List<BrokerInfo> activeBrokers) {
        int effectiveRF = Math.min(defaultReplicationFactor, activeBrokers.size());
        if (effectiveRF < defaultReplicationFactor) {
            log.warn("Only {}/{} brokers — creating topic '{}' with replication factor {} (wanted {})",
                    activeBrokers.size(), defaultReplicationFactor, topic, effectiveRF, defaultReplicationFactor);
        }

        List<PartitionMetadata> list = new ArrayList<>();
        for (int i = 0; i < defaultPartitions; i++) {
            BrokerInfo leader = activeBrokers.get(i % activeBrokers.size());
            List<BrokerInfo> replicas = new ArrayList<>();
            for (int j = 1; j < effectiveRF; j++) {
                replicas.add(activeBrokers.get((i + j) % activeBrokers.size()));
            }
            list.add(new PartitionMetadata(topic, i, leader, replicas));
        }

        metadata.updatePartitions(topic, list);
        log.info("Auto-created metadata: topic='{}' partitions={} replication={}", topic, defaultPartitions, effectiveRF);
    }
}
