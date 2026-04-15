package com.bounteous.FlowTide.server.broker;

import com.bounteous.FlowTide.cluster.metadata.MetadataController;
import com.bounteous.FlowTide.server.registry.BrokerInfo;
import com.bounteous.FlowTide.server.registry.PartitionMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Provides static accessor methods for topic-partition metadata, backed by
 * {@link MetadataController} as the single source of truth.
 *
 * <p>Two modes:
 * <ul>
 *   <li><b>Registry JVM</b> ({@code kafka.registry.start=true}): {@link MetadataController}
 *       is already authoritative — no TCP call needed.
 *   <li><b>Broker-only JVM</b> ({@code kafka.registry.start=false}): fetches from the
 *       registry TCP server on cache miss and stores the result in the local
 *       {@link MetadataController} for subsequent reads.
 * </ul>
 *
 * <p>Static methods are needed because {@link RequestHandler} is not a Spring bean
 * (it's created per-connection). Spring injects the singleton references via setters.
 */
@Component
public class MetadataCache {

    private static final Logger log = LoggerFactory.getLogger(MetadataCache.class);

    private static MetadataController metadataController;
    private static String registryHost = "localhost";
    private static int registryPort = 7070;
    private static boolean isLocalRegistry = true;

    @Autowired
    public void setMetadataController(MetadataController mc) {
        MetadataCache.metadataController = mc;
    }

    @Value("${kafka.registry.host:localhost}")
    public void setRegistryHost(String host) { MetadataCache.registryHost = host; }

    @Value("${kafka.registry.port:7070}")
    public void setRegistryPort(int port) { MetadataCache.registryPort = port; }

    @Value("${kafka.registry.start:true}")
    public void setIsLocalRegistry(boolean isRegistry) { MetadataCache.isLocalRegistry = isRegistry; }

    // ─────────────────────────────────────────────────────────────────────────
    //  Public static accessors  (called by RequestHandler, ConsumerController)
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Returns the leader broker for a topic-partition.
     * Refreshes from the registry TCP if this is a broker-only node and the topic is unknown.
     */
    public static Optional<BrokerInfo> getLeader(String topic, int partition) {
        ensureLoaded(topic);
        return metadataController != null ? metadataController.getLeader(topic, partition) : Optional.empty();
    }

    /**
     * Returns follower brokers for a topic-partition.
     * Refreshes from the registry TCP if this is a broker-only node and the topic is unknown.
     */
    public static List<BrokerInfo> getFollowers(String topic, int partition) {
        ensureLoaded(topic);
        return metadataController != null ? metadataController.getFollowers(topic, partition) : Collections.emptyList();
    }

    /**
     * Explicitly refresh all metadata from the registry TCP server.
     * No-op if this JVM IS the registry (MetadataController is already authoritative).
     */
    public static void refresh() {
        if (isLocalRegistry) return; // already authoritative — nothing to fetch
        try (Socket socket = new Socket(registryHost, registryPort)) {
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.writeObject("GET_METADATA");
            out.flush();

            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
            @SuppressWarnings("unchecked")
            Map<String, List<PartitionMetadata>> fetched =
                    (Map<String, List<PartitionMetadata>>) in.readObject();

            if (metadataController != null) {
                fetched.forEach(metadataController::updatePartitions);
            }
            log.debug("Metadata refreshed from registry {}:{} — {} topics", registryHost, registryPort, fetched.size());
        } catch (Exception e) {
            log.error("Failed to refresh metadata from registry at {}:{}", registryHost, registryPort, e);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Internal
    // ─────────────────────────────────────────────────────────────────────────

    /** Lazy load: fetch from registry only if this topic is not yet known locally. */
    private static void ensureLoaded(String topic) {
        if (!isLocalRegistry && metadataController != null
                && metadataController.getPartitionMetadata(topic).isEmpty()) {
            refresh();
        }
    }
}
