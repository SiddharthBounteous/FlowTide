package com.bounteous.FlowTide.server;

import com.bounteous.FlowTide.cluster.metadata.MetadataController;
import com.bounteous.FlowTide.server.registry.RegistryServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 * Starts the Registry TCP server at application boot — always first (@Order(1)).
 *
 * <p><strong>Multi-broker deployment:</strong><br>
 * Set {@code kafka.registry.start=false} on any JVM that should run as a
 * broker-only node. Only ONE node in the cluster should run the registry.
 *
 * <p>Example (three-node cluster):
 * <pre>
 * Node 1:  kafka.registry.start=true   kafka.broker.port=9092  ← runs registry + broker
 * Node 2:  kafka.registry.start=false  kafka.broker.port=9093  ← broker only
 * Node 3:  kafka.registry.start=false  kafka.broker.port=9094  ← broker only
 * </pre>
 */
@Component
@Order(1)
public class RegistryStartup implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(RegistryStartup.class);

    private final MetadataController metadataController;

    @Value("${kafka.registry.start:true}")
    private boolean registryStart;

    @Value("${kafka.registry.port:7070}")
    private int registryPort;

    @Value("${kafka.topic.default-partitions:3}")
    private int defaultPartitions;

    @Value("${kafka.topic.default-replication-factor:2}")
    private int defaultReplicationFactor;

    @Value("${kafka.registry.thread-pool-size:5}")
    private int threadPoolSize;

    public RegistryStartup(MetadataController metadataController) {
        this.metadataController = metadataController;
    }

    @Override
    public void run(String... args) {
        if (!registryStart) {
            log.info("Registry start is DISABLED (broker-only mode). " +
                     "This node will register with the external registry.");
            return;
        }

        RegistryServer registry = new RegistryServer(
                registryPort, defaultPartitions, defaultReplicationFactor,
                threadPoolSize, metadataController);

        Thread registryThread = new Thread(() -> {
            try {
                registry.start();
            } catch (Exception e) {
                log.error("Registry server failed", e);
            }
        }, "kafka-registry-thread");

        registryThread.setDaemon(false);
        registryThread.start();
        log.info("Registry startup initiated on port {}", registryPort);
    }
}
