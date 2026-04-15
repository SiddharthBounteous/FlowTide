package com.bounteous.FlowTide.cluster;

import com.bounteous.FlowTide.model.HeartbeatMessage;
import com.bounteous.FlowTide.server.log.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.ObjectOutputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.net.Socket;

/**
 * Periodically sends a heartbeat from this broker to the registry,
 * proving the node is alive and sharing basic health data.
 *
 * <p>The registry's {@link com.bounteous.FlowTide.cluster.ClusterManager} marks a node dead
 * if it misses {@code kafka.cluster.node-timeout-ms} milliseconds of heartbeats.
 */
@Component
public class HeartbeatScheduler {

    private static final Logger log = LoggerFactory.getLogger(HeartbeatScheduler.class);

    private final LogManager logManager;

    @Value("${kafka.broker.host:localhost}")
    private String brokerHost;

    @Value("${kafka.broker.port:9092}")
    private int brokerPort;

    @Value("${kafka.registry.host:localhost}")
    private String registryHost;

    @Value("${kafka.registry.port:7070}")
    private int registryPort;

    public HeartbeatScheduler(LogManager logManager) {
        this.logManager = logManager;
    }

    @Scheduled(fixedDelayString = "${kafka.cluster.heartbeat-interval-ms:5000}")
    public void sendHeartbeat() {
        MemoryMXBean mem = ManagementFactory.getMemoryMXBean();
        long usedHeap = mem.getHeapMemoryUsage().getUsed();

        HeartbeatMessage hb = new HeartbeatMessage(
                brokerHost,
                brokerPort,
                System.currentTimeMillis(),
                logManager.totalEventsStored(),
                getCpuUsage(),
                usedHeap
        );

        try (
            Socket socket = new Socket(registryHost, registryPort);
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())
        ) {
            out.writeObject(hb);
            out.flush();
            log.trace("Heartbeat sent to registry");
        } catch (Exception e) {
            log.warn("Failed to send heartbeat to registry: {}", e.getMessage());
        }
    }

    private double getCpuUsage() {
        try {
            com.sun.management.OperatingSystemMXBean os =
                    (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
            return os.getCpuLoad() * 100.0;
        } catch (Exception e) {
            return -1;
        }
    }
}
