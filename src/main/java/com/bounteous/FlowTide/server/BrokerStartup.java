package com.bounteous.FlowTide.server;

import com.bounteous.FlowTide.server.broker.Broker;
import com.bounteous.FlowTide.server.log.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

@Component
@Order(2)
public class BrokerStartup implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(BrokerStartup.class);

    private final LogManager logManager;

    @Value("${kafka.broker.host:localhost}")
    private String brokerHost;

    @Value("${kafka.broker.port:9092}")
    private int brokerPort;

    @Value("${kafka.registry.host:localhost}")
    private String registryHost;

    @Value("${kafka.registry.port:7070}")
    private int registryPort;

    @Value("${kafka.broker.thread-pool-size:10}")
    private int threadPoolSize;

    public BrokerStartup(LogManager logManager) {
        this.logManager = logManager;
    }

    @Override
    public void run(String... args) {
        Broker broker = new Broker(brokerHost, brokerPort, registryHost, registryPort, threadPoolSize, logManager);

        Thread brokerThread = new Thread(() -> {
            try {
                broker.start();
            } catch (Exception e) {
                log.error("Broker failed to start", e);
            }
        }, "kafka-broker-thread");

        brokerThread.setDaemon(false);
        brokerThread.start();
        log.info("Broker startup initiated on port {}", brokerPort);
    }
}
