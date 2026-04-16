package com.bounteous.FlowTide.server;

import com.bounteous.FlowTide.cluster.metadata.MetadataController;
import com.bounteous.FlowTide.server.registry.BrokerInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/**
 * Registers this broker with the local MetadataController at startup.
 *
 * <p>Replaces the old TCP-based BrokerStartup + RegistryStartup pair.
 * Since the broker IS the registry (same JVM), no network call is needed —
 * we simply inject BrokerInfo directly into MetadataController.
 */
@Component
public class BrokerSelfRegistration implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(BrokerSelfRegistration.class);

    private final MetadataController metadataController;

    @Value("${server.host:localhost}")
    private String host;

    @Value("${server.port:8083}")
    private int port;

    public BrokerSelfRegistration(MetadataController metadataController) {
        this.metadataController = metadataController;
    }

    @Override
    public void run(ApplicationArguments args) {
        BrokerInfo self = new BrokerInfo(host, port);
        metadataController.registerBroker(self);
        log.info("Broker self-registered: {}:{}", host, port);
    }
}
