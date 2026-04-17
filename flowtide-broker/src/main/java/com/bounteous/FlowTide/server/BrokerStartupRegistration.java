package com.bounteous.FlowTide.server;

import com.bounteous.FlowTide.client.ControllerClient;
import com.bounteous.FlowTide.client.model.BrokerRegistration;
import com.bounteous.FlowTide.client.model.PartitionAssignment;
import com.bounteous.FlowTide.cluster.metadata.MetadataController;
import com.bounteous.FlowTide.server.registry.BrokerInfo;
import com.bounteous.FlowTide.server.registry.PartitionMetadata;
import com.bounteous.FlowTide.partition.PartitionOwnershipService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.web.context.WebServerApplicationContext;
//import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * On startup, registers this broker with the flowtide-controller service.
 *
 * <p>Flow:
 * <ol>
 *   <li>POST /controller/brokers/register → controller assigns partitions
 *   <li>Store assignment in PartitionOwnershipService (which partitions I lead)
 *   <li>Also self-register in local MetadataController for admin API
 * </ol>
 *
 * <p>Replaces the old BrokerSelfRegistration which only updated local state.
 */
@Component
public class BrokerStartupRegistration implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(BrokerStartupRegistration.class);

    private final ControllerClient          controllerClient;
    private final PartitionOwnershipService ownershipService;
    private final MetadataController        metadataController;
    private final WebServerApplicationContext webServerApplicationContext;

    @Value("${server.host:localhost}")
    private String host;

//    /** Injected after Tomcat binds — gives the actual port even when server.port=0. */
//    @LocalServerPort
//    private int port;

    public BrokerStartupRegistration(ControllerClient controllerClient,
                                     PartitionOwnershipService ownershipService,
                                     MetadataController metadataController, WebServerApplicationContext webServerApplicationContext) {
        this.controllerClient   = controllerClient;
        this.ownershipService   = ownershipService;
        this.metadataController = metadataController;
        this.webServerApplicationContext = webServerApplicationContext;
    }

    @Override
    public void run(ApplicationArguments args) {
        int port = webServerApplicationContext.getWebServer().getPort();
        log.info("Registering broker {}:{} with flowtide-controller...", host, port);

        try {
            BrokerRegistration registration = new BrokerRegistration(host, port);
            PartitionAssignment assignment  = controllerClient.register(registration);

            // Store which partitions this broker leads
            ownershipService.applyAssignment(assignment);

            // Also register in local MetadataController for admin API
            metadataController.registerBroker(new BrokerInfo(host, port));

            log.info("Broker registered successfully. Leading {} partition(s).",
                    ownershipService.getLeaderPartitionCount());

        } catch (Exception e) {
            log.error("Failed to register with controller: {} — broker will still start " +
                      "but partition ownership is unknown. Ensure flowtide-controller is running.",
                      e.getMessage());
        }
    }
}
