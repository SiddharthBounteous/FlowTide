package com.bounteous.FlowTide.server;

import com.bounteous.FlowTide.client.ControllerClient;
import com.bounteous.FlowTide.client.model.BrokerRegistration;
import com.bounteous.FlowTide.client.model.PartitionAssignment;
import com.bounteous.FlowTide.partition.PartitionOwnershipService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/**
 * On startup, registers this broker with the flowtide-controller.
 *
 * <p>Flow:
 * <ol>
 *   <li>POST /controller/brokers/register → controller assigns partitions
 *   <li>Apply assignment in PartitionOwnershipService (which partitions this broker leads)
 * </ol>
 *
 * <p>If registration fails (controller not yet ready), the broker still starts.
 * The heartbeat scheduler will auto-register on the next heartbeat cycle (within 5s).
 */
@Component
public class BrokerStartupRegistration implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(BrokerStartupRegistration.class);

    private final ControllerClient          controllerClient;
    private final PartitionOwnershipService ownershipService;
    private final ServerPortProvider        portProvider;

    @Value("${server.host:localhost}")
    private String host;

    public BrokerStartupRegistration(ControllerClient controllerClient,
                                     PartitionOwnershipService ownershipService,
                                     ServerPortProvider portProvider) {
        this.controllerClient = controllerClient;
        this.ownershipService = ownershipService;
        this.portProvider     = portProvider;
    }

    @Override
    public void run(ApplicationArguments args) {
        int port = portProvider.getPort();
        log.info("Registering broker {}:{} with flowtide-controller...", host, port);

        try {
            BrokerRegistration  registration = new BrokerRegistration(host, port);
            PartitionAssignment assignment   = controllerClient.register(registration);
            ownershipService.applyAssignment(assignment);

            log.info("Broker registered at {}:{}. Leading {} partition(s).",
                    host, port, ownershipService.getLeaderPartitionCount());
        } catch (Exception e) {
            log.warn("Failed to register with controller: {} " +
                     "— broker will start anyway and auto-register on first heartbeat.", e.getMessage());
        }
    }
}
