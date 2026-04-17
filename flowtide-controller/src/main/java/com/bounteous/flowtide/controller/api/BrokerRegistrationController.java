package com.bounteous.flowtide.controller.api;

import com.bounteous.flowtide.controller.model.BrokerInfo;
import com.bounteous.flowtide.controller.model.BrokerRegistration;
import com.bounteous.flowtide.controller.model.HeartbeatRequest;
import com.bounteous.flowtide.controller.model.PartitionAssignment;
import com.bounteous.flowtide.controller.service.BrokerRegistryService;
import com.bounteous.flowtide.controller.service.PartitionAssignmentService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * REST API for broker lifecycle management.
 *
 * <p>Endpoints:
 * <ul>
 *   <li>POST /controller/brokers/register  — broker startup registration
 *   <li>POST /controller/brokers/heartbeat — periodic alive signal
 *   <li>GET  /controller/brokers           — list all brokers
 *   <li>GET  /controller/brokers/active    — list active brokers only
 * </ul>
 */
@RestController
@RequestMapping("/controller/brokers")
public class BrokerRegistrationController {

    private static final Logger log = LoggerFactory.getLogger(BrokerRegistrationController.class);

    private final BrokerRegistryService      brokerRegistry;
    private final PartitionAssignmentService partitionAssignment;

    public BrokerRegistrationController(BrokerRegistryService brokerRegistry,
                                        PartitionAssignmentService partitionAssignment) {
        this.brokerRegistry      = brokerRegistry;
        this.partitionAssignment = partitionAssignment;
    }

    /**
     * Called by each broker on startup.
     * Registers the broker and returns its partition assignments.
     *
     * <p>If this is a brand-new broker joining an existing cluster,
     * existing topics already have assignments — the broker learns which
     * partitions it leads from the returned PartitionAssignment.
     */
    @PostMapping("/register")
    public ResponseEntity<PartitionAssignment> register(@RequestBody BrokerRegistration registration) {
        log.info("Broker registration request: {}", registration.getId());
        boolean isNew = brokerRegistry.register(registration);

        if (isNew) {
            log.info("New broker joined cluster: {} — existing topic assignments unchanged", registration.getId());
            // New broker only gets assigned to future topics or after explicit rebalance
            // For now return empty assignment — broker will get work on topic creation
        }

        PartitionAssignment assignment = partitionAssignment.getAssignmentForBroker(registration.getId());
        log.info("Returning {} partition roles to broker {}", assignment.getAssignments().size(), registration.getId());
        return ResponseEntity.ok(assignment);
    }

    /**
     * Called by each broker every kafka.cluster.heartbeat-interval-ms.
     * Confirms the broker is alive.
     */
    @PostMapping("/heartbeat")
    public ResponseEntity<String> heartbeat(@RequestBody HeartbeatRequest request) {
        brokerRegistry.heartbeat(request.getHost(), request.getPort());
        return ResponseEntity.ok("OK");
    }

    /**
     * Returns all brokers (active + dead).
     */
    @GetMapping
    public List<BrokerInfo> getAllBrokers() {
        return brokerRegistry.getAllBrokers();
    }

    /**
     * Returns only active brokers.
     */
    @GetMapping("/active")
    public List<BrokerInfo> getActiveBrokers() {
        return brokerRegistry.getActiveBrokers();
    }
}
