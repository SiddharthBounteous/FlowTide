package com.bounteous.flowtide.controller.api;

import com.bounteous.flowtide.controller.model.BrokerInfo;
import com.bounteous.flowtide.controller.model.BrokerRegistration;
import com.bounteous.flowtide.controller.model.HeartbeatRequest;
import com.bounteous.flowtide.controller.model.PartitionAssignment;
import com.bounteous.flowtide.controller.service.BrokerRegistryService;
import com.bounteous.flowtide.controller.service.PartitionAssignmentService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * REST API for broker lifecycle management.
 *
 * <p>Endpoints:
 * <ul>
 *   <li>POST /controller/brokers/register  — broker startup registration
 *   <li>POST /controller/brokers/heartbeat — periodic alive signal; returns current assignment
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
     *
     * <p>If this is a brand-new broker joining the cluster:
     * <ol>
     *   <li>Register it in the active broker pool.
     *   <li>Trigger a full rebalance so existing topics get followers on the new broker.
     *   <li>Return the new broker's partition assignments (populated by the rebalance).
     * </ol>
     *
     * <p>If the broker is reconnecting (was already known), just refresh its heartbeat
     * and return its current assignment.
     */
    @PostMapping("/register")
    public ResponseEntity<PartitionAssignment> register(@RequestBody BrokerRegistration registration) {
        log.info("Broker registration request: {}", registration.getId());
        boolean isNew = brokerRegistry.register(registration);

        if (isNew) {
            List<BrokerInfo> activeBrokers = brokerRegistry.getActiveBrokers();
            log.info("New broker joined: {} — triggering rebalance across {} active brokers",
                    registration.getId(), activeBrokers.size());
            // Rebalance redistributes all existing topics so the new broker
            // gets follower (and possibly leader) assignments.
            partitionAssignment.rebalanceAll(activeBrokers);
        }

        PartitionAssignment assignment = partitionAssignment.getAssignmentForBroker(registration.getId());
        log.info("Returning {} partition role(s) to broker {}",
                assignment.getAssignments().size(), registration.getId());
        return ResponseEntity.ok(assignment);
    }

    /**
     * Called by each broker every {@code kafka.cluster.heartbeat-interval-ms}.
     *
     * <p>Returns the broker's <b>current</b> partition assignment so the broker
     * can detect if the controller has changed its roles (e.g. after a failover
     * or rebalance) and update its local {@code PartitionOwnershipService} accordingly.
     *
     * <h3>Self-healing registration</h3>
     * If the heartbeat arrives from an <em>unknown</em> broker (i.e. the broker's
     * startup {@code /register} call failed — e.g. controller was not yet ready),
     * the broker is automatically registered here and a full rebalance is triggered.
     * This way a broker that missed its startup window is silently recovered within
     * one heartbeat interval (default 5 s) without any manual intervention.
     */
    @PostMapping("/heartbeat")
    public ResponseEntity<PartitionAssignment> heartbeat(@RequestBody HeartbeatRequest request) {
        String brokerId = request.getHost() + ":" + request.getPort();

        if (!brokerRegistry.isKnown(brokerId)) {
            log.info("Heartbeat from unregistered broker {} — auto-registering and rebalancing",
                    brokerId);
            BrokerRegistration autoReg = new BrokerRegistration(request.getHost(), request.getPort());
            brokerRegistry.register(autoReg);
            List<BrokerInfo> activeBrokers = brokerRegistry.getActiveBrokers();
            partitionAssignment.rebalanceAll(activeBrokers);
        }

        brokerRegistry.heartbeat(request.getHost(), request.getPort());
        PartitionAssignment assignment = partitionAssignment.getAssignmentForBroker(brokerId);
        return ResponseEntity.ok(assignment);
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

    /**
     * Gracefully removes a broker from the cluster.
     *
     * <p>Safe descaling flow:
     * <ol>
     *   <li>Mark the broker as draining (no new partition leadership assigned)
     *   <li>Trigger failover — promote followers to leaders for all partitions
     *       the draining broker leads
     *   <li>Remove the broker from the active pool
     *   <li>Rebalance remaining brokers so followers are redistributed
     * </ol>
     *
     * <p>This ensures no partition goes leaderless during node removal.
     *
     * @param brokerId broker id in "host:port" format
     */
    @DeleteMapping("/{brokerId}")
    public ResponseEntity<String> removeBroker(@PathVariable String brokerId) {
        log.info("Graceful descaling requested for broker: {}", brokerId);

        if (!brokerRegistry.isKnown(brokerId)) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body("Broker not found: " + brokerId);
        }

        List<BrokerInfo> activeBefore = brokerRegistry.getActiveBrokers();
        if (activeBefore.size() <= 1) {
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body("Cannot remove last active broker — cluster would have no nodes");
        }

        // Step 1: Mark dead so failover promotes its followers
        brokerRegistry.getBroker(brokerId).ifPresent(b -> b.markDead());
        log.info("Broker {} marked as draining", brokerId);

        // Step 2: Promote followers to leaders for all affected partitions
        List<BrokerInfo> remaining = brokerRegistry.getActiveBrokers();
        List<String> affected = partitionAssignment.handleBrokerFailure(brokerId, remaining);
        log.info("Failover complete for {}: {} topic(s) affected", brokerId, affected.size());

        // Step 3: Rebalance remaining cluster so follower slots are filled
        partitionAssignment.rebalanceAll(remaining);
        log.info("Rebalance complete after removing {}: {} active brokers remain", brokerId, remaining.size());

        return ResponseEntity.ok(String.format(
                "Broker '%s' removed gracefully. %d topic(s) failed over. %d broker(s) remain.",
                brokerId, affected.size(), remaining.size()));
    }
}
