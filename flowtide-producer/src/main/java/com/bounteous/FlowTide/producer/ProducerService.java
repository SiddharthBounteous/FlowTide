package com.bounteous.FlowTide.producer;

import com.bounteous.FlowTide.client.BrokerClient;
import com.bounteous.FlowTide.model.PublishRequest;
import com.bounteous.FlowTide.model.PublishResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Publishes events to the broker via Feign client.
 *
 * <p>Flow: REST → ProducerService → BrokerClient (Feign) → POST /internal/events
 *
 * <p>Partition selection and event storage are handled by the broker.
 * This service focuses only on request validation and routing.
 */
@Service
public class ProducerService {

    private static final Logger log = LoggerFactory.getLogger(ProducerService.class);

    private final BrokerClient brokerClient;

    public ProducerService(BrokerClient brokerClient) {
        this.brokerClient = brokerClient;
    }

    public PublishResponse publish(PublishRequest request) {
        log.debug("Publishing to topic={} key={}", request.getTopic(), request.getKey());
        return brokerClient.send(request);
    }

    public List<PublishResponse> publishBatch(List<PublishRequest> requests) {
        return requests.stream()
                .map(brokerClient::send)
                .toList();
    }
}
