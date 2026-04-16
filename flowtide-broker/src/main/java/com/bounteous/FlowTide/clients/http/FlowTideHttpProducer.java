package com.bounteous.FlowTide.clients.http;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.bounteous.FlowTide.model.PublishRequest;
import com.bounteous.FlowTide.model.PublishResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * HTTP producer client — publishes events to FlowTide over REST.
 *
 * <p>Replaces direct TCP usage for external applications. Internally calls:
 * <ul>
 *   <li>{@code POST /api/producer/send}       — single event
 *   <li>{@code POST /api/producer/send/batch} — multiple events
 * </ul>
 *
 * <p>Usage:
 * <pre>
 * FlowTideHttpProducer producer = new FlowTideHttpProducer("http://localhost:8080");
 * PublishResponse response = producer.send("orders", "key1", "hello world");
 * </pre>
 */
public class FlowTideHttpProducer {

    private static final Logger log = LoggerFactory.getLogger(FlowTideHttpProducer.class);

    private static final String SEND_PATH  = "/api/producer/send";
    private static final String BATCH_PATH = "/api/producer/send/batch";

    private final String baseUrl;
    private final HttpClient httpClient;
    private final ObjectMapper mapper;

    public FlowTideHttpProducer(String baseUrl) {
        this.baseUrl = baseUrl.stripTrailing().replaceAll("/$", "");
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
        this.mapper = new ObjectMapper();
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Single event
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Publishes a single event to the given topic.
     *
     * @param topic   destination topic
     * @param key     optional partition key (null = round-robin)
     * @param payload message body
     * @return broker-assigned topic, partition, and offset
     * @throws FlowTideHttpException on HTTP error or backpressure (429)
     */
    public PublishResponse send(String topic, String key, String payload) {
        PublishRequest request = new PublishRequest();
        request.setTopic(topic);
        request.setKey(key);
        request.setPayload(payload);
        return send(request);
    }

    /**
     * Publishes a single event with custom headers.
     */
    public PublishResponse send(String topic, String key, String payload, Map<String, String> headers) {
        PublishRequest request = new PublishRequest();
        request.setTopic(topic);
        request.setKey(key);
        request.setPayload(payload);
        request.setHeaders(headers);
        return send(request);
    }

    /**
     * Publishes a fully built {@link PublishRequest}.
     */
    public PublishResponse send(PublishRequest request) {
        try {
            String body = mapper.writeValueAsString(request);

            HttpRequest httpRequest = HttpRequest.newBuilder()
                    .uri(URI.create(baseUrl + SEND_PATH))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .timeout(Duration.ofSeconds(10))
                    .build();

            HttpResponse<String> response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 429) {
                String retryAfter = response.headers().firstValue("Retry-After").orElse("5");
                throw new FlowTideHttpException("Broker is overloaded (backpressure). Retry after " + retryAfter + "s", 429);
            }

            if (response.statusCode() != 201) {
                throw new FlowTideHttpException("Unexpected response: " + response.statusCode() + " — " + response.body(), response.statusCode());
            }

            PublishResponse result = mapper.readValue(response.body(), PublishResponse.class);
            log.debug("Published to topic={} partition={} offset={}", result.getTopic(), result.getPartition(), result.getOffset());
            return result;

        } catch (FlowTideHttpException e) {
            throw e;
        } catch (Exception e) {
            throw new FlowTideHttpException("Failed to publish event: " + e.getMessage(), e);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Batch
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Publishes multiple events in a single HTTP call.
     * Each event is routed independently to its partition.
     *
     * @param requests list of publish requests
     * @return one response per event, in the same order
     * @throws FlowTideHttpException on HTTP error or backpressure (429)
     */
    public List<PublishResponse> sendBatch(List<PublishRequest> requests) {
        try {
            String body = mapper.writeValueAsString(Map.of("events", requests));

            HttpRequest httpRequest = HttpRequest.newBuilder()
                    .uri(URI.create(baseUrl + BATCH_PATH))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .timeout(Duration.ofSeconds(30))
                    .build();

            HttpResponse<String> response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 429) {
                String retryAfter = response.headers().firstValue("Retry-After").orElse("5");
                throw new FlowTideHttpException("Broker is overloaded (backpressure). Retry after " + retryAfter + "s", 429);
            }

            if (response.statusCode() != 201) {
                throw new FlowTideHttpException("Unexpected response: " + response.statusCode() + " — " + response.body(), response.statusCode());
            }

            List<PublishResponse> results = mapper.readValue(response.body(), new TypeReference<>() {});
            log.debug("Batch published {} events", results.size());
            return results;

        } catch (FlowTideHttpException e) {
            throw e;
        } catch (Exception e) {
            throw new FlowTideHttpException("Failed to publish batch: " + e.getMessage(), e);
        }
    }
}
