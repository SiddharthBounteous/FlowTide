package com.bounteous.FlowTide.config;

import com.bounteous.FlowTide.clients.producer.MyProducer;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring configuration for the Kafka platform.
 *
 * <p>{@link MyProducer} is registered as a singleton {@code @Bean} here so that:
 * <ul>
 *   <li>Its thread pool (8 sender threads) is created ONCE and reused for every request.
 *   <li>It can be injected into {@link com.bounteous.FlowTide.producer.ProducerService}.
 *   <li>Its {@code close()} (graceful thread-pool shutdown) is called automatically on app stop.
 * </ul>
 *
 * <p>All other platform beans ({@code @Service}, {@code @Component}) are
 * auto-discovered by Spring's component scan.
 */
@Configuration
public class KafkaConfig {

    @Value("${kafka.registry.host:localhost}")
    private String registryHost;

    @Value("${kafka.registry.port:7070}")
    private int registryPort;

    @Value("${kafka.producer.thread-pool-size:8}")
    private int producerThreadPoolSize;

    private MyProducer<String, String> producer;

    /**
     * The singleton producer that routes every published event over TCP
     * to the correct broker, based on live registry metadata.
     */
    @Bean
    public MyProducer<String, String> myProducer() {
        producer = new MyProducer<>(registryHost, registryPort, producerThreadPoolSize);
        return producer;
    }

    @PreDestroy
    public void shutdownProducer() {
        if (producer != null) {
            producer.close();
        }
    }
}
