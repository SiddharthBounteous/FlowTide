package com.bounteous.flowtide.gateway.config;

import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.cloud.gateway.route.builder.RouteLocatorBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * API Gateway route configuration for FlowTide.
 *
 * <p>All external traffic enters on port 8080 (gateway) and is forwarded
 * to the appropriate microservice resolved via Eureka service discovery.
 *
 * <p>lb://flowtide-broker resolves the service by its spring.application.name
 * registered in Eureka, with client-side load balancing across all instances.
 */
@Configuration
public class RouteConfig {

    @Bean
    public RouteLocator gatewayRoutes(RouteLocatorBuilder builder) {
        return builder.routes()

                // Producer service — publish events (own microservice on port 8081)
                .route("flowtide-producer", r -> r
                        .path("/api/producer/**")
                        .uri("lb://flowtide-producer"))

                // Consumer service — poll events, offsets, groups (own microservice on port 8082)
                .route("flowtide-consumer", r -> r
                        .path("/api/consumer/**")
                        .uri("lb://flowtide-consumer"))

                // Topic management — broker handles this (port 8083)
                .route("flowtide-topics", r -> r
                        .path("/api/topics/**")
                        .uri("lb://flowtide-broker"))

                // Admin API — broker handles this
                .route("flowtide-admin", r -> r
                        .path("/api/admin/**")
                        .uri("lb://flowtide-broker"))

                // Actuator — broker health/metrics
                .route("flowtide-actuator", r -> r
                        .path("/actuator/**")
                        .uri("lb://flowtide-broker"))

                // Controller service — partition assignment, broker registry, topic metadata (port 8090)
                .route("flowtide-controller", r -> r
                        .path("/controller/**")
                        .uri("lb://flowtide-controller"))

                // Web UI
                .route("flowtide-ui", r -> r
                        .path("/")
                        .uri("lb://flowtide-broker"))

                .build();
    }
}
