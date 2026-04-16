package com.bounteous.flowtide.registry;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.netflix.eureka.server.EnableEurekaServer;

@SpringBootApplication
@EnableEurekaServer
public class FlowtideRegistryApplication {

    public static void main(String[] args) {
        SpringApplication.run(FlowtideRegistryApplication.class, args);
    }
}
