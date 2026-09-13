package io.github.lakehouseflow;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Main Spring Boot Application entry point for Lakehouse Flow.
 * 
 * This application is an event-driven scheduling system for modern CDC lakehouse architectures.
 * It supports Paimon, Iceberg, and Hudi snapshot events to trigger downstream workflows.
 */
@SpringBootApplication
@EnableScheduling
public class LakehouseFlowApplication {

    /**
     * Start the Lakehouse Flow Spring Boot application.
     *
     * @param args command line arguments passed to Spring Boot
     */
    public static void main(String[] args) {
        SpringApplication.run(LakehouseFlowApplication.class, args);
    }
}
