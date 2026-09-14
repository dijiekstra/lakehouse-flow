package io.github.lakehouseflow;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.env.Environment;
import org.springframework.test.context.ActiveProfiles;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Application smoke test.
 *
 * Verifies that the Spring Boot application can load its main class
 * without errors. Full integration tests with database would run
 * in a separate test class with @DataJpaTest.
 */
@SpringBootTest(classes = LakehouseFlowApplication.class)
@ActiveProfiles("test")
class LakehouseFlowApplicationTests {

    @Autowired
    private Environment environment;

    /** Verifies that the complete application context can start. */
    @Test
    void applicationClassCanBeInstantiated() {
        assertNotNull(LakehouseFlowApplication.class);
    }

    /** Keeps destructive or implicit Flyway operations disabled by default. */
    @Test
    void flywayProductionSafetyDefaultsAreFailClosed() {
        assertEquals(Boolean.FALSE, environment.getProperty("spring.flyway.baseline-on-migrate", Boolean.class));
        assertEquals(Boolean.TRUE, environment.getProperty("spring.flyway.validate-on-migrate", Boolean.class));
        assertEquals(Boolean.TRUE, environment.getProperty("spring.flyway.clean-disabled", Boolean.class));
    }
}
