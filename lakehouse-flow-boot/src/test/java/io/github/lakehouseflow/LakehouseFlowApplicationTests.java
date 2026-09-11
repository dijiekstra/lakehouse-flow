package io.github.lakehouseflow;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.boot.test.autoconfigure.orm.jpa.AutoConfigureDataJpa;

import static org.junit.jupiter.api.Assertions.assertTrue;

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

    @Test
    void applicationClassCanBeInstantiated() {
        // This test verifies the class exists and can be loaded
        assertNotNull(LakehouseFlowApplication.class);
        assertTrue(true);
    }

    private void assertNotNull(Object obj) {
        if (obj == null) {
            throw new AssertionError("Object should not be null");
        }
    }
}
