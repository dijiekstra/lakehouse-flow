package io.github.lakehouseflow.dao;

import org.junit.jupiter.api.BeforeAll;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Base integration test class for DAO layer tests using Testcontainers PostgreSQL.
 *
 * This base class sets up a containerized PostgreSQL instance for integration tests.
 * All DAO tests should extend this class to ensure consistent database environment.
 *
 * Example usage:
 * <pre>
 * @SpringBootTest
 * class MyDaoTest extends DaoIntegrationTestBase {
 *     @Autowired
 *     private MyRepository repository;
 *
 *     @Test
 *     void testSomething() {
 *         // Your test code here
 *     }
 * }
 * </pre>
 */
@Testcontainers
@DataJpaTest
public abstract class DaoIntegrationTestBase {

    @Container
    protected static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15-alpine")
            .withDatabaseName("lakehouse_flow")
            .withUsername("postgres")
            .withPassword("postgres")
            .withReuse(true)
            .withEnv("POSTGRES_INITDB_ARGS", "-c max_connections=100");

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @BeforeAll
    static void setup() {
        // Ensure container is running before all tests
        if (!postgres.isRunning()) {
            postgres.start();
        }
    }
}
