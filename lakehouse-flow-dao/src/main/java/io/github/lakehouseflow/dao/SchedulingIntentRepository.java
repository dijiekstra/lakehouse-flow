package io.github.lakehouseflow.dao;

import io.github.lakehouseflow.model.SchedulingIntent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * Repository for immutable scheduling intent outbox records.
 */
@Repository
public interface SchedulingIntentRepository extends JpaRepository<SchedulingIntent, Long> {

    /**
     * Find an immutable intent by the attribution key written into a snapshot.
     *
     * @param intentKey scheduler-generated snapshot attribution key
     * @return matching intent when the key belongs to Lakehouse Flow
     */
    Optional<SchedulingIntent> findByIntentKey(String intentKey);

    /**
     * Find the single immutable intent emitted for a task instance.
     *
     * @param taskInstanceId task scheduling instance id
     * @return matching intent when already published
     */
    Optional<SchedulingIntent> findByTaskInstanceId(Long taskInstanceId);
}
