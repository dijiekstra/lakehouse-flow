package io.github.lakehouseflow.dao;

import io.github.lakehouseflow.model.JobControlIntent;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/**
 * Repository for immutable writer lifecycle instructions and snapshot evidence.
 */
@Repository
public interface JobControlIntentRepository extends JpaRepository<JobControlIntent, Long> {

    /**
     * Lock one control intent while snapshot evidence is evaluated and persisted.
     *
     * @param id immutable control intent id
     * @return locked control intent when present
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT intent FROM JobControlIntent intent WHERE intent.id = :id")
    Optional<JobControlIntent> findByIdForUpdate(@Param("id") Long id);

    /** Find one control intent by its epoch-derived idempotency key. */
    Optional<JobControlIntent> findByIntentKey(String intentKey);

    /** Find one control intent by the caller's operation idempotency key. */
    Optional<JobControlIntent> findByRequestKey(String requestKey);

    /** Find all control intents still waiting for writer snapshot evidence. */
    List<JobControlIntent> findBySnapshotResultOrderByCreatedAtAsc(String snapshotResult);
}
