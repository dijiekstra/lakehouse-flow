package io.github.lakehouseflow.dao;

import io.github.lakehouseflow.model.WriterJobBinding;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * Repository for globally unique physical-table writer ownership.
 */
@Repository
public interface WriterJobBindingRepository extends JpaRepository<WriterJobBinding, Long> {

    /** Find a writer binding by its stable platform routing key. */
    Optional<WriterJobBinding> findByWriterJobKey(String writerJobKey);

    /** Find the sole writer binding for a normalized physical table. */
    Optional<WriterJobBinding> findByTableAssetKey(String tableAssetKey);

    /** Lock one writer before allocating or releasing a fenced generation. */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT binding FROM WriterJobBinding binding WHERE binding.writerJobKey = :writerJobKey")
    Optional<WriterJobBinding> findByWriterJobKeyForUpdate(@Param("writerJobKey") String writerJobKey);
}
