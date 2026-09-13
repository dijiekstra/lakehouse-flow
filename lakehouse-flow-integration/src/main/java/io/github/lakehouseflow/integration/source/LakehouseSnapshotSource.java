package io.github.lakehouseflow.integration.source;

import java.util.Comparator;
import java.util.List;

/**
 * Pull-based metadata adapter for one lakehouse table.
 *
 * <p>The scheduler reads only immutable snapshot metadata through this SPI. Implementations do not
 * execute downstream work and must return every retained snapshot after the supplied offset.
 */
public interface LakehouseSnapshotSource {

    /**
     * Return the stable source and target-table identity.
     *
     * @return source identity
     */
    LakehouseSourceIdentity identity();

    /**
     * Scan snapshots after an opaque durable offset.
     *
     * @param offsetExclusive last completely projected source offset, or {@code null} initially
     * @return source snapshots; callers will order them with {@link #offsetComparator()}
     */
    List<LakehouseSnapshot> scanAfter(String offsetExclusive);

    /**
     * Compare the durable ingestion offset with the source's currently retained range.
     *
     * <p>The adapter owns retention-gap and offset ordering semantics. The scheduler must not infer
     * them from format-specific identifiers.
     *
     * @param durableOffset last completely projected source offset, or {@code null}
     * @return current format-neutral source position
     */
    SnapshotSourcePosition inspectPosition(String durableOffset);

    /**
     * Return the format-specific ordering for source offsets.
     *
     * @return offset comparator
     */
    Comparator<String> offsetComparator();
}
