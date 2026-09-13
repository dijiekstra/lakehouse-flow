package io.github.lakehouseflow.integration.paimon;

import io.github.lakehouseflow.common.SnapshotIds;
import io.github.lakehouseflow.integration.source.LakehouseSnapshot;
import io.github.lakehouseflow.integration.source.LakehouseSnapshotSource;
import io.github.lakehouseflow.integration.source.LakehouseSourceIdentity;
import io.github.lakehouseflow.integration.source.SnapshotSourcePosition;

import java.util.Comparator;
import java.util.List;

/**
 * Paimon implementation of the format-neutral lakehouse snapshot source SPI.
 */
public class PaimonSnapshotSource implements LakehouseSnapshotSource {

    private final PaimonSourceDefinition definition;
    private final PaimonCatalogSnapshotReader snapshotReader;

    /**
     * Create a source for one configured Paimon table.
     *
     * @param definition immutable source definition
     * @param snapshotReader native Paimon metadata reader
     */
    public PaimonSnapshotSource(
            PaimonSourceDefinition definition,
            PaimonCatalogSnapshotReader snapshotReader) {
        this.definition = definition;
        this.snapshotReader = snapshotReader;
    }

    /**
     * Return this Paimon table's stable source identity.
     *
     * @return source identity
     */
    @Override
    public LakehouseSourceIdentity identity() {
        return definition.identity();
    }

    /**
     * Read retained Paimon snapshots after the supplied snapshot id.
     *
     * @param offsetExclusive previous Paimon snapshot id
     * @return new snapshots
     */
    @Override
    public List<LakehouseSnapshot> scanAfter(String offsetExclusive) {
        return snapshotReader.scanAfter(definition, offsetExclusive);
    }

    /**
     * Inspect Paimon's retained snapshot range against the durable scheduler offset.
     *
     * @param durableOffset last completely projected Paimon snapshot id, or null
     * @return current Paimon source position
     */
    @Override
    public SnapshotSourcePosition inspectPosition(String durableOffset) {
        return snapshotReader.inspectPosition(definition, durableOffset);
    }

    /**
     * Compare Paimon snapshot ids numerically without narrowing them to primitive values.
     *
     * @return Paimon snapshot-id comparator
     */
    @Override
    public Comparator<String> offsetComparator() {
        return SnapshotIds::compare;
    }
}
