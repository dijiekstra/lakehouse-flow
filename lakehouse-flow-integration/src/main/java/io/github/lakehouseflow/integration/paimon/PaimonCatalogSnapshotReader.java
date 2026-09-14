package io.github.lakehouseflow.integration.paimon;

import io.github.lakehouseflow.integration.source.LakehouseSnapshot;
import io.github.lakehouseflow.integration.source.SnapshotSourceException;
import io.github.lakehouseflow.integration.source.SnapshotSourceOffsetStatus;
import io.github.lakehouseflow.integration.source.SnapshotSourcePosition;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.SimpleFileReader;
import org.apache.paimon.utils.SnapshotManager;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Reads retained Paimon snapshots and their changed partitions through the native Catalog API.
 */
@Component
public class PaimonCatalogSnapshotReader {

    private final PaimonCatalogFactory catalogFactory;

    /**
     * Create a native Paimon metadata reader.
     *
     * @param catalogFactory factory used to open one catalog per scan
     */
    public PaimonCatalogSnapshotReader(PaimonCatalogFactory catalogFactory) {
        this.catalogFactory = catalogFactory;
    }

    /**
     * Read a bounded ascending range after the last durably projected Paimon snapshot.
     *
     * <p>A retention gap fails closed because silently skipping expired snapshots could omit an
     * asset-state transition or a scheduling decision.
     *
     * @param definition configured source definition
     * @param offsetExclusive last projected snapshot id, or {@code null} initially
     * @return format-neutral snapshot observations
     */
    public List<LakehouseSnapshot> scanAfter(
            PaimonSourceDefinition definition,
            String offsetExclusive) {
        Long previousSnapshotId = parseOffset(offsetExclusive);
        try (Catalog catalog = catalogFactory.openCatalog(definition.catalogOptions())) {
            FileStoreTable table = loadFileStoreTable(catalog, definition);
            SnapshotManager snapshotManager = table.snapshotManager();
            Long earliest = snapshotManager.earliestSnapshotId();
            Long latest = snapshotManager.latestSnapshotId();
            if (earliest == null
                    || latest == null
                    || previousSnapshotId != null && previousSnapshotId >= latest) {
                return List.of();
            }

            long first = previousSnapshotId == null
                    ? switch (definition.startupMode()) {
                        case LATEST -> latest;
                        case EARLIEST -> earliest;
                    }
                    : previousSnapshotId + 1;
            if (first < earliest) {
                throw new SnapshotSourceException(
                        "Paimon snapshot retention gap for " + definition.identity().sourceName()
                                + ": expected " + first + " but earliest retained is " + earliest);
            }
            long last = Math.min(latest, boundedLast(first, definition.batchSize()));
            List<LakehouseSnapshot> snapshots = new ArrayList<>((int) (last - first + 1));
            for (long snapshotId = first; ; snapshotId++) {
                Snapshot snapshot = snapshotManager.snapshot(snapshotId);
                List<String> changedPartitions = readChangedPartitions(table, snapshot);
                snapshots.add(toPaimonSnapshot(snapshot, changedPartitions)
                        .toLakehouseSnapshot(definition.zoneId()));
                if (snapshotId == last) {
                    break;
                }
            }
            return snapshots;
        } catch (SnapshotSourceException e) {
            throw e;
        } catch (Exception e) {
            throw new SnapshotSourceException(
                    "Unable to scan Paimon source " + definition.identity().sourceName(), e);
        }
    }

    /**
     * Compare a durable offset with Paimon's retained snapshot range without reading manifests.
     *
     * @param definition configured source definition
     * @param durableOffset last completely projected snapshot id, or null
     * @return current position and exact numeric lag evidence
     */
    public SnapshotSourcePosition inspectPosition(
            PaimonSourceDefinition definition,
            String durableOffset) {
        Long previousSnapshotId = parseOffset(durableOffset);
        try (Catalog catalog = catalogFactory.openCatalog(definition.catalogOptions())) {
            FileStoreTable table = loadFileStoreTable(catalog, definition);
            SnapshotManager snapshotManager = table.snapshotManager();
            Long earliest = snapshotManager.earliestSnapshotId();
            Long latest = snapshotManager.latestSnapshotId();
            return position(definition, previousSnapshotId, earliest, latest);
        } catch (SnapshotSourceException e) {
            throw e;
        } catch (Exception e) {
            throw new SnapshotSourceException(
                    "Unable to inspect Paimon source " + definition.identity().sourceName(), e);
        }
    }

    /**
     * Load one configured Paimon table and reject non-file-store table implementations.
     *
     * @param catalog open native catalog
     * @param definition source definition
     * @return file-store table used for snapshot metadata reads
     * @throws Catalog.TableNotExistException when the configured table is absent
     */
    private FileStoreTable loadFileStoreTable(Catalog catalog, PaimonSourceDefinition definition)
            throws Catalog.TableNotExistException {
        Identifier identifier = Identifier.create(
                definition.identity().databaseName(), definition.identity().tableName());
        Table table = catalog.getTable(identifier);
        if (table instanceof FileStoreTable fileStoreTable) {
            return fileStoreTable;
        }
        throw new SnapshotSourceException(
                "Paimon source is not a FileStoreTable: " + definition.identity().sourceName());
    }

    /**
     * Derive deterministic changed-partition evidence from one snapshot's delta manifests.
     *
     * @param table source table
     * @param snapshot native snapshot
     * @return sorted distinct partition paths
     */
    private List<String> readChangedPartitions(FileStoreTable table, Snapshot snapshot) {
        if (table.partitionKeys().isEmpty() || snapshot.deltaManifestList() == null) {
            return List.of();
        }
        SimpleFileReader<ManifestFileMeta> manifestListReader = table.manifestListReader();
        SimpleFileReader<ManifestEntry> manifestFileReader = table.manifestFileReader();
        FileStorePathFactory pathFactory = table.store().pathFactory();
        return manifestListReader.read(snapshot.deltaManifestList()).stream()
                .flatMap(meta -> manifestFileReader.read(meta.fileName()).stream())
                .map(ManifestEntry::partition)
                .filter(Objects::nonNull)
                .map(pathFactory::getPartitionString)
                .map(PaimonCatalogSnapshotReader::normalizePartitionPath)
                .filter(partition -> !partition.isBlank())
                .distinct()
                .sorted()
                .toList();
    }

    /**
     * Remove Paimon's trailing directory separator from the scheduler partition identity.
     *
     * @param partitionPath partition path emitted by Paimon's path factory
     * @return canonical partition identity without a trailing slash
     */
    private static String normalizePartitionPath(String partitionPath) {
        if (partitionPath == null) {
            return "";
        }
        int end = partitionPath.length();
        while (end > 0 && partitionPath.charAt(end - 1) == '/') {
            end--;
        }
        return partitionPath.substring(0, end);
    }

    /**
     * Copy native Paimon metadata into the immutable adapter record.
     *
     * @param snapshot native snapshot
     * @param changedPartitions changed partitions derived from manifests
     * @return adapter snapshot
     */
    private PaimonSnapshot toPaimonSnapshot(Snapshot snapshot, List<String> changedPartitions) {
        return new PaimonSnapshot(
                snapshot.id(),
                snapshot.schemaId(),
                snapshot.commitUser(),
                snapshot.commitIdentifier(),
                snapshot.commitKind() == null ? null : snapshot.commitKind().name(),
                snapshot.timeMillis(),
                snapshot.watermark(),
                snapshot.totalRecordCount(),
                snapshot.deltaRecordCount(),
                snapshot.changelogRecordCount(),
                snapshot.properties(),
                changedPartitions);
    }

    /**
     * Parse the persisted Paimon offset as a non-negative snapshot id.
     *
     * @param offsetExclusive persisted offset, or null
     * @return parsed offset, or null
     */
    private Long parseOffset(String offsetExclusive) {
        if (offsetExclusive == null) {
            return null;
        }
        try {
            long offset = Long.parseLong(offsetExclusive);
            if (offset < 0) {
                throw new NumberFormatException("negative snapshot id");
            }
            return offset;
        } catch (NumberFormatException e) {
            throw new SnapshotSourceException("Invalid Paimon snapshot offset: " + offsetExclusive, e);
        }
    }

    /**
     * Compute an inclusive bounded scan endpoint without overflowing a long.
     *
     * @param first first snapshot id in the scan
     * @param batchSize configured maximum result count
     * @return inclusive last snapshot id
     */
    private long boundedLast(long first, int batchSize) {
        long increment = batchSize - 1L;
        return first > Long.MAX_VALUE - increment ? Long.MAX_VALUE : first + increment;
    }

    /**
     * Classify a Paimon offset using contiguous numeric snapshot-id semantics.
     *
     * @param definition configured source definition
     * @param durableOffset parsed durable offset, or null
     * @param earliest earliest retained snapshot id, or null
     * @param latest latest retained snapshot id, or null
     * @return format-neutral source position
     */
    private SnapshotSourcePosition position(
            PaimonSourceDefinition definition,
            Long durableOffset,
            Long earliest,
            Long latest) {
        String durable = durableOffset == null ? null : durableOffset.toString();
        if ((earliest == null) != (latest == null)) {
            throw new SnapshotSourceException(
                    "Paimon source returned an inconsistent retained snapshot range");
        }
        if (earliest == null || latest == null) {
            SnapshotSourceOffsetStatus status = durableOffset == null
                    ? SnapshotSourceOffsetStatus.EMPTY
                    : SnapshotSourceOffsetStatus.OFFSET_AHEAD;
            String detail = durableOffset == null
                    ? "Paimon table has no snapshots"
                    : "Paimon table has no retained snapshots but a durable offset exists";
            return new SnapshotSourcePosition(status, durable, null, null, null, 0L, detail);
        }

        if (durableOffset == null) {
            long pending = definition.startupMode() == io.github.lakehouseflow.integration.source.SnapshotSourceStartupMode.LATEST
                    ? 1L
                    : latest - earliest + 1L;
            return new SnapshotSourcePosition(
                    SnapshotSourceOffsetStatus.UNINITIALIZED,
                    null,
                    earliest.toString(),
                    latest.toString(),
                    latest.toString(),
                    pending,
                    "Paimon source has not established a durable offset");
        }
        if (durableOffset > latest) {
            return new SnapshotSourcePosition(
                    SnapshotSourceOffsetStatus.OFFSET_AHEAD,
                    durable,
                    earliest.toString(),
                    latest.toString(),
                    latest.toString(),
                    0L,
                    "Durable Paimon offset is newer than the source latest snapshot");
        }
        if (durableOffset.equals(latest)) {
            return new SnapshotSourcePosition(
                    SnapshotSourceOffsetStatus.IN_SYNC,
                    durable,
                    earliest.toString(),
                    latest.toString(),
                    latest.toString(),
                    0L,
                    "Durable Paimon offset matches the source latest snapshot");
        }
        if (durableOffset < earliest && durableOffset != earliest - 1L) {
            return new SnapshotSourcePosition(
                    SnapshotSourceOffsetStatus.RETENTION_GAP,
                    durable,
                    earliest.toString(),
                    latest.toString(),
                    latest.toString(),
                    latest - earliest + 1L,
                    "Required Paimon snapshots have expired before the retained range");
        }
        return new SnapshotSourcePosition(
                SnapshotSourceOffsetStatus.LAGGING,
                durable,
                earliest.toString(),
                latest.toString(),
                latest.toString(),
                latest - durableOffset,
                "Retained Paimon snapshots are waiting for ingestion");
    }
}
