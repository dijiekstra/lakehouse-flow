package io.github.lakehouseflow.writer.paimon;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

/**
 * Reusable Flink-side Paimon writer adapter for attributed and epoch-fenced snapshots.
 *
 * <p>A Flink sink writes rows and prepares checkpoint messages through this adapter. Both
 * streaming checkpoint completion and bounded batch completion publish through the same fenced
 * {@link #commit(long, List)} method, so snapshot properties and writer generation checks cannot
 * be accidentally skipped by one processing mode.
 */
public final class FlinkPaimonWriterAdapter implements AutoCloseable {

    private final Catalog catalog;
    private final TableWriteImpl<?> writer;
    private final TableCommitImpl committer;
    private final WriterCommitContext context;
    private final WriterEpochFence epochFence;

    /** Create an adapter around already opened Paimon resources. */
    FlinkPaimonWriterAdapter(
            Catalog catalog,
            TableWriteImpl<?> writer,
            TableCommitImpl committer,
            WriterCommitContext context,
            WriterEpochFence epochFence) {
        this.catalog = catalog;
        this.writer = writer;
        this.committer = committer;
        this.context = context;
        this.epochFence = epochFence;
    }

    /**
     * Open one filesystem or metastore-backed Paimon table for a fenced writer generation.
     *
     * @param catalogOptions complete Paimon catalog options including warehouse and metastore
     * @param databaseTable physical database.table name
     * @param context immutable writer and snapshot attribution
     * @param epochFence authoritative execution-plane epoch fence
     * @return opened writer adapter
     * @throws Exception when the catalog or physical table cannot be opened
     */
    public static FlinkPaimonWriterAdapter open(
            Options catalogOptions,
            String databaseTable,
            WriterCommitContext context,
            WriterEpochFence epochFence) throws Exception {
        if (catalogOptions == null) {
            throw new IllegalArgumentException("Paimon catalog options are required");
        }
        if (context == null) {
            throw new IllegalArgumentException("writer commit context is required");
        }
        if (epochFence == null) {
            throw new IllegalArgumentException("writer epoch fence is required");
        }
        QualifiedTable qualifiedTable = QualifiedTable.parse(databaseTable);
        Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(catalogOptions));
        boolean opened = false;
        try {
            Table table = catalog.getTable(Identifier.create(
                    qualifiedTable.database(), qualifiedTable.table()));
            if (!(table instanceof FileStoreTable fileStoreTable)) {
                throw new IllegalStateException("Not a Paimon FileStoreTable: " + databaseTable);
            }
            String commitUser = context.writerJobKey() + "-epoch-" + context.writerEpoch();
            FlinkPaimonWriterAdapter adapter = new FlinkPaimonWriterAdapter(
                    catalog,
                    fileStoreTable.newWrite(commitUser),
                    fileStoreTable.newCommit(commitUser),
                    context,
                    epochFence);
            opened = true;
            return adapter;
        } finally {
            if (!opened) {
                catalog.close();
            }
        }
    }

    /** Write one Flink internal row into the current Paimon transaction. */
    public void write(InternalRow row) throws Exception {
        if (row == null) {
            throw new IllegalArgumentException("Paimon row is required");
        }
        writer.write(row);
    }

    /** Prepare files at a Flink checkpoint barrier without publishing a snapshot. */
    public List<CommitMessage> prepareCheckpoint(long checkpointId) throws Exception {
        if (checkpointId < 0) {
            throw new IllegalArgumentException("checkpointId must not be negative");
        }
        return List.copyOf(writer.prepareCommit(false, checkpointId));
    }

    /** Prepare the final files produced by one bounded Flink run. */
    public List<CommitMessage> prepareBatch() throws Exception {
        return List.copyOf(writer.prepareCommit());
    }

    /**
     * Publish one attributed Paimon snapshot while retaining the authoritative epoch lock.
     *
     * @param identifier Flink checkpoint id or stable bounded-intent identifier
     * @param messages prepared Paimon commit messages
     * @throws Exception when the writer is fenced or Paimon cannot publish the snapshot
     */
    public void commit(long identifier, List<CommitMessage> messages) throws Exception {
        if (identifier < 0) {
            throw new IllegalArgumentException("commit identifier must not be negative");
        }
        if (messages == null || messages.isEmpty()) {
            throw new IllegalArgumentException("Paimon commit messages must not be empty");
        }
        try (WriterEpochFence.CommitPermit ignored = epochFence.acquireCommitPermit(context)) {
            ManifestCommittable committable = new ManifestCommittable(identifier);
            messages.forEach(committable::addFileCommittable);
            context.snapshotProperties().forEach(committable::addProperty);
            committer.commit(committable);
        }
    }

    /** Abort files prepared by a checkpoint that Flink discarded. */
    public void abort(List<CommitMessage> messages) {
        if (messages != null && !messages.isEmpty()) {
            committer.abort(messages);
        }
    }

    /** Derive a deterministic positive Paimon identifier from an immutable intent key. */
    public static long stableCommitIdentifier(String intentKey) {
        if (intentKey == null || intentKey.isBlank()) {
            throw new IllegalArgumentException("intentKey must not be blank");
        }
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(intentKey.getBytes(StandardCharsets.UTF_8));
            long value = 0L;
            for (int index = 0; index < Long.BYTES; index++) {
                value = (value << 8) | Byte.toUnsignedLong(digest[index]);
            }
            return value & Long.MAX_VALUE;
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is required by Java 17", exception);
        }
    }

    /** Close writer, committer, and catalog resources while retaining suppressed failures. */
    @Override
    public void close() throws Exception {
        Exception failure = null;
        try {
            writer.close();
        } catch (Exception exception) {
            failure = exception;
        }
        try {
            committer.close();
        } catch (Exception exception) {
            failure = append(failure, exception);
        }
        try {
            catalog.close();
        } catch (Exception exception) {
            failure = append(failure, exception);
        }
        if (failure != null) {
            throw failure;
        }
    }

    /** Retain the first close failure and attach later failures for diagnostics. */
    private static Exception append(Exception current, Exception next) {
        if (current == null) {
            return next;
        }
        current.addSuppressed(next);
        return current;
    }

    /** Parsed physical Paimon database and table pair. */
    private record QualifiedTable(String database, String table) {

        /** Parse one strict database.table identifier. */
        private static QualifiedTable parse(String value) {
            String[] parts = value == null ? new String[0] : value.split("\\.", -1);
            if (parts.length != 2 || parts[0].isBlank() || parts[1].isBlank()) {
                throw new IllegalArgumentException("Expected database.table but got: " + value);
            }
            return new QualifiedTable(parts[0], parts[1]);
        }
    }
}
