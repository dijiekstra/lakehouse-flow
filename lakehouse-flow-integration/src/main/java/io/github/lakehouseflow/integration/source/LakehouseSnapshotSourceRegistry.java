package io.github.lakehouseflow.integration.source;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Resolves configured snapshot sources and enforces identity isolation across adapters.
 */
@Component
@RequiredArgsConstructor
public class LakehouseSnapshotSourceRegistry {

    private final List<LakehouseSnapshotSourceProvider> sourceProviders;

    /**
     * Return all configured sources after validating offset and asset identity uniqueness.
     *
     * @return immutable configured source list
     */
    public List<LakehouseSnapshotSource> sources() {
        List<LakehouseSnapshotSource> sources = sourceProviders.stream()
                .flatMap(provider -> provider.sources().stream())
                .toList();
        Set<String> identities = new HashSet<>();
        Set<String> assetKeys = new HashSet<>();
        for (LakehouseSnapshotSource source : sources) {
            LakehouseSourceIdentity identity = source.identity();
            String offsetKey = identity.sourceType() + ":" + identity.sourceName();
            if (!identities.add(offsetKey)) {
                throw new IllegalStateException("Duplicate snapshot source identity: " + offsetKey);
            }
            if (!assetKeys.add(identity.assetKey())) {
                throw new IllegalStateException(
                        "Duplicate lakehouse asset identity: " + identity.assetKey());
            }
        }
        return List.copyOf(sources);
    }
}
