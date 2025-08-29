package org.opencb.opencga.storage.core.variant.search.solr;

import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.utils.Watchdog;

import java.util.concurrent.TimeUnit;

public class VariantSearchLoadingWatchdog extends Watchdog {

    private final VariantStorageMetadataManager metadataManager;

    public VariantSearchLoadingWatchdog(VariantStorageMetadataManager metadataManager) {
        this(metadataManager, 1, TimeUnit.MINUTES);
    }

    public VariantSearchLoadingWatchdog(VariantStorageMetadataManager metadataManager, long timeout, TimeUnit timeUnit) {
        super("VariantSearchLoadingWatchdog", timeout, timeUnit);
        this.metadataManager = metadataManager;
    }

    @Override
    protected void updateStatus() throws Exception {
        metadataManager.updateProjectMetadata(projectMetadata -> {
            projectMetadata.getSecondaryAnnotationIndex().refreshSolrUpdateTimestamp(timeoutMillis * 2, TimeUnit.MILLISECONDS);
        });
    }

    @Override
    protected void onShutdown() throws Exception {
        metadataManager.updateProjectMetadata(projectMetadata -> {
            projectMetadata.getSecondaryAnnotationIndex().resetSolrUpdateTimestamp();
        });
    }

}
