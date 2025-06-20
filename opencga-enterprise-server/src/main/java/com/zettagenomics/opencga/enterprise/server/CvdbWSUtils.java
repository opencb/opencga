package com.zettagenomics.opencga.enterprise.server;

import com.zettagenomics.opencga.enterprise.core.configuration.EnterpriseConfiguration;
import com.zettagenomics.opencga.enterprise.cvdb.CvdbSolrEngine;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.project.SearchIndexMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

public class CvdbWSUtils {

    protected static Logger logger = LoggerFactory.getLogger(CvdbWSUtils.class);

    public static CvdbSolrEngine getCvdbSolrEngine(CatalogManager catalogManager, Path opencgaHome)
            throws StorageEngineException, IOException {
        logger.info("Initializing CVDB Solr Engine");

        EnterpriseConfiguration enterpriseConfiguration = EnterpriseConfiguration.load(opencgaHome);

        VariantStorageEngine variantStorageEngine;
        InputStream inputStream = Files.newInputStream(opencgaHome.resolve("conf/storage-configuration.yml"));
        StorageConfiguration storageConfiguration = StorageConfiguration.load(inputStream);
        String storageEngine = storageConfiguration.getVariant().getDefaultEngine();
        logger.debug("Storage Engine set to '{}'", storageEngine);
        StorageEngineFactory storageEngineFactory = StorageEngineFactory.get(storageConfiguration);
        if (storageEngine == null || storageEngine.isEmpty()) {
            variantStorageEngine = storageEngineFactory.getVariantStorageEngine();
        } else {
            variantStorageEngine = storageEngineFactory.getVariantStorageEngine(storageEngine, "");
        }
        SearchIndexMetadata searchIndexMetadata = variantStorageEngine.getVariantSearchManager().getSearchIndexMetadata();
        VariantStorageMetadataManager metadataManager = variantStorageEngine.getMetadataManager();

        return new CvdbSolrEngine(enterpriseConfiguration.getCvdb(), catalogManager, metadataManager,
                searchIndexMetadata);
    }
}

