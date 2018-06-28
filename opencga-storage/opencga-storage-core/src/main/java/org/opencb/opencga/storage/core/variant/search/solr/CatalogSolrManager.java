package org.opencb.opencga.storage.core.variant.search.solr;

import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.stats.solr.converters.CatalogSampleToSolrSampleConverter;
import org.opencb.opencga.core.models.File;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by wasim on 27/06/18.
 */
public class CatalogSolrManager {

    private CatalogManager catalogManager;
    private SolrManager solrManager;
    private StorageConfiguration storageConfiguration;
    private CatalogSampleToSolrSampleConverter catalogSampleToSolrSampleConverter;

    private Logger logger;

    public CatalogSolrManager(CatalogManager catalogManager, StorageConfiguration storageConfiguration) {
        this.catalogManager = catalogManager;
        this.storageConfiguration = storageConfiguration;
        this.solrManager = new SolrManager(storageConfiguration.getSearch().getHost(), storageConfiguration.getSearch().getMode(),
                storageConfiguration.getSearch().getTimeout());
        this.catalogSampleToSolrSampleConverter = new CatalogSampleToSolrSampleConverter();
        logger = LoggerFactory.getLogger(VariantSearchManager.class);
    }

    public void indexCatalogFilesIntoSolr(Query query) throws CatalogException {
        DBIterator<File> iterator = catalogManager.getFileManager().indexSolr(query);
        // Iterate
        // ConvertToSolrModel
        // InsertToSolr

    }
}
