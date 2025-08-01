package com.zettagenomics.opencga.enterprise.server;

import com.zettagenomics.opencga.enterprise.core.configuration.EnterpriseConfiguration;
import com.zettagenomics.opencga.enterprise.cvdb.CvdbSolrEngine;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;

public class CvdbWSUtils {

    protected static Logger logger = LoggerFactory.getLogger(CvdbWSUtils.class);

    public static CvdbSolrEngine getCvdbSolrEngine(CatalogManager catalogManager, Path opencgaHome) {
        logger.info("Initializing CVDB Solr Engine");

        EnterpriseConfiguration enterpriseConfiguration = EnterpriseConfiguration.load(opencgaHome);

        return new CvdbSolrEngine(enterpriseConfiguration.getCvdb(), catalogManager);
    }
}

