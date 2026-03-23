package com.zettagenomics.opencga.enterprise.server;

import com.zettagenomics.opencga.enterprise.cvdb.CvdbSolrEngine;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CvdbWSUtils {

    protected static Logger logger = LoggerFactory.getLogger(CvdbWSUtils.class);

    public static CvdbSolrEngine getCvdbSolrEngine(CatalogManager catalogManager) {
        logger.info("Initializing CVDB Solr Engine");
        return new CvdbSolrEngine(catalogManager.getConfiguration().getCvdb(), catalogManager);
    }
}

