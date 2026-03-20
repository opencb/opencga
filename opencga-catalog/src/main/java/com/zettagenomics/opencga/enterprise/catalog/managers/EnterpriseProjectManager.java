package com.zettagenomics.opencga.enterprise.catalog.managers;

import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.config.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnterpriseProjectManager extends EnterpriseAbstractManager {

    protected static Logger logger = LoggerFactory.getLogger(EnterpriseProjectManager.class);

    public EnterpriseProjectManager(CatalogManager catalogManager, Configuration configuration) {
        super(catalogManager, configuration);
    }




}
