package com.zettagenomics.opencga.enterprise.catalog.managers;

import com.zettagenomics.opencga.enterprise.core.configuration.EnterpriseConfiguration;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProjectManager extends AbstractManager {

    protected static Logger logger = LoggerFactory.getLogger(ProjectManager.class);

    public ProjectManager(CatalogManager catalogManager, EnterpriseConfiguration enterpriseConfiguration) {
        super(catalogManager, enterpriseConfiguration);
    }




}
