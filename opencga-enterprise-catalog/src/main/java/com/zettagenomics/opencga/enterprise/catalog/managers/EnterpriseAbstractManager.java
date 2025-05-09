package com.zettagenomics.opencga.enterprise.catalog.managers;

import com.zettagenomics.opencga.enterprise.core.configuration.EnterpriseConfiguration;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.managers.CatalogManager;

public class EnterpriseAbstractManager {

    protected final CatalogManager catalogManager;
    protected final EnterpriseConfiguration enterpriseConfiguration;
    protected final String opencgaToken;

    public EnterpriseAbstractManager(CatalogManager catalogManager, EnterpriseConfiguration enterpriseConfiguration,
                                     String opencgaToken) {
        this.catalogManager = catalogManager;
        this.enterpriseConfiguration = enterpriseConfiguration;
        this.opencgaToken = opencgaToken;
    }
}
