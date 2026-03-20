package com.zettagenomics.opencga.enterprise.catalog.managers;

import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.managers.AuditManager;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.config.Configuration;

public class EnterpriseAbstractManager {

    protected final CatalogManager catalogManager;
    protected final Configuration configuration;

    protected final AuthorizationManager authorizationManager;
    protected final EnterpriseAuditManager auditManager;
    protected final DBAdaptorFactory dbAdaptorFactory;

    public EnterpriseAbstractManager(CatalogManager catalogManager, Configuration configuration) {
        this.catalogManager = catalogManager;
        this.configuration = configuration;

        this.authorizationManager = catalogManager.getAuthorizationManager();
        this.auditManager = EnterpriseFactory.getEnterpriseAuditManager();
        this.dbAdaptorFactory = EnterpriseFactory.getCatalogDBAdaptorFactory();
    }
}
