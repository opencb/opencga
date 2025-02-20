package com.zettagenomics.opencga.enterprise.catalog.managers;

import com.zettagenomics.opencga.enterprise.core.configuration.EnterpriseConfiguration;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.managers.AuditManager;
import org.opencb.opencga.catalog.managers.CatalogManager;

public class EnterpriseAbstractManager {

    protected final CatalogManager catalogManager;
    protected final EnterpriseConfiguration enterpriseConfiguration;

    protected final AuthorizationManager authorizationManager;
    protected final EnterpriseAuditManager auditManager;
    protected final DBAdaptorFactory dbAdaptorFactory;

    public EnterpriseAbstractManager(CatalogManager catalogManager, EnterpriseConfiguration enterpriseConfiguration) {
        this.catalogManager = catalogManager;
        this.enterpriseConfiguration = enterpriseConfiguration;

        this.authorizationManager = catalogManager.getAuthorizationManager();
        this.auditManager = EnterpriseFactory.getEnterpriseAuditManager();
        this.dbAdaptorFactory = EnterpriseFactory.getCatalogDBAdaptorFactory();
    }
}
