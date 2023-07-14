package org.opencb.opencga.catalog.managers;

import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.core.config.Configuration;

public class OrganizationManager extends AbstractManager {

    private final CatalogIOManager catalogIOManager;

    OrganizationManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                   DBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManager catalogIOManager, Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, configuration);
        this.catalogIOManager = catalogIOManager;
    }

    



}
