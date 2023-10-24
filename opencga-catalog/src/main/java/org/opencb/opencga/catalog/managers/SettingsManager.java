package org.opencb.opencga.catalog.managers;

import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.JwtPayload;
import org.opencb.opencga.core.models.settings.Settings;
import org.opencb.opencga.core.models.settings.SettingsCreateParams;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SettingsManager extends AbstractManager {

    private final Logger logger;

    SettingsManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                    DBAdaptorFactory catalogDBAdaptorFactory, Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, configuration);
        this.logger = LoggerFactory.getLogger(SettingsManager.class);
    }

    public OpenCGAResult<Settings> create(SettingsCreateParams settingsCreateParams, String token) throws CatalogException {
        JwtPayload jwtPayload = catalogManager.getUserManager().validateToken(token);
        String organizationId = jwtPayload.getOrganization();
        Settings settings = settingsCreateParams.toSettings(jwtPayload.getUserId());

        return catalogDBAdaptorFactory.getCatalogSettingsDBAdaptor(organizationId).insert(settings);
    }
}
