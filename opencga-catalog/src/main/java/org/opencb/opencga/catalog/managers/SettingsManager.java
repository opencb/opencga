package org.opencb.opencga.catalog.managers;

import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UuidUtils;
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
        validateNewSettings(settings, jwtPayload.getUserId());

        return catalogDBAdaptorFactory.getCatalogSettingsDBAdaptor(organizationId).insert(settings);
    }

    public void validateNewSettings(Settings settings, String userId) throws CatalogParameterException {
        ParamUtils.checkIdentifier(settings.getId(), "id");
        settings.setUserId(userId);
        settings.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.SETTINGS));
        settings.setVersion(1);
        settings.setCreationDate(ParamUtils.checkDateOrGetCurrentDate(settings.getCreationDate(),
                SampleDBAdaptor.QueryParams.CREATION_DATE.key()));
        settings.setModificationDate(ParamUtils.checkDateOrGetCurrentDate(settings.getModificationDate(),
                SampleDBAdaptor.QueryParams.MODIFICATION_DATE.key()));
        ParamUtils.checkObj(settings.getValueType(), "valueType");
        ParamUtils.checkObj(settings.getValue(), "value");
    }
}
