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
import org.opencb.opencga.core.models.notes.Notes;
import org.opencb.opencga.core.models.notes.NotesCreateParams;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NotesManager extends AbstractManager {

    private final Logger logger;

    NotesManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                 DBAdaptorFactory catalogDBAdaptorFactory, Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, configuration);
        this.logger = LoggerFactory.getLogger(NotesManager.class);
    }

    public OpenCGAResult<Notes> create(NotesCreateParams notesCreateParams, String token) throws CatalogException {
        JwtPayload jwtPayload = catalogManager.getUserManager().validateToken(token);

        String organizationId = jwtPayload.getOrganization();
        Notes notes = notesCreateParams.toSettings(jwtPayload.getUserId());
        validateNewSettings(notes, jwtPayload.getUserId());

        return catalogDBAdaptorFactory.getCatalogSettingsDBAdaptor(organizationId).insert(notes);
    }

    public void validateNewSettings(Notes notes, String userId) throws CatalogParameterException {
        ParamUtils.checkIdentifier(notes.getId(), "id");
        notes.setUserId(userId);
        notes.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.NOTES));
        notes.setVersion(1);
        notes.setCreationDate(ParamUtils.checkDateOrGetCurrentDate(notes.getCreationDate(),
                SampleDBAdaptor.QueryParams.CREATION_DATE.key()));
        notes.setModificationDate(ParamUtils.checkDateOrGetCurrentDate(notes.getModificationDate(),
                SampleDBAdaptor.QueryParams.MODIFICATION_DATE.key()));
        ParamUtils.checkObj(notes.getValueType(), "valueType");
        ParamUtils.checkObj(notes.getValue(), "value");
    }
}
