package com.zettagenomics.opencga.enterprise.catalog.managers;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.managers.AuditManager;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.audit.AuditRecord;
import org.opencb.opencga.core.models.common.Enums;

public class EnterpriseAuditManager extends AuditManager {

    public EnterpriseAuditManager(AuthorizationManager authorizationManager, CatalogManager catalogManager,
                                  DBAdaptorFactory dbAdaptorFactory, Configuration configuration) {
        super(authorizationManager, catalogManager, dbAdaptorFactory, configuration);
    }

    protected void audit(String organizationId, String userId, Enums.Action action, Enums.Resource resource,
                         String resourceId, String resourceUuid, String studyId, String studyUuid, ObjectMap params,
                         AuditRecord.Status status) {
        audit(organizationId, UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT), userId, action.name(), resource, resourceId,
                resourceUuid, studyId, studyUuid, params, status, new ObjectMap());
    }

    protected void audit(String organizationId, String userId, Enums.Action action, Enums.Resource resource, String resourceId,
                         String resourceUuid, String studyId, String studyUuid, ObjectMap params, AuditRecord.Status status,
                         ObjectMap attributes) {
        audit(organizationId, UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT), userId, action.name(), resource, resourceId,
                resourceUuid, studyId, studyUuid, params, status, attributes);
    }

    protected void audit(String organizationId, String operationId, String userId, Enums.Action action, Enums.Resource resource,
                         String resourceId, String resourceUuid, String studyId, String studyUuid, ObjectMap params,
                         AuditRecord.Status status, ObjectMap attributes) {
        audit(organizationId, operationId, userId, action.name(), resource, resourceId, resourceUuid, studyId, studyUuid, params, status,
                attributes);
    }


}
