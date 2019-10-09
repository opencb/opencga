/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.catalog.audit;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.AuditDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.utils.UUIDUtils;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * Created on 18/08/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class AuditManager {

    protected static Logger logger = LoggerFactory.getLogger(AuditManager.class);

    private final CatalogManager catalogManager;
    private final AuthorizationManager authorizationManager;
    private final AuditDBAdaptor auditDBAdaptor;

    public AuditManager(AuthorizationManager authorizationManager, CatalogManager catalogManager, DBAdaptorFactory catalogDBAdaptorFactory,
                        Configuration configuration) {
        this.catalogManager = catalogManager;
        this.authorizationManager = authorizationManager;
        this.auditDBAdaptor = catalogDBAdaptorFactory.getCatalogAuditDbAdaptor();
    }

    public void audit(AuditRecord auditRecord) throws CatalogException {
        auditDBAdaptor.insertAuditRecord(auditRecord);
    }

    public void audit(List<AuditRecord> auditRecordList) throws CatalogException {
        for (AuditRecord auditRecord : auditRecordList) {
            auditDBAdaptor.insertAuditRecord(auditRecord);
        }
    }

    public void auditCreate(String userId, AuditRecord.Resource resource, String resourceId, String resourceUuid, String studyId,
                            String studyUuid, ObjectMap params, AuditRecord.Status status) {
        audit(userId, AuditRecord.Action.CREATE, resource, resourceId, resourceUuid, studyId, studyUuid, params, status, new ObjectMap());
    }

    public void auditCreate(String userId, AuditRecord.Action action, AuditRecord.Resource resource, String resourceId, String resourceUuid,
                            String studyId, String studyUuid, ObjectMap params, AuditRecord.Status status) {
        String operationUuid = UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.AUDIT);
        audit(operationUuid, userId, action, resource, resourceId, resourceUuid, studyId, studyUuid, params, status, new ObjectMap());
    }

    public void auditUpdate(String operationId, String userId, AuditRecord.Resource resource, String resourceId, String resourceUuid,
                            String studyId, String studyUuid, ObjectMap params, AuditRecord.Status status) {
        audit(operationId, userId, AuditRecord.Action.UPDATE, resource, resourceId, resourceUuid, studyId, studyUuid, params, status,
                new ObjectMap());
    }

    public void auditUpdate(String userId, AuditRecord.Resource resource, String resourceId, String resourceUuid, String studyId,
                            String studyUuid, ObjectMap params, AuditRecord.Status status) {
        audit(userId, AuditRecord.Action.UPDATE, resource, resourceId, resourceUuid, studyId, studyUuid, params, status, new ObjectMap());
    }

    public void auditDelete(String userId, AuditRecord.Resource resource, String resourceId, String resourceUuid, String studyId,
                            String studyUuid, ObjectMap params, AuditRecord.Status status) {
        audit(userId, AuditRecord.Action.UPDATE, resource, resourceId, resourceUuid, studyId, studyUuid, params, status, new ObjectMap());
    }

    public void auditDelete(String operationId, String userId, AuditRecord.Resource resource, String resourceId, String resourceUuid,
                            String studyId, String studyUuid, ObjectMap params, AuditRecord.Status status) {
        audit(operationId, userId, AuditRecord.Action.UPDATE, resource, resourceId, resourceUuid, studyId, studyUuid, params, status,
                new ObjectMap());
    }

    public void auditUser(String userId, AuditRecord.Action action, String resourceId, AuditRecord.Status status) {
        audit(userId, action, AuditRecord.Resource.USER, resourceId, "", "", "", new ObjectMap(), status, new ObjectMap());
    }

    public void auditUser(String userId, AuditRecord.Action action, String resourceId, ObjectMap params, AuditRecord.Status status) {
        audit(userId, action, AuditRecord.Resource.USER, resourceId, "", "", "", params, status, new ObjectMap());
    }

    public void auditInfo(String userId, AuditRecord.Resource resource, String resourceId, String resourceUuid, String studyId,
                          String studyUuid, ObjectMap params, AuditRecord.Status status) {
        audit(userId, AuditRecord.Action.INFO, resource, resourceId, resourceUuid, studyId, studyUuid, params, status, new ObjectMap());
    }

    public void auditInfo(String operationId, String userId, AuditRecord.Resource resource, String resourceId, String resourceUuid,
                          String studyId, String studyUuid, ObjectMap params, AuditRecord.Status status) {
        audit(operationId, userId, AuditRecord.Action.INFO, resource, resourceId, resourceUuid, studyId, studyUuid, params, status,
                new ObjectMap());
    }

    public void auditSearch(String userId, AuditRecord.Resource resource, String studyId, String studyUuid, ObjectMap params,
                            AuditRecord.Status status) {
        audit(userId, AuditRecord.Action.SEARCH, resource, "", "", studyId, studyUuid, params, status, new ObjectMap());
    }

    public void auditCount(String userId, AuditRecord.Resource resource, String studyId, String studyUuid, ObjectMap params,
                           AuditRecord.Status status) {
        audit(userId, AuditRecord.Action.COUNT, resource, "", "", studyId, studyUuid, params, status, new ObjectMap());
    }

    public void auditFacet(String userId, AuditRecord.Resource resource, String studyId, String studyUuid, ObjectMap params,
                           AuditRecord.Status status) {
        audit(userId, AuditRecord.Action.FACET, resource, "", "", studyUuid, studyId, params, status, new ObjectMap());
    }

    public void audit(String userId, AuditRecord.Action action, AuditRecord.Resource resource, String resourceId, String resourceUuid,
                      String studyId, String studyUuid, ObjectMap params, AuditRecord.Status status) {
        audit(userId, action, resource, resourceId, resourceUuid, studyId, studyUuid, params, status, new ObjectMap());
    }

    public void audit(String userId, AuditRecord.Action action, AuditRecord.Resource resource, String resourceId, String resourceUuid,
                      String studyId, String studyUuid, ObjectMap params, AuditRecord.Status status, ObjectMap attributes) {
        audit(UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.AUDIT), userId, action, resource, resourceId, resourceUuid, studyId, studyUuid,
                params, status, attributes);
    }

    public void audit(String operationId, String userId, AuditRecord.Action action, AuditRecord.Resource resource, String resourceId,
                      String resourceUuid, String studyId, String studyUuid, ObjectMap params, AuditRecord.Status status,
                      ObjectMap attributes) {
        String apiVersion = GitRepositoryState.get().getBuildVersion();
        Date date = TimeUtils.getDate();

        String auditId = UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.AUDIT);

        AuditRecord auditRecord = new AuditRecord(auditId, operationId, userId, apiVersion, action, resource, resourceId, resourceUuid,
                studyId, studyUuid, params, status, date, attributes);
        try {
            auditDBAdaptor.insertAuditRecord(auditRecord);
        } catch (CatalogDBException e) {
            logger.error("Could not audit '{}' -> Error: {}", auditRecord, e.getMessage(), e);
        }
    }

    /**
     * Groups the matching entries by some fields.
     *
     * @param query     Query object.
     * @param fields    A field or a comma separated list of fields by which the results will be grouped in.
     * @param options   QueryOptions object.
     * @param sessionId Session id of the user logged in.
     * @return A DataResult object containing the results of the query grouped by the fields.
     * @throws CatalogException CatalogException
     */
    public DataResult groupBy(Query query, String fields, QueryOptions options, String sessionId) throws CatalogException {
        if (StringUtils.isEmpty(fields)) {
            throw new CatalogException("Empty fields parameter.");
        }
        return groupBy(query, Arrays.asList(fields.split(",")), options, sessionId);
    }

    public DataResult groupBy(Query query, List<String> fields, QueryOptions options, String token) throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(token);
        if (authorizationManager.checkIsAdmin(userId)) {
            return auditDBAdaptor.groupBy(query, fields, options);
        }
        throw new CatalogAuthorizationException("Only root of OpenCGA can query the audit database");
    }
}
