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
import org.opencb.commons.datastore.core.QueryResult;
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

    public void auditCreate(String userId, String id, String uuid, String studyId, String studyUuid, ObjectMap params,
                            AuditRecord.Entity entity, int status) {
        audit(userId, id, uuid, studyId, studyUuid, new Query(), params, entity, AuditRecord.Action.CREATE, status, new ObjectMap());
    }

    public void auditCreate(String userId, String id, String uuid, String studyId, String studyUuid, ObjectMap params,
                            AuditRecord.Entity entity, AuditRecord.Action action, int status) {
        String operationUuid = UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.AUDIT);
        audit(userId, operationUuid, id, uuid, studyId, studyUuid, new Query(), params, entity, action, status, new ObjectMap());
    }

    public void auditUpdate(String userId, String id, String uuid, String studyId, String studyUuid, ObjectMap params,
                            AuditRecord.Entity entity, int status) {
        audit(userId, id, uuid, studyId, studyUuid, new Query(), params, entity, AuditRecord.Action.UPDATE, status, new ObjectMap());
    }

    public void auditDelete(String userId, String id, String uuid, String studyId, String studyUuid, Query query, ObjectMap params,
                            AuditRecord.Entity entity, int status) {
        audit(userId, id, uuid, studyId, studyUuid, query, params, entity, AuditRecord.Action.UPDATE, status, new ObjectMap());
    }

    public void auditDelete(String userId, String operationUuid, String id, String uuid, String studyId, String studyUuid, Query query,
                            ObjectMap params, AuditRecord.Entity entity, int status) {
        audit(userId, operationUuid, id, uuid, studyId, studyUuid, query, params, entity, AuditRecord.Action.UPDATE, status,
                new ObjectMap());
    }

    public void auditUser(String userId, String id, AuditRecord.Action action, int status) {
        audit(userId, id, "", "", "", new Query(), new ObjectMap(), AuditRecord.Entity.USER, action, status, new ObjectMap());
    }

    public void auditUser(String userId, String id, ObjectMap params, AuditRecord.Action action, int status) {
        audit(userId, id, "", "", "", new Query(), params, AuditRecord.Entity.USER, action, status, new ObjectMap());
    }

    public void auditInfo(String userId, String id, String uuid, String studyId, String studyUuid, ObjectMap params,
                          AuditRecord.Entity entity, int status) {
        audit(userId, id, uuid, studyId, studyUuid, new Query(), params, entity, AuditRecord.Action.INFO, status, new ObjectMap());
    }

    public void auditInfo(String userId, String operationUuid, String id, String uuid, String studyId, String studyUuid, ObjectMap params,
                          AuditRecord.Entity entity, int status) {
        audit(userId, operationUuid, id, uuid, studyId, studyUuid, new Query(), params, entity, AuditRecord.Action.INFO, status,
                new ObjectMap());
    }

    public void auditSearch(String userId, String studyId, String studyUuid, Query query, ObjectMap params, AuditRecord.Entity entity,
                            int status) {
        audit(userId, "", "", studyId, studyUuid, query, params, entity, AuditRecord.Action.SEARCH, status, new ObjectMap());
    }

    public void auditCount(String userId, String studyId, String studyUuid, Query query, ObjectMap params, AuditRecord.Entity entity,
                            int status) {
        audit(userId, "", "", studyId, studyUuid, query, params, entity, AuditRecord.Action.COUNT, status, new ObjectMap());
    }

    public void auditFacet(String userId, String studyId, String studyUuid, Query query, ObjectMap params, AuditRecord.Entity entity,
                           int status) {
        audit(userId, "", "", studyId, studyUuid, query, params, entity, AuditRecord.Action.FACET, status, new ObjectMap());
    }

    public void audit(String userId, String id, String uuid, String studyId, String studyUuid, ObjectMap params, AuditRecord.Entity entity,
                      AuditRecord.Action action, int status) {
        audit(userId, id, uuid, studyId, studyUuid, new Query(), params, entity, action, status, new ObjectMap());
    }

    public void audit(String userId, String id, String uuid, String studyId, String studyUuid, Query query,
                      ObjectMap params, AuditRecord.Entity entity, AuditRecord.Action action, int status, ObjectMap attributes) {
        audit(userId, UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.AUDIT), id, uuid, studyId, studyUuid, query, params, entity, action,
                status, attributes);
    }

    public void audit(String userId, String operationUuid, String id, String uuid, String studyId, String studyUuid, Query query,
                      ObjectMap params, AuditRecord.Entity entity, AuditRecord.Action action, int status, ObjectMap attributes) {
        String apiVersion = GitRepositoryState.get().getBuildVersion();
        Date date = TimeUtils.getDate();

        AuditRecord auditRecord = new AuditRecord(userId, apiVersion, operationUuid, id, uuid, studyId, studyUuid, params, entity,
                action, status, date, attributes, );
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
     * @return A QueryResult object containing the results of the query grouped by the fields.
     * @throws CatalogException CatalogException
     */
    public QueryResult groupBy(Query query, String fields, QueryOptions options, String sessionId) throws CatalogException {
        if (StringUtils.isEmpty(fields)) {
            throw new CatalogException("Empty fields parameter.");
        }
        return groupBy(query, Arrays.asList(fields.split(",")), options, sessionId);
    }

    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options, String token) throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(token);
        if (authorizationManager.checkIsAdmin(userId)) {
            return auditDBAdaptor.groupBy(query, fields, options);
        }
        throw new CatalogAuthorizationException("Only root of OpenCGA can query the audit database");
    }
}
