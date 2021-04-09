/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.catalog.managers;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.AuditDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

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

    private final Map<String, List<AuditRecord>> auditRecordMap;
    private static final int MAX_BATCH_SIZE = 100;

    public AuditManager(AuthorizationManager authorizationManager, CatalogManager catalogManager, DBAdaptorFactory catalogDBAdaptorFactory,
                        Configuration configuration) {
        this.catalogManager = catalogManager;
        this.authorizationManager = authorizationManager;
        this.auditDBAdaptor = catalogDBAdaptorFactory.getCatalogAuditDbAdaptor();
        this.auditRecordMap = new HashMap<>();
    }

    public void audit(AuditRecord auditRecord) throws CatalogException {
        auditDBAdaptor.insertAuditRecord(auditRecord);
    }

    public void audit(List<AuditRecord> auditRecordList) throws CatalogException {
        for (AuditRecord auditRecord : auditRecordList) {
            auditDBAdaptor.insertAuditRecord(auditRecord);
        }
    }

    public void initAuditBatch(String operationId) {
        this.auditRecordMap.put(operationId, new LinkedList<>());
    }

    public void finishAuditBatch(String operationId) throws CatalogException {
        if (!this.auditRecordMap.containsKey(operationId)) {
            throw new CatalogException("Cannot audit. Operation id '" + operationId + "' not found.");
        }
        try {
            if (!this.auditRecordMap.get(operationId).isEmpty()) {
                auditDBAdaptor.insertAuditRecords(this.auditRecordMap.get(operationId));
            }
        } catch (CatalogDBException e) {
            logger.error("Could not audit operation '{}' -> Error: {}", operationId, e.getMessage(), e);
        } finally {
            this.auditRecordMap.remove(operationId);
        }
    }

    public void auditCreate(String userId, Enums.Resource resource, String resourceId, String resourceUuid, String studyId,
                            String studyUuid, ObjectMap params, AuditRecord.Status status) {
        audit(userId, Enums.Action.CREATE, resource, resourceId, resourceUuid, studyId, studyUuid, params, status, new ObjectMap());
    }

    public void auditCreate(String userId, Enums.Action action, Enums.Resource resource, String resourceId, String resourceUuid,
                            String studyId, String studyUuid, ObjectMap params, AuditRecord.Status status) {
        String operationUuid = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);
        audit(operationUuid, userId, action, resource, resourceId, resourceUuid, studyId, studyUuid, params, status, new ObjectMap());
    }

    public void auditUpdate(String operationId, String userId, Enums.Resource resource, String resourceId, String resourceUuid,
                            String studyId, String studyUuid, ObjectMap params, AuditRecord.Status status) {
        audit(operationId, userId, Enums.Action.UPDATE, resource, resourceId, resourceUuid, studyId, studyUuid, params, status,
                new ObjectMap());
    }

    public void auditUpdate(String userId, Enums.Resource resource, String resourceId, String resourceUuid, String studyId,
                            String studyUuid, ObjectMap params, AuditRecord.Status status) {
        audit(userId, Enums.Action.UPDATE, resource, resourceId, resourceUuid, studyId, studyUuid, params, status, new ObjectMap());
    }

    public void auditDelete(String userId, Enums.Resource resource, String resourceId, String resourceUuid, String studyId,
                            String studyUuid, ObjectMap params, AuditRecord.Status status) {
        audit(userId, Enums.Action.UPDATE, resource, resourceId, resourceUuid, studyId, studyUuid, params, status, new ObjectMap());
    }

    public void auditDelete(String operationId, String userId, Enums.Resource resource, String resourceId, String resourceUuid,
                            String studyId, String studyUuid, ObjectMap params, AuditRecord.Status status) {
        audit(operationId, userId, Enums.Action.UPDATE, resource, resourceId, resourceUuid, studyId, studyUuid, params, status,
                new ObjectMap());
    }

    public void auditUser(String userId, Enums.Action action, String resourceId, AuditRecord.Status status) {
        audit(userId, action, Enums.Resource.USER, resourceId, "", "", "", new ObjectMap(), status, new ObjectMap());
    }

    public void auditUser(String userId, Enums.Action action, String resourceId, ObjectMap params, AuditRecord.Status status) {
        audit(userId, action, Enums.Resource.USER, resourceId, "", "", "", params, status, new ObjectMap());
    }

    public void auditInfo(String userId, Enums.Resource resource, String resourceId, String resourceUuid, String studyId,
                          String studyUuid, ObjectMap params, AuditRecord.Status status) {
        audit(userId, Enums.Action.INFO, resource, resourceId, resourceUuid, studyId, studyUuid, params, status, new ObjectMap());
    }

    public void auditInfo(String operationId, String userId, Enums.Resource resource, String resourceId, String resourceUuid,
                          String studyId, String studyUuid, ObjectMap params, AuditRecord.Status status) {
        audit(operationId, userId, Enums.Action.INFO, resource, resourceId, resourceUuid, studyId, studyUuid, params, status,
                new ObjectMap());
    }

    public void auditSearch(String userId, Enums.Resource resource, String studyId, String studyUuid, ObjectMap params,
                            AuditRecord.Status status) {
        audit(userId, Enums.Action.SEARCH, resource, "", "", studyId, studyUuid, params, status, new ObjectMap());
    }

    public void auditCount(String userId, Enums.Resource resource, String studyId, String studyUuid, ObjectMap params,
                           AuditRecord.Status status) {
        audit(userId, Enums.Action.COUNT, resource, "", "", studyId, studyUuid, params, status, new ObjectMap());
    }

    public void auditDistinct(String userId, Enums.Resource resource, String studyId, String studyUuid, ObjectMap params,
                            AuditRecord.Status status) {
        audit(userId, Enums.Action.DISTINCT, resource, "", "", studyId, studyUuid, params, status, new ObjectMap());
    }

    public void auditFacet(String userId, Enums.Resource resource, String studyId, String studyUuid, ObjectMap params,
                           AuditRecord.Status status) {
        audit(userId, Enums.Action.FACET, resource, "", "", studyUuid, studyId, params, status, new ObjectMap());
    }

    public void audit(String userId, Enums.Action action, Enums.Resource resource, String resourceId, String resourceUuid,
                      String studyId, String studyUuid, ObjectMap params, AuditRecord.Status status) {
        audit(userId, action, resource, resourceId, resourceUuid, studyId, studyUuid, params, status, new ObjectMap());
    }

    public void audit(String userId, Enums.Action action, Enums.Resource resource, String resourceId, String resourceUuid,
                      String studyId, String studyUuid, ObjectMap params, AuditRecord.Status status, ObjectMap attributes) {
        audit(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT), userId, action, resource, resourceId, resourceUuid, studyId, studyUuid,
                params, status, attributes);
    }

    public void audit(String operationId, String userId, Enums.Action action, Enums.Resource resource, String resourceId,
                      String resourceUuid, String studyId, String studyUuid, ObjectMap params, AuditRecord.Status status) {
        audit(operationId, userId, action, resource, resourceId, resourceUuid, studyId, studyUuid, params, status, new ObjectMap());
    }

    public void audit(String operationId, String userId, Enums.Action action, Enums.Resource resource, String resourceId,
                      String resourceUuid, String studyId, String studyUuid, ObjectMap params, AuditRecord.Status status,
                      ObjectMap attributes) {
        String apiVersion = GitRepositoryState.get().getBuildVersion();
        Date date = TimeUtils.getDate();

        String auditId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        AuditRecord auditRecord = new AuditRecord(auditId, operationId, userId, apiVersion, action, resource, resourceId, resourceUuid,
                studyId, studyUuid, params, status, date, attributes);

        if (this.auditRecordMap.containsKey(operationId)) {
            this.auditRecordMap.get(operationId).add(auditRecord);

            if (this.auditRecordMap.size() == MAX_BATCH_SIZE) {
                try {
                    auditDBAdaptor.insertAuditRecords(this.auditRecordMap.get(operationId));
                } catch (CatalogDBException e) {
                    logger.error("Could not audit operation '{}' -> Error: {}", operationId, e.getMessage(), e);
                } finally {
                    this.auditRecordMap.get(operationId).clear();
                }
            }
        } else {
            try {
                auditDBAdaptor.insertAuditRecord(auditRecord);
            } catch (CatalogDBException e) {
                logger.error("Could not audit '{}' -> Error: {}", auditRecord, e.getMessage(), e);
            }
        }
    }

    public OpenCGAResult<AuditRecord> search(String studyStr, Query query, QueryOptions options, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyStr)
                .append("query", new Query(query))
                .append("options", options)
                .append("token", token);
        try {
            authorizationManager.checkIsOwnerOrAdmin(study.getUid(), userId);

            query.remove(AuditDBAdaptor.QueryParams.STUDY_ID.key());
            query.put(AuditDBAdaptor.QueryParams.STUDY_UUID.key(), study.getUuid());
            OpenCGAResult<AuditRecord> result = auditDBAdaptor.get(query, options);

            auditSearch(userId, Enums.Resource.AUDIT, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return result;
        } catch (CatalogException e) {
            auditSearch(userId, Enums.Resource.AUDIT, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    /**
     * Groups the matching entries by some fields.
     *
     * @param query     Query object.
     * @param fields    A field or a comma separated list of fields by which the results will be grouped in.
     * @param options   QueryOptions object.
     * @param sessionId Session id of the user logged in.
     * @return A OpenCGAResult object containing the results of the query grouped by the fields.
     * @throws CatalogException CatalogException
     */
    public OpenCGAResult groupBy(Query query, String fields, QueryOptions options, String sessionId) throws CatalogException {
        if (StringUtils.isEmpty(fields)) {
            throw new CatalogException("Empty fields parameter.");
        }
        return groupBy(query, Arrays.asList(fields.split(",")), options, sessionId);
    }

    public OpenCGAResult groupBy(Query query, List<String> fields, QueryOptions options, String token) throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(token);
        if (authorizationManager.isInstallationAdministrator(userId)) {
            return auditDBAdaptor.groupBy(query, fields, options);
        }
        throw new CatalogAuthorizationException("Only root of OpenCGA can query the audit database");
    }
}
