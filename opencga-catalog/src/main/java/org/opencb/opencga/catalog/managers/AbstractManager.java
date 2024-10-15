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

import org.apache.commons.collections4.CollectionUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.*;
import org.opencb.opencga.catalog.models.InternalGetDataResult;
import org.opencb.opencga.catalog.utils.CatalogFqn;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.events.OpencgaEvent;
import org.opencb.opencga.core.models.IPrivateStudyUid;
import org.opencb.opencga.core.models.JwtPayload;
import org.opencb.opencga.core.models.common.EntryParam;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.event.CatalogEvent;
import org.opencb.opencga.core.models.study.Group;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Created by hpccoll1 on 12/05/15.
 */
public abstract class AbstractManager {

    protected final Logger logger;
    protected final AuthorizationManager authorizationManager;
    protected final AuditManager auditManager;
    protected final CatalogManager catalogManager;

    protected Configuration configuration;

    protected final DBAdaptorFactory catalogDBAdaptorFactory;

    public static final String OPENCGA = ParamConstants.OPENCGA_USER_ID;
    public static final String ANONYMOUS = ParamConstants.ANONYMOUS_USER_ID;

    public static final int BATCH_OPERATION_SIZE = 100;
    public static final int DEFAULT_LIMIT = 10;
    public static final int MAX_LIMIT = 5000;

    protected static final String INTERNAL_DELIMITER = "__";

    AbstractManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                    DBAdaptorFactory catalogDBAdaptorFactory, Configuration configuration) {
        this.authorizationManager = authorizationManager;
        this.auditManager = auditManager;
        this.configuration = configuration;
        this.catalogDBAdaptorFactory = catalogDBAdaptorFactory;
        this.catalogManager = catalogManager;

        logger = LoggerFactory.getLogger(this.getClass());
    }

    protected DBAdaptorFactory getCatalogDBAdaptorFactory() {
        return catalogDBAdaptorFactory;
    }

    protected MigrationDBAdaptor getMigrationDBAdaptor(String organization) throws CatalogDBException {
        return getCatalogDBAdaptorFactory().getMigrationDBAdaptor(organization);
    }

    protected MetaDBAdaptor getCatalogMetaDBAdaptor(String organization) throws CatalogDBException {
        return getCatalogDBAdaptorFactory().getCatalogMetaDBAdaptor(organization);
    }

    protected OrganizationDBAdaptor getOrganizationDBAdaptor(String organization) throws CatalogDBException {
        return getCatalogDBAdaptorFactory().getCatalogOrganizationDBAdaptor(organization);
    }

    protected UserDBAdaptor getUserDBAdaptor(String organization) throws CatalogDBException {
        return getCatalogDBAdaptorFactory().getCatalogUserDBAdaptor(organization);
    }

    protected ProjectDBAdaptor getProjectDBAdaptor(String organization) throws CatalogDBException {
        return getCatalogDBAdaptorFactory().getCatalogProjectDbAdaptor(organization);
    }

    protected StudyDBAdaptor getStudyDBAdaptor(String organization) throws CatalogDBException {
        return getCatalogDBAdaptorFactory().getCatalogStudyDBAdaptor(organization);
    }

    protected FileDBAdaptor getFileDBAdaptor(String organization) throws CatalogDBException {
        return getCatalogDBAdaptorFactory().getCatalogFileDBAdaptor(organization);
    }

    protected SampleDBAdaptor getSampleDBAdaptor(String organization) throws CatalogDBException {
        return getCatalogDBAdaptorFactory().getCatalogSampleDBAdaptor(organization);
    }

    protected IndividualDBAdaptor getIndividualDBAdaptor(String organization) throws CatalogDBException {
        return getCatalogDBAdaptorFactory().getCatalogIndividualDBAdaptor(organization);
    }

    protected JobDBAdaptor getJobDBAdaptor(String organization) throws CatalogDBException {
        return getCatalogDBAdaptorFactory().getCatalogJobDBAdaptor(organization);
    }

    protected AuditDBAdaptor getAuditDbAdaptor(String organization) throws CatalogDBException {
        return getCatalogDBAdaptorFactory().getCatalogAuditDbAdaptor(organization);
    }

    protected CohortDBAdaptor getCohortDBAdaptor(String organization) throws CatalogDBException {
        return getCatalogDBAdaptorFactory().getCatalogCohortDBAdaptor(organization);
    }

    protected PanelDBAdaptor getPanelDBAdaptor(String organization) throws CatalogDBException {
        return getCatalogDBAdaptorFactory().getCatalogPanelDBAdaptor(organization);
    }

    protected FamilyDBAdaptor getFamilyDBAdaptor(String organization) throws CatalogDBException {
        return getCatalogDBAdaptorFactory().getCatalogFamilyDBAdaptor(organization);
    }

    protected ClinicalAnalysisDBAdaptor getClinicalAnalysisDBAdaptor(String organization) throws CatalogDBException {
        return getCatalogDBAdaptorFactory().getClinicalAnalysisDBAdaptor(organization);
    }

    protected InterpretationDBAdaptor getInterpretationDBAdaptor(String organization) throws CatalogDBException {
        return getCatalogDBAdaptorFactory().getInterpretationDBAdaptor(organization);
    }

    protected void fixQueryObject(Query query) {
        changeQueryId(query, ParamConstants.INTERNAL_STATUS_PARAM, "internal.status");
    }

    /**
     * Change key used in query object.
     * This method is called internally by the managers to change the keys used by users to query the data for the ones the corresponding
     * DBAdaptors will understand.
     *
     * @param query      Query object.
     * @param currentKey Public field offered to users to query.
     * @param newKey     Internal field that needs to be replaced with.
     */
    protected void changeQueryId(Query query, String currentKey, String newKey) {
        if (query != null && query.containsKey(currentKey)) {
            Object value = query.get(currentKey);
            query.remove(currentKey);
            query.put(newKey, value);
        }
    }


    /**
     * Return the results in the OpenCGAResult object in the same order they were queried by the list of entries.
     * For entities with version where all versions have been requested, call to InternalGetDataResult.getVersionedResults() to get
     * a list of lists of T.
     *
     * @param entries         Original list used to perform the query.
     * @param getId           Generic function that will fetch the id that will be used to compare with the list of entries.
     * @param queryResult     OpenCGAResult object.
     * @param silent          Boolean indicating whether we will fail in case of an inconsistency or not.
     * @param keepAllVersions Boolean indicating whether to keep all versions of fail in case of id duplicities.
     * @param <T>             Generic entry (Sample, File, Cohort...)
     * @return the OpenCGAResult with the proper order of results.
     * @throws CatalogException In case of inconsistencies found.
     */
    <T extends IPrivateStudyUid> InternalGetDataResult<T> keepOriginalOrder(List<String> entries, Function<T, String> getId,
                                                                            OpenCGAResult<T> queryResult, boolean silent,
                                                                            boolean keepAllVersions) throws CatalogException {
        InternalGetDataResult<T> internalGetDataResult = new InternalGetDataResult<>(queryResult);

        Map<String, List<T>> resultMap = new HashMap<>();

        for (T entry : internalGetDataResult.getResults()) {
            String id = getId.apply(entry);
            if (!resultMap.containsKey(id)) {
                resultMap.put(id, new ArrayList<>());
            } else if (!keepAllVersions) {
                throw new CatalogException("Duplicated entry " + id + " found");
            }
            resultMap.get(id).add(entry);
        }

        List<T> orderedEntryList = new ArrayList<>(internalGetDataResult.getNumResults());
        List<Integer> groups = new ArrayList<>(entries.size());
        for (String entry : entries) {
            if (resultMap.containsKey(entry)) {
                orderedEntryList.addAll(resultMap.get(entry));
                groups.add(resultMap.get(entry).size());
            } else {
                if (!silent) {
                    throw new CatalogException("Entry " + entry + " not found in OpenCGAResult");
                }
                groups.add(0);
                internalGetDataResult.addMissing(entry, "Not found or user does not have permissions.");
            }
        }

        internalGetDataResult.setResults(orderedEntryList);
        internalGetDataResult.setGroups(groups);
        return internalGetDataResult;
    }

    /**
     * This method will make sure that 'field' is included in case there is a INCLUDE or never excluded in case there is a EXCLUDE list.
     *
     * @param options QueryOptions object.
     * @param field   field that needs to remain.
     * @return a new QueryOptions with the necessary modifications.
     */
    static QueryOptions keepFieldInQueryOptions(QueryOptions options, String field) {
        return keepFieldsInQueryOptions(options, Collections.singletonList(field));
    }

    /**
     * This method will make sure that 'field' is included in case there is a INCLUDE or never excluded in case there is a EXCLUDE list.
     *
     * @param options QueryOptions object.
     * @param fields  fields that need to remain.
     * @return a new QueryOptions with the necessary modifications.
     */
    static QueryOptions keepFieldsInQueryOptions(QueryOptions options, List<String> fields) {
        if (options.isEmpty() || CollectionUtils.isEmpty(fields)) {
            // Everything will be included, so we don't need to do anything
            return options;
        }

        QueryOptions queryOptions = new QueryOptions(options);
        Set<String> includeSet = new HashSet<>(queryOptions.getAsStringList(QueryOptions.INCLUDE));
        if (!includeSet.isEmpty()) {
            // We need to add the fields
            includeSet.addAll(fields);
            queryOptions.put(QueryOptions.INCLUDE, new ArrayList<>(includeSet));
        }

        Set<String> excludeSet = new HashSet<>(queryOptions.getAsStringList(QueryOptions.EXCLUDE));
        if (!excludeSet.isEmpty()) {
            fields.forEach(excludeSet::remove);
            queryOptions.put(QueryOptions.EXCLUDE, new ArrayList<>(excludeSet));
        }

        return queryOptions;
    }

    /**
     * Obtains a list containing the entries that are in {@code originalEntries} that are not in {@code finalEntries}.
     *
     * @param originalEntries Original list that will be used to compare against.
     * @param finalEntries    List of {@code T} that will be compared against the {@code originalEntries}.
     * @param getId           Generic function to get the string used to make the comparison.
     * @param <T>             Generic entry (Sample, File, Cohort...)
     * @return a list containing the entries that are in {@code originalEntries} that are not in {@code finalEntries}.
     */
    <T extends IPrivateStudyUid> List<String> getMissingFields(List<String> originalEntries, List<T> finalEntries,
                                                               Function<T, String> getId) {
        Set<String> entrySet = new HashSet<>();
        for (T finalEntry : finalEntries) {
            entrySet.add(getId.apply(finalEntry));
        }

        List<String> differences = new ArrayList<>();
        for (String originalEntry : originalEntries) {
            if (!entrySet.contains(originalEntry)) {
                differences.add(originalEntry);
            }
        }

        return differences;
    }

    /**
     * Checks if the list of members are all valid.
     * <p>
     * The "members" can be:
     * - '*' referring to all the users.
     * - 'anonymous' referring to the anonymous user.
     * - '@{groupId}' referring to a {@link Group}.
     * - '{userId}' referring to a specific user.
     *
     * @param organization organization
     * @param studyId studyId
     * @param members List of members
     * @throws CatalogDBException            CatalogDBException
     * @throws CatalogParameterException     if there is any formatting error.
     * @throws CatalogAuthorizationException if the user is not authorised to perform the query.
     */
    protected void checkMembers(String organization, long studyId, List<String> members)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        for (String member : members) {
            checkMember(organization, studyId, member);
        }
    }

    /**
     * Checks if the member is valid.
     * <p>
     * The "member" can be:
     * - '*' referring to all the users.
     * - '@{groupId}' referring to a {@link Group}.
     * - '{userId}' referring to a specific user.
     *
     * @param organization organization
     * @param studyId studyId
     * @param member  member
     * @throws CatalogDBException            CatalogDBException
     * @throws CatalogParameterException     if there is any formatting error.
     * @throws CatalogAuthorizationException if the user is not authorised to perform the query.
     */
    protected void checkMember(String organization, long studyId, String member)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        if (member.equals("*")) {
            return;
        } else if (member.startsWith("@")) {
            OpenCGAResult<Group> queryResult = getStudyDBAdaptor(organization).getGroup(studyId, member, Collections.emptyList());
            if (queryResult.getNumResults() == 0) {
                throw CatalogDBException.idNotFound("Group", member);
            }
        } else {
            getUserDBAdaptor(organization).checkId(member);
        }
    }

    public interface ExecuteOperation<T> {
        OpenCGAResult<T> execute(String organizationId, Study study, String userId, JwtPayload tokenPayload) throws CatalogException;
    }
//
//    public interface ExecuteBatchOperation<T> {
//        T execute(Study study, String userId, QueryOptions queryOptions, String auditOperationUuid) throws CatalogException, IOException;
//    }
//
//    protected <T> OpenCGAResult<T> run(ObjectMap params, Enums.Action action, Enums.Resource resource, String studyStr, String token,
//                                       QueryOptions options, ExecuteOperation<T> body)
//            throws CatalogException {
//        return run(params, action, resource, studyStr, token, options, QueryOptions.empty(), body);
//    }
//
//    protected <T> OpenCGAResult<T> run(ObjectMap params, Enums.Action action, Enums.Resource resource, String studyStr, String token,
//                                             QueryOptions options, QueryOptions studyIncludeList, ExecuteOperation<T> body)
//            throws CatalogException {
//        return run(params, resource, action, studyStr, null, token, options, studyIncludeList, body);
//    }

    protected <T> OpenCGAResult<T> run(ObjectMap params, Enums.Resource resource, Enums.Action action, String studyStr,
                                       @Nullable EntryParam entryParam, String token, QueryOptions studyIncludeList,
                                       ExecuteOperation<T> body) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);

        Supplier<Study> studySupplier = () -> {
            try {
                return catalogManager.getStudyManager().resolveId(studyFqn, studyIncludeList, tokenPayload);
            } catch (CatalogException e) {
                throw new CatalogRuntimeException(e);
            }
        };

        return notify(resource, action, organizationId, studySupplier, entryParam, userId, params, tokenPayload, body);
    }

    private <T> OpenCGAResult<T> notify(Enums.Resource resource, Enums.Action action, String organizationId, Supplier<Study> studySupplier,
                                        @Nullable EntryParam entryParam, String userId, ObjectMap params, JwtPayload payload,
                                        ExecuteOperation<T> executeOperation) throws CatalogException {
        String eventId = resource.name().toLowerCase() + "." + action.name().toLowerCase();

        String resourceId = entryParam != null ? entryParam.getId() : "";
        String resourceUuid = entryParam != null ? entryParam.getUuid() : "";
        OpencgaEvent opencgaEvent = OpencgaEvent.build(eventId, params, organizationId, resourceId, resourceUuid, userId,
                payload.getToken());
        CatalogEvent catalogEvent = CatalogEvent.build(opencgaEvent);
        try {
            // Get study
            Study study = studySupplier.get();
            opencgaEvent.setStudyFqn(study.getFqn());
            opencgaEvent.setStudyUuid(study.getUuid());

            // Execute code
            OpenCGAResult<T> execute = executeOperation.execute(organizationId, study, userId, payload);
            if (entryParam != null) {
                // In case the id and uuid have been populated after the execution
                opencgaEvent.setResourceId(entryParam.getId());
                opencgaEvent.setResourceUuid(entryParam.getUuid());
            }

            // Notify event
            EventManager.getInstance().notify(catalogEvent);

            return execute;
        } catch (Exception e) {
            EventManager.getInstance().notify(catalogEvent, e);
            throw e;
        }
    }

//    protected <T, S extends ObjectMap> T run(ObjectMap params, Enums.Action action, Enums.Resource resource, String operationUuid,
//                                             Study study, String userId, S options, ExecuteOperation<T> body) throws CatalogException {
//        StopWatch totalStopWatch = StopWatch.createStarted();
//        Exception exception = null;
//        ReferenceParam referenceParam = new ReferenceParam();
//        try {
//            QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
//            return body.execute(study, userId, referenceParam, queryOptions);
//        } catch (Exception e) {
//            exception = e;
//            throw e;
//        } finally {
//            try {
//                String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);
//                AuditRecord auditRecord = new AuditRecord(operationId, operationUuid, userId, GitRepositoryState.get().getBuildVersion(),
//                        action, resource, referenceParam.getId(), referenceParam.getUuid(), study.getId(), study.getUuid(), params,
//                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS), TimeUtils.getDate(),
//                        new ObjectMap("totalTimeMillis", totalStopWatch.getTime(TimeUnit.MILLISECONDS)));
//                if (exception != null) {
//                    auditRecord.setStatus(new AuditRecord.Status(AuditRecord.Status.Result.ERROR, new Error(1, exception.getMessage(),
//                            "")));
//                    auditRecord.getAttributes()
//                            .append("errorType", exception.getClass())
//                            .append("errorMessage", exception.getMessage());
//                }
//                auditManager.audit(auditRecord);
//            } catch (Exception e2) {
//                if (exception != null) {
//                    exception.addSuppressed(e2);
//                } else {
//                    throw e2;
//                }
//            }
//        }
//    }
//
//    protected <T, S extends ObjectMap> T run(ObjectMap params, Enums.Action action, Enums.Resource resource, String operationUuid,
//                                             Study study, String userId, S options, ExecuteOperation<T> body) throws CatalogException {
//        StopWatch totalStopWatch = StopWatch.createStarted();
//        Exception exception = null;
//        ReferenceParam referenceParam = new ReferenceParam();
//
//        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
//        Supplier<T> supplier = () -> {
//            try {
//                return body.execute(study, userId, referenceParam, queryOptions);
//            } catch (CatalogException e) {
//                throw new CatalogRuntimeException(e);
//            }
//        };
//
//
//
//        return body.execute(study, userId, referenceParam, queryOptions);
//
//        try {
//            QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
//            return body.execute(study, userId, referenceParam, queryOptions);
//        } catch (Exception e) {
//            exception = e;
//            throw e;
//        } finally {
//            try {
//                String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);
//                AuditRecord auditRecord = new AuditRecord(operationId, operationUuid, userId, GitRepositoryState.get().getBuildVersion(),
//                        action, resource, referenceParam.getId(), referenceParam.getUuid(), study.getId(), study.getUuid(), params,
//                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS), TimeUtils.getDate(),
//                        new ObjectMap("totalTimeMillis", totalStopWatch.getTime(TimeUnit.MILLISECONDS)));
//                if (exception != null) {
//                    auditRecord.setStatus(new AuditRecord.Status(AuditRecord.Status.Result.ERROR, new Error(1, exception.getMessage(),
//                            "")));
//                    auditRecord.getAttributes()
//                            .append("errorType", exception.getClass())
//                            .append("errorMessage", exception.getMessage());
//                }
//                auditManager.audit(auditRecord);
//            } catch (Exception e2) {
//                if (exception != null) {
//                    exception.addSuppressed(e2);
//                } else {
//                    throw e2;
//                }
//            }
//        }
//    }
//
//
//    protected <T> T runBatch(ObjectMap params, Enums.Action action, Enums.Resource resource, String studyStr, String token,
//                             QueryOptions options, ExecuteBatchOperation<T> body) throws CatalogException {
//        StopWatch totalStopWatch = StopWatch.createStarted();
//        String userId = catalogManager.getUserManager().getUserId(token);
//        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId, StudyManager.INCLUDE_BASE);
//        String operationUuid = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);
//        auditManager.initAuditBatch(operationUuid);
//        Exception exception = null;
//        try {
//            QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
//            return body.execute(study, userId, queryOptions, operationUuid);
//        } catch (IOException e) {
//            exception = new CatalogException(e);
//            ObjectMap auditAttributes = new ObjectMap()
//                    .append("totalTimeMillis", totalStopWatch.getTime(TimeUnit.MILLISECONDS))
//                    .append("errorType", e.getClass())
//                    .append("errorMessage", e.getMessage());
//            AuditRecord.Status status = new AuditRecord.Status(AuditRecord.Status.Result.ERROR, new Error(0, "", e.getMessage()));
//            AuditRecord auditRecord = new AuditRecord(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT), operationUuid, userId,
//                    GitRepositoryState.get().getBuildVersion(), action, resource, "", "", study.getId(), study.getUuid(), params,
//                    status, TimeUtils.getDate(), auditAttributes);
//            auditManager.audit(auditRecord);
//            throw (CatalogException) exception;
//        } catch (Exception e) {
//            exception = e;
//            ObjectMap auditAttributes = new ObjectMap()
//                    .append("totalTimeMillis", totalStopWatch.getTime(TimeUnit.MILLISECONDS))
//                    .append("errorType", e.getClass())
//                    .append("errorMessage", e.getMessage());
//            AuditRecord.Status status = new AuditRecord.Status(AuditRecord.Status.Result.ERROR, new Error(0, "", e.getMessage()));
//            AuditRecord auditRecord = new AuditRecord(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT), operationUuid, userId,
//                    GitRepositoryState.get().getBuildVersion(), action, resource, "", "", study.getId(), study.getUuid(), params,
//                    status, TimeUtils.getDate(), auditAttributes);
//            auditManager.audit(auditRecord);
//            throw e;
//        } finally {
//            try {
//                auditManager.finishAuditBatch(operationUuid);
//            } catch (Exception e2) {
//                if (exception != null) {
//                    exception.addSuppressed(e2);
//                } else {
//                    throw e2;
//                }
//            }
//        }
//    }

}
