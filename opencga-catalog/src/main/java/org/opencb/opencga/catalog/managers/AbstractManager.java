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
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.commons.datastore.core.result.Error;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.models.InternalGetDataResult;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.AuthenticationOrigin;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.IPrivateStudyUid;
import org.opencb.opencga.core.models.audit.AuditRecord;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.ReferenceParam;
import org.opencb.opencga.core.models.study.Group;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Created by hpccoll1 on 12/05/15.
 */
public abstract class AbstractManager {

    protected final Logger logger;
    protected final AuthorizationManager authorizationManager;
    private final AuditManager auditManager;
    protected final CatalogManager catalogManager;

    protected Configuration configuration;

    protected final DBAdaptorFactory catalogDBAdaptorFactory;
    protected final UserDBAdaptor userDBAdaptor;
    protected final ProjectDBAdaptor projectDBAdaptor;
    protected final StudyDBAdaptor studyDBAdaptor;
    protected final FileDBAdaptor fileDBAdaptor;
    protected final IndividualDBAdaptor individualDBAdaptor;
    protected final SampleDBAdaptor sampleDBAdaptor;
    protected final CohortDBAdaptor cohortDBAdaptor;
    protected final FamilyDBAdaptor familyDBAdaptor;
    protected final JobDBAdaptor jobDBAdaptor;
    protected final PanelDBAdaptor panelDBAdaptor;
    protected final ClinicalAnalysisDBAdaptor clinicalDBAdaptor;
    protected final InterpretationDBAdaptor interpretationDBAdaptor;

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
        this.userDBAdaptor = catalogDBAdaptorFactory.getCatalogUserDBAdaptor();
        this.studyDBAdaptor = catalogDBAdaptorFactory.getCatalogStudyDBAdaptor();
        this.fileDBAdaptor = catalogDBAdaptorFactory.getCatalogFileDBAdaptor();
        this.individualDBAdaptor = catalogDBAdaptorFactory.getCatalogIndividualDBAdaptor();
        this.sampleDBAdaptor = catalogDBAdaptorFactory.getCatalogSampleDBAdaptor();
        this.jobDBAdaptor = catalogDBAdaptorFactory.getCatalogJobDBAdaptor();
        this.cohortDBAdaptor = catalogDBAdaptorFactory.getCatalogCohortDBAdaptor();
        this.familyDBAdaptor = catalogDBAdaptorFactory.getCatalogFamilyDBAdaptor();
        this.panelDBAdaptor = catalogDBAdaptorFactory.getCatalogPanelDBAdaptor();
        this.clinicalDBAdaptor = catalogDBAdaptorFactory.getClinicalAnalysisDBAdaptor();
        this.interpretationDBAdaptor = catalogDBAdaptorFactory.getInterpretationDBAdaptor();
        this.catalogDBAdaptorFactory = catalogDBAdaptorFactory;
        this.catalogManager = catalogManager;

        projectDBAdaptor = catalogDBAdaptorFactory.getCatalogProjectDbAdaptor();

        logger = LoggerFactory.getLogger(this.getClass());
    }

    public interface ExecuteOperation<T> {
        T execute(Study study, String userId, ReferenceParam rp, QueryOptions queryOptions) throws CatalogException;
    }

    public interface ExecuteBatchOperation<T> {
        T execute(Study study, String userId, QueryOptions queryOptions, String auditOperationUuid) throws CatalogException, IOException;
    }

    protected <T, S extends ObjectMap> T run(ObjectMap params, Enums.Action action, Enums.Resource resource, String studyStr, String token,
                                             S options, ExecuteOperation<T> body) throws CatalogException {
        return run(params, action, resource, studyStr, token, options, Collections.emptyList(), body);
    }

    protected <T, S extends ObjectMap> T run(ObjectMap params, Enums.Action action, Enums.Resource resource, String studyStr, String token,
                                             S options, List<String> studyIncludeList, ExecuteOperation<T> body) throws CatalogException {
        String userId = null;
        if (StringUtils.isNotEmpty(token)) {
            userId = catalogManager.getUserManager().getUserId(token);
        }
        Study study = null;
        if (StringUtils.isNotEmpty(studyStr)) {
            QueryOptions studyOptions = StudyManager.INCLUDE_BASE;
            if (CollectionUtils.isNotEmpty(studyIncludeList)) {
                studyOptions = keepFieldsInQueryOptions(studyOptions, studyIncludeList);
            }
            study = catalogManager.getStudyManager().resolveId(studyStr, userId, studyOptions);
        }
        String operationUuid = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);
        return run(params, action, resource, operationUuid, study, userId, options, body);
    }

    protected <T, S extends ObjectMap> T run(ObjectMap params, Enums.Action action, Enums.Resource resource, String operationUuid,
                                             Study study, String userId, S options, ExecuteOperation<T> body) throws CatalogException {
        StopWatch totalStopWatch = StopWatch.createStarted();
        Exception exception = null;
        ReferenceParam referenceParam = new ReferenceParam();
        try {
            QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
            return body.execute(study, userId, referenceParam, queryOptions);
        } catch (Exception e) {
            exception = e;
            throw e;
        } finally {
            try {
                String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);
                AuditRecord auditRecord = new AuditRecord(operationId, operationUuid, userId, GitRepositoryState.get().getBuildVersion(),
                        action, resource, referenceParam.getId(), referenceParam.getUuid(), study.getId(), study.getUuid(), params,
                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS), TimeUtils.getDate(),
                        new ObjectMap("totalTimeMillis", totalStopWatch.getTime(TimeUnit.MILLISECONDS)));
                if (exception != null) {
                    auditRecord.setStatus(new AuditRecord.Status(AuditRecord.Status.Result.ERROR, new Error(1, exception.getMessage(),
                            "")));
                    auditRecord.getAttributes()
                            .append("errorType", exception.getClass())
                            .append("errorMessage", exception.getMessage());
                }
                auditManager.audit(auditRecord);
            } catch (Exception e2) {
                if (exception != null) {
                    exception.addSuppressed(e2);
                } else {
                    throw e2;
                }
            }
        }
    }

    protected <T> T runBatch(ObjectMap params, Enums.Action action, Enums.Resource resource, String studyStr, String token,
                             QueryOptions options, ExecuteBatchOperation<T> body) throws CatalogException {
        StopWatch totalStopWatch = StopWatch.createStarted();
        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId, StudyManager.INCLUDE_BASE);
        String operationUuid = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);
        auditManager.initAuditBatch(operationUuid);
        Exception exception = null;
        try {
            QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
            return body.execute(study, userId, queryOptions, operationUuid);
        } catch (IOException e) {
            exception = new CatalogException(e);
            ObjectMap auditAttributes = new ObjectMap()
                    .append("totalTimeMillis", totalStopWatch.getTime(TimeUnit.MILLISECONDS))
                    .append("errorType", e.getClass())
                    .append("errorMessage", e.getMessage());
            AuditRecord.Status status = new AuditRecord.Status(AuditRecord.Status.Result.ERROR, new Error(0, "", e.getMessage()));
            AuditRecord auditRecord = new AuditRecord(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT), operationUuid, userId,
                    GitRepositoryState.get().getBuildVersion(), action, resource, "", "", study.getId(), study.getUuid(), params,
                    status, TimeUtils.getDate(), auditAttributes);
            auditManager.audit(auditRecord);
            throw (CatalogException) exception;
        } catch (Exception e) {
            exception = e;
            ObjectMap auditAttributes = new ObjectMap()
                    .append("totalTimeMillis", totalStopWatch.getTime(TimeUnit.MILLISECONDS))
                    .append("errorType", e.getClass())
                    .append("errorMessage", e.getMessage());
            AuditRecord.Status status = new AuditRecord.Status(AuditRecord.Status.Result.ERROR, new Error(0, "", e.getMessage()));
            AuditRecord auditRecord = new AuditRecord(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT), operationUuid, userId,
                    GitRepositoryState.get().getBuildVersion(), action, resource, "", "", study.getId(), study.getUuid(), params,
                    status, TimeUtils.getDate(), auditAttributes);
            auditManager.audit(auditRecord);
            throw e;
        } finally {
            try {
                auditManager.finishAuditBatch(operationUuid);
            } catch (Exception e2) {
                if (exception != null) {
                    exception.addSuppressed(e2);
                } else {
                    throw e2;
                }
            }
        }
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
     * Prior to the conversion to a numerical featureId, there is a need to know in which user/project/study look for the string.
     * This method calculates those parameters to know how to obtain the numerical id.
     *
     * @param userId     User id of the user asking for the id. If no user is found in featureStr, we will assume that it is asking for its
     *                   projects/studies...
     * @param featureStr Feature id in string format. Could be one of [user@aliasProject:aliasStudy:XXXXX
     *                   | user@aliasStudy:XXXXX | aliasStudy:XXXXX | XXXXX].
     * @return an objectMap with the following possible keys: "user", "project", "study", "featureName"
     */
    protected ObjectMap parseFeatureId(String userId, String featureStr) {
        ObjectMap result = new ObjectMap("user", userId);

        String[] split = featureStr.split("@");
        if (split.length == 2) { // user@project:study
            result.put("user", split[0]);
            featureStr = split[1];
        }

        split = featureStr.split(":", 3);
        if (split.length == 2) {
            result.put("study", split[0]);
            result.put("featureName", split[1]);
        } else if (split.length == 3) {
            result.put("project", split[0]);
            result.put("study", split[1]);
            result.put("featureName", split[2]);
        } else {
            result.put("featureName", featureStr);
        }
        return result;
    }

    AuthenticationOrigin getAuthenticationOrigin(String authOrigin) {
        if (configuration.getAuthentication().getAuthenticationOrigins() != null) {
            for (AuthenticationOrigin authenticationOrigin : configuration.getAuthentication().getAuthenticationOrigins()) {
                if (authOrigin.equals(authenticationOrigin.getId())) {
                    return authenticationOrigin;
                }
            }
        }
        return null;
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
     * @param studyId studyId
     * @param members List of members
     * @throws CatalogDBException            CatalogDBException
     * @throws CatalogParameterException     if there is any formatting error.
     * @throws CatalogAuthorizationException if the user is not authorised to perform the query.
     */
    protected void checkMembers(long studyId, List<String> members)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        for (String member : members) {
            checkMember(studyId, member);
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
     * @param studyId studyId
     * @param member  member
     * @throws CatalogDBException            CatalogDBException
     * @throws CatalogParameterException     if there is any formatting error.
     * @throws CatalogAuthorizationException if the user is not authorised to perform the query.
     */
    protected void checkMember(long studyId, String member)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        if (member.equals("*")) {
            return;
        } else if (member.startsWith("@")) {
            OpenCGAResult<Group> queryResult = studyDBAdaptor.getGroup(studyId, member, Collections.emptyList());
            if (queryResult.getNumResults() == 0) {
                throw CatalogDBException.idNotFound("Group", member);
            }
        } else {
            userDBAdaptor.checkId(member);
        }
    }

    protected static Class<?> getTypeClass(QueryParam.Type type) {
        switch (type) {
            case STRING:
            case TEXT:
            case TEXT_ARRAY:
                return String.class;
            case INTEGER_ARRAY:
            case INTEGER:
                return Integer.class;
            case DOUBLE:
            case DECIMAL:
            case DECIMAL_ARRAY:
                return Double.class;
            case LONG:
            case LONG_ARRAY:
                return Long.class;
            case BOOLEAN:
            case BOOLEAN_ARRAY:
                return Boolean.class;
            case MAP:
                return Map.class;
            case OBJECT:
                return Object.class;
            case DATE:
            case TIMESTAMP:
                return Date.class;
            default:
                throw new IllegalArgumentException("Unknown or unrecognised type '" + type + "'");
        }
    }

    @Deprecated
    public static class MyResourceId {
        private String user;
        private long studyId;
        private long resourceId;

        public MyResourceId() {
        }

        public MyResourceId(String user, long studyId, long resourceId) {
            this.user = user;
            this.studyId = studyId;
            this.resourceId = resourceId;
        }

        public String getUser() {
            return user;
        }

        public MyResourceId setUser(String user) {
            this.user = user;
            return this;
        }

        public long getStudyId() {
            return studyId;
        }

        public MyResourceId setStudyId(long studyId) {
            this.studyId = studyId;
            return this;
        }

        public long getResourceId() {
            return resourceId;
        }

        public MyResourceId setResourceId(long resourceId) {
            this.resourceId = resourceId;
            return this;
        }
    }

    @Deprecated
    public static class MyResourceIds {
        private String user;
        private long studyId;
        private List<Long> resourceIds;

        public MyResourceIds() {
        }

        public MyResourceIds(String user, long studyId, List<Long> resourceIds) {
            this.user = user;
            this.studyId = studyId;
            this.resourceIds = resourceIds;
        }

        public String getUser() {
            return user;
        }

        public MyResourceIds setUser(String user) {
            this.user = user;
            return this;
        }

        public long getStudyId() {
            return studyId;
        }

        public MyResourceIds setStudyId(long studyId) {
            this.studyId = studyId;
            return this;
        }

        public List<Long> getResourceIds() {
            return resourceIds;
        }

        public MyResourceIds setResourceIds(List<Long> resourceIds) {
            this.resourceIds = resourceIds;
            return this;
        }
    }
}
