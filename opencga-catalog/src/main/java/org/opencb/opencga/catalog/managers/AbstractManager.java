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
import org.opencb.commons.datastore.core.*;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.models.InternalGetDataResult;
import org.opencb.opencga.catalog.utils.CatalogFqn;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.events.OpencgaEvent;
import org.opencb.opencga.core.models.IPrivateFields;
import org.opencb.opencga.core.models.JwtPayload;
import org.opencb.opencga.core.models.common.EntryParam;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.event.CatalogEvent;
import org.opencb.opencga.core.models.study.Group;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;

/**
 * Created by hpccoll1 on 12/05/15.
 */
public abstract class AbstractManager<R extends IPrivateFields> {

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

    AbstractManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                    DBAdaptorFactory catalogDBAdaptorFactory, Configuration configuration) {
        this.authorizationManager = authorizationManager;
        this.auditManager = auditManager;
        this.configuration = configuration;
        this.catalogDBAdaptorFactory = catalogDBAdaptorFactory;
        this.catalogManager = catalogManager;

        logger = LoggerFactory.getLogger(this.getClass());
    }

    abstract Enums.Resource getResource();

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

    protected WorkflowDBAdaptor getWorkflowDBAdaptor(String organization) throws CatalogDBException {
        return catalogDBAdaptorFactory.getWorkflowDBAdaptor(organization);
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
    <T> InternalGetDataResult<T> keepOriginalOrder(List<String> entries, Function<T, String> getId, OpenCGAResult<T> queryResult,
                                                   boolean silent, boolean keepAllVersions) throws CatalogException {
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
    <T> List<String> getMissingFields(List<String> originalEntries, List<T> finalEntries, Function<T, String> getId) {
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

    protected void checkIsNotAFederatedUser(String organizationId, List<String> users) throws CatalogException {
        if (CollectionUtils.isNotEmpty(users)) {
            Query query = new Query(UserDBAdaptor.QueryParams.ID.key(), users);
            OpenCGAResult<User> result = catalogDBAdaptorFactory.getCatalogUserDBAdaptor(organizationId).get(query,
                    UserManager.INCLUDE_INTERNAL);
            if (result.getNumResults() != users.size()) {
                throw new CatalogException("Some users were not found.");
            }
            for (User user : result.getResults()) {
                ParamUtils.checkObj(user.getInternal(), "internal");
                ParamUtils.checkObj(user.getInternal().getAccount(), "internal.account");
                ParamUtils.checkObj(user.getInternal().getAccount().getAuthentication(), "internal.account.authentication");
                if (user.getInternal().getAccount().getAuthentication().isFederation()) {
                    throw new CatalogException("User '" + user.getId() + "' is a federated user.");
                }
            }
        }
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

    /**
     * Returns result if there are no ERROR events or ignoreException is true. Otherwise, raise an exception with all error messages.
     *
     * @param result Final OpenCGA result.
     * @param ignoreException Boolean indicating whether to raise an exception in case of an ERROR event.
     * @return result if ignoreException is true or there are no ERROR events.
     * @throws CatalogException if ignoreException is false and there are ERROR events.
     */
    public OpenCGAResult<R> endResult(OpenCGAResult<R> result, boolean ignoreException) throws CatalogException {
        if (!ignoreException) {
            if (CollectionUtils.isNotEmpty(result.getEvents())) {
                List<String> errors = new ArrayList<>();
                for (Event event : result.getEvents()) {
                    if (event.getType() == Event.Type.ERROR) {
                        errors.add(event.getMessage());
                    }
                }
                if (!errors.isEmpty()) {
                    throw new CatalogException(StringUtils.join(errors, "\n"));
                }
            }
        }
        return result;
    }

    /** Centralised code to handle all operations **/

    /**
     * Interface to execute any checks that could be made before processing the element.
     */
    public interface BeforeCheckOperation {
        void execute(String organizationId, Study study, String userId) throws CatalogException;
    }

    /**
     * Interface to execute an operation over a single element.
     * @param <R> Type of the element that will be processed.
     */
    public interface ExecuteOperationForSingleEntry<R> {
        OpenCGAResult<R> execute(String organizationId, Study study, String userId, EntryParam entryParam) throws CatalogException;
    }

    /**
     * Interface to execute anything when an error happens.
     */
    public interface ExecuteOnError {
        void execute(String organizationId, Study study, String userId, EntryParam entryParam, Exception e) throws CatalogException;
    }

    /**
     * Interface to execute an operation over a single element.
     */
    public interface ExecuteGenericOperation<T> {
        T execute(String organizationId, Study study, String userId, EntryParam entryParam) throws CatalogException;
    }

    /**
     * Interface to obtain an iterator of the elements that will be processed.
     * @param <R> Type of the elements that will be processed.
     */
    public interface ExecuteOperationIterator<R> {
        DBIterator<R> iterator(String organizationId, Study study, String userId) throws CatalogException;
    }

    /**
     * Interface to execute a batch operation over the elements. This always goes together with the {@link ExecuteOperationIterator}.
     * The {@link ExecuteOperationIterator} will provide the elements to be processed and this interface will execute the operation over
     * each of the elements.
     * @param <R> Type of the elements that will be processed.
     */
    public interface ExecuteBatchOperation<R> {
        OpenCGAResult<R> execute(String organizationId, Study study, String userId, R entry) throws CatalogException;
    }

    protected OpenCGAResult<R> create(String studyStr, Object object, QueryOptions options, String token, QueryOptions studyIncludeList,
                                      ExecuteOperationForSingleEntry<R> execution) throws CatalogException {
        return create(new ObjectMap(), studyStr, object, options, token, studyIncludeList, execution);
    }

    protected OpenCGAResult<R> create(ObjectMap params, String studyStr, Object object, QueryOptions options, String token,
                                      QueryOptions studyIncludeList, ExecuteOperationForSingleEntry<R> execution)
            throws CatalogException {
        params = params != null ? params : new ObjectMap();
        params.putIfAbsent("study", studyStr);
        params.putIfAbsent("object", object);
        params.putIfAbsent("options", options);
        params.putIfAbsent("token", token);

        return runForSingleEntry(params, Enums.Action.CREATE, studyStr, token, studyIncludeList, execution, null);
    }

    protected OpenCGAResult<R> update(String studyStr, String id, Object updateParams, QueryOptions options, String token,
                                      QueryOptions studyIncludeList, ExecuteOperationForSingleEntry<R> execution)
            throws CatalogException {
        return update(new ObjectMap(), studyStr, id, updateParams, options, token, studyIncludeList, execution);
    }

    protected OpenCGAResult<R> update(ObjectMap params, String studyStr, String id, Object updateParams, QueryOptions options, String token,
                                      QueryOptions studyIncludeList, ExecuteOperationForSingleEntry<R> execution)
            throws CatalogException {
        params = params != null ? params : new ObjectMap();
        params.putIfAbsent("study", studyStr);
        params.putIfAbsent("id", id);
        params.putIfAbsent("updateParams", updateParams);
        params.putIfAbsent("options", options);
        params.putIfAbsent("token", token);
        return runForSingleEntry(params, Enums.Action.UPDATE, studyStr, token, studyIncludeList, execution, null);
    }

    protected OpenCGAResult<R> updateMany(String studyStr, List<String> idList, Object updateParams, boolean ignoreException,
                                          QueryOptions options, String token, QueryOptions studyIncludeList,
                                          ExecuteOperationForSingleEntry<R> execution) throws CatalogException {
        return updateMany(new ObjectMap(), studyStr, idList, updateParams, ignoreException, token, options, studyIncludeList, execution);
    }

    protected OpenCGAResult<R> updateMany(ObjectMap params, String studyStr, List<String> idList, Object updateParams,
                                          boolean ignoreException, String token, QueryOptions options, QueryOptions studyIncludeList,
                                          ExecuteOperationForSingleEntry<R> execution) throws CatalogException {
        params = params != null ? params : new ObjectMap();
        params.putIfAbsent("study", studyStr);
        params.putIfAbsent("ids", idList);
        params.putIfAbsent("updateParams", updateParams);
        params.putIfAbsent("ignoreException", ignoreException);
        params.putIfAbsent("options", options);
        params.putIfAbsent("token", token);
        OpenCGAResult<R> result = runList(params, Enums.Action.UPDATE, studyStr, idList, token, studyIncludeList, null, execution);
        return endResult(result, ignoreException);
    }

    protected OpenCGAResult<R> updateMany(String studyStr, Query query, Object updateParams, boolean ignoreException,
                                          QueryOptions options, String token, QueryOptions studyIncludeList,
                                          ExecuteOperationIterator<R> operationIterator, ExecuteBatchOperation<R> execution,
                                          String errorMessage) throws CatalogException {
        ObjectMap params = new ObjectMap()
                .append("study", studyStr)
                .append("query", query)
                .append("updateParams", updateParams)
                .append("ignoreException", ignoreException)
                .append("options", options)
                .append("token", token);
        return updateMany(params, studyStr, ignoreException, token, studyIncludeList, operationIterator, execution, errorMessage);
    }

    protected OpenCGAResult<R> updateMany(ObjectMap params, String studyStr, boolean ignoreException, String token,
                                          QueryOptions studyIncludeList, ExecuteOperationIterator<R> operationIterator,
                                          ExecuteBatchOperation<R> execution, String errorMessage) throws CatalogException {
        OpenCGAResult<R> result = runIterator(params, Enums.Action.UPDATE, studyStr, token, studyIncludeList, operationIterator, execution,
                errorMessage);
        return endResult(result, ignoreException);
    }

    protected OpenCGAResult<R> deleteMany(String studyStr, List<String> idList, ObjectMap deleteParams, boolean ignoreException,
                                          String token, ExecuteOperationForSingleEntry<R> execution) throws CatalogException {
        return deleteMany(studyStr, idList, deleteParams, ignoreException, token, null, execution);
    }

    protected OpenCGAResult<R> deleteMany(String studyStr, List<String> idList, ObjectMap deleteParams, boolean ignoreException,
                                          String token, BeforeCheckOperation before, ExecuteOperationForSingleEntry<R> execution)
            throws CatalogException {
        ObjectMap params = new ObjectMap()
                .append("study", studyStr)
                .append("ids", idList)
                .append("params", deleteParams)
                .append("ignoreException", ignoreException)
                .append("token", token);
        OpenCGAResult<R> result = runList(params, Enums.Action.DELETE, studyStr, idList, token, QueryOptions.empty(), before, execution);
        return endResult(result, ignoreException);
    }

    protected OpenCGAResult<R> deleteMany(String studyStr, Query query, ObjectMap deleteParams, boolean ignoreException, String token,
                                          ExecuteOperationIterator<R> iteratorSupplier, ExecuteBatchOperation<R> execution)
            throws CatalogException {
        return deleteMany(studyStr, query, deleteParams, ignoreException, token, QueryOptions.empty(), iteratorSupplier, execution);
    }

    protected OpenCGAResult<R> deleteMany(String studyStr, Query query, ObjectMap deleteParams, boolean ignoreException, String token,
                                          QueryOptions studyIncludeList, ExecuteOperationIterator<R> iteratorSupplier,
                                          ExecuteBatchOperation<R> execution) throws CatalogException {
        ObjectMap params = new ObjectMap()
                .append("study", studyStr)
                .append("query", query)
                .append("params", deleteParams)
                .append("ignoreException", ignoreException)
                .append("token", token);
        OpenCGAResult<R> result = runIterator(params, Enums.Action.DELETE, studyStr, token, studyIncludeList, iteratorSupplier, execution,
                null);
        return endResult(result, ignoreException);
    }

    protected OpenCGAResult<FacetField> facet(String studyStr, Query query, String facet, String token, QueryOptions studyIncludeList,
                                              ExecuteQueryOperation<FacetField> execution) throws CatalogException {
        ObjectMap params = new ObjectMap()
                .append("study", studyStr)
                .append("query", query)
                .append("facet", facet)
                .append("token", token);
        return runForQueryOperation(params, Enums.Action.FACET, studyStr, token, studyIncludeList, execution, null);
    }

    protected OpenCGAResult<R> rank(String studyStr, Query query, String field, int numResults, boolean asc, String token,
                                    ExecuteQueryOperation<R> execution) throws CatalogException {
        ObjectMap params = new ObjectMap()
                .append("study", studyStr)
                .append("query", query)
                .append("field", field)
                .append("numResults", numResults)
                .append("asc", asc)
                .append("token", token);
        return runForQueryOperation(params, Enums.Action.RANK, studyStr, token, QueryOptions.empty(), execution, null);
    }

    protected OpenCGAResult<R> groupBy(String studyStr, Query query, List<String> fields, QueryOptions options, String token,
                                       ExecuteQueryOperation<R> execution) throws CatalogException {
        ObjectMap params = new ObjectMap()
                .append("study", studyStr)
                .append("query", query)
                .append("fields", fields)
                .append("options", options)
                .append("token", token);
        return runForQueryOperation(params, Enums.Action.GROUP_BY, studyStr, token, QueryOptions.empty(), execution, null);
    }

    protected OpenCGAResult<R> search(String studyStr, Query query, QueryOptions queryOptions, String token,
                                      QueryOptions studyIncludeList, ExecuteQueryOperation<R> execution) throws CatalogException {
        ObjectMap params = new ObjectMap()
                .append("study", studyStr)
                .append("query", query)
                .append("options", queryOptions)
                .append("token", token);
        return runForQueryOperation(params, Enums.Action.SEARCH, studyStr, token, studyIncludeList, execution, null);
    }

    protected DBIterator<R> iterator(String studyStr, Query query, QueryOptions options, QueryOptions studyIncludeList, String token,
                                     ExecuteDBIteratorOperation<R> execution) throws CatalogException {
        ObjectMap params = new ObjectMap()
                .append("study", studyStr)
                .append("query", query)
                .append("options", options)
                .append("token", token);
        return iterator(params, studyStr, studyIncludeList, token, execution, null);
    }

    protected <T> OpenCGAResult<T> distinct(String studyStr, List<String> fields, Query query, String token, QueryOptions studyIncludeList,
                                            ExecuteQueryOperation<T> execution) throws CatalogException {
        ObjectMap params = new ObjectMap()
                .append("study", studyStr)
                .append("fields", fields)
                .append("query", query)
                .append("token", token);
        return runForQueryOperation(params, Enums.Action.DISTINCT, studyStr, token, studyIncludeList, execution, null);
    }

    protected OpenCGAResult<R> count(String studyStr, Query query, String token, QueryOptions studyIncludeList,
                                     ExecuteQueryOperation<R> execution) throws CatalogException {
        ObjectMap params = new ObjectMap()
                .append("study", studyStr)
                .append("query", query)
                .append("token", token);
        return runForQueryOperation(params, Enums.Action.COUNT, studyStr, token, studyIncludeList, execution, null);
    }

    protected <T> OpenCGAResult<T> getAcls(String studyStr, List<String> idList, List<String> members, boolean ignoreException,
                                                     String token, ExecuteMultiOperation<T> execution)
            throws CatalogException {
        ObjectMap params = new ObjectMap()
                .append("study", studyStr)
                .append("idList", idList)
                .append("members", members)
                .append("ignoreException", ignoreException)
                .append("token", token);
        return runForMultiOperation(params, Enums.Action.FETCH_ACLS, studyStr, token, QueryOptions.empty(), execution, null);
    }

    protected <T> OpenCGAResult<T> updateAcls(String studyStr, List<String> idList, String memberList, Object aclParams,
                                                        ParamUtils.AclAction action, String token,
                                                        ExecuteMultiOperation<T> execution)
            throws CatalogException {
        return updateAcls(new ObjectMap(), studyStr, idList, memberList, aclParams, action, token, execution);
    }

    protected <T> OpenCGAResult<T> updateAcls(ObjectMap params, String studyStr, List<String> idList, String memberList,
                                                        Object aclParams, ParamUtils.AclAction action, String token,
                                                        ExecuteMultiOperation<T> execution)
            throws CatalogException {
        params = params != null ? params : new ObjectMap();
        params.putIfAbsent("study", studyStr);
        params.putIfAbsent("idList", idList);
        params.putIfAbsent("memberList", memberList);
        params.putIfAbsent("aclParams", aclParams);
        params.putIfAbsent("action", action);
        params.putIfAbsent("token", token);
        return runForMultiOperation(params, Enums.Action.UPDATE_ACLS, studyStr, token, StudyManager.INCLUDE_CONFIGURATION, execution,
                "Could not update ACLs");
    }

    protected <T> OpenCGAResult<T> runForSingleEntry(ObjectMap params, Enums.Action action, String studyStr, String token,
                                                     ExecuteOperationForSingleEntry<T> execution) throws CatalogException {
        return runForSingleEntry(params, action, studyStr, token, QueryOptions.empty(), execution, null);
    }

    protected <T> OpenCGAResult<T> runForSingleEntry(ObjectMap params, Enums.Action action, String studyStr, String token,
                                                     ExecuteOperationForSingleEntry<T> execution, String errorMessage)
            throws CatalogException {
        return runForSingleEntry(params, action, studyStr, token, QueryOptions.empty(), execution, errorMessage);
    }

    protected <T> OpenCGAResult<T> runForSingleEntry(ObjectMap params, Enums.Action action, String studyStr, String token,
                                                     QueryOptions studyIncludeList, ExecuteOperationForSingleEntry<T> execution)
            throws CatalogException {
        return runForSingleEntry(params, action, studyStr, token, studyIncludeList, execution, null);
    }

    protected <T> OpenCGAResult<T> runForSingleEntry(ObjectMap params, Enums.Action action, String studyStr, String token,
                                                     QueryOptions studyIncludeList, ExecuteOperationForSingleEntry<T> execution,
                                                     String errorMessage) throws CatalogException {
        return runForSingleEntry(params, action, studyStr, token, studyIncludeList, execution, null, errorMessage);
    }

    protected void echo(Runnable runnable) throws CatalogException {

    }

    protected <T> OpenCGAResult<T> runForSingleEntry(ObjectMap params, Enums.Action action, String studyStr, String token,
                                                     QueryOptions studyIncludeList, ExecuteOperationForSingleEntry<T> execution,
                                                     ExecuteOnError onError, String errorMessage) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);

        String eventId = getResource().name().toLowerCase() + "." + action.name().toLowerCase();

        String eventUuid = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.EVENT);
        OpencgaEvent opencgaEvent = OpencgaEvent.build(eventUuid, eventId, params, organizationId, userId, tokenPayload.getToken());
        CatalogEvent catalogEvent = CatalogEvent.build(opencgaEvent);
        EntryParam entryParam = new EntryParam();
        Study study = null;
        try {
            // Get study
            study = catalogManager.getStudyManager().resolveId(studyFqn, studyIncludeList, tokenPayload);
            opencgaEvent.setStudyFqn(study.getFqn());
            opencgaEvent.setStudyUuid(study.getUuid());

            // Execute code
            OpenCGAResult<T> execute = execution.execute(organizationId, study, userId, entryParam);

            // Fill missing OpencgaEvent entries
            opencgaEvent.setResult(execute);
            opencgaEvent.setEntries(Collections.singletonList(entryParam));

            // Notify event
            EventManager.getInstance().notify(catalogEvent);

            return execute;
        } catch (Exception e) {
            EventManager.getInstance().notify(catalogEvent, e);
            if (onError != null && study != null) {
                try {
                    onError.execute(organizationId, study, userId, entryParam, e);
                } catch (RuntimeException e1) {
                    logger.error("Error running onErrorRunnable", e1);
                    e.addSuppressed(e1);
                }
            }
            if (StringUtils.isNotEmpty(errorMessage)) {
                throw new CatalogException(errorMessage, e);
            } else {
                throw e;
            }
        }
    }

    protected <T> T runGenericOperation(ObjectMap params, Enums.Action action, String studyStr, String token,
                                        ExecuteGenericOperation<T> execution) throws CatalogException {
        return runGenericOperation(params, action, studyStr, token, QueryOptions.empty(), execution, null);
    }

    protected <T> T runGenericOperation(ObjectMap params, Enums.Action action, String studyStr, String token, QueryOptions studyIncludeList,
                                        ExecuteGenericOperation<T> execution, String errorMessage) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);

        String eventId = getResource().name().toLowerCase() + "." + action.name().toLowerCase();

        String eventUuid = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.EVENT);
        OpencgaEvent opencgaEvent = OpencgaEvent.build(eventUuid, eventId, params, organizationId, userId, tokenPayload.getToken());
        CatalogEvent catalogEvent = CatalogEvent.build(opencgaEvent);
        try {
            // Get study
            Study study = catalogManager.getStudyManager().resolveId(studyFqn, studyIncludeList, tokenPayload);
            opencgaEvent.setStudyFqn(study.getFqn());
            opencgaEvent.setStudyUuid(study.getUuid());

            // Execute code
            EntryParam entryParam = new EntryParam();
            T execute = execution.execute(organizationId, study, userId, entryParam);

            // Fill missing OpencgaEvent entries
            opencgaEvent.setEntries(Collections.singletonList(entryParam));

            // Notify event
            EventManager.getInstance().notify(catalogEvent);

            return execute;
        } catch (Exception e) {
            EventManager.getInstance().notify(catalogEvent, e);
            if (StringUtils.isNotEmpty(errorMessage)) {
                throw new CatalogException(errorMessage, e);
            } else {
                throw e;
            }
        }
    }

    /**
     * Interface to execute a query operation. No particular elements are provided by the user.
     * @param <R> Type of the element that will be processed.
     */
    public interface ExecuteQueryOperation<R> {
        OpenCGAResult<R> execute(String organizationId, Study study, String userId) throws CatalogException;
    }

    public interface ExecuteDBIteratorOperation<R> {
        DBIterator<R> execute(String organizationId, Study study, String userId) throws CatalogException;
    }

    protected OpenCGAResult<R> runForQueryOperation(ObjectMap params, Enums.Action action, String studyStr, String token,
                                                    ExecuteQueryOperation<R> execution) throws CatalogException {
        return runForQueryOperation(params, action, studyStr, token, QueryOptions.empty(), execution, null);
    }

    protected OpenCGAResult<R> runForQueryOperation(ObjectMap params, Enums.Action action, String studyStr, String token,
                                                    ExecuteQueryOperation<R> execution, String errorMessage) throws CatalogException {
        return runForQueryOperation(params, action, studyStr, token, QueryOptions.empty(), execution, errorMessage);
    }

    protected OpenCGAResult<R> runForQueryOperation(ObjectMap params, Enums.Action action, String studyStr, String token,
                                                    QueryOptions studyIncludeList, ExecuteQueryOperation<R> execution)
            throws CatalogException {
        return runForQueryOperation(params, action, studyStr, token, studyIncludeList, execution, null);
    }

    private <T> OpenCGAResult<T> runForQueryOperation(ObjectMap params, Enums.Action action, String studyStr, String token,
                                                      QueryOptions studyIncludeList, ExecuteQueryOperation<T> execution,
                                                      String errorMessage) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);

        String eventId = getResource().name().toLowerCase() + "." + action.name().toLowerCase();

        String eventUuid = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.EVENT);
        OpencgaEvent opencgaEvent = OpencgaEvent.build(eventUuid, eventId, params, organizationId, userId, tokenPayload.getToken());
        CatalogEvent catalogEvent = CatalogEvent.build(opencgaEvent);
        try {
            // Get study
            Study study = catalogManager.getStudyManager().resolveId(studyFqn, studyIncludeList, tokenPayload);
            opencgaEvent.setStudyFqn(study.getFqn());
            opencgaEvent.setStudyUuid(study.getUuid());

            // Execute code
            OpenCGAResult<T> execute = execution.execute(organizationId, study, userId);

            // Fill missing OpencgaEvent entries
            opencgaEvent.setResult(execute);

            // Notify event
            EventManager.getInstance().notify(catalogEvent);

            return execute;
        } catch (Exception e) {
            EventManager.getInstance().notify(catalogEvent, e);
            if (StringUtils.isNotEmpty(errorMessage)) {
                throw new CatalogException(errorMessage, e);
            } else {
                throw e;
            }
        }
    }

    protected DBIterator<R> iterator(ObjectMap params, String studyStr, QueryOptions studyIncludeList, String token,
                                     ExecuteDBIteratorOperation<R> execution, String errorMessage) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Enums.Action action = Enums.Action.ITERATOR;

        String eventId = getResource().name().toLowerCase() + "." + action.name().toLowerCase();

        String eventUuid = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.EVENT);
        OpencgaEvent opencgaEvent = OpencgaEvent.build(eventUuid, eventId, params, organizationId, userId, tokenPayload.getToken());
        CatalogEvent catalogEvent = CatalogEvent.build(opencgaEvent);
        try {
            // Get study
            Study study = catalogManager.getStudyManager().resolveId(studyFqn, studyIncludeList, tokenPayload);
            opencgaEvent.setStudyFqn(study.getFqn());
            opencgaEvent.setStudyUuid(study.getUuid());

            // Execute code
            DBIterator<R> execute = execution.execute(organizationId, study, userId);

//             Fill missing OpencgaEvent entries
//            opencgaEvent.setResult(execute);

            // Notify event
            EventManager.getInstance().notify(catalogEvent);

            return execute;
        } catch (Exception e) {
            EventManager.getInstance().notify(catalogEvent, e);
            if (StringUtils.isNotEmpty(errorMessage)) {
                throw new CatalogException(errorMessage, e);
            } else {
                throw e;
            }
        }
    }

    /**
     * Interface to execute a multi operation. The user will execute the multi operation in a single call and will provide the list of
     * elements that are processed.
     * @param <R> Type of the elements that will be processed.
     */
    public interface ExecuteMultiOperation<R> {
        OpenCGAResult<R> execute(String organizationId, Study study, String userId, List<EntryParam> entryParamList) throws Exception;
    }

    protected OpenCGAResult<R> runForMultiOperation(ObjectMap params, Enums.Action action, String studyStr, String token,
                                                    ExecuteMultiOperation<R> execution) throws CatalogException {
        return runForMultiOperation(params, action, studyStr, token, QueryOptions.empty(), execution, null);
    }

    protected OpenCGAResult<R> runForMultiOperation(ObjectMap params, Enums.Action action, String studyStr, String token,
                                                    ExecuteMultiOperation<R> execution, String errorMessage) throws CatalogException {
        return runForMultiOperation(params, action, studyStr, token, QueryOptions.empty(), execution, errorMessage);
    }

    protected OpenCGAResult<R> runForMultiOperation(ObjectMap params, Enums.Action action, String studyStr, String token,
                                                    QueryOptions studyIncludeList, ExecuteMultiOperation<R> execution)
            throws CatalogException {
        return runForMultiOperation(params, action, studyStr, token, studyIncludeList, execution, null);
    }

    protected <T> OpenCGAResult<T> runForMultiOperation(ObjectMap params, Enums.Action action, String studyStr, String token,
                                                        QueryOptions studyIncludeList, ExecuteMultiOperation<T> execution,
                                                        String errorMessage) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);

        String eventId = getResource().name().toLowerCase() + "." + action.name().toLowerCase();

        String eventUuid = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.EVENT);
        OpencgaEvent opencgaEvent = OpencgaEvent.build(eventUuid, eventId, params, organizationId, userId, tokenPayload.getToken());
        CatalogEvent catalogEvent = CatalogEvent.build(opencgaEvent);
        try {
            // Get study
            Study study = catalogManager.getStudyManager().resolveId(studyFqn, studyIncludeList, tokenPayload);
            opencgaEvent.setStudyFqn(study.getFqn());
            opencgaEvent.setStudyUuid(study.getUuid());

            // Execute code
            List<EntryParam> entryParamList = new LinkedList<>();
            opencgaEvent.setEntries(new LinkedList<>());
            OpenCGAResult<T> execute = execution.execute(organizationId, study, userId, entryParamList);

            // Fill missing OpencgaEvent entries
            opencgaEvent.setResult(execute);

            // Notify event
            EventManager.getInstance().notify(catalogEvent);

            return execute;
        } catch (Exception e) {
            EventManager.getInstance().notify(catalogEvent, e);
            if (StringUtils.isNotEmpty(errorMessage)) {
                throw new CatalogException(errorMessage, e);
            } else if (e instanceof CatalogException) {
                throw (CatalogException) e;
            } else {
                throw new CatalogException(e);
            }
        }
    }

    protected OpenCGAResult<R> runIterator(ObjectMap params, Enums.Action action, String studyStr, String token,
                                           ExecuteOperationIterator<R> iteratorSupplier, ExecuteBatchOperation<R> execution)
            throws CatalogException {
        return runIterator(params, action, studyStr, token, QueryOptions.empty(), iteratorSupplier, execution, null);
    }

    protected OpenCGAResult<R> runIterator(ObjectMap params, Enums.Action action, String studyStr, String token,
                                           QueryOptions studyIncludeList, ExecuteOperationIterator<R> operationIterator,
                                           ExecuteBatchOperation<R> execution, String errorMessage) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);

        String eventId = getResource().name().toLowerCase() + "." + action.name().toLowerCase();

        String eventUuid = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.EVENT);
        OpencgaEvent opencgaEvent = OpencgaEvent.build(eventUuid, eventId, params, organizationId, userId, tokenPayload.getToken());
        CatalogEvent catalogEvent = CatalogEvent.build(opencgaEvent);
        Study study;
        try {
            // Get study
            study = catalogManager.getStudyManager().resolveId(studyFqn, studyIncludeList, tokenPayload);
        } catch (Exception e) {
            EventManager.getInstance().notify(catalogEvent, e);
            if (StringUtils.isNotEmpty(errorMessage)) {
                throw new CatalogException(errorMessage, e);
            } else {
                throw e;
            }
        }

        DBIterator<R> iterator;
        try {
            iterator = operationIterator.iterator(organizationId, study, userId);
        } catch (Exception e) {
            EventManager.getInstance().notify(catalogEvent, e);
            if (StringUtils.isNotEmpty(errorMessage)) {
                throw new CatalogException(errorMessage, e);
            } else {
                throw e;
            }
        }

        // Execute code
        OpenCGAResult<R> result = OpenCGAResult.empty();
        while (iterator.hasNext()) {
            R object = iterator.next();

            EntryParam entryParam = new EntryParam(object.getId(), object.getUuid());
            opencgaEvent = OpencgaEvent.build(eventUuid, eventId, params, organizationId, study.getFqn(), study.getUuid(), userId,
                    tokenPayload.getToken());
            opencgaEvent.setEntries(Collections.singletonList(entryParam));
            catalogEvent = CatalogEvent.build(opencgaEvent);
            try {
                OpenCGAResult<R> execute = execution.execute(organizationId, study, userId, object);
                result.append(execute);
                opencgaEvent.setResult(execute);
                // Notify event
                EventManager.getInstance().notify(catalogEvent);
            } catch (Exception e) {
                // Add error event to the main result object
                Event event = new Event(Event.Type.ERROR, entryParam.getId(), e.getMessage());
                result.getEvents().add(event);
                result.setNumErrors(result.getNumErrors() + 1);

                // Notify error and continue processing
                EventManager.getInstance().notify(catalogEvent, e);
            }
        }
        iterator.close();

        return result;
    }


    protected OpenCGAResult<R> runList(ObjectMap params, Enums.Action action, String studyStr, List<String> idList, String token,
                                       ExecuteOperationForSingleEntry<R> execution) throws CatalogException {
        return runList(params, action, studyStr, idList, token, QueryOptions.empty(), null, execution);
    }

    protected OpenCGAResult<R> runList(ObjectMap params, Enums.Action action, String studyStr, List<String> idList, String token,
                                       QueryOptions studyIncludeList, BeforeCheckOperation before,
                                       ExecuteOperationForSingleEntry<R> execution) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);

        String eventId = getResource().name().toLowerCase() + "." + action.name().toLowerCase();

        String eventUuid = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.EVENT);
        OpencgaEvent opencgaEvent = OpencgaEvent.build(eventUuid, eventId, params, organizationId, userId, tokenPayload.getToken());
        CatalogEvent catalogEvent = CatalogEvent.build(opencgaEvent);
        Study study;
        try {
            // Get study
            study = catalogManager.getStudyManager().resolveId(studyFqn, studyIncludeList, tokenPayload);
        } catch (Exception e) {
            EventManager.getInstance().notify(catalogEvent, e);
            throw e;
        }

        if (CollectionUtils.isEmpty(idList)) {
            CatalogException exception = new CatalogException("Missing list of ids to process.");
            EventManager.getInstance().notify(catalogEvent, exception);
            throw exception;
        }
        OpenCGAResult<R> result = OpenCGAResult.empty();

        if (before != null) {
            try {
                before.execute(organizationId, study, userId);
            } catch (Exception e) {
                EventManager.getInstance().notify(catalogEvent, e);
                throw e;
            }
        }

        // Execute code
        for (String id : idList) {
            opencgaEvent = OpencgaEvent.build(eventUuid, eventId, params, organizationId, study.getFqn(), study.getUuid(), userId,
                    tokenPayload.getToken());
            catalogEvent = CatalogEvent.build(opencgaEvent);
            try {
                EntryParam entryParam = new EntryParam().setId(id);
                OpenCGAResult<R> execute = execution.execute(organizationId, study, userId, entryParam);
                result.append(execute);

                // Fill missing OpenCGAEvent fields
                opencgaEvent.setResult(execute);
                opencgaEvent.setEntries(Collections.singletonList(entryParam));

                // Notify event
                EventManager.getInstance().notify(catalogEvent);
            } catch (Exception e) {
                // Add error event to the main result object
                Event event = new Event(Event.Type.ERROR, id, e.getMessage());
                result.getEvents().add(event);
                result.setNumErrors(result.getNumErrors() + 1);

                // Notify error and continue processing
                EventManager.getInstance().notify(catalogEvent, e);
            }
        }

        return result;
    }


}
