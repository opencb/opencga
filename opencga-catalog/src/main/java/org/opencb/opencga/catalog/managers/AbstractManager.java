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
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.models.InternalGetDataResult;
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

//    /**
//     * Interface to execute an operation over a single element.
//     * @param <R> Type of the element that will be processed.
//     */
//    public interface ExecuteOperationForSingleEntry<R> {
//        OpenCGAResult<R> execute(String organizationId, String userId, EntryParam entryParam) throws CatalogException;
//    }

    /**
     * Interface to execute an operation over a single element.
     * @param <R> Type of the element that will be processed.
     */
    public interface ExecuteOperationForSingleEntry<R> {
        OpenCGAResult<R> execute(String organizationId, JwtPayload payload, EntryParam entryParam) throws CatalogException;
    }

    public interface ExecuteQueryOperation<R> {
        OpenCGAResult<R> execute(String organizationId, JwtPayload payload) throws CatalogException;
    }

    /**
     * Interface to execute anything when an error happens.
     */
    public interface ExecuteOnError {
        void execute(String organizationId, String userId, EntryParam entryParam, Exception e) throws CatalogException;
    }

    protected OpenCGAResult<R> create(Object object, QueryOptions options, String token, ExecuteOperationForSingleEntry<R> execution)
            throws CatalogException {
        return create(new ObjectMap(), object, options, token, execution);
    }

    protected OpenCGAResult<R> create(ObjectMap params, Object object, QueryOptions options, String token,
                                      ExecuteOperationForSingleEntry<R> execution) throws CatalogException {
        params = params != null ? params : new ObjectMap();
        params.putIfAbsent("object", object);
        params.putIfAbsent("options", options);
        params.putIfAbsent("token", token);

        return runForSingleEntry(params, Enums.Action.CREATE, token, execution, null);
    }

    protected OpenCGAResult<R> get(String id, QueryOptions options, String token, ExecuteOperationForSingleEntry<R> execution)
            throws CatalogException {
        return get(new ObjectMap(), id, options, token, execution);
    }

    protected OpenCGAResult<R> get(ObjectMap params, String id, QueryOptions options, String token,
                                   ExecuteOperationForSingleEntry<R> execution)
            throws CatalogException {
        params = params != null ? params : new ObjectMap();
        params.putIfAbsent("id", id);
        params.putIfAbsent("options", options);
        params.putIfAbsent("token", token);

        return runForSingleEntry(params, Enums.Action.INFO, token, execution, null);
    }

    protected OpenCGAResult<R> update(ObjectMap params, String id, ObjectMap updateParams, QueryOptions options, String token,
                                      ExecuteOperationForSingleEntry<R> execution)
            throws CatalogException {
        params = params != null ? params : new ObjectMap();
        params.putIfAbsent("id", id);
        params.putIfAbsent("updateParams", updateParams);
        params.putIfAbsent("options", options);
        params.putIfAbsent("token", token);

        return runForSingleEntry(params, Enums.Action.UPDATE, token, execution, null);
    }


    protected OpenCGAResult<R> search(ObjectMap params, Query query, QueryOptions options, String token, ExecuteQueryOperation<R> execution)
            throws CatalogException {
        params = params != null ? params : new ObjectMap();
        params.putIfAbsent("query", query);
        params.putIfAbsent("options", options);
        params.putIfAbsent("token", token);

        return runForQueryOperation(params, Enums.Action.SEARCH, token, execution, null);
    }

    protected <T> OpenCGAResult<T> runForSingleEntry(ObjectMap params, Enums.Action action, String token,
                                                     ExecuteOperationForSingleEntry<T> execution, String errorMessage)
            throws CatalogException {
        return runForSingleEntry(params, action, token, execution, null, errorMessage);
    }

    protected <T> OpenCGAResult<T> runForSingleEntry(ObjectMap params, Enums.Action action, String token,
                                                     ExecuteOperationForSingleEntry<T> execution, ExecuteOnError onError,
                                                     String errorMessage) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        String organizationId = tokenPayload.getOrganization();
        String userId = tokenPayload.getUserId(organizationId);

        String eventId = getResource().name().toLowerCase() + "." + action.name().toLowerCase();

        String eventUuid = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.EVENT);
        OpencgaEvent opencgaEvent = OpencgaEvent.build(eventUuid, eventId, params, organizationId, userId, tokenPayload.getToken());
        CatalogEvent catalogEvent = CatalogEvent.build(opencgaEvent);
        EntryParam entryParam = new EntryParam();
        try {
            // Execute code
            OpenCGAResult<T> execute = execution.execute(organizationId, tokenPayload, entryParam);

            // Fill missing OpencgaEvent entries
            opencgaEvent.setResult(execute);
            opencgaEvent.setEntries(Collections.singletonList(entryParam));

            // Notify event
            EventManager.getInstance().notify(catalogEvent);

            return execute;
        } catch (Exception e) {
            EventManager.getInstance().notify(catalogEvent, e);
            if (onError != null) {
                try {
                    onError.execute(organizationId, userId, entryParam, e);
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

    private <T> OpenCGAResult<T> runForQueryOperation(ObjectMap params, Enums.Action action, String token,
                                                      ExecuteQueryOperation<T> execution,  String errorMessage) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        String organizationId = tokenPayload.getOrganization();
        String userId = tokenPayload.getUserId(organizationId);

        String eventId = getResource().name().toLowerCase() + "." + action.name().toLowerCase();

        String eventUuid = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.EVENT);
        OpencgaEvent opencgaEvent = OpencgaEvent.build(eventUuid, eventId, params, organizationId, userId, tokenPayload.getToken());
        CatalogEvent catalogEvent = CatalogEvent.build(opencgaEvent);
        try {
            // Execute code
            OpenCGAResult<T> execute = execution.execute(organizationId, tokenPayload);

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



}
