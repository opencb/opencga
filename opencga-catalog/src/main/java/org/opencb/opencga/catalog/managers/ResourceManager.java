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
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.InternalGetDataResult;
import org.opencb.opencga.catalog.utils.CatalogFqn;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.events.OpencgaEvent;
import org.opencb.opencga.core.models.AclEntryList;
import org.opencb.opencga.core.models.IPrivateStudyUid;
import org.opencb.opencga.core.models.JwtPayload;
import org.opencb.opencga.core.models.audit.AuditRecord;
import org.opencb.opencga.core.models.common.EntryParam;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.event.CatalogEvent;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.response.OpenCGAResult;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by pfurio on 07/08/17.
 */
public abstract class ResourceManager<R extends IPrivateStudyUid, S extends Enum<S>> extends AbstractManager {

    ResourceManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                    DBAdaptorFactory catalogDBAdaptorFactory, Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, configuration);
    }

    abstract Enums.Resource getResource();

    OpenCGAResult<R> internalGet(String organizationId, long studyUid, String entry, QueryOptions options, String user)
            throws CatalogException {
        return internalGet(organizationId, studyUid, entry, null, options, user);
    }

    OpenCGAResult<R> internalGet(String organizationId, long studyUid, String entry, @Nullable Query query, QueryOptions options,
                                 String user) throws CatalogException {
        ParamUtils.checkIsSingleID(entry);
        return internalGet(organizationId, studyUid, Collections.singletonList(entry), query, options, user, false);
    }

    InternalGetDataResult<R> internalGet(String organizationId, long studyUid, List<String> entryList, QueryOptions options, String user,
                                         boolean ignoreException) throws CatalogException {
        return internalGet(organizationId, studyUid, entryList, null, options, user, ignoreException);
    }

    InternalGetDataResult<R> internalGet(String organizationId, long studyUid, String entry, @Nullable Query query,
                                         QueryOptions options, String user, boolean ignoreException) throws CatalogException {
        return internalGet(organizationId, studyUid, Collections.singletonList(entry), query, options, user, ignoreException);
    }

    abstract InternalGetDataResult<R> internalGet(String organizationId, long studyUid, List<String> entryList, @Nullable Query query,
                                                  QueryOptions options, String user, boolean ignoreException) throws CatalogException;

    /**
     * Create an entry in catalog.
     *
     * @param studyStr Study id in string format. Could be one of
     *                [id|organization@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy]
     * @param entry    entry that needs to be added in Catalog.
     * @param options  QueryOptions object.
     * @param token    Session id of the user logged in.
     * @return A OpenCGAResult of the object created.
     * @throws CatalogException if any parameter from the entry is incorrect, the user does not have permissions...
     */
    public abstract OpenCGAResult<R> create(String studyStr, R entry, QueryOptions options, String token)
            throws CatalogException;

    /**
     * Fetch the R object.
     *
     * @param studyStr Study id in string format. Could be one of
     *                 [id|organization@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy]
     * @param entryStr Entry id to be fetched.
     * @param options  QueryOptions object, like "include", "exclude", "limit" and "skip".
     * @param token    token
     * @return All matching elements.
     * @throws CatalogException CatalogException.
     */
    public OpenCGAResult<R> get(String studyStr, String entryStr, QueryOptions options, String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId, organizationId);
        return internalGet(organizationId, study.getUid(), entryStr, options, userId);
    }

    /**
     * Fetch all the R objects matching the query.
     *
     * @param studyStr  Study id in string format. Could be one of
     *                  [id|organization@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy]
     * @param entryList Comma separated list of entries to be fetched.
     * @param options   QueryOptions object, like "include", "exclude", "limit" and "skip".
     * @param token     token
     * @return All matching elements.
     * @throws CatalogException CatalogException.
     */
    public OpenCGAResult<R> get(String studyStr, List<String> entryList, QueryOptions options, String token) throws CatalogException {
        return get(studyStr, entryList, new Query(), options, false, token);
    }

    public OpenCGAResult<R> get(String studyStr, List<String> entryList, QueryOptions options, boolean ignoreException, String token)
            throws CatalogException {
        return get(studyStr, entryList, new Query(), options, ignoreException, token);
    }

    public OpenCGAResult<R> get(String studyId, List<String> entryList, Query query, QueryOptions options, boolean ignoreException,
                                String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyId, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = catalogManager.getStudyManager().resolveId(studyId, userId, organizationId);

        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("entryList", entryList)
                .append("query", new Query(query))
                .append("options", new QueryOptions(options))
                .append("ignoreException", ignoreException)
                .append("token", token);
        String operationUuid = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);
        auditManager.initAuditBatch(operationUuid);

        try {
            OpenCGAResult<R> result = OpenCGAResult.empty();

            options.remove(QueryOptions.LIMIT);
            InternalGetDataResult<R> responseResult = internalGet(organizationId, study.getUid(), entryList, query, options, userId,
                    ignoreException);

            Map<String, InternalGetDataResult.Missing> missingMap = new HashMap<>();
            if (responseResult.getMissing() != null) {
                missingMap = responseResult.getMissing().stream()
                        .collect(Collectors.toMap(InternalGetDataResult.Missing::getId, Function.identity()));
            }

            List<List<R>> versionedResults = responseResult.getVersionedResults();
            for (int i = 0; i < versionedResults.size(); i++) {
                String entryId = entryList.get(i);
                if (versionedResults.get(i).isEmpty()) {
                    Event event = new Event(Event.Type.ERROR, entryId, missingMap.get(entryId).getErrorMsg());
                    // Missing
                    result.getEvents().add(event);
//                    resultList.add(new OpenCGAResult<>(responseResult.getTime(), Collections.singletonList(event), 0,
//                            Collections.emptyList(), 0));
                } else {
                    int size = versionedResults.get(i).size();
                    result.append(new OpenCGAResult<>(0, Collections.emptyList(), size, versionedResults.get(i), size));
//                    resultList.add(new OpenCGAResult<>(responseResult.getTime(), Collections.emptyList(), size, versionedResults.get(i),
//                            size));

                    R entry = versionedResults.get(i).get(0);
                    auditManager.auditInfo(organizationId, operationUuid, userId, getResource(), entry.getId(), entry.getUuid(),
                            study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
                }
            }

            return result;
        } catch (CatalogException e) {
            for (String entryId : entryList) {
                auditManager.auditInfo(organizationId, operationUuid, userId, getResource(), entryId, "", study.getId(), study.getUuid(),
                        auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
            throw e;
        } finally {
            auditManager.finishAuditBatch(organizationId, operationUuid);
        }
    }

    /**
     * Obtain an entry iterator to iterate over the matching entries.
     *
     * @param studyStr study id in string format. Could be one of
     *                 [id|organization@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy]
     * @param query    Query object.
     * @param options  QueryOptions object.
     * @param token    Session id of the user logged in.
     * @return An iterator.
     * @throws CatalogException if there is any internal error.
     */
    public abstract DBIterator<R> iterator(String studyStr, Query query, QueryOptions options, String token)
            throws CatalogException;

    public abstract OpenCGAResult<FacetField> facet(String studyStr, Query query, String facet, String token) throws CatalogException;

    /**
     * Search of entries in catalog.
     *
     * @param studyId study id in string format. Could be one of
     *                [id|organization@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy]
     * @param query   Query object.
     * @param options QueryOptions object.
     * @param token   Session id of the user logged in.
     * @return The list of entries matching the query.
     * @throws CatalogException catalogException.
     */
    public abstract OpenCGAResult<R> search(String studyId, Query query, QueryOptions options, String token) throws CatalogException;

    /**
     * Fetch a list containing all the distinct values of the key {@code field}.
     *
     * @param studyId study id in string format. Could be one of
     *                [id|organization@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy]
     * @param field   The field for which to return distinct values.
     * @param query   Query object.
     * @param token   Token of the user logged in.
     * @return The list of distinct values.
     * @throws CatalogException CatalogException.
     */
    public OpenCGAResult<?> distinct(String studyId, String field, Query query, String token) throws CatalogException {
        return distinct(studyId, Collections.singletonList(field), query, token);
    }

    /**
     * Fetch a list containing all the distinct values of the key {@code field}.
     *
     * @param studyId study id in string format. Could be one of
     *                [id|organization@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy]
     * @param fields  Fields for which to return distinct values.
     * @param query   Query object.
     * @param token   Token of the user logged in.
     * @return The list of distinct values.
     * @throws CatalogException CatalogException.
     */
    public abstract OpenCGAResult<?> distinct(String studyId, List<String> fields, Query query, String token) throws CatalogException;

    /**
     * Count matching entries in catalog.
     *
     * @param studyId study id in string format. Could be one of
     *                [id|organization@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy]
     * @param query   Query object.
     * @param token   Session id of the user logged in.
     * @return A OpenCGAResult with the total number of entries matching the query.
     * @throws CatalogException catalogException.
     */
    public abstract OpenCGAResult<R> count(String studyId, Query query, String token) throws CatalogException;

    public OpenCGAResult<R> delete(String studyStr, String id, QueryOptions options, String token) throws CatalogException {
        return delete(studyStr, Collections.singletonList(id), options, token);
    }

    public abstract OpenCGAResult<R> delete(String studyStr, List<String> ids, QueryOptions options, String token)
            throws CatalogException;

    /**
     * Delete all entries matching the query.
     *
     * @param studyStr Study id in string format. Could be one of
     *                 [id|organization@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy]
     * @param query    Query object.
     * @param options  Map containing additional parameters to be considered for the deletion.
     * @param token    Session id of the user logged in.
     * @return A OpenCGAResult object containing the number of matching elements, deleted and elements that could not be deleted.
     * @throws CatalogException if the study or the user do not exist.
     */
    public abstract OpenCGAResult delete(String studyStr, Query query, QueryOptions options, String token)
            throws CatalogException;

    /**
     * Ranks the elements queried, groups them by the field(s) given and return it sorted.
     *
     * @param studyStr   study id in string format. Could be one of
     *                   [id|organization@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy]
     * @param query      Query object containing the query that will be executed.
     * @param field      A field or a comma separated list of fields by which the results will be grouped in.
     * @param numResults Maximum number of results to be reported.
     * @param asc        Order in which the results will be reported.
     * @param token      Session id of the user logged in.
     * @return A OpenCGAResult object containing each of the fields in field and the count of them matching the query.
     * @throws CatalogException CatalogException
     */
    public abstract OpenCGAResult rank(String studyStr, Query query, String field, int numResults, boolean asc,
                                       String token) throws CatalogException;

    /**
     * Groups the matching entries by some fields.
     *
     * @param organizationId Organization id.
     * @param studyStr       study id in string format. Could be one of
     *                       [id|organization@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy]
     * @param query          Query object.
     * @param fields         A field or a comma separated list of fields by which the results will be grouped in.
     * @param options        QueryOptions object.
     * @param token          Session id of the user logged in.
     * @return A OpenCGAResult object containing the results of the query grouped by the fields.
     * @throws CatalogException CatalogException
     */
    public OpenCGAResult groupBy(String organizationId, @Nullable String studyStr, Query query, String fields, QueryOptions options,
                                 String token) throws CatalogException {
        if (StringUtils.isEmpty(fields)) {
            throw new CatalogException("Empty fields parameter.");
        }
        return groupBy(studyStr, query, Arrays.asList(fields.split(",")), options, token);
    }

    /**
     * Groups the matching entries by some fields.
     *
     * @param studyStr study id in string format. Could be one of
     *                 [id|organization@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy]
     * @param query    Query object.
     * @param fields   A field or a comma separated list of fields by which the results will be grouped in.
     * @param options  QueryOptions object.
     * @param token    Session id of the user logged in.
     * @return A OpenCGAResult object containing the results of the query grouped by the fields.
     * @throws CatalogException CatalogException
     */
    public abstract OpenCGAResult groupBy(@Nullable String studyStr, Query query, List<String> fields,
                                          QueryOptions options, String token) throws CatalogException;

    public OpenCGAResult<AclEntryList<S>> getAcls(String studyId, List<String> idList, String member, boolean ignoreException, String token)
            throws CatalogException {
        return getAcls(studyId, idList, StringUtils.isNotEmpty(member) ? Collections.singletonList(member) : Collections.emptyList(),
                ignoreException, token);
    }

    public abstract OpenCGAResult<AclEntryList<S>> getAcls(String studyId, List<String> idList, List<String> members,
                                                           boolean ignoreException, String token) throws CatalogException;

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
     * Interface to execute an operation over a single element.
     * @param <R> Type of the element that will be processed.
     */
    public interface ExecuteOperationForSingleEntry<R> {
        OpenCGAResult<R> execute(String organizationId, Study study, String userId, EntryParam entryParam) throws CatalogException;
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
        ObjectMap params = new ObjectMap()
                .append("study", studyStr)
                .append("ids", idList)
                .append("updateParams", updateParams)
                .append("ignoreException", ignoreException)
                .append("options", options)
                .append("token", token);
        OpenCGAResult<R> result = runList(params, Enums.Action.UPDATE, studyStr, idList, token, studyIncludeList, execution);
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
        OpenCGAResult<R> result = runIterator(params, Enums.Action.UPDATE, studyStr, token, studyIncludeList, operationIterator, execution,
                errorMessage);
        return endResult(result, ignoreException);
    }

    protected OpenCGAResult<R> deleteMany(String studyStr, List<String> idList, ObjectMap deleteParams, boolean ignoreException,
                                          String token, ExecuteOperationForSingleEntry<R> execution) throws CatalogException {
        ObjectMap params = new ObjectMap()
                .append("study", studyStr)
                .append("ids", idList)
                .append("params", deleteParams)
                .append("ignoreException", ignoreException)
                .append("token", token);
        OpenCGAResult<R> result = runList(params, Enums.Action.DELETE, studyStr, idList, token, QueryOptions.empty(), execution);
        return endResult(result, ignoreException);
    }

    protected OpenCGAResult<R> deleteMany(String studyStr, Query query, ObjectMap deleteParams, boolean ignoreException, String token,
                                          ExecuteOperationIterator<R> iteratorSupplier, ExecuteBatchOperation<R> execution)
            throws CatalogException {
        ObjectMap params = new ObjectMap()
                .append("study", studyStr)
                .append("query", query)
                .append("params", deleteParams)
                .append("ignoreException", ignoreException)
                .append("token", token);
        OpenCGAResult<R> result = runIterator(params, Enums.Action.DELETE, studyStr, token, QueryOptions.empty(), iteratorSupplier,
                execution, null);
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

    protected OpenCGAResult<AclEntryList<S>> getAcls(String studyStr, List<String> idList, List<String> members, boolean ignoreException,
                                                     String token, ExecuteMultiOperation<AclEntryList<S>> execution)
            throws CatalogException {
        ObjectMap params = new ObjectMap()
                .append("study", studyStr)
                .append("idList", idList)
                .append("members", members)
                .append("ignoreException", ignoreException)
                .append("token", token);
        return runForMultiOperation(params, Enums.Action.FETCH_ACLS, studyStr, token, QueryOptions.empty(), execution, null);
    }

    protected OpenCGAResult<AclEntryList<S>> updateAcls(String studyStr, List<String> idList, String memberList, Object aclParams,
                                                        ParamUtils.AclAction action, String token,
                                                        ExecuteMultiOperation<AclEntryList<S>> execution)
            throws CatalogException {
        return updateAcls(new ObjectMap(), studyStr, idList, memberList, aclParams, action, token, execution);
    }

    protected OpenCGAResult<AclEntryList<S>> updateAcls(ObjectMap params, String studyStr, List<String> idList, String memberList,
                                                        Object aclParams, ParamUtils.AclAction action, String token,
                                                        ExecuteMultiOperation<AclEntryList<S>> execution)
            throws CatalogException {
        params = params != null ? params : new ObjectMap();
        params.putIfAbsent("studyStr", studyStr);
        params.putIfAbsent("idList", idList);
        params.putIfAbsent("memberList", memberList);
        params.putIfAbsent("aclParams", aclParams);
        params.putIfAbsent("action", action);
        params.putIfAbsent("token", token);
        return runForMultiOperation(params, Enums.Action.UPDATE_ACLS, studyStr, token, StudyManager.INCLUDE_CONFIGURATION, execution,
                "Could not update ACLs");
    }

    protected OpenCGAResult<R> runForSingleEntry(ObjectMap params, Enums.Action action, String studyStr, String token,
                                                 ExecuteOperationForSingleEntry<R> execution) throws CatalogException {
        return runForSingleEntry(params, action, studyStr, token, QueryOptions.empty(), execution, null);
    }

    protected OpenCGAResult<R> runForSingleEntry(ObjectMap params, Enums.Action action, String studyStr, String token,
                                                 ExecuteOperationForSingleEntry<R> execution, String errorMessage)
            throws CatalogException {
        return runForSingleEntry(params, action, studyStr, token, QueryOptions.empty(), execution, errorMessage);
    }

    protected OpenCGAResult<R> runForSingleEntry(ObjectMap params, Enums.Action action, String studyStr, String token,
                                                 QueryOptions studyIncludeList, ExecuteOperationForSingleEntry<R> execution)
            throws CatalogException {
        return runForSingleEntry(params, action, studyStr, token, studyIncludeList, execution, null);
    }

    protected OpenCGAResult<R> runForSingleEntry(ObjectMap params, Enums.Action action, String studyStr, String token,
                                                 QueryOptions studyIncludeList, ExecuteOperationForSingleEntry<R> execution,
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
            EntryParam entryParam = new EntryParam();
            OpenCGAResult<R> execute = execution.execute(organizationId, study, userId, entryParam);

            // Fill missing OpencgaEvent entries
            opencgaEvent.setResult(execute);
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
        OpenCGAResult<R> execute(String organizationId, Study study, String userId, List<EntryParam> entryParamList)
                throws CatalogException;
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
            } else {
                throw e;
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
        return runList(params, action, studyStr, idList, token, QueryOptions.empty(), execution);
    }

    protected OpenCGAResult<R> runList(ObjectMap params, Enums.Action action, String studyStr, List<String> idList, String token,
                                       QueryOptions studyIncludeList, ExecuteOperationForSingleEntry<R> execution)
            throws CatalogException {
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

        // Execute code
        OpenCGAResult<R> result = OpenCGAResult.empty();
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

//    protected OpenCGAResult<R> run(ObjectMap params, Enums.Resource resource, Enums.Action action, String studyStr,
//                                       List<String> ids, String token, ExecuteBatchOperation<R> body) throws CatalogException {
//        return run(params, resource, action, studyStr, ids, token, QueryOptions.empty(), body);
//    }

    //    protected OpenCGAResult<R> run(ObjectMap params, Enums.Resource resource, Enums.Action action, String studyStr,
//                                       List<String> ids, String token, QueryOptions studyIncludeList, ExecuteBatchOperation<R> body)
//            throws CatalogException {
//        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
//        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
//        String organizationId = studyFqn.getOrganizationId();
//        String userId = tokenPayload.getUserId(organizationId);
//
//        Supplier<Study> studySupplier = () -> {
//            try {
//                return catalogManager.getStudyManager().resolveId(studyFqn, studyIncludeList, tokenPayload);
//            } catch (CatalogException e) {
//                throw new CatalogRuntimeException(e);
//            }
//        };
//
//        for (String id : ids) {
//            return notify(resource, action, organizationId, studySupplier, userId, params, tokenPayload, body);
//        }
//    }
//
//    protected <T, S extends ObjectMap> T run(ObjectMap params, Enums.Action action, Enums.Resource resource, String operationUuid,
//                                             Study study, String userId, S options, ExecuteOperation<R> body) throws CatalogException {
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
//                                             Study study, String userId, S options, ExecuteOperation<R> body) throws CatalogException {
//        StopWatch totalStopWatch = StopWatch.createStarted();
//        Exception exception = null;
//        ReferenceParam referenceParam = new ReferenceParam();
//
//        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
//        Supplier<R> supplier = () -> {
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
//    protected <R> T runBatch(ObjectMap params, Enums.Action action, Enums.Resource resource, String studyStr, String token,
//                             QueryOptions options, ExecuteBatchOperation<R> body) throws CatalogException {
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
