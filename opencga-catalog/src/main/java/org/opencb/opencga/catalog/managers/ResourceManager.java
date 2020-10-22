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
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.InternalGetDataResult;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.IPrivateStudyUid;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.response.OpenCGAResult;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by pfurio on 07/08/17.
 */
public abstract class ResourceManager<R extends IPrivateStudyUid> extends AbstractManager {

    ResourceManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                    DBAdaptorFactory catalogDBAdaptorFactory, Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, configuration);
    }

    abstract Enums.Resource getEntity();

    OpenCGAResult<R> internalGet(long studyUid, String entry, QueryOptions options, String user) throws CatalogException {
        return internalGet(studyUid, entry, null, options, user);
    }

    abstract OpenCGAResult<R> internalGet(long studyUid, String entry, @Nullable Query query, QueryOptions options, String user)
            throws CatalogException;

    InternalGetDataResult<R> internalGet(long studyUid, List<String> entryList, QueryOptions options, String user, boolean ignoreException)
            throws CatalogException {
        return internalGet(studyUid, entryList, null, options, user, ignoreException);
    }

    abstract InternalGetDataResult<R> internalGet(long studyUid, List<String> entryList, @Nullable Query query, QueryOptions options,
                                                   String user, boolean ignoreException) throws CatalogException;

    /**
     * Create an entry in catalog.
     *
     * @param studyStr  Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param entry     entry that needs to be added in Catalog.
     * @param options   QueryOptions object.
     * @param token Session id of the user logged in.
     * @return A OpenCGAResult of the object created.
     * @throws CatalogException if any parameter from the entry is incorrect, the user does not have permissions...
     */
    public abstract OpenCGAResult<R> create(String studyStr, R entry, QueryOptions options, String token) throws CatalogException;

    /**
     * Fetch the R object.
     *
     * @param studyStr  Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param entryStr  Entry id to be fetched.
     * @param options   QueryOptions object, like "include", "exclude", "limit" and "skip".
     * @param token token
     * @return All matching elements.
     * @throws CatalogException CatalogException.
     */
    public OpenCGAResult<R> get(String studyStr, String entryStr, QueryOptions options, String token) throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);
        return internalGet(study.getUid(), entryStr, options, userId);
    }

    /**
     * Fetch all the R objects matching the query.
     *
     * @param studyStr  Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param entryList Comma separated list of entries to be fetched.
     * @param options   QueryOptions object, like "include", "exclude", "limit" and "skip".
     * @param token token
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
        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyId, userId);

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
            InternalGetDataResult<R> responseResult = internalGet(study.getUid(), entryList, query, options, userId, ignoreException);

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
                    auditManager.auditInfo(operationUuid, userId, getEntity(), entry.getId(), entry.getUuid(),
                            study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
                }
            }

            return result;
        } catch (CatalogException e) {
            for (String entryId : entryList) {
                auditManager.auditInfo(operationUuid, userId, getEntity(), entryId, "", study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
            throw e;
        } finally {
            auditManager.finishAuditBatch(operationUuid);
        }
    }

    /**
     * Obtain an entry iterator to iterate over the matching entries.
     *
     * @param studyStr  study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param query     Query object.
     * @param options   QueryOptions object.
     * @param token Session id of the user logged in.
     * @return An iterator.
     * @throws CatalogException if there is any internal error.
     */
    public abstract DBIterator<R> iterator(String studyStr, Query query, QueryOptions options, String token) throws CatalogException;

    /**
     * Search of entries in catalog.
     *
     * @param studyId  study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param query     Query object.
     * @param options   QueryOptions object.
     * @param token Session id of the user logged in.
     * @return The list of entries matching the query.
     * @throws CatalogException catalogException.
     */
    public abstract OpenCGAResult<R> search(String studyId, Query query, QueryOptions options, String token) throws CatalogException;

    /**
     * Fetch a list containing all the distinct values of the key {@code field}.
     *
     * @param studyId study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param field The field for which to return distinct values.
     * @param query Query object.
     * @param token Token of the user logged in.
     * @return The list of distinct values.
     * @throws CatalogException CatalogException.
     */
    public abstract OpenCGAResult<?> distinct(String studyId, String field, Query query, String token) throws CatalogException;

    /**
     * Count matching entries in catalog.
     *
     * @param studyId  study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param query     Query object.
     * @param token Session id of the user logged in.
     * @return A OpenCGAResult with the total number of entries matching the query.
     * @throws CatalogException catalogException.
     */
    public abstract OpenCGAResult<R> count(String studyId, Query query, String token) throws CatalogException;

    public abstract OpenCGAResult delete(String studyStr, List<String> ids, ObjectMap params, String token) throws CatalogException;

    /**
     * Delete all entries matching the query.
     *
     * @param studyStr Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param query Query object.
     * @param params Map containing additional parameters to be considered for the deletion.
     * @param token Session id of the user logged in.
     * @throws CatalogException if the study or the user do not exist.
     * @return A OpenCGAResult object containing the number of matching elements, deleted and elements that could not be deleted.
     */
    public abstract OpenCGAResult delete(String studyStr, Query query, ObjectMap params, String token) throws CatalogException;

    /**
     * Ranks the elements queried, groups them by the field(s) given and return it sorted.
     *
     * @param studyStr   study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param query      Query object containing the query that will be executed.
     * @param field      A field or a comma separated list of fields by which the results will be grouped in.
     * @param numResults Maximum number of results to be reported.
     * @param asc        Order in which the results will be reported.
     * @param token  Session id of the user logged in.
     * @return A OpenCGAResult object containing each of the fields in field and the count of them matching the query.
     * @throws CatalogException CatalogException
     */
    public abstract OpenCGAResult rank(String studyStr, Query query, String field, int numResults, boolean asc, String token)
            throws CatalogException;

    /**
     * Groups the matching entries by some fields.
     *
     * @param studyStr  study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param query     Query object.
     * @param fields    A field or a comma separated list of fields by which the results will be grouped in.
     * @param options   QueryOptions object.
     * @param token Session id of the user logged in.
     * @return A OpenCGAResult object containing the results of the query grouped by the fields.
     * @throws CatalogException CatalogException
     */
    public OpenCGAResult groupBy(@Nullable String studyStr, Query query, String fields, QueryOptions options, String token)
            throws CatalogException {
        if (StringUtils.isEmpty(fields)) {
            throw new CatalogException("Empty fields parameter.");
        }
        return groupBy(studyStr, query, Arrays.asList(fields.split(",")), options, token);
    }

    /**
     * Groups the matching entries by some fields.
     *
     * @param studyStr  study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param query     Query object.
     * @param options   QueryOptions object.
     * @param fields    A field or a comma separated list of fields by which the results will be grouped in.
     * @param token Session id of the user logged in.
     * @return A OpenCGAResult object containing the results of the query grouped by the fields.
     * @throws CatalogException CatalogException
     */
    public abstract OpenCGAResult groupBy(@Nullable String studyStr, Query query, List<String> fields, QueryOptions options, String token)
            throws CatalogException;

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
            if (ListUtils.isNotEmpty(result.getEvents())) {
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

}
