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

package org.opencb.opencga.catalog.managers.api;

/**
 * Created by pfurio on 06/07/16.
 */

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.CohortDBAdaptor;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.AbstractManager;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.models.acls.AclParams;
import org.opencb.opencga.catalog.models.acls.permissions.CohortAclEntry;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Deprecated
public interface ICohortManager extends ResourceManager<Long, Cohort>, IAnnotationSetManager {

    QueryResult<Cohort> create(long studyId, String name, Study.Type type, String description, List<Sample> samples, List<AnnotationSet>
            annotationSetList, Map<String, Object> attributes, String sessionId) throws CatalogException;

    Long getStudyId(long cohortId) throws CatalogException;

    /**
     * Obtains the numeric cohort id given a string.
     *
     * @param userId User id of the user asking for the cohort id.
     * @param cohortStr Cohort id in string format. Could be one of [id | user@aliasProject:aliasStudy:cohortName
     *                | user@aliasStudy:cohortName | aliasStudy:cohortName | cohortName].
     * @return the numeric cohort id.
     * @throws CatalogException when more than one cohort id is found or .
     */
    Long getId(String userId, String cohortStr) throws CatalogException;

    QueryResult<Cohort> get(long studyId, Query query, QueryOptions options, String sessionId) throws CatalogException;

    DBIterator<Cohort> iterator(long studyId, Query query, QueryOptions options, String sessionId) throws CatalogException;

    /**     * Obtains the list of cohort ids corresponding to the comma separated list of cohort strings given in cohortStr.
     *
     * @param userId User demanding the action.
     * @param cohortStr Comma separated list of cohort ids.
     * @return A list of cohort ids.
     * @throws CatalogException CatalogException.
     */
    default List<Long> getIds(String userId, String cohortStr) throws CatalogException {
        List<Long> cohortIds = new ArrayList<>();
        for (String cohortId : cohortStr.split(",")) {
            cohortIds.add(getId(userId, cohortId));
        }
        return cohortIds;
    }

    /**
     * Obtains the resource java bean containing the requested ids.
     *
     * @param cohortStr Cohort id in string format. Could be either the id or name.
     * @param studyStr Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param sessionId Session id of the user logged.
     * @return the resource java bean containing the requested ids.
     * @throws CatalogException when more than one cohort id is found.
     */
    AbstractManager.MyResourceId getId(String cohortStr, @Nullable String studyStr, String sessionId) throws CatalogException;

    /**
     * Obtains the resource java bean containing the requested ids.
     *
     * @param cohortStr Cohort id in string format. Could be either the id or alias.
     * @param studyStr Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param sessionId Session id of the user logged.
     * @return the resource java bean containing the requested ids.
     * @throws CatalogException CatalogException.
     */
    AbstractManager.MyResourceIds getIds(String cohortStr, @Nullable String studyStr, String sessionId) throws CatalogException;

    /**
     * Multi-study search of cohorts in catalog.
     *
     * @param studyStr Study string that can point to several studies of the same project.
     * @param query    Query object.
     * @param options  QueryOptions object.
     * @param sessionId Session id.
     * @return The list of cohorts matching the query.
     * @throws CatalogException catalogException.
     */
    QueryResult<Cohort> search(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<Cohort> count(String studyStr, Query query, String sessionId) throws CatalogException;

    /**
     * Delete entries from Catalog.
     *
     * @param ids       Comma separated list of ids corresponding to the objects to delete
     * @param studyStr  Study string.
     * @param options   Deleting options.
     * @param sessionId sessionId
     * @return A list with the deleted objects.
     * @throws CatalogException CatalogException
     * @throws IOException IOException.
     */
    List<QueryResult<Cohort>> delete(String ids, @Nullable String studyStr, QueryOptions options, String sessionId)
            throws CatalogException, IOException;

    default QueryResult groupBy(@Nullable String studyStr, Query query, QueryOptions options, String fields, String sessionId)
            throws CatalogException {
        if (StringUtils.isEmpty(fields)) {
            throw new CatalogException("Empty fields parameter.");
        }
        return groupBy(studyStr, query, Arrays.asList(fields.split(",")), options, sessionId);
    }

    QueryResult groupBy(@Nullable String studyStr, Query query, List<String> fields, QueryOptions options, String sessionId)
            throws CatalogException;

    @Deprecated
    @Override
    default QueryResult groupBy(Query query, String field, QueryOptions options, String sessionId) throws CatalogException {
        throw new NotImplementedException("Group by has to be called passing the study string");
    }

    @Deprecated
    @Override
    default QueryResult groupBy(Query query, List<String> fields, QueryOptions options, String sessionId) throws CatalogException {
        throw new NotImplementedException("Group by has to be called passing the study string");
    }

    /**
     * Ranks the elements queried, groups them by the field(s) given and return it sorted.
     *
     * @param studyId    Study id.
     * @param query      Query object containing the query that will be executed.
     * @param field      A field or a comma separated list of fields by which the results will be grouped in.
     * @param numResults Maximum number of results to be reported.
     * @param asc        Order in which the results will be reported.
     * @param sessionId  sessionId.
     * @return           A QueryResult object containing each of the fields in field and the count of them matching the query.
     * @throws CatalogException CatalogException
     */
    QueryResult rank(long studyId, Query query, String field, int numResults, boolean asc, String sessionId) throws CatalogException;

    default QueryResult rank(Query query, String field, int numResults, boolean asc, String sessionId) throws CatalogException {
        long studyId = query.getLong(CohortDBAdaptor.QueryParams.STUDY_ID.key());
        if (studyId == 0L) {
            throw new CatalogException("Cohort[rank]: Study id not found in the query");
        }
        return rank(studyId, query, field, numResults, asc, sessionId);
    }

    List<QueryResult<CohortAclEntry>> updateAcl(String cohort, String studyStr, String memberId, AclParams aclParams, String sessionId)
            throws CatalogException;

}
