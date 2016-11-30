/*
 * Copyright 2015-2016 OpenCB
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

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Project;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Deprecated
public interface IProjectManager extends ResourceManager<Long, Project> {

    String getUserId(long projectId) throws CatalogException;

    /**
     * Obtains the numeric project id given a string.
     *
     * @param userId User id of the user asking for the project id.
     * @param projectStr Project id in string format. Could be one of [id | user@project | project].
     * @return the numeric project id.
     * @throws CatalogDBException CatalogDBException.
     */
    long getId(String userId, String projectStr) throws CatalogDBException;

    /**
     * Obtains the list of projectIds corresponding to the comma separated list of project strings given in projectStr.
     *
     * @param userId User demanding the action.
     * @param projectStr Comma separated list of project ids.
     * @return A list of project ids.
     * @throws CatalogException CatalogException.
     */
    default List<Long> getIds(String userId, String projectStr) throws CatalogException {
        List<Long> projectIds = new ArrayList<>();
        for (String projectId : projectStr.split(",")) {
            projectIds.add(getId(userId, projectId));
        }
        return projectIds;
    }

    @Deprecated
    long getId(String projectId) throws CatalogException;

    QueryResult<Project> create(String name, String alias, String description, String organization, QueryOptions options,
                                String sessionId) throws CatalogException;

    /**
     * Ranks the elements queried, groups them by the field(s) given and return it sorted.
     *
     * @param userId  User id.
     * @param query      Query object containing the query that will be executed.
     * @param field      A field or a comma separated list of fields by which the results will be grouped in.
     * @param numResults Maximum number of results to be reported.
     * @param asc        Order in which the results will be reported.
     * @param sessionId  sessionId.
     * @return           A QueryResult object containing each of the fields in field and the count of them matching the query.
     * @throws CatalogException CatalogException
     */
    QueryResult rank(String userId, Query query, String field, int numResults, boolean asc, String sessionId) throws CatalogException;

    default QueryResult rank(Query query, String field, int numResults, boolean asc, String sessionId) throws CatalogException {
        String userId = query.getString(ProjectDBAdaptor.QueryParams.USER_ID.key());
        if (!userId.equals("")) {
            throw new CatalogException("Project[rank]: User id not found in the query");
        }
        return rank(userId, query, field, numResults, asc, sessionId);
    }

    /**
     * Groups the elements queried by the field(s) given.
     *
     * @param userId  User id.
     * @param query   Query object containing the query that will be executed.
     * @param field   Field by which the results will be grouped in.
     * @param options QueryOptions object.
     * @param sessionId  sessionId.
     * @return        A QueryResult object containing the results of the query grouped by the field.
     * @throws CatalogException CatalogException
     */
    QueryResult groupBy(String userId, Query query, String field, QueryOptions options, String sessionId) throws CatalogException;

    default QueryResult groupBy(Query query, String field, QueryOptions options, String sessionId) throws CatalogException {
        String userId = query.getString(ProjectDBAdaptor.QueryParams.USER_ID.key());
        if (!userId.equals("")) {
            throw new CatalogException("Project[groupBy]: User id not found in the query");
        }
        return groupBy(userId, query, field, options, sessionId);
    }

    /**
     * Groups the elements queried by the field(s) given.
     *
     * @param userId  User id.
     * @param query   Query object containing the query that will be executed.
     * @param fields  List of fields by which the results will be grouped in.
     * @param options QueryOptions object.
     * @param sessionId  sessionId.
     * @return        A QueryResult object containing the results of the query grouped by the fields.
     * @throws CatalogException CatalogException
     */
    QueryResult groupBy(String userId, Query query, List<String> fields, QueryOptions options, String sessionId) throws CatalogException;

    default QueryResult groupBy(Query query, List<String> field, QueryOptions options, String sessionId) throws CatalogException {
        String userId = query.getString(ProjectDBAdaptor.QueryParams.USER_ID.key());
        if (!userId.equals("")) {
            throw new CatalogException("Project[groupBy]: User id not found in the query");
        }
        return groupBy(userId, query, field, options, sessionId);
    }

    /**
     * Obtain the list of projects and studies that are shared with the user.
     *
     * @param userId user whose projects and studies are being shared with.
     * @param queryOptions QueryOptions object.
     * @param sessionId Session id which should correspond to userId.
     * @return A QueryResult object containing the list of projects and studies that are shared with the user.
     * @throws CatalogException CatalogException
     */
    QueryResult<Project> getSharedProjects(String userId, QueryOptions queryOptions, String sessionId) throws CatalogException;
}
