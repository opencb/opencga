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

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogException;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface ResourceManager<I, R> {

    /**
     * Reads an object from Catalog given an ID.
     *
     * @param id        Id of the object to read
     * @param options   Read options
     * @param sessionId sessionId
     * @return The specified object
     * @throws CatalogException CatalogException
     */
    QueryResult<R> get(I id, QueryOptions options, String sessionId) throws CatalogException;

    /**
     * Read all the R objects matching with the query on the QueryOptions.
     *
     * @param query     Query to catalog.
     * @param options   Query options, like "include", "exclude", "limit" and "skip"
     * @param sessionId sessionId
     * @return All matching elements.
     * @throws CatalogException CatalogException
     */
    QueryResult<R> get(Query query, QueryOptions options, String sessionId) throws CatalogException;

    /**
     * Update an existing catalog entry.
     *
     * @param id         Id of the object to update
     * @param parameters Parameters to change.
     * @param options    options
     * @param sessionId  sessionId
     * @return The modified entry.
     * @throws CatalogException CatalogException
     */
    QueryResult<R> update(I id, ObjectMap parameters, QueryOptions options, String sessionId) throws CatalogException;

    /**
     * Delete the entries satisfying the query.
     *
     * @param query     Query of the objects to be deleted.
     * @param options   Deleting options.
     * @param sessionId sessionId.
     * @return A list with the deleted objects.
     * @throws CatalogException CatalogException
     * @throws IOException IOException.
     */
    List<QueryResult<R>> delete(Query query, QueryOptions options, String sessionId) throws CatalogException, IOException;

    /**
     * Restore deleted entries from Catalog.
     *
     * @param ids       Comma separated list of ids of the objects to restore.
     * @param options   Restore options.
     * @param sessionId sessionId.
     * @return A list with the restored objects.
     * @throws CatalogException CatalogException
     */
    List<QueryResult<R>> restore(String ids, QueryOptions options, String sessionId) throws CatalogException;

    /**
     * Restore the entries satisfying the query.
     *
     * @param query     Query of the objects to be restored.
     * @param options   Restore options.
     * @param sessionId sessionId.
     * @return A list with the restored objects.
     * @throws CatalogException CatalogException
     */
    List<QueryResult<R>> restore(Query query, QueryOptions options, String sessionId) throws CatalogException;


    /**
     * Ranks the elements queried, groups them by the field(s) given and return it sorted.
     *
     *
     * @param query      Query object containing the query that will be executed.
     * @param field      A field or a comma separated list of fields by which the results will be grouped in.
     * @param numResults Maximum number of results to be reported.
     * @param asc        Order in which the results will be reported.
     * @param sessionId  sessionId.
     * @return           A QueryResult object containing each of the fields in field and the count of them matching the query.
     * @throws CatalogException CatalogException
     */
    QueryResult rank(Query query, String field, int numResults, boolean asc, String sessionId) throws CatalogException;

    /**
     * Groups the elements queried by the field(s) given.
     *
     *
     * @param query   Query object containing the query that will be executed.
     * @param field   Field by which the results will be grouped in.
     * @param options QueryOptions object.
     * @param sessionId  sessionId.
     * @return        A QueryResult object containing the results of the query grouped by the field.
     * @throws CatalogException CatalogException
     */
    QueryResult groupBy(Query query, String field, QueryOptions options, String sessionId) throws CatalogException;

    /**
     * Groups the elements queried by the field(s) given.
     *
     * @param query   Query object containing the query that will be executed.
     * @param fields  List of fields by which the results will be grouped in.
     * @param options QueryOptions object.
     * @param sessionId  sessionId.
     * @return        A QueryResult object containing the results of the query grouped by the fields.
     * @throws CatalogException CatalogException
     */
    QueryResult groupBy(Query query, List<String> fields, QueryOptions options, String sessionId) throws CatalogException;

    /**
     * Change the status of an entry.
     *
     * @param id id of the entry.
     * @param status new status that will be set.
     * @param message message of the status.
     * @param sessionId session id of the user ordering the action.
     * @throws CatalogException if any error occurs (user with no permissions, invalid status, id not found).
     */
    void setStatus(String id, @Nullable String status, @Nullable String message, String sessionId) throws CatalogException;

}
