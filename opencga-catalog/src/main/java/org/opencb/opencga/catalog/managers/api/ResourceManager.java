package org.opencb.opencga.catalog.managers.api;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogException;

import java.io.IOException;
import java.util.List;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface ResourceManager<I, R> {

    /**
     * Creates a R object entry in Catalog.
     *
     * @param objectMap Object with all the attributes of the object
     * @param options Object the includes/excludes to obtain the object after the creation
     * @param sessionId sessionId
     * @return The created object
     * @throws CatalogException CatalogException
     */
    @Deprecated
    QueryResult<R> create(ObjectMap objectMap, QueryOptions options, String sessionId) throws CatalogException;

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
     * Delete entries from Catalog.
     *
     * @param ids       Comma separated list of ids corresponding to the objects to delete
     * @param options   Deleting options.
     * @param sessionId sessionId
     * @return A list with the deleted objects
     * @throws CatalogException CatalogException
     * @throws IOException IOException.
     */
    List<QueryResult<R>> delete(String ids, QueryOptions options, String sessionId) throws CatalogException, IOException;

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

}
