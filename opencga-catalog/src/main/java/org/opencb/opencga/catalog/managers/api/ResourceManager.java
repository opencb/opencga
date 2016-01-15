package org.opencb.opencga.catalog.managers.api;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogException;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface ResourceManager<I, R> {

    /**
     * Creates a R object entry in Catalog.
     *
     * @param params    Object with all the attributes of the object
     * @param sessionId sessionId
     * @return The created object
     * @throws CatalogException CatalogException
     */
    QueryResult<R> create(QueryOptions params, String sessionId) throws CatalogException;

    /**
     * Reads an object from Catalog given an ID.
     *
     * @param id      Id of the object to read
     * @param options Read options
     * @param sessionId sessionId
     * @return The specified object
     * @throws CatalogException CatalogException
     */
    QueryResult<R> read(I id, QueryOptions options, String sessionId)
            throws CatalogException;

    /**
     * Read all the R objects matching with the query on the QueryOptions.
     *
     * @param query   Query to catalog.
     * @param options Query options, like "include", "exclude", "limit" and "skip"
     * @param sessionId sessionId
     * @return All matching elements.
     * @throws CatalogException CatalogException
     */
    QueryResult<R> readAll(QueryOptions query, QueryOptions options, String sessionId)
            throws CatalogException;

    /**
     * Update fields of an existing catalog entry.
     *
     * @param id         Id of the object to update
     * @param parameters Parameters to change.
     * @param options options
     * @param sessionId sessionId
     * @return The modified entry.
     * @throws CatalogException CatalogException
     */
    QueryResult<R> update(I id, ObjectMap parameters, QueryOptions options, String sessionId)
            throws CatalogException;

    /**
     * Delete an specified entry from Catalog.
     *
     * @param id      Id of the object to delete
     * @param options Deleting options.
     * @param sessionId sessionId
     * @return The deleted object
     * @throws CatalogException CatalogException
     */
    QueryResult<R> delete(I id, QueryOptions options, String sessionId)
            throws CatalogException;

}
