package org.opencb.opencga.catalog.api;

import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.CatalogException;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Pattern;

/**
* @author Jacobo Coll <jacobo167@gmail.com>
*/
public interface ResourceManager<I, R>{

    /**
     * Creates a R object entry in Catalog
     *
     * @param params    Object with all the attributes of the object
     * @param sessionId
     * @return          The created object
     * @throws CatalogException
     */
    public QueryResult<R> create(QueryOptions params, String sessionId) throws CatalogException;

    /**
     * Reads an object from Catalog given an ID
     *
     * @param id        Id of the object to read
     * @param options   Read options
     * @return          The specified object
     * @throws CatalogException
     */
    public QueryResult<R> read(I id, QueryOptions options, String sessionId)
            throws CatalogException;

    /**
     * Read all the R objects matching with the query on the QueryOptions.
     *
     * @param query     Query to catalog.
     * @param options   Query options, like "include", "exclude", "limit" and "skip"
     * @return          All matching elements.
     * @throws CatalogException
     */
    public QueryResult<R> readAll(QueryOptions query, QueryOptions options, String sessionId)
            throws CatalogException;

    /**
     * Update fields of an existing catalog entry.
     *
     * @param id          Id of the object to update
     * @param parameters  Parameters to change.
     * @param options
     * @return            The modified entry.
     * @throws CatalogException
     */
    public QueryResult<R> update(I id, ObjectMap parameters, QueryOptions options, String sessionId)
            throws CatalogException;

    /**
     * Delete an specified entry from Catalog.
     *
     * @param id        Id of the object to delete
     * @param options   Deleting options.
     * @return          The deleted object
     * @throws CatalogException
     */
    public QueryResult<R> delete(I id, QueryOptions options, String sessionId)
            throws CatalogException;

}
