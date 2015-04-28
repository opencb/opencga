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
    public QueryResult<R> read(I id, QueryOptions options, String sessionId) throws CatalogException;

    /**
     * Read all the R objects matching with the query on the QueryOptions.
     *
     * @param options   Query to catalog.
     * @return          All matching elements.
     * @throws CatalogException
     */
    public QueryResult<R> readAll(QueryOptions options, String sessionId) throws CatalogException;

    /**
     * Update fields of an existing catalog entry.
     *
     * @param id        Id of the object to update
     * @param params    Parameters to change.
     * @return          The modified entry.
     * @throws CatalogException
     */
    public QueryResult<R> update(I id, ObjectMap params, String sessionId) throws CatalogException;

    /**
     * Delete an specified entry from Catalog.
     *
     * @param id        Id of the object to delete
     * @param options   Deleting options.
     * @return          The deleted object
     * @throws CatalogException
     */
    public QueryResult<R> delete(I id, QueryOptions options, String sessionId) throws CatalogException;




    default void checkId(int id, String name) throws CatalogException {
        if (id < 0) {
            throw new CatalogException("Error in id: '" + name + "' is not valid: "
                    + id + ".");
        }
    }

    default void checkParameter(String param, String name) throws CatalogException {
        if (param == null || param.equals("") || param.equals("null")) {
            throw new CatalogException("Error in parameter: parameter '" + name + "' is null or empty: "
                    + param + ".");
        }
    }

    default void checkParameters(String... args) throws CatalogException {
        if (args.length % 2 == 0) {
            for (int i = 0; i < args.length; i += 2) {
                checkParameter(args[i], args[i + 1]);
            }
        } else {
            throw new CatalogException("Error in parameter: parameter list is not multiple of 2");
        }
    }

    default void checkObj(Object obj, String name) throws CatalogException {
        if (obj == null) {
            throw new CatalogException("parameter '" + name + "' is null.");
        }
    }

    default void checkRegion(String regionStr, String name) throws CatalogException {
        if (Pattern.matches("^([a-zA-Z0-9])+:([0-9])+-([0-9])+$", regionStr)) {//chr:start-end
            throw new CatalogException("region '" + name + "' is not valid");
        }
    }

    default void checkPath(String path, String name) throws CatalogException {
        if (path == null) {
            throw new CatalogException("parameter '" + name + "' is null.");
        }
        checkPath(Paths.get(path), name);
    }

    default void checkPath(Path path, String name) throws CatalogException {
        checkObj(path, name);
        if (path.isAbsolute()) {
            throw new CatalogException("Error in path: Path '" + name + "' can't be absolute");
        } else if (path.toString().matches("\\.|\\.\\.")) {
            throw new CatalogException("Error in path: Path '" + name + "' can't have relative names '.' or '..'");
        }
    }

    default void checkAlias(String alias, String name) throws CatalogException {
        if (alias == null || alias.isEmpty() || !alias.matches("^[_A-Za-z0-9-\\+]+$")) {
            throw new CatalogException("Error in alias: Invalid alias for '" + name + "'.");
        }
    }

    default String defaultString(String string, String defaultValue) {
        if (string == null || string.isEmpty()) {
            string = defaultValue;
        }
        return string;
    }

    default <O> O defaultObject(O object, O defaultObject) {
        if (object == null) {
            object = defaultObject;
        }
        return object;
    }


}
